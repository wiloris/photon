package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	photonpb "github.com/wiloris/photon-core/protobuf"
)

// Вложения разговоров ENGRAM — действия 703/704 (ADR-124 M1; контракт —
// photon-quic-bridge/docs/conversation-implementation-contract.md §8.3–§8.4).
// Кадр тот же, что у 700/701; отличие — область прав (разговор) и ключ
// идемпотентности загрузки. Методы 700–702 выше не меняются.

// Rejection — вердикт сервера ok:false. Отдельный тип, а не только
// ErrRejected: вызывающему (локальные действия 815/816) нужен сам код, чтобы
// вернуть его в конверте при PHOTON_OK (ADR-038), а не превратить в отказ
// вызова.
type Rejection struct{ Code string }

func (r *Rejection) Error() string { return fmt.Sprintf("%v: %s", ErrRejected, r.Code) }

// Unwrap оставляет errors.Is(err, ErrRejected) истинным для вердикта.
func (r *Rejection) Unwrap() error { return ErrRejected }

// ConvUploadRequest — 703. SHA256 и Size вычисляет вызывающий по самому
// файлу ДО передачи: проверка целостности двусторонняя, как у 700.
type ConvUploadRequest struct {
	ConversationID string
	UploadKey      string
	Kind           string
	Name           string
	SHA256         string
	Size           int64
}

func (r ConvUploadRequest) validate() error {
	switch {
	case r.ConversationID == "":
		return fmt.Errorf("%w: conversation_id is required", ErrInvalidRequest)
	case r.UploadKey == "":
		return fmt.Errorf("%w: upload_key is required", ErrInvalidRequest)
	case r.Kind == "":
		return fmt.Errorf("%w: kind is required", ErrInvalidRequest)
	case r.Name == "":
		return fmt.Errorf("%w: name is required", ErrInvalidRequest)
	case !sha256Hex.MatchString(r.SHA256):
		return fmt.Errorf("%w: sha256 must be 64 lowercase hex chars", ErrInvalidRequest)
	case r.Size <= 0:
		return fmt.Errorf("%w: size must be positive", ErrInvalidRequest)
	}
	return nil
}

type convUploadPayload struct {
	ConversationID string `json:"conversation_id"`
	UploadKey      string `json:"upload_key"`
	Kind           string `json:"kind"`
	Name           string `json:"name"`
	SHA256         string `json:"sha256"`
	BodyLength     int64  `json:"body_length"`
}

// ConvMedia — данные кадра ответа 704 и вложения.
type ConvMedia struct {
	MediaID string `json:"media_id"`
	Kind    string `json:"kind"`
	Mime    string `json:"mime"`
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// ConvUpload — 703: ровно req.Size байт из body. Успех — только ответ сервера,
// совпадающий с объявленными длиной и хэшем; вердикт ok:false — *Rejection.
func (c *Client) ConvUpload(ctx context.Context, req ConvUploadRequest, body io.Reader) (*Ref, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("%w: body is required", ErrInvalidRequest)
	}
	payload, err := json.Marshal(convUploadPayload{
		ConversationID: req.ConversationID, UploadKey: req.UploadKey, Kind: req.Kind,
		Name: req.Name, SHA256: req.SHA256, BodyLength: req.Size,
	})
	if err != nil {
		return nil, fmt.Errorf("media: marshal payload: %w", err)
	}
	resp, err := c.s.DoStream(ctx, int32(photonpb.Action_ACTION_CONV_MEDIA_UPLOAD), payload,
		io.LimitReader(body, req.Size), nil)
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, &Rejection{Code: resp.Error}
	}
	var ref Ref
	if err := json.Unmarshal(resp.Data, &ref); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if ref.MediaID == "" {
		return nil, fmt.Errorf("%w: media_id is empty", ErrMalformedResponse)
	}
	if ref.Size != req.Size {
		return nil, fmt.Errorf("%w: сервер принял %d при объявленных %d", ErrSizeMismatch, ref.Size, req.Size)
	}
	if ref.SHA256 != req.SHA256 {
		return nil, fmt.Errorf("%w: сервер сообщил %s при объявленном %s", ErrIntegrity, ref.SHA256, req.SHA256)
	}
	return &ref, nil
}

// ConvDownload — 704: тело пишется в sink. Успех — только полное тело длины
// из кадра ответа с совпавшим SHA-256; иначе ошибка, даже если часть байтов
// уже записана в sink (вызывающий её отбрасывает).
func (c *Client) ConvDownload(ctx context.Context, mediaID string, sink io.Writer) (*ConvMedia, error) {
	if mediaID == "" {
		return nil, fmt.Errorf("%w: media_id is required", ErrInvalidRequest)
	}
	if sink == nil {
		return nil, fmt.Errorf("%w: sink is required", ErrInvalidRequest)
	}
	payload, err := json.Marshal(struct {
		MediaID string `json:"media_id"`
	}{mediaID})
	if err != nil {
		return nil, fmt.Errorf("media: marshal payload: %w", err)
	}
	h := sha256.New()
	counter := &countingWriter{}
	resp, err := c.s.DoStream(ctx, int32(photonpb.Action_ACTION_CONV_MEDIA_DOWNLOAD), payload,
		nil, io.MultiWriter(sink, h, counter))
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, &Rejection{Code: resp.Error}
	}
	var info ConvMedia
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if info.MediaID != mediaID {
		return nil, fmt.Errorf("%w: запрошен %s, отвечено про %s", ErrMalformedResponse, mediaID, info.MediaID)
	}
	if !sha256Hex.MatchString(info.SHA256) {
		return nil, fmt.Errorf("%w: sha256 отсутствует или неверного формата", ErrMalformedResponse)
	}
	if info.Size != counter.n {
		return &info, fmt.Errorf("%w: принято %d при объявленных %d", ErrSizeMismatch, counter.n, info.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != info.SHA256 {
		return &info, fmt.Errorf("%w: принят %s при объявленном %s", ErrIntegrity, got, info.SHA256)
	}
	return &info, nil
}
