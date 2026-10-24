package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	photonpb "github.com/wiloris/photon/photon-core/protobuf"
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
	payload, err := json.Marshal(convUploadPayload{
		ConversationID: req.ConversationID, UploadKey: req.UploadKey, Kind: req.Kind,
		Name: req.Name, SHA256: req.SHA256, BodyLength: req.Size,
	})
	if err != nil {
		return nil, fmt.Errorf("media: marshal payload: %w", err)
	}
	return c.uploadStream(ctx, int32(photonpb.Action_ACTION_CONV_MEDIA_UPLOAD), payload, req.SHA256, req.Size, body)
}

// uploadStream — общий путь потоковой загрузки 703/706: ровно size байт из
// body; успех — только ответ, совпадающий с объявленными длиной и хэшем.
func (c *Client) uploadStream(ctx context.Context, action int32, payload []byte, sum string, size int64, body io.Reader) (*Ref, error) {
	if body == nil {
		return nil, fmt.Errorf("%w: body is required", ErrInvalidRequest)
	}
	resp, err := c.s.DoStream(ctx, action, payload, io.LimitReader(body, size), nil)
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
	if ref.Size != size {
		return nil, fmt.Errorf("%w: сервер принял %d при объявленных %d", ErrSizeMismatch, ref.Size, size)
	}
	if ref.SHA256 != sum {
		return nil, fmt.Errorf("%w: сервер сообщил %s при объявленном %s", ErrIntegrity, ref.SHA256, sum)
	}
	return &ref, nil
}

// ConvDownload — 704: тело пишется в sink. Успех — только полное тело длины
// из кадра ответа с совпавшим SHA-256; иначе ошибка, даже если часть байтов
// уже записана в sink (вызывающий её отбрасывает).
func (c *Client) ConvDownload(ctx context.Context, mediaID string, sink io.Writer) (*ConvMedia, error) {
	var info ConvMedia
	err := c.downloadStream(ctx, int32(photonpb.Action_ACTION_CONV_MEDIA_DOWNLOAD), mediaID, sink,
		func(data []byte) (string, string, int64, error) {
			if err := json.Unmarshal(data, &info); err != nil {
				return "", "", 0, err
			}
			return info.MediaID, info.SHA256, info.Size, nil
		})
	if err != nil && info.MediaID == "" {
		return nil, err
	}
	return &info, err
}

// downloadStream — общий путь потоковой выдачи 704/707: тело в sink; parse
// разбирает данные кадра ответа и отдаёт media_id, sha256 и size. Успех —
// только полное тело объявленной длины с совпавшим SHA-256.
func (c *Client) downloadStream(ctx context.Context, action int32, mediaID string, sink io.Writer,
	parse func(data []byte) (id, sum string, size int64, err error)) error {
	if mediaID == "" {
		return fmt.Errorf("%w: media_id is required", ErrInvalidRequest)
	}
	if sink == nil {
		return fmt.Errorf("%w: sink is required", ErrInvalidRequest)
	}
	payload, err := json.Marshal(struct {
		MediaID string `json:"media_id"`
	}{mediaID})
	if err != nil {
		return fmt.Errorf("media: marshal payload: %w", err)
	}
	h := sha256.New()
	counter := &countingWriter{}
	resp, err := c.s.DoStream(ctx, action, payload, nil, io.MultiWriter(sink, h, counter))
	if err != nil {
		return err
	}
	if !resp.OK {
		return &Rejection{Code: resp.Error}
	}
	id, sum, size, err := parse(resp.Data)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if id != mediaID {
		return fmt.Errorf("%w: запрошен %s, отвечено про %s", ErrMalformedResponse, mediaID, id)
	}
	if !sha256Hex.MatchString(sum) {
		return fmt.Errorf("%w: sha256 отсутствует или неверного формата", ErrMalformedResponse)
	}
	if size != counter.n {
		return fmt.Errorf("%w: принято %d при объявленных %d", ErrSizeMismatch, counter.n, size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sum {
		return fmt.Errorf("%w: принят %s при объявленном %s", ErrIntegrity, got, sum)
	}
	return nil
}
