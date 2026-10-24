package media

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	photonpb "github.com/wiloris/photon/photon-core/protobuf"
)

// Галерея профиля ENGRAM — действия 706/707 (ADR-127, ПРЕДЛОЖЕНО; контракт —
// photon-quic-bridge/docs/conversation-implementation-contract.md §13). Кадр
// и проверки целостности те же, что у 703/704; отличие — область прав
// (владелец профиля) и роль объекта (оригинал или миниатюра).

// ProfileUploadRequest — 706. SHA256 и Size вычисляет вызывающий по самому
// файлу ДО передачи.
type ProfileUploadRequest struct {
	UploadKey string
	Role      string // "original" | "thumb"
	Mime      string
	SHA256    string
	Size      int64
}

func (r ProfileUploadRequest) validate() error {
	switch {
	case r.UploadKey == "":
		return fmt.Errorf("%w: upload_key is required", ErrInvalidRequest)
	case r.Role == "":
		return fmt.Errorf("%w: role is required", ErrInvalidRequest)
	case r.Mime == "":
		return fmt.Errorf("%w: mime is required", ErrInvalidRequest)
	case !sha256Hex.MatchString(r.SHA256):
		return fmt.Errorf("%w: sha256 must be 64 lowercase hex chars", ErrInvalidRequest)
	case r.Size <= 0:
		return fmt.Errorf("%w: size must be positive", ErrInvalidRequest)
	}
	return nil
}

type profileUploadPayload struct {
	UploadKey  string `json:"upload_key"`
	Role       string `json:"role"`
	Mime       string `json:"mime"`
	SHA256     string `json:"sha256"`
	BodyLength int64  `json:"body_length"`
}

// ProfileMedia — данные кадра ответа 707.
type ProfileMedia struct {
	MediaID string `json:"media_id"`
	Role    string `json:"role"`
	Mime    string `json:"mime"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// ProfileUpload — 706: ровно req.Size байт из body; вердикт ok:false — *Rejection.
func (c *Client) ProfileUpload(ctx context.Context, req ProfileUploadRequest, body io.Reader) (*Ref, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(profileUploadPayload{
		UploadKey: req.UploadKey, Role: req.Role, Mime: req.Mime, SHA256: req.SHA256, BodyLength: req.Size,
	})
	if err != nil {
		return nil, fmt.Errorf("media: marshal payload: %w", err)
	}
	return c.uploadStream(ctx, int32(photonpb.Action_ACTION_PROFILE_MEDIA_UPLOAD), payload, req.SHA256, req.Size, body)
}

// ProfileDownload — 707: тело пишется в sink. Успех — только полное тело
// объявленной длины с совпавшим SHA-256.
func (c *Client) ProfileDownload(ctx context.Context, mediaID string, sink io.Writer) (*ProfileMedia, error) {
	var info ProfileMedia
	err := c.downloadStream(ctx, int32(photonpb.Action_ACTION_PROFILE_MEDIA_DOWNLOAD), mediaID, sink,
		func(data []byte) (string, string, int64, error) {
			if err := json.Unmarshal(data, &info); err != nil {
				return "", "", 0, err
			}
			return info.MediaID, info.SHA256, info.Size, nil
		})
	if err != nil {
		return nil, err
	}
	return &info, nil
}
