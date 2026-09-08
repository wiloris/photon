// Package media — клиентская сторона передачи вложений ENGRAM.
//
// Что делает: складывает согласованный payload, отдаёт тело потоком через
// session.DoStream и проверяет целостность принятого.
//
// Чего НЕ делает и не должно:
//
//   - не знает о ports.Stream и не открывает стримов: кадрирование живёт в
//     Codec, сырой стрим наружу не отдаётся (ADR-049 §4);
//   - не хранит байты и не кеширует: локальный кеш — отдельный слой
//     (ADR-048 §14);
//   - не разбирает домен глубже полей вложения;
//   - не добавляет транспортного API.
//
// Контракт провода — docs/media-transfer.md.
package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	photonpb "github.com/wiloris/photon-core/protobuf"
	"github.com/wiloris/photon-core/services/session"
)

// Streamer — то, что этому слою нужно от сессии, и ничего сверх.
//
// Узкий интерфейс, а не *session.Session, по двум причинам: клиент
// проверяется без транспорта, и видно, что media не пользуется ничем, кроме
// одного вызова. Новым транспортным API это не является — метод уже
// существует (ADR-049 §4).
type Streamer interface {
	DoStream(ctx context.Context, action int32, payload []byte,
		body io.Reader, sink io.Writer) (session.Response, error)
}

var (
	// ErrIntegrity — принятые байты не сошлись с объявленным sha256.
	//
	// Отличима от транспортной ошибки намеренно: связь сработала, ответ
	// пришёл, не сошлось СОДЕРЖИМОЕ. Повторять такой вызов имеет смысл,
	// повторять обрыв — тоже, но решения разные, и сводить их к одному
	// значит заставлять вызывающего гадать (та же логика, что у
	// session.ErrConnectionLost против ErrNotReady).
	ErrIntegrity = errors.New("media: integrity check failed")

	// ErrSizeMismatch — принято не столько байт, сколько объявлено. Ловится
	// раньше хеша: дешёвый и ранний детектор обрыва (media-transfer.md §3).
	ErrSizeMismatch = errors.New("media: size mismatch")

	// ErrInvalidRequest — запрос не соответствует контракту.
	ErrInvalidRequest = errors.New("media: invalid request")

	// ErrMalformedResponse — сервер ответил ok, но ответ не разбирается или
	// не содержит обязательных полей. Отдельно от ErrIntegrity: там не сошлось
	// содержимое, здесь не сошёлся сам ответ.
	ErrMalformedResponse = errors.New("media: malformed response")

	// ErrRejected — сервер отказал. Текст причины из конверта сохраняется:
	// forbidden, unavailable и invalid различимы по нему
	// (media-transfer.md §5).
	ErrRejected = errors.New("media: rejected by server")
)

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Client — клиент медийных операций.
type Client struct{ s Streamer }

func New(s Streamer) *Client { return &Client{s: s} }

// UploadRequest — то, что клиент объявляет ДО передачи тела.
type UploadRequest struct {
	ChannelID string
	Name      string
	Mime      string
	// SHA256 объявляется заранее: это делает проверку целостности
	// двусторонней, а не только клиентской (media-transfer.md §9).
	SHA256 string
	// Size — длина тела. Она же уходит на провод как body_length и она же
	// ограничивает чтение из body.
	Size int64
}

func (r UploadRequest) validate() error {
	switch {
	case r.ChannelID == "":
		return fmt.Errorf("%w: channel_id is required", ErrInvalidRequest)
	case r.Name == "":
		return fmt.Errorf("%w: name is required", ErrInvalidRequest)
	case !sha256Hex.MatchString(r.SHA256):
		return fmt.Errorf("%w: sha256 must be 64 lowercase hex chars", ErrInvalidRequest)
	case r.Size <= 0:
		return fmt.Errorf("%w: size must be positive", ErrInvalidRequest)
	}
	return nil
}

// Ref — то, что вернул сервер о загруженном объекте.
type Ref struct {
	MediaID string `json:"media_id"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
}

type uploadPayload struct {
	ChannelID  string `json:"channel_id"`
	Name       string `json:"name"`
	Mime       string `json:"mime,omitempty"`
	SHA256     string `json:"sha256"`
	BodyLength int64  `json:"body_length"`
}

// Upload отдаёт тело потоком и возвращает ссылку на созданный объект.
//
// Тело целиком в память не собирается: принимается io.Reader и ограничивается
// io.LimitReader по объявленной длине. Ограничение делает вызывающий слой, а
// не транспорт: длина принадлежит домену, и сигнатура DoStream её не несёт
// (ADR-049 §4).
func (c *Client) Upload(ctx context.Context, req UploadRequest, body io.Reader) (*Ref, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("%w: body is required", ErrInvalidRequest)
	}

	payload, err := json.Marshal(uploadPayload{
		ChannelID: req.ChannelID, Name: req.Name, Mime: req.Mime,
		SHA256: req.SHA256, BodyLength: req.Size,
	})
	if err != nil {
		return nil, fmt.Errorf("media: marshal payload: %w", err)
	}

	resp, err := c.s.DoStream(ctx, int32(photonpb.Action_ACTION_MEDIA_UPLOAD), payload,
		io.LimitReader(body, req.Size), nil)
	if err != nil {
		// Транспортная ошибка проходит НАСКВОЗЬ, без переупаковки: вызывающий
		// обязан сохранить возможность различить обрыв через errors.Is.
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("%w: %s", ErrRejected, resp.Error)
	}

	var ref Ref
	if err := json.Unmarshal(resp.Data, &ref); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if ref.MediaID == "" {
		return nil, fmt.Errorf("%w: media_id is empty", ErrMalformedResponse)
	}
	// Сервер обязан подтвердить ТО ЖЕ содержимое, что было объявлено.
	// Расхождение здесь означает, что записано не то, что отправлено.
	if ref.Size != req.Size {
		return nil, fmt.Errorf("%w: сервер принял %d при объявленных %d", ErrSizeMismatch, ref.Size, req.Size)
	}
	if ref.SHA256 != req.SHA256 {
		return nil, fmt.Errorf("%w: сервер сообщил %s при объявленном %s", ErrIntegrity, ref.SHA256, req.SHA256)
	}
	return &ref, nil
}

// Info — метаданные скачанного объекта, как их сообщил сервер.
type Info struct {
	MediaID string `json:"media_id"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
	Mime    string `json:"mime,omitempty"`
	Name    string `json:"name,omitempty"`
}

// Download пишет тело в sink и проверяет его целостность.
//
// Хеш считается ПОТОКОВО, по мере записи в sink: собирать тело ради проверки
// значило бы нарушить §10 контракта ровно там, где он важнее всего.
//
// ВАЖНОЕ СЛЕДСТВИЕ, которое обязан знать вызывающий: при ошибке целостности
// байты УЖЕ записаны в sink — отменить запись клиент не может. Файл считается
// полученным только при nil-ошибке (media-transfer.md §7); при любой другой
// вызывающий обязан выбросить написанное.
func (c *Client) Download(ctx context.Context, mediaID string, sink io.Writer) (*Info, error) {
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
	resp, err := c.s.DoStream(ctx, int32(photonpb.Action_ACTION_MEDIA_DOWNLOAD), payload,
		nil, io.MultiWriter(sink, h, counter))
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("%w: %s", ErrRejected, resp.Error)
	}

	var info Info
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if info.MediaID != mediaID {
		// Ответ не про тот объект, который спрашивали.
		return nil, fmt.Errorf("%w: запрошен %s, отвечено про %s", ErrMalformedResponse, mediaID, info.MediaID)
	}
	if !sha256Hex.MatchString(info.SHA256) {
		return nil, fmt.Errorf("%w: sha256 отсутствует или неверного формата", ErrMalformedResponse)
	}

	// Размер сверяется ПЕРВЫМ: он дешевле и локализует обрыв точнее хеша.
	if info.Size != counter.n {
		return &info, fmt.Errorf("%w: принято %d при объявленных %d", ErrSizeMismatch, counter.n, info.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != info.SHA256 {
		return &info, fmt.Errorf("%w: принят %s при объявленном %s", ErrIntegrity, got, info.SHA256)
	}
	return &info, nil
}

type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }
