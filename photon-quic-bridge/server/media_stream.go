package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/quic-go/quic-go"
)

// Кадрирование медийного стрима (ADR-049 §1-§3, media-transfer.md §1-§3).
//
// Медийные действия отличаются от обычных ровно одним: за двумя JSON-кадрами
// на том же стриме идёт двоичный хвост. Отдельного соединения и отдельного
// режима handshake не заводится — стрим приходит в тот же цикл, и перепроверка
// сессии (F6) достаётся ему даром.

// mediaStreamDeadline — скользящий предел на блок передачи.
//
// Сбрасывается перед КАЖДЫМ блоком: здоровая передача любого размера его не
// задевает, зависшая упирается за фиксированное время. Без него брошенный
// клиентом стрим держал бы горутину, открытый файл и слот из ста до закрытия
// соединения, а KeepAlivePeriod делает этот срок неограниченным (ADR-049 §5).
// Переменная, а не константа, ровно по одной причине: тест зависшей передачи
// иначе ждал бы полминуты реального времени. В продакшне значение не меняется.
var mediaStreamDeadline = 30 * time.Second

// isMediaTransfer — действия, чей стрим несёт двоичный хвост.
//
// 702 сюда НЕ входит: это обычное JSON-действие, и оно идёт общим путём через
// Dispatch. Разделение по коду, а не по содержимому кадра: направление тела
// задаётся действием, а не переговорами.
func isMediaTransfer(action int) bool {
	return action == ActionMediaUpload || action == ActionMediaDownload ||
		action == ActionConvMediaUpload || action == ActionConvMediaDownload
}

// bodyAfterFrames — тело, начинающееся СРАЗУ после последнего JSON-кадра.
//
// Обе половины правила обязательны, и каждая проверена саботажем (ADR-049 §2):
//
//   - dec.Buffered() — декодер читает ВПРОК, и часть тела к этому моменту уже
//     лежит в его буфере, а на стриме её больше нет. При теле меньше буфера
//     без этого теряется всё тело целиком, а SHA-256 совпадает с хешем пустой
//     строки;
//   - пропуск ведущих пробелов — json.Encoder дописывает "\n" после каждого
//     значения, а json.Decoder этот разделитель не потребляет: без отбрасывания
//     он приклеивается к началу тела.
func bodyAfterFrames(dec *json.Decoder, stream io.Reader) io.Reader {
	return io.MultiReader(skipLeadingWhitespace(dec.Buffered()), stream)
}

func skipLeadingWhitespace(r io.Reader) io.Reader {
	br := bufio.NewReader(r)
	for {
		b, err := br.ReadByte()
		if err != nil {
			return br
		}
		if b == ' ' || b == '\t' || b == '\r' || b == '\n' {
			continue
		}
		_ = br.UnreadByte()
		return br
	}
}

// deadlineReader / deadlineWriter — скользящий дедлайн вокруг потока.
type deadlineReader struct {
	stream *quic.Stream
	body   io.Reader
	d      time.Duration
}

func (r deadlineReader) Read(p []byte) (int, error) {
	if err := r.stream.SetReadDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.body.Read(p)
}

type deadlineWriter struct {
	stream *quic.Stream
	d      time.Duration
}

func (w deadlineWriter) Write(p []byte) (int, error) {
	if err := w.stream.SetWriteDeadline(time.Now().Add(w.d)); err != nil {
		return 0, err
	}
	return w.stream.Write(p)
}

// MediaUploadPayload — второй JSON-кадр загрузки.
type MediaUploadPayload struct {
	ChannelID  string `json:"channel_id"`
	Name       string `json:"name"`
	Mime       string `json:"mime,omitempty"`
	SHA256     string `json:"sha256"`
	BodyLength int64  `json:"body_length"`
}

// mediaResponse — конверт ответа с длиной тела.
//
// body_length — поле КОНВЕРТА, а не data: длина есть свойство кадра, она
// говорит транспорту, сколько байт читать. Внутри data она заставила бы
// клиентский транспорт разбирать доменный payload.
type mediaResponse struct {
	OK         bool        `json:"ok"`
	Data       interface{} `json:"data,omitempty"`
	Error      string      `json:"error,omitempty"`
	BodyLength int64       `json:"body_length,omitempty"`
}

// handleMediaStream — медийная ветка.
//
// Авторизация выполняется ТЕМ ЖЕ Dispatcher.Authorize, что и на обычном пути.
// Своей проверки здесь нет и быть не должно: две точки вызова допустимы, две
// точки решения — нет (ADR-039 §6).
func (ql *QuicListener) handleMediaStream(stream *quic.Stream, dec *json.Decoder,
	userID string, header HeaderFrame, payload json.RawMessage) {

	if deny := ql.dispatcher.Authorize(userID, header, payload); deny != nil {
		writeResponse(stream, *deny)
		return
	}

	switch header.Action {
	case ActionMediaUpload:
		ql.mediaUpload(stream, dec, userID, payload)
	case ActionMediaDownload:
		ql.mediaDownload(stream, userID, payload)
	case ActionConvMediaUpload:
		ql.convMediaUpload(stream, dec, userID, payload)
	case ActionConvMediaDownload:
		ql.convMediaDownload(stream, userID, payload)
	}
}

func (ql *QuicListener) mediaUpload(stream *quic.Stream, dec *json.Decoder,
	userID string, payload json.RawMessage) {

	var p MediaUploadPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		writeResponse(stream, Response{OK: false, Error: "invalid: " + err.Error()})
		return
	}

	// Тело читается ПОТОКОВО: MediaService принимает io.Reader и копирует
	// блоками. Ни одной []byte на весь файл здесь нет и быть не должно.
	body := deadlineReader{
		stream: stream,
		body:   bodyAfterFrames(dec, stream),
		d:      mediaStreamDeadline,
	}

	obj, err := ql.dispatcher.mediaSvc().Upload(UploadRequest{
		ChannelID:  p.ChannelID,
		UploaderID: userID,
		Name:       p.Name,
		Mime:       p.Mime,
		SHA256:     p.SHA256,
		BodyLength: p.BodyLength,
	}, body)
	if err != nil {
		ql.plugin.API.LogWarn("[Photon][Media] Upload rejected",
			"user_id", userID, "error", err.Error())
		writeResponse(stream, Response{OK: false, Error: mediaErrorText(err)})
		return
	}

	ql.plugin.API.LogInfo("[Photon][Media] Uploaded",
		"user_id", userID, "media_id", obj.MediaID, "size", obj.Size)
	writeResponse(stream, Response{OK: true, Data: map[string]interface{}{
		"media_id": obj.MediaID, "sha256": obj.SHA256, "size": obj.Size,
	}})
}

func (ql *QuicListener) mediaDownload(stream *quic.Stream, userID string, payload json.RawMessage) {
	var p MediaIDPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		writeResponse(stream, Response{OK: false, Error: "invalid: " + err.Error()})
		return
	}

	svc := ql.dispatcher.mediaSvc()
	obj, err := svc.Info(p.MediaID)
	if err != nil {
		writeResponse(stream, Response{OK: false, Error: mediaErrorText(err)})
		return
	}

	// Кадр ответа идёт ПЕРЕД телом: иначе клиент не отличил бы отказ от начала
	// данных, не прочитав их (media-transfer.md §1).
	if err := json.NewEncoder(stream).Encode(mediaResponse{
		OK: true,
		Data: map[string]interface{}{
			"media_id": obj.MediaID, "sha256": obj.SHA256, "size": obj.Size,
			"mime": obj.Mime, "name": obj.Name,
		},
		BodyLength: obj.Size,
	}); err != nil {
		return
	}

	if _, err := svc.Download(p.MediaID, deadlineWriter{stream: stream, d: mediaStreamDeadline}); err != nil {
		// Кадр ответа уже отправлен: сообщить об ошибке нечем, кроме обрыва
		// тела. Клиент обнаружит его по недобору объявленной длины — ровно для
		// этого длина и обязательна (media-transfer.md §3).
		ql.plugin.API.LogError("[Photon][Media] Download failed after header",
			"user_id", userID, "media_id", p.MediaID, "error", err.Error())
		return
	}
	ql.plugin.API.LogInfo("[Photon][Media] Downloaded",
		"user_id", userID, "media_id", p.MediaID, "size", obj.Size)
}

// mediaErrorText — словарь строк из media-transfer.md §5.
//
// forbidden объединяет «нет прав» и «нет объекта» намеренно (ADR-039):
// различать их значило бы позволить пробовать существование чужих media_id.
func mediaErrorText(err error) string {
	switch {
	case errors.Is(err, ErrMediaUnavailable):
		return "unavailable"
	case errors.Is(err, ErrMediaNotFound), errors.Is(err, ErrMediaInvalidID):
		return errForbidden.Error()
	case errors.Is(err, ErrMediaIntegrity):
		return "integrity"
	case errors.Is(err, ErrMediaInvalidRequest), errors.Is(err, ErrMediaSizeMismatch):
		return "invalid"
	default:
		return fmt.Sprintf("invalid: %v", err)
	}
}
