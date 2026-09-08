package session

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// JSONCodec — реализация Codec по умолчанию и единственная сейчас.
//
// Формат не выбран этим пакетом, а описан по факту: сервер уже развёрнут и
// работает, источник истины — photon-quic-bridge/server/protocol.go и
// test-client/test_photon.go, клиент проверенный против живого сервера
// (ADR-034, bridge-session.md §2).
//
// Каждый фрейм — одно JSON-значение через json.Encoder (то есть с
// завершающим "\n"); чтение — потоковым json.Decoder. Явной длины фрейма на
// проводе нет: границу задаёт сам JSON.
type JSONCodec struct{}

var _ Codec = JSONCodec{}

// ── Handshake ───────────────────────────────────────────────────────────────

type authFrame struct {
	SessionToken string `json:"session_token"`
}

type modeFrame struct {
	Mode string `json:"mode"`

	// ProtocolVersion — новое поле (bridge-session.md §5). Развёрнутый
	// сервер о нём не знает и молча проигнорирует: encoding/json пропускает
	// неизвестные поля. Именно поэтому добавление обратно совместимо и не
	// требует согласованного обновления сервера.
	ProtocolVersion uint32 `json:"protocol_version,omitempty"`
}

type ackFrame struct {
	OK              bool   `json:"ok"`
	Error           string `json:"error,omitempty"`
	ProtocolVersion uint32 `json:"protocol_version,omitempty"`

	// Data — на проводе сервер кладёт подтверждение режима СЮДА, а не
	// полем верхнего уровня: {"ok":true,"data":{"status":"ready"}}
	// (photon-quic-bridge/server/quic_listener.go:279, :287). Схема
	// (photon.proto, message HandshakeResponse) объявляет status полем
	// верхнего уровня — это ещё одно расхождение «схема ≠ провод»
	// (ADR-034), проверенное по коду сервера, а не предположенное.
	//
	// Тип RawMessage, а не структура: data не типизирована (§2.2), и
	// строгий тип превратил бы неожиданную форму (массив, null, число) в
	// ошибку разбора всего handshake'а.
	Data json.RawMessage `json:"data,omitempty"`
}

func (JSONCodec) WriteHandshake(w io.Writer, h Handshake) error {
	enc := json.NewEncoder(w)
	if err := enc.Encode(authFrame{SessionToken: h.SessionToken}); err != nil {
		return fmt.Errorf("session: write auth frame: %w", err)
	}
	if err := enc.Encode(modeFrame{Mode: string(h.Mode), ProtocolVersion: h.ProtocolVersion}); err != nil {
		return fmt.Errorf("session: write mode frame: %w", err)
	}
	return nil
}

func (JSONCodec) ReadHandshakeAck(r io.Reader) (HandshakeAck, error) {
	var f ackFrame
	if err := json.NewDecoder(r).Decode(&f); err != nil {
		// Обрыв и мусор различаются вызывающим по errors.Is(err, io.EOF)
		// внутри цепочки, но для классификации достаточно ErrMalformedFrame:
		// в обоих случаях handshake не состоялся.
		return HandshakeAck{}, fmt.Errorf("%w: handshake ack: %v", ErrMalformedFrame, err)
	}
	return HandshakeAck{
		OK:              f.OK,
		Error:           f.Error,
		ProtocolVersion: f.ProtocolVersion,
		Status:          statusFromAckData(f.Data),
	}, nil
}

// statusFromAckData достаёт подтверждение режима из data.
//
// Ошибка разбора намеренно проглатывается и даёт пустой Status, а не отказ:
// data не типизирована, и её неожиданная форма не должна ронять handshake,
// который в остальном состоялся. Пустая строка трактуется вызывающим как
// "сервер не сообщил" — то же, что делает развёрнутый сервер при ответе без
// data.
func statusFromAckData(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}
	var d struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return ""
	}
	return d.Status
}

// ── Action ──────────────────────────────────────────────────────────────────

type headerFrame struct {
	Action int32 `json:"action"`

	// Resource на проводе — строка ("PostResource"), а не enum Resource из
	// схемы. Одно из расхождений «схема ≠ провод» (ADR-034); выводится из
	// action-кода, см. resource.go.
	Resource string `json:"resource"`
}

type responseFrame struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`

	// BodyLength — длина двоичного хвоста за этим кадром. Поле конверта:
	// см. комментарий у Response.BodyLength. Отсутствует у всех действий,
	// кроме медийного скачивания, и omitempty оставляет провод прежним.
	BodyLength int64 `json:"body_length,omitempty"`
}

func (JSONCodec) WriteRequest(w io.Writer, req Request) error {
	enc := json.NewEncoder(w)
	if err := enc.Encode(headerFrame{Action: req.Action, Resource: req.Resource}); err != nil {
		return fmt.Errorf("session: write header frame: %w", err)
	}

	payload := req.Payload
	if len(payload) == 0 {
		// Сервер ждёт ровно два фрейма; пустого фрейма в протоколе нет.
		// Тест-клиент шлёт {} — здесь то же самое.
		payload = []byte("{}")
	}
	// Валидность проверяется ДО записи: невалидный JSON, дописанный в стрим,
	// ломает не текущий запрос, а разбор всего последующего потока на
	// стороне сервера — ошибка проявилась бы далеко от причины.
	if !json.Valid(payload) {
		return ErrInvalidPayload
	}

	// Фрейм уходит ОДНИМ Write — значение вместе с завершающим "\n", ровно
	// как это делает json.Encoder.Encode для остальных фреймов.
	//
	// Это не микрооптимизация. Payload и "\n" двумя вызовами Write дают
	// дедлок на небуферизованном соединении: получатель дочитывает значение,
	// перестаёт читать и начинает писать ответ, а отправитель всё ещё висит
	// на втором Write — обе стороны пишут, никто не читает. Найдено тестом
	// T-7 (зависание до таймаута), не ревью.
	frame := make([]byte, 0, len(payload)+1)
	frame = append(frame, payload...)
	frame = append(frame, '\n')
	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("session: write payload frame: %w", err)
	}
	return nil
}

func (JSONCodec) ReadResponse(r io.Reader) (Response, error) {
	var f responseFrame
	if err := json.NewDecoder(r).Decode(&f); err != nil {
		return Response{}, fmt.Errorf("%w: response: %v", ErrMalformedFrame, err)
	}
	// f.Data передаётся дальше как есть — без разбора и пересборки.
	// json.RawMessage это []byte, но копируется явно: срез смотрит в буфер
	// декодера, переиспользуемый между вызовами.
	var data []byte
	if len(f.Data) > 0 {
		data = append([]byte(nil), f.Data...)
	}
	return Response{OK: f.OK, Error: f.Error, Data: data, BodyLength: f.BodyLength}, nil
}

// ── Потоковая передача ──────────────────────────────────────────────────────

// skipLeadingWhitespace отбрасывает пробельные байты, которыми JSON разделяет
// значения.
//
// Нужен потому, что json.Encoder дописывает "\n" после каждого значения, а
// json.Decoder этот разделитель не потребляет: он остаётся в Buffered() и без
// отбрасывания попал бы В НАЧАЛО тела. Измерено — тело выходило на один байт
// длиннее отправленного (ADR-049 §2).
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

func (c JSONCodec) WriteRequestStream(w io.Writer, req Request, body io.Reader) error {
	if err := c.WriteRequest(w, req); err != nil {
		return err
	}
	if body == nil {
		return nil
	}
	// Копируется ВСЁ, что отдаст body, и ничего сверх того: сколько байт
	// отправить, решает вызывающий, объявивший длину в payload'е. Расхождение
	// объявленного с отправленным обнаруживает приёмник — он читает ровно
	// объявленное (media-transfer.md §3).
	if _, err := io.Copy(w, body); err != nil {
		return fmt.Errorf("session: write body: %w", err)
	}
	return nil
}

func (JSONCodec) ReadResponseStream(r io.Reader, sink io.Writer) (Response, error) {
	dec := json.NewDecoder(r)
	var f responseFrame
	if err := dec.Decode(&f); err != nil {
		return Response{}, fmt.Errorf("%w: response: %v", ErrMalformedFrame, err)
	}
	var data []byte
	if len(f.Data) > 0 {
		data = append([]byte(nil), f.Data...)
	}
	resp := Response{OK: f.OK, Error: f.Error, Data: data, BodyLength: f.BodyLength}

	// Тела нет: либо его не ждут, либо сервер отказал. Отказ хвоста не несёт —
	// иначе клиент читал бы данные из ответа, которого не было.
	if sink == nil || !f.OK {
		return resp, nil
	}
	if f.BodyLength < 0 {
		return resp, ErrBodyLengthInvalid
	}
	if f.BodyLength == 0 {
		return resp, ErrBodyLengthRequired
	}

	// ГРАНИЦА ТЕЛА. Обе половины обязательны и обе проверены саботажем:
	// без Buffered() теряется прочитанное декодером впрок (при теле меньше
	// буфера — всё тело целиком), без отбрасывания пробелов к телу
	// приклеивается разделитель (ADR-049 §2).
	body := io.MultiReader(skipLeadingWhitespace(dec.Buffered()), r)

	n, err := io.CopyN(sink, body, f.BodyLength)
	if err == io.EOF || (err == nil && n != f.BodyLength) {
		return resp, fmt.Errorf("%w: получено %d из %d", ErrBodyTruncated, n, f.BodyLength)
	}
	if err != nil {
		return resp, fmt.Errorf("session: read body: %w", err)
	}
	return resp, nil
}

// ── Push ────────────────────────────────────────────────────────────────────

type pushFrame struct {
	Event  string          `json:"event"`
	UserID string          `json:"user_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

func (JSONCodec) ReadPush(r io.Reader) (Push, error) {
	var f pushFrame
	if err := json.NewDecoder(r).Decode(&f); err != nil {
		return Push{}, fmt.Errorf("%w: push: %v", ErrMalformedFrame, err)
	}
	var data []byte
	if len(f.Data) > 0 {
		data = append([]byte(nil), f.Data...)
	}
	return Push{Event: f.Event, UserID: f.UserID, Data: data}, nil
}
