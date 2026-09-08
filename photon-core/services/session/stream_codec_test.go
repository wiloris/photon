package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// Тесты границы кадра для потоковой передачи (media-transfer.md §2, §3).
//
// Проверяется РОВНО то, что при нарушении теряется тихо: длина правдоподобна,
// поток валиден, данные неверны. Поэтому почти каждый тест сравнивает байты, а
// не длины.

// jc — кодек под тестом. Переменная, а не литерал в месте вызова:
// JSONCodec{}.Method(...) внутри if разбирается как композитный литерал в
// условии и не компилируется.
var jc JSONCodec

// serverFrames собирает то, что пишет сервер: JSON-кадр ответа через
// json.Encoder (с завершающим "\n", как у настоящего сервера), затем тело.
//
// Именно этот "\n" и есть причина, по которой одного dec.Buffered()
// недостаточно: декодер его не потребляет.
func serverFrames(t *testing.T, resp any, body []byte) io.Reader {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		t.Fatalf("encode: %v", err)
	}
	buf.Write(body)
	return bytes.NewReader(buf.Bytes())
}

// jsonBait — тело, НАЧИНАЮЩЕЕСЯ с байтов, похожих на JSON-кадр. Если граница
// определена неверно, декодер съест их как ещё одно значение, и поломка
// окажется тихой.
const jsonBait = "{\"looks\":\"like a frame\"}\n"

func bodyOf(n int) []byte {
	b := make([]byte, 0, n)
	b = append(b, jsonBait...)
	for i := len(b); i < n; i++ {
		b = append(b, byte(i%251))
	}
	return b[:n]
}

func okResp(bodyLen int) map[string]any {
	return map[string]any{"ok": true, "data": map[string]any{"media_id": "m1"}, "body_length": bodyLen}
}

// ── граница тела ─────────────────────────────────────────────────────────────

func TestReadResponseStreamExtractsBodyExactly(t *testing.T) {
	// Размеры по обе стороны буфера чтения json.Decoder: тело меньше буфера
	// целиком остаётся в Buffered(), тело больше — частью на стриме.
	for _, n := range []int{1, 40, 512, 4096, 200_000} {
		t.Run(fmt.Sprintf("тело_%d", n), func(t *testing.T) {
			want := bodyOf(n)
			r := serverFrames(t, okResp(n), want)

			var got bytes.Buffer
			resp, err := jc.ReadResponseStream(r, &got)
			if err != nil {
				t.Fatalf("ReadResponseStream: %v", err)
			}
			if !resp.OK || resp.BodyLength != int64(n) {
				t.Fatalf("конверт: ok=%v body_length=%d", resp.OK, resp.BodyLength)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("тело не совпало: получено %d байт из %d, первые расхождения при %d",
					got.Len(), n, firstDiff(got.Bytes(), want))
			}
			if string(resp.Data) == "" {
				t.Fatal("data потерялась")
			}
		})
	}
}

func firstDiff(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

// Объявлено больше, чем прислано: усечение обязано быть ошибкой, а не
// частичным успехом (media-transfer.md §7).
func TestReadResponseStreamRejectsShortBody(t *testing.T) {
	want := bodyOf(100)
	r := serverFrames(t, okResp(500), want)
	var got bytes.Buffer
	_, err := jc.ReadResponseStream(r, &got)
	if !errors.Is(err, ErrBodyTruncated) {
		t.Fatalf("err = %v, want ErrBodyTruncated", err)
	}
}

// Прислано больше объявленного: читается РОВНО объявленное, лишнее не трогаем.
// Для получателя его не существует (media-transfer.md §3 п. 3).
func TestReadResponseStreamIgnoresExtraBody(t *testing.T) {
	want := bodyOf(100)
	extra := []byte("ЛИШНЕЕ-ЛИШНЕЕ-ЛИШНЕЕ")
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(okResp(100)); err != nil {
		t.Fatal(err)
	}
	buf.Write(want)
	buf.Write(extra)

	var got bytes.Buffer
	if _, err := jc.ReadResponseStream(bytes.NewReader(buf.Bytes()), &got); err != nil {
		t.Fatalf("ReadResponseStream: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("прочитано не ровно объявленное: %d байт вместо %d", got.Len(), len(want))
	}
}

// Длина обязательна: читать «до конца» нельзя, конец потока неотличим от
// обрыва (media-transfer.md §3).
func TestReadResponseStreamRequiresDeclaredLength(t *testing.T) {
	r := serverFrames(t, map[string]any{"ok": true, "data": map[string]any{}}, bodyOf(10))
	var got bytes.Buffer
	_, err := jc.ReadResponseStream(r, &got)
	if !errors.Is(err, ErrBodyLengthRequired) {
		t.Fatalf("err = %v, want ErrBodyLengthRequired", err)
	}
	if got.Len() != 0 {
		t.Fatal("без объявленной длины не должно быть прочитано ни байта")
	}
}

func TestReadResponseStreamRejectsNegativeLength(t *testing.T) {
	r := serverFrames(t, map[string]any{"ok": true, "body_length": -1}, nil)
	var got bytes.Buffer
	_, err := jc.ReadResponseStream(r, &got)
	if !errors.Is(err, ErrBodyLengthInvalid) {
		t.Fatalf("err = %v, want ErrBodyLengthInvalid", err)
	}
}

// Отказ хвоста не несёт: читать тело после ok:false значило бы брать данные из
// ответа, которого не было.
func TestReadResponseStreamSkipsBodyOnRejection(t *testing.T) {
	r := serverFrames(t, map[string]any{"ok": false, "error": "forbidden", "body_length": 100}, bodyOf(100))
	var got bytes.Buffer
	resp, err := jc.ReadResponseStream(r, &got)
	if err != nil {
		t.Fatalf("отказ не должен быть ошибкой транспорта: %v", err)
	}
	if resp.OK || resp.Error != "forbidden" {
		t.Fatalf("конверт отказа искажён: %+v", resp)
	}
	if got.Len() != 0 {
		t.Fatalf("после отказа прочитано %d байт тела", got.Len())
	}
}

// sink не задан — тело не читается вовсе, даже если объявлено.
func TestReadResponseStreamWithoutSinkReadsNoBody(t *testing.T) {
	r := serverFrames(t, okResp(100), bodyOf(100))
	resp, err := jc.ReadResponseStream(r, nil)
	if err != nil {
		t.Fatalf("ReadResponseStream: %v", err)
	}
	if resp.BodyLength != 100 {
		t.Fatalf("длина потерялась: %d", resp.BodyLength)
	}
}

// ── запись ───────────────────────────────────────────────────────────────────

func TestWriteRequestStreamEmitsFramesThenBody(t *testing.T) {
	body := bodyOf(1000)
	var w bytes.Buffer
	err := jc.WriteRequestStream(&w,
		Request{Action: 700, Resource: "MediaResource", Payload: []byte(`{"body_length":1000}`)},
		bytes.NewReader(body))
	if err != nil {
		t.Fatalf("WriteRequestStream: %v", err)
	}

	// Разбираем как сервер: два JSON-кадра, затем граница тела.
	// Ссылка на нижележащий читатель обязана сохраниться: в Buffered() лежит
	// лишь прочитанное декодером впрок, остаток тела — на самом потоке.
	src := bytes.NewReader(w.Bytes())
	dec := json.NewDecoder(src)
	var hdr map[string]any
	if err := dec.Decode(&hdr); err != nil {
		t.Fatalf("header: %v", err)
	}
	var pl map[string]any
	if err := dec.Decode(&pl); err != nil {
		t.Fatalf("payload: %v", err)
	}
	got, err := io.ReadAll(io.MultiReader(skipLeadingWhitespace(dec.Buffered()), src))
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("тело искажено: %d байт из %d, расхождение при %d", len(got), len(body), firstDiff(got, body))
	}
	if hdr["action"].(float64) != 700 {
		t.Fatalf("header: %v", hdr)
	}
}

// countingReader — считает, какими порциями читалось тело.
type countingReader struct {
	r       io.Reader
	maxRead int
	total   int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > c.maxRead {
		c.maxRead = n
	}
	c.total += n
	return n, err
}

// Тело не собирается в память целиком — требование контракта, а не свойство
// реализации (media-transfer.md §10). Доказательство: наибольшее одиночное
// чтение много меньше тела.
func TestWriteRequestStreamDoesNotBufferWholeBody(t *testing.T) {
	const size = 4 << 20 // 4 МБ
	cr := &countingReader{r: io.LimitReader(newPattern(), size)}
	err := jc.WriteRequestStream(io.Discard,
		Request{Action: 700, Resource: "MediaResource", Payload: []byte(`{}`)}, cr)
	if err != nil {
		t.Fatalf("WriteRequestStream: %v", err)
	}
	if cr.total != size {
		t.Fatalf("отправлено %d из %d", cr.total, size)
	}
	if cr.maxRead > 1<<20 {
		t.Fatalf("наибольшее одиночное чтение %d байт при теле %d — тело собирается целиком",
			cr.maxRead, size)
	}
}

func newPattern() io.Reader { return &pattern{} }

type pattern struct{ i int }

func (p *pattern) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte(p.i % 251)
		p.i++
	}
	return len(b), nil
}

// ── DoStream ─────────────────────────────────────────────────────────────────

// Направление задаётся действием, а не переговорами: стрим не может нести
// хвост в обе стороны. Проверяется ДО открытия стрима.
func TestDoStreamRejectsBothDirections(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
	before := conn.streamsOpened()
	_, err := s.DoStream(context.Background(), 700, nil, strings.NewReader("x"), io.Discard)
	if !errors.Is(err, ErrStreamDirection) {
		t.Fatalf("err = %v, want ErrStreamDirection", err)
	}
	if conn.streamsOpened() != before {
		t.Fatal("стрим не должен открываться при ошибке вызывающего")
	}
}

// Неизвестный action отвергается без сети, как и в Do.
func TestDoStreamRejectsUnknownActionWithoutNetwork(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
	before := conn.streamsOpened()
	if _, err := s.DoStream(context.Background(), 750, nil, nil, nil); !errors.Is(err, ErrUnknownAction) {
		t.Fatalf("err = %v, want ErrUnknownAction", err)
	}
	if conn.streamsOpened() != before {
		t.Fatal("неизвестный action не должен открывать стрим")
	}
}

// Сквозной путь скачивания: сервер отвечает кадром с длиной и телом, клиент
// получает байты в sink.
func TestDoStreamDownloadDeliversBody(t *testing.T) {
	want := bodyOf(3000)
	handler := func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		if idx == 0 {
			okHandshake(map[string]interface{}{"ok": true})(t, idx, srv)
			return
		}
		var hdr, pl map[string]any
		if err := readFrames(t, srv, &hdr, &pl); err != nil {
			return
		}
		writeJSON(t, srv, map[string]any{
			"ok": true, "data": map[string]any{"media_id": "m1"}, "body_length": len(want),
		})
		_, _ = srv.Write(want)
	}
	s, _ := newReadySession(t, handler, ModeAction)

	// Контекст ОГРАНИЧЕН намеренно: чтение «до конца потока» вместо
	// объявленной длины даёт не ошибку, а вечное ожидание — фиктивный
	// транспорт EOF не отдаёт. Зависший тест дефект не ловит, поэтому предел
	// обязателен (тот же урок, что в ADR-047).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var got bytes.Buffer
	resp, err := s.DoStream(ctx, 701, []byte(`{"media_id":"m1"}`), nil, &got)
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	if !resp.OK {
		t.Fatalf("ответ: %+v", resp)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("тело искажено: %d из %d, расхождение при %d",
			got.Len(), len(want), firstDiff(got.Bytes(), want))
	}
}

// Отказ доезжает до вызывающего конвертом, а не ошибкой транспорта: различие
// «вызов не состоялся» и «сервер отказал» — ADR-038.
func TestDoStreamSurfacesServerRejection(t *testing.T) {
	handler := func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		if idx == 0 {
			okHandshake(map[string]interface{}{"ok": true})(t, idx, srv)
			return
		}
		var hdr, pl map[string]any
		if err := readFrames(t, srv, &hdr, &pl); err != nil {
			return
		}
		writeJSON(t, srv, map[string]any{"ok": false, "error": "forbidden"})
	}
	s, _ := newReadySession(t, handler, ModeAction)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var got bytes.Buffer
	resp, err := s.DoStream(ctx, 701, []byte(`{"media_id":"m1"}`), nil, &got)
	if err != nil {
		t.Fatalf("отказ сервера не должен быть ошибкой вызова: %v", err)
	}
	if resp.OK || resp.Error != "forbidden" {
		t.Fatalf("конверт отказа искажён: %+v", resp)
	}
	if got.Len() != 0 {
		t.Fatal("после отказа тело читаться не должно")
	}
}
