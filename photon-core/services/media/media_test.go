package media

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/wiloris/photon/photon-core/services/session"
)

// Тесты клиентского слоя. Транспорта нет: Streamer подменяется, потому что
// проверяется поведение клиента, а не кадрирование — оно уже закрыто тестами
// services/session.

// fakeStreamer — подделка сессии. Позволяет задать ответ и посмотреть, что
// клиент реально отправил и сколько прочитал из источника.
type fakeStreamer struct {
	// serve вызывается вместо DoStream.
	serve func(ctx context.Context, action int32, payload []byte,
		body io.Reader, sink io.Writer) (session.Response, error)

	gotAction     int32
	gotPayload    []byte
	readFromBody  int64
	maxSingleRead int
}

func (f *fakeStreamer) DoStream(ctx context.Context, action int32, payload []byte,
	body io.Reader, sink io.Writer) (session.Response, error) {
	f.gotAction = action
	f.gotPayload = append([]byte(nil), payload...)
	return f.serve(ctx, action, payload, body, sink)
}

// drain — типичное поведение сервера на загрузке: дочитать тело и ответить.
func drain(resp session.Response) func(context.Context, int32, []byte, io.Reader, io.Writer) (session.Response, error) {
	return func(_ context.Context, _ int32, _ []byte, body io.Reader, _ io.Writer) (session.Response, error) {
		if body != nil {
			_, _ = io.Copy(io.Discard, body)
		}
		return resp, nil
	}
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func okResp(v any) session.Response {
	raw, _ := json.Marshal(v)
	return session.Response{OK: true, Data: raw}
}

func req(b []byte) UploadRequest {
	return UploadRequest{ChannelID: "c1", Name: "a.bin", Mime: "application/octet-stream",
		SHA256: sum(b), Size: int64(len(b))}
}

// ── загрузка ─────────────────────────────────────────────────────────────────

func TestUploadSendsDeclaredPayloadAndReturnsRef(t *testing.T) {
	body := payload(4096)
	f := &fakeStreamer{serve: drain(okResp(Ref{MediaID: "m1", SHA256: sum(body), Size: 4096}))}

	ref, err := New(f).Upload(context.Background(), req(body), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if ref.MediaID != "m1" {
		t.Fatalf("media_id подменён: %q", ref.MediaID)
	}

	var sent uploadPayload
	if err := json.Unmarshal(f.gotPayload, &sent); err != nil {
		t.Fatalf("payload не разбирается: %v", err)
	}
	if sent.BodyLength != 4096 || sent.SHA256 != sum(body) || sent.ChannelID != "c1" {
		t.Fatalf("на провод ушёл не тот payload: %+v", sent)
	}
	if f.gotAction != 700 {
		t.Fatalf("action = %d, want 700", f.gotAction)
	}
}

// Тело не собирается в память: наибольшее одиночное чтение много меньше тела.
func TestUploadDoesNotBufferWholeBody(t *testing.T) {
	const size = 4 << 20
	body := payload(size)
	f := &fakeStreamer{}
	f.serve = func(_ context.Context, _ int32, _ []byte, b io.Reader, _ io.Writer) (session.Response, error) {
		buf := make([]byte, 64<<10)
		for {
			n, err := b.Read(buf)
			f.readFromBody += int64(n)
			if n > f.maxSingleRead {
				f.maxSingleRead = n
			}
			if err != nil {
				break
			}
		}
		return okResp(Ref{MediaID: "m1", SHA256: sum(body), Size: size}), nil
	}
	if _, err := New(f).Upload(context.Background(), req(body), bytes.NewReader(body)); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if f.readFromBody != size {
		t.Fatalf("прочитано %d из %d", f.readFromBody, size)
	}
	if f.maxSingleRead > 1<<20 {
		t.Fatalf("наибольшее одиночное чтение %d при теле %d", f.maxSingleRead, size)
	}
}

// Клиент отдаёт РОВНО объявленную длину, даже если источник длиннее: лишнее
// уехало бы в следующий кадр и сломало разбор.
func TestUploadSendsExactlyDeclaredLength(t *testing.T) {
	body := payload(1000)
	src := io.MultiReader(bytes.NewReader(body), bytes.NewReader(payload(500)))
	f := &fakeStreamer{}
	var got bytes.Buffer
	f.serve = func(_ context.Context, _ int32, _ []byte, b io.Reader, _ io.Writer) (session.Response, error) {
		_, _ = io.Copy(&got, b)
		return okResp(Ref{MediaID: "m1", SHA256: sum(body), Size: 1000}), nil
	}
	if _, err := New(f).Upload(context.Background(), req(body), src); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if got.Len() != 1000 || !bytes.Equal(got.Bytes(), body) {
		t.Fatalf("отправлено %d байт вместо 1000", got.Len())
	}
}

// Сервер подтвердил другой хеш — принято не то, что отправлено.
func TestUploadRejectsHashMismatchFromServer(t *testing.T) {
	body := payload(100)
	f := &fakeStreamer{serve: drain(okResp(Ref{MediaID: "m1", SHA256: sum(payload(99)), Size: 100}))}
	_, err := New(f).Upload(context.Background(), req(body), bytes.NewReader(body))
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("err = %v, want ErrIntegrity", err)
	}
}

func TestUploadRejectsSizeMismatchFromServer(t *testing.T) {
	body := payload(100)
	f := &fakeStreamer{serve: drain(okResp(Ref{MediaID: "m1", SHA256: sum(body), Size: 99}))}
	_, err := New(f).Upload(context.Background(), req(body), bytes.NewReader(body))
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("err = %v, want ErrSizeMismatch", err)
	}
}

func TestUploadValidatesRequest(t *testing.T) {
	body := payload(10)
	base := req(body)
	bad := map[string]func(*UploadRequest){
		"без канала":     func(r *UploadRequest) { r.ChannelID = "" },
		"без имени":      func(r *UploadRequest) { r.Name = "" },
		"кривой sha256":  func(r *UploadRequest) { r.SHA256 = "zz" },
		"нулевой размер": func(r *UploadRequest) { r.Size = 0 },
	}
	f := &fakeStreamer{serve: drain(okResp(Ref{}))}
	for name, mut := range bad {
		r := base
		mut(&r)
		if _, err := New(f).Upload(context.Background(), r, bytes.NewReader(body)); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: err = %v, want ErrInvalidRequest", name, err)
		}
	}
}

// Отказ сервера не заставляет клиента дочитывать источник до конца: тело
// перестают читать там, где остановился транспорт.
func TestUploadDoesNotDrainSourceAfterServerRejection(t *testing.T) {
	const size = 4 << 20
	body := payload(size)
	src := &countingSrc{r: bytes.NewReader(body)}
	f := &fakeStreamer{}
	f.serve = func(_ context.Context, _ int32, _ []byte, b io.Reader, _ io.Writer) (session.Response, error) {
		// Сервер отверг рано: прочитал немного и ответил отказом.
		_, _ = io.CopyN(io.Discard, b, 4096)
		return session.Response{OK: false, Error: "forbidden"}, nil
	}
	_, err := New(f).Upload(context.Background(), req(body), src)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("err = %v, want ErrRejected", err)
	}
	if src.total >= size {
		t.Fatalf("источник дочитан целиком (%d из %d) несмотря на отказ", src.total, size)
	}
}

type countingSrc struct {
	r     io.Reader
	total int
}

func (c *countingSrc) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.total += n
	return n, err
}

// Транспортная ошибка проходит насквозь и остаётся отличимой от integrity.
func TestUploadPropagatesTransportError(t *testing.T) {
	body := payload(10)
	f := &fakeStreamer{}
	f.serve = func(context.Context, int32, []byte, io.Reader, io.Writer) (session.Response, error) {
		return session.Response{}, session.ErrConnectionLost
	}
	_, err := New(f).Upload(context.Background(), req(body), bytes.NewReader(body))
	if !errors.Is(err, session.ErrConnectionLost) {
		t.Fatalf("err = %v, want ErrConnectionLost", err)
	}
	if errors.Is(err, ErrIntegrity) || errors.Is(err, ErrSizeMismatch) {
		t.Fatal("транспортная ошибка не должна выглядеть как ошибка целостности")
	}
}

// ── скачивание ───────────────────────────────────────────────────────────────

// serveBody — сервер отдаёт метаданные и пишет тело в sink, как это делает
// ReadResponseStream после разбора кадра.
func serveBody(info Info, blob []byte) func(context.Context, int32, []byte, io.Reader, io.Writer) (session.Response, error) {
	return func(_ context.Context, _ int32, _ []byte, _ io.Reader, sink io.Writer) (session.Response, error) {
		if sink != nil {
			if _, err := sink.Write(blob); err != nil {
				return session.Response{}, err
			}
		}
		return okResp(info), nil
	}
}

func TestDownloadWritesBodyAndVerifies(t *testing.T) {
	blob := payload(5000)
	info := Info{MediaID: "m1", SHA256: sum(blob), Size: 5000, Name: "a.bin"}
	f := &fakeStreamer{serve: serveBody(info, blob)}

	var got bytes.Buffer
	back, err := New(f).Download(context.Background(), "m1", &got)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if !bytes.Equal(got.Bytes(), blob) {
		t.Fatal("тело искажено")
	}
	if back.MediaID != "m1" || back.Size != 5000 {
		t.Fatalf("метаданные искажены: %+v", back)
	}
	if f.gotAction != 701 {
		t.Fatalf("action = %d, want 701", f.gotAction)
	}
}

func TestDownloadDoesNotBufferWholeBody(t *testing.T) {
	const size = 4 << 20
	blob := payload(size)
	info := Info{MediaID: "m1", SHA256: sum(blob), Size: size}
	f := &fakeStreamer{}
	f.serve = func(_ context.Context, _ int32, _ []byte, _ io.Reader, sink io.Writer) (session.Response, error) {
		// Пишем блоками, как настоящий поток.
		for off := 0; off < size; off += 64 << 10 {
			end := off + 64<<10
			if end > size {
				end = size
			}
			if _, err := sink.Write(blob[off:end]); err != nil {
				return session.Response{}, err
			}
		}
		return okResp(info), nil
	}
	w := &countingSink{}
	if _, err := New(f).Download(context.Background(), "m1", w); err != nil {
		t.Fatalf("Download: %v", err)
	}
	if w.total != size {
		t.Fatalf("записано %d из %d", w.total, size)
	}
	if w.maxWrite > 1<<20 {
		t.Fatalf("наибольшая одиночная запись %d при теле %d — тело собирается целиком", w.maxWrite, size)
	}
}

type countingSink struct {
	total    int
	maxWrite int
}

func (c *countingSink) Write(p []byte) (int, error) {
	c.total += len(p)
	if len(p) > c.maxWrite {
		c.maxWrite = len(p)
	}
	return len(p), nil
}

// Тело не то, что объявлено хешем.
func TestDownloadRejectsWrongHash(t *testing.T) {
	blob := payload(1000)
	info := Info{MediaID: "m1", SHA256: sum(payload(999)), Size: 1000}
	f := &fakeStreamer{serve: serveBody(info, blob)}
	var got bytes.Buffer
	_, err := New(f).Download(context.Background(), "m1", &got)
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("err = %v, want ErrIntegrity", err)
	}
}

// Пришло меньше объявленного.
func TestDownloadRejectsShortBody(t *testing.T) {
	blob := payload(1000)
	info := Info{MediaID: "m1", SHA256: sum(blob), Size: 5000}
	f := &fakeStreamer{serve: serveBody(info, blob)}
	var got bytes.Buffer
	_, err := New(f).Download(context.Background(), "m1", &got)
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("err = %v, want ErrSizeMismatch", err)
	}
}

// Пришло больше объявленного — тоже не успех.
func TestDownloadRejectsExtraBody(t *testing.T) {
	blob := payload(1000)
	info := Info{MediaID: "m1", SHA256: sum(blob), Size: 1000}
	f := &fakeStreamer{serve: serveBody(info, append(append([]byte(nil), blob...), payload(20)...))}
	var got bytes.Buffer
	_, err := New(f).Download(context.Background(), "m1", &got)
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("err = %v, want ErrSizeMismatch", err)
	}
}

// Ответ про другой объект — не тот, что спрашивали.
func TestDownloadRejectsWrongMediaID(t *testing.T) {
	blob := payload(100)
	info := Info{MediaID: "другой", SHA256: sum(blob), Size: 100}
	f := &fakeStreamer{serve: serveBody(info, blob)}
	var got bytes.Buffer
	_, err := New(f).Download(context.Background(), "m1", &got)
	if !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("err = %v, want ErrMalformedResponse", err)
	}
}

// Отмена не оставляет скачивание успешным, даже если часть байтов записана.
func TestDownloadCancellationIsNotSuccess(t *testing.T) {
	blob := payload(5000)
	f := &fakeStreamer{}
	f.serve = func(ctx context.Context, _ int32, _ []byte, _ io.Reader, sink io.Writer) (session.Response, error) {
		_, _ = sink.Write(blob[:1000]) // часть уже записана
		return session.Response{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var got bytes.Buffer
	info, err := New(f).Download(ctx, "m1", &got)
	if err == nil {
		t.Fatal("отменённое скачивание не может быть успешным")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if info != nil {
		t.Fatal("при отмене метаданные не считаются полученными")
	}
}

func TestDownloadSurfacesServerRejection(t *testing.T) {
	f := &fakeStreamer{}
	f.serve = func(context.Context, int32, []byte, io.Reader, io.Writer) (session.Response, error) {
		return session.Response{OK: false, Error: "unavailable"}, nil
	}
	var got bytes.Buffer
	_, err := New(f).Download(context.Background(), "m1", &got)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("err = %v, want ErrRejected", err)
	}
	// Причина обязана сохраниться: unavailable отличим от forbidden.
	if got.Len() != 0 {
		t.Fatal("при отказе тело не должно записываться")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("unavailable")) {
		t.Fatalf("причина отказа потеряна: %v", err)
	}
}

func TestDownloadValidatesArguments(t *testing.T) {
	f := &fakeStreamer{serve: drain(okResp(Info{}))}
	var got bytes.Buffer
	if _, err := New(f).Download(context.Background(), "", &got); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("пустой media_id: %v", err)
	}
	if _, err := New(f).Download(context.Background(), "m1", nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil sink: %v", err)
	}
}
