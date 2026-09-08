package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Тесты MediaService (этап 4). Транспорта здесь нет: сервис принимает
// io.Reader и io.Writer, кадрирование живёт слоем выше.

func svc(t *testing.T) (*MediaService, *fakeMediaStore) {
	t.Helper()
	store := newFakeMediaStore()
	s := NewMediaService(store)
	// Детерминированные время и идентификатор: без них нельзя проверить, что
	// успешная загрузка ставит именно те поля, которые вернулись клиенту.
	s.now = func() time.Time { return time.UnixMilli(1_700_000_000_000) }
	ids := 0
	s.newID = func() string { ids++; return "mediaobj0000000000000000" + string(rune('a'+ids-1)) }
	return s, store
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func upReq(b []byte) UploadRequest {
	return UploadRequest{
		ChannelID: "c1", UploaderID: "u1", Name: "a.bin", Mime: "application/octet-stream",
		SHA256: sum(b), BodyLength: int64(len(b)),
	}
}

// ── загрузка ─────────────────────────────────────────────────────────────────

func TestUploadStoresVerifiedObject(t *testing.T) {
	s, store := svc(t)
	body := payload(5000)

	obj, err := s.Upload(upReq(body), bytes.NewReader(body))
	require.NoError(t, err)

	require.Equal(t, MediaStored, obj.State, "успешная загрузка обязана дать STORED")
	require.Equal(t, sum(body), obj.SHA256)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Empty(t, obj.PostID, "непрлкреплённый объект обязан иметь пустой post_id")
	require.Equal(t, int64(1_700_000_000_000), obj.CreatedAt)
	require.Greater(t, obj.ExpiresAt, obj.CreatedAt, "срок обязан записываться при создании")

	// Байты действительно легли, и именно те.
	rc, err := store.OpenBlob(obj.MediaID)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	require.True(t, bytes.Equal(got, body), "на диске лежат не те байты")

	// Метаданные доступны под тем же идентификатором, что вернулся клиенту.
	back, err := store.Get(obj.MediaID)
	require.NoError(t, err)
	require.Equal(t, obj.MediaID, back.MediaID)
	require.Equal(t, obj.SHA256, back.SHA256)
}

// Хеш считается по РЕАЛЬНО принятым байтам, а не по объявленному.
func TestUploadRejectsWrongHash(t *testing.T) {
	s, store := svc(t)
	body := payload(1000)
	req := upReq(body)
	req.SHA256 = sum(payload(999)) // объявлен хеш других байтов

	_, err := s.Upload(req, bytes.NewReader(body))
	require.ErrorIs(t, err, ErrMediaIntegrity)
	require.Empty(t, store.objs, "объект не должен появляться при несовпадении хеша")
	require.Empty(t, store.blobs, "байты не должны оставаться при несовпадении хеша")
}

// Объявлено больше, чем прислано: обрыв, а не частичный успех.
func TestUploadRejectsShortBody(t *testing.T) {
	s, store := svc(t)
	body := payload(1000)
	req := upReq(body)
	req.BodyLength = 5000

	_, err := s.Upload(req, bytes.NewReader(body))
	require.ErrorIs(t, err, ErrMediaSizeMismatch)
	require.Empty(t, store.objs)
	require.Empty(t, store.blobs)
}

// Прислано больше объявленного: принимается ровно объявленное, объект — по
// объявленной длине, лишнее не попадает ни в тело, ни в хеш.
func TestUploadReadsExactlyDeclaredLength(t *testing.T) {
	s, store := svc(t)
	want := payload(1000)
	req := upReq(want)
	stream := io.MultiReader(bytes.NewReader(want), strings.NewReader("ЛИШНЕЕ"))

	obj, err := s.Upload(req, stream)
	require.NoError(t, err)
	require.Equal(t, int64(1000), obj.Size)

	rc, _ := store.OpenBlob(obj.MediaID)
	got, _ := io.ReadAll(rc)
	require.True(t, bytes.Equal(got, want), "в тело попало лишнее")
}

func TestUploadValidatesRequest(t *testing.T) {
	s, _ := svc(t)
	body := payload(10)
	base := upReq(body)

	bad := map[string]func(*UploadRequest){
		"без канала":       func(r *UploadRequest) { r.ChannelID = "" },
		"без загрузившего": func(r *UploadRequest) { r.UploaderID = "" },
		"без имени":        func(r *UploadRequest) { r.Name = "" },
		"кривой sha256":    func(r *UploadRequest) { r.SHA256 = "XYZ" },
		"sha256 в верхнем регистре": func(r *UploadRequest) {
			r.SHA256 = strings.ToUpper(base.SHA256)
		},
		"нулевая длина":       func(r *UploadRequest) { r.BodyLength = 0 },
		"отрицательная длина": func(r *UploadRequest) { r.BodyLength = -1 },
		"длина сверх предела": func(r *UploadRequest) { r.BodyLength = MaxMediaBytes + 1 },
	}
	for name, mut := range bad {
		r := base
		mut(&r)
		_, err := s.Upload(r, bytes.NewReader(body))
		require.ErrorIsf(t, err, ErrMediaInvalidRequest, "%s: должно быть отвергнуто", name)
	}
}

// Обрыв источника посреди тела не оставляет ни объекта, ни ложного состояния.
func TestUploadFailureLeavesNothingBehind(t *testing.T) {
	s, store := svc(t)
	body := payload(5000)
	req := upReq(body)

	_, err := s.Upload(req, io.MultiReader(bytes.NewReader(body[:100]), errReader{}))
	require.Error(t, err)
	require.Empty(t, store.objs, "после обрыва не должно остаться метаданных")
	require.Empty(t, store.blobs, "после обрыва не должно остаться байтов")
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("оборвалось") }

// Тело не собирается в память целиком: сигнатура принимает io.Reader, и
// наибольшее одиночное чтение много меньше тела.
func TestUploadDoesNotBufferWholeBody(t *testing.T) {
	s, _ := svc(t)
	const size = 2 << 20
	body := payload(size)
	cr := &countingSrc{r: bytes.NewReader(body)}
	req := upReq(body)

	_, err := s.Upload(req, cr)
	require.NoError(t, err)
	require.Equal(t, size, cr.total)
	require.LessOrEqual(t, cr.maxRead, 1<<20,
		"наибольшее одиночное чтение %d при теле %d — тело собирается целиком", cr.maxRead, size)
}

type countingSrc struct {
	r       io.Reader
	maxRead int
	total   int
}

func (c *countingSrc) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > c.maxRead {
		c.maxRead = n
	}
	c.total += n
	return n, err
}

// ── скачивание ───────────────────────────────────────────────────────────────

func TestDownloadReturnsExactBytesOfRequestedObject(t *testing.T) {
	s, _ := svc(t)
	a := payload(1000)
	b := payload(777)
	for i := range b {
		b[i] ^= 0xFF
	}
	objA, err := s.Upload(upReq(a), bytes.NewReader(a))
	require.NoError(t, err)
	objB, err := s.Upload(upReq(b), bytes.NewReader(b))
	require.NoError(t, err)
	require.NotEqual(t, objA.MediaID, objB.MediaID)

	var gotA, gotB bytes.Buffer
	_, err = s.Download(objA.MediaID, &gotA)
	require.NoError(t, err)
	_, err = s.Download(objB.MediaID, &gotB)
	require.NoError(t, err)

	require.True(t, bytes.Equal(gotA.Bytes(), a), "отданы байты не того объекта")
	require.True(t, bytes.Equal(gotB.Bytes(), b), "отданы байты не того объекта")
}

func TestDownloadUnknownObject(t *testing.T) {
	s, _ := svc(t)
	var got bytes.Buffer
	_, err := s.Download("nosuchobject0000000000000", &got)
	require.Error(t, err)
	require.Zero(t, got.Len())
}

// Объект есть, байтов нет: unavailable, а не «нет объекта».
func TestDownloadReportsUnavailable(t *testing.T) {
	s, store := svc(t)
	store.add(&MediaObject{MediaID: "expired00000000000000000", ChannelID: "c1", State: MediaExpired, Size: 10})
	var got bytes.Buffer
	_, err := s.Download("expired00000000000000000", &got)
	require.ErrorIs(t, err, ErrMediaUnavailable)
	require.Zero(t, got.Len(), "недоступный объект не должен отдавать байты")
}

// Тело на диске короче объявленного размера — порча, а не усечённая выдача.
func TestDownloadRejectsTruncatedBlob(t *testing.T) {
	s, store := svc(t)
	body := payload(1000)
	obj, err := s.Upload(upReq(body), bytes.NewReader(body))
	require.NoError(t, err)
	store.blobs[obj.MediaID] = body[:500] // порча на диске

	var got bytes.Buffer
	_, err = s.Download(obj.MediaID, &got)
	require.ErrorIs(t, err, ErrMediaSizeMismatch)
}

// ── прикрепление ─────────────────────────────────────────────────────────────

func TestAttachMovesThroughLifecycle(t *testing.T) {
	s, store := svc(t)
	body := payload(100)
	obj, err := s.Upload(upReq(body), bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, MediaStored, obj.State)

	att, err := s.Attach(obj.MediaID, "post1")
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, att.State)
	require.Equal(t, "post1", att.PostID)

	back, err := store.Get(obj.MediaID)
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, back.State, "состояние обязано быть записано")
	require.Equal(t, "post1", back.PostID)
}

// Прикреплён, но байтов нет — объект остаётся ATTACHED и отличим от нормы.
func TestAttachWithoutBytesStaysAttached(t *testing.T) {
	s, store := svc(t)
	body := payload(100)
	obj, err := s.Upload(upReq(body), bytes.NewReader(body))
	require.NoError(t, err)
	delete(store.blobs, obj.MediaID) // байты исчезли

	att, err := s.Attach(obj.MediaID, "post1")
	require.Error(t, err)
	require.Equal(t, MediaAttached, att.State, "объект без байтов не может стать AVAILABLE")
}

func TestAttachToAnotherPostRejected(t *testing.T) {
	s, _ := svc(t)
	body := payload(100)
	obj, _ := s.Upload(upReq(body), bytes.NewReader(body))
	_, err := s.Attach(obj.MediaID, "post1")
	require.NoError(t, err)

	_, err = s.Attach(obj.MediaID, "post2")
	require.ErrorIs(t, err, ErrMediaAlreadyAttached)
}

// Повторное прикрепление к тому же посту — не ошибка: повтор запроса не должен
// ломать результат.
func TestAttachIsIdempotentForSamePost(t *testing.T) {
	s, _ := svc(t)
	body := payload(100)
	obj, _ := s.Upload(upReq(body), bytes.NewReader(body))
	_, err := s.Attach(obj.MediaID, "post1")
	require.NoError(t, err)
	again, err := s.Attach(obj.MediaID, "post1")
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, again.State)
}

// Непрлкреплённая загрузка остаётся обнаружимой: пустой post_id и состояние
// STORED — то, по чему её найдёт будущая уборка (ADR-048 §12 A).
func TestUnattachedUploadRemainsDiscoverable(t *testing.T) {
	s, store := svc(t)
	body := payload(100)
	obj, err := s.Upload(upReq(body), bytes.NewReader(body))
	require.NoError(t, err)

	back, err := store.Get(obj.MediaID)
	require.NoError(t, err)
	require.Equal(t, MediaStored, back.State)
	require.Empty(t, back.PostID)
	require.NotZero(t, back.CreatedAt, "без времени создания брошенную загрузку не отличить по возрасту")
	require.NotZero(t, back.ExpiresAt)
}
