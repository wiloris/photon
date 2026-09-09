package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// MediaService — исполнитель медийных операций поверх MediaStore.
//
// Границы, которые он НЕ пересекает:
//
//   - о транспорте не знает ничего: принимает io.Reader и io.Writer, а не
//     стрим. Кадрирование живёт слоем выше (media-transfer.md §12), и здесь
//     нет ни одной ссылки на quic;
//   - авторизацию НЕ выполняет: субъект, дошедший сюда, уже проверен
//     Dispatcher.Authorize через objMedia. Вторая проверка была бы второй
//     точкой принятия решения (ADR-039 §6);
//   - домен не разбирает: тело — непрозрачные байты.
//
// Чего здесь нет и не должно появиться в первом срезе: возобновляемость,
// диапазоны, дедупликация, планировщик уборки, вытеснение, превью, квоты,
// жизненный цикл удаления (ADR-048 §14, ADR-049 §11).
type MediaService struct {
	store MediaStore
	// ttl — срок жизни объекта. Записывается при создании: иначе объект
	// нельзя будет убрать, не додумывая срок задним числом
	// (media-transfer.md §11).
	ttl time.Duration
	// now и newID — точки подмены для тестов. Без них проверить, что успешная
	// загрузка ставит именно STORED и именно с тем идентификатором, который
	// вернулся клиенту, можно было бы только косвенно.
	now   func() time.Time
	newID func() string
}

// DefaultMediaTTL — срок по умолчанию. Значение развёртывания, не константа
// протокола; уборка по нему в первом срезе не запускается.
const DefaultMediaTTL = 30 * 24 * time.Hour

// MaxMediaBytes — верхний предел тела. Параметр развёртывания
// (media-transfer.md §3): значение вне диапазона отвергается ДО чтения тела.
const MaxMediaBytes int64 = 100 << 20

func NewMediaService(store MediaStore) *MediaService {
	return &MediaService{
		store: store,
		ttl:   DefaultMediaTTL,
		now:   time.Now,
		newID: model.NewId,
	}
}

var (
	// ErrMediaInvalidRequest — запрос не соответствует контракту: пустые
	// обязательные поля, длина вне диапазона, sha256 не того формата.
	ErrMediaInvalidRequest = errors.New("media: invalid request")

	// ErrMediaIntegrity — принятые байты не сошлись с объявленным sha256.
	// Отличается от ErrMediaInvalidRequest намеренно: запрос был корректен,
	// не сошлось содержимое (media-transfer.md §5).
	ErrMediaIntegrity = errors.New("media: integrity check failed")

	// ErrMediaSizeMismatch — принято не столько байт, сколько объявлено.
	// Ловится РАНЬШЕ хеша: дешёвый и ранний детектор обрыва (§3).
	ErrMediaSizeMismatch = errors.New("media: body length mismatch")

	// ErrMediaUnavailable — объект известен, но байтов больше нет.
	ErrMediaUnavailable = errors.New("media: unavailable")

	// ErrMediaAlreadyAttached — объект уже прикреплён к другому посту.
	// Прикрепление к тому же посту повторно — не ошибка: повтор запроса не
	// должен ломать результат.
	ErrMediaAlreadyAttached = errors.New("media: already attached to another post")
)

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// UploadRequest — то, что объявил клиент до передачи тела.
type UploadRequest struct {
	ChannelID  string
	UploaderID string
	Name       string
	Mime       string
	SHA256     string // объявлен ДО передачи: проверка целостности двусторонняя
	BodyLength int64
}

func (r UploadRequest) validate() error {
	switch {
	case r.ChannelID == "":
		return fmt.Errorf("%w: channel_id is required", ErrMediaInvalidRequest)
	case r.UploaderID == "":
		return fmt.Errorf("%w: uploader is required", ErrMediaInvalidRequest)
	case r.Name == "":
		return fmt.Errorf("%w: name is required", ErrMediaInvalidRequest)
	case !sha256Hex.MatchString(r.SHA256):
		return fmt.Errorf("%w: sha256 must be 64 lowercase hex chars", ErrMediaInvalidRequest)
	case r.BodyLength <= 0 || r.BodyLength > MaxMediaBytes:
		return fmt.Errorf("%w: body_length %d out of range", ErrMediaInvalidRequest, r.BodyLength)
	}
	return nil
}

// Upload принимает тело потоково и создаёт объект в состоянии STORED.
//
// ПОРЯДОК СУЩЕСТВЕН и является частью контракта: метаданные пишутся ТОЛЬКО
// после того, как тело принято, сверено и зафиксировано. Обратный порядок
// оставил бы при обрыве объект в STORED, заявляющий байты, которых нет, —
// а STORED означает «байты записаны и проверены» (media-transfer.md §11).
//
// Тело целиком в память не собирается: io.Reader, копирование блоками,
// хеширование на лету (§10).
func (s *MediaService) Upload(req UploadRequest, body io.Reader) (*MediaObject, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("%w: body is required", ErrMediaInvalidRequest)
	}

	mediaID := s.newID()
	w, err := s.store.CreateBlob(mediaID)
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	// CopyN, а не Copy: принимается РОВНО объявленное. Больше — значит клиент
	// нарушил контракт и остаток попал бы в следующий кадр; меньше — обрыв.
	n, err := io.CopyN(io.MultiWriter(w, h), body, req.BodyLength)
	if err != nil && !errors.Is(err, io.EOF) {
		_ = w.Discard()
		_ = s.store.DiscardObject(mediaID)
		return nil, fmt.Errorf("media: read body: %w", err)
	}
	if n != req.BodyLength {
		_ = w.Discard()
		_ = s.store.DiscardObject(mediaID)
		return nil, fmt.Errorf("%w: получено %d из %d", ErrMediaSizeMismatch, n, req.BodyLength)
	}

	got := hex.EncodeToString(h.Sum(nil))
	if got != req.SHA256 {
		_ = w.Discard()
		_ = s.store.DiscardObject(mediaID)
		return nil, fmt.Errorf("%w: объявлен %s, принят %s", ErrMediaIntegrity, req.SHA256, got)
	}

	if err := w.Commit(); err != nil {
		_ = s.store.DiscardObject(mediaID)
		return nil, err
	}

	now := s.now()
	obj := &MediaObject{
		MediaID:    mediaID,
		ChannelID:  req.ChannelID,
		UploaderID: req.UploaderID,
		SHA256:     got,
		Size:       n,
		Mime:       req.Mime,
		Name:       req.Name,
		State:      MediaStored,
		CreatedAt:  now.UnixMilli(),
		ExpiresAt:  now.Add(s.ttl).UnixMilli(),
	}
	if err := s.store.PutMeta(obj); err != nil {
		// Байты без метаданных недостижимы и подлежат уборке: объект, о
		// котором нечему знать, — мусор, а не результат.
		_ = s.store.DiscardObject(mediaID)
		return nil, err
	}
	return obj, nil
}

// Info возвращает метаданные объекта, если его байты доступны.
//
// Нужен отдельно от Download потому, что на скачивании JSON-ответ идёт ПЕРЕД
// телом (media-transfer.md §1), а в ответе обязана быть длина. Узнать её,
// не прочитав метаданные заранее, нельзя.
func (s *MediaService) Info(mediaID string) (*MediaObject, error) {
	obj, err := s.store.Get(mediaID)
	if err != nil {
		return nil, err
	}
	if !obj.HasBytes() {
		return obj, ErrMediaUnavailable
	}
	return obj, nil
}

// Download отдаёт байты объекта потоково.
//
// Авторизация здесь НЕ выполняется: она уже сделана до входа в сервис. Здесь
// различаются только «объект есть и байты доступны» и «байтов больше нет».
func (s *MediaService) Download(mediaID string, sink io.Writer) (*MediaObject, error) {
	if sink == nil {
		return nil, fmt.Errorf("%w: sink is required", ErrMediaInvalidRequest)
	}
	obj, err := s.store.Get(mediaID)
	if err != nil {
		return nil, err
	}
	if !obj.HasBytes() {
		return obj, ErrMediaUnavailable
	}

	rc, err := s.store.OpenBlob(mediaID)
	if err != nil {
		return obj, err
	}
	defer rc.Close()

	// Отдаётся РОВНО Size байт: тело на диске длиннее объявленного означало бы
	// порчу, и молча отдать лишнее хуже, чем отказать.
	n, err := io.CopyN(sink, rc, obj.Size)
	if err != nil && !errors.Is(err, io.EOF) {
		return obj, fmt.Errorf("media: read blob: %w", err)
	}
	if n != obj.Size {
		return obj, fmt.Errorf("%w: на диске %d из %d", ErrMediaSizeMismatch, n, obj.Size)
	}
	return obj, nil
}

// Attach связывает объект с постом: STORED → ATTACHED → AVAILABLE.
//
// Два перехода, а не один, и это не формальность. ATTACHED означает «назван в
// живом посте», AVAILABLE — «назван и байты доступны» (ADR-048 §7). Если тело
// исчезло, объект остаётся ATTACHED, и это состояние отличимо от нормального:
// сообщение ссылается на вложение, которого нет.
func (s *MediaService) Attach(mediaID, postID string) (*MediaObject, error) {
	if postID == "" {
		return nil, fmt.Errorf("%w: post_id is required", ErrMediaInvalidRequest)
	}
	obj, err := s.store.Get(mediaID)
	if err != nil {
		return nil, err
	}
	if obj.PostID != "" && obj.PostID != postID {
		return obj, ErrMediaAlreadyAttached
	}
	if !obj.HasBytes() {
		return obj, ErrMediaUnavailable
	}

	obj.PostID = postID
	obj.State = MediaAttached
	if err := s.store.PutMeta(obj); err != nil {
		return obj, err
	}

	rc, err := s.store.OpenBlob(mediaID)
	if err != nil {
		// Прикреплён, но байтов нет: объект остаётся ATTACHED намеренно.
		return obj, err
	}
	_ = rc.Close()

	obj.State = MediaAvailable
	if err := s.store.PutMeta(obj); err != nil {
		return obj, err
	}
	return obj, nil
}
