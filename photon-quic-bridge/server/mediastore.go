package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mattermost/mattermost/server/public/model"
)

// MediaStore — хранилище media ENGRAM: байты и индекс метаданных.
//
// Модель — ADR-048: байты живут ЗДЕСЬ, а не в файловом хранилище Mattermost;
// Message несёт только ссылку. MediaStore является АВТОРИТЕТНЫМ источником для
// media_id, sha256, size, mime, владения, канала, связи с постом, состояния и
// expires_at (ADR-048 §7); props сообщения — проекция, а не хранилище истины.
//
// Абстракция, а не бэкенд (ADR-049 §8): локальная файловая система — реализация
// MVP, и замена её на объектное хранилище не должна менять ни протокол, ни
// контракт media-transfer.md.
//
// Чего здесь НЕТ: возобновляемость, диапазоны, дедупликация, планировщик
// уборки, вытеснение, квоты. Всё это отнесено ADR-048 §14 и ADR-049 §11.
//
// Удаление по удалению поста (уборка C) с ADR-055 ЕСТЬ и выражено ровно одним
// примитивом — DeleteBlob. Остальные четыре уборки ADR-048 §12 сюда
// по-прежнему не входят, и перечисления объектов у хранилища нет намеренно:
// подметание осиротевших (B) — отдельное решение, а метод, заведённый под
// него заранее, был бы формой, выведенной не из наблюдения.

// MediaState — серверный жизненный цикл объекта (ADR-048 §7).
//
// В props сообщения состояние НЕ попадает: истечение срока не должно требовать
// правки сообщения (ADR-048 §3).
type MediaState string

const (
	// MediaStored — байты записаны и проверены, post_id пуст.
	MediaStored MediaState = "STORED"
	// MediaAttached — объект назван в photon_media живого поста.
	MediaAttached MediaState = "ATTACHED"
	// MediaAvailable — прикреплён и байты доступны.
	MediaAvailable MediaState = "AVAILABLE"
	// MediaExpired / MediaDeleted определены ADR-048 §7. В первом срезе
	// достижимы только явной уборкой — планировщика нет (ADR-049 §11).
	MediaExpired MediaState = "EXPIRED"
	MediaDeleted MediaState = "DELETED"
)

// MediaObject — минимальная авторитетная схема из ADR-048 §7.
// Поля сверх перечисленных там не вводятся — кроме ConversationID (ADR-124 M1):
// объект разговора принадлежит разговору, а не каналу, и channel_id у него пуст.
type MediaObject struct {
	MediaID   string `json:"media_id"`
	PostID    string `json:"post_id,omitempty"` // пуст до прикрепления
	ChannelID string `json:"channel_id"`
	// ConversationID — область объекта вложения разговора (ADR-124 M1). Права
	// на такой объект решает обработчик по таблицам Bridge; путь 700–702
	// (objMedia) отказывает по пустому ChannelID.
	ConversationID string     `json:"conversation_id,omitempty"`
	UploaderID     string     `json:"uploader_id"`
	SHA256         string     `json:"sha256"`
	Size           int64      `json:"size"`
	Mime           string     `json:"mime"`
	Name           string     `json:"name"`
	W              int32      `json:"w,omitempty"`
	H              int32      `json:"h,omitempty"`
	State          MediaState `json:"state"`
	CreatedAt      int64      `json:"created_at"`
	ExpiresAt      int64      `json:"expires_at"`
}

// HasBytes — доступны ли байты объекта.
//
// Отделено от «объект существует» намеренно: media-transfer.md §5 требует
// отличать unavailable (объект известен субъекту, байтов нет) от forbidden
// (нет права ЛИБО нет объекта — намеренно неразличимые, ADR-039).
func (m *MediaObject) HasBytes() bool {
	return m.State != MediaExpired && m.State != MediaDeleted
}

var (
	// ErrMediaNotFound — объекта нет в индексе. Наружу в таком виде НЕ уходит:
	// вызывающий обязан превратить его в forbidden, иначе появится способ
	// пробовать существование чужих media_id (ADR-039, media-transfer.md §5).
	ErrMediaNotFound = errors.New("media: not found")

	// ErrMediaInvalidID — идентификатор не может быть путём в файловой системе.
	ErrMediaInvalidID = errors.New("media: invalid id")
)

// BlobWriter — запись тела с ЯВНОЙ фиксацией.
//
// Commit/Discard вынесены в контракт, а не спрятаны в реализацию, потому что
// от них зависит корректность: недописанное тело не должно быть наблюдаемым ни
// одной секунды. Загрузка, оборвавшаяся на середине, обязана не оставить
// следов, а не оставить объект с половиной байтов.
type BlobWriter interface {
	io.Writer
	// Commit делает записанное видимым.
	Commit() error
	// Discard выбрасывает недописанное. Идемпотентен.
	Discard() error
}

// MediaStore — контракт хранилища. Интерфейс, а не структура: ADR-049 §8
// требует, чтобы бэкенд заменялся без изменения протокола.
type MediaStore interface {
	// Get возвращает метаданные объекта или ErrMediaNotFound.
	Get(mediaID string) (*MediaObject, error)
	// PutMeta записывает метаданные объекта.
	PutMeta(m *MediaObject) error
	// CreateBlob открывает запись тела.
	CreateBlob(mediaID string) (BlobWriter, error)
	// OpenBlob открывает чтение тела или возвращает ErrMediaNotFound.
	OpenBlob(mediaID string) (io.ReadCloser, error)
	// DiscardObject убирает объект целиком — тело и метаданные.
	//
	// Это НЕ жизненный цикл удаления media (он вне первого среза, ADR-048
	// §12): единственный вызывающий — неудавшаяся загрузка, которая обязана
	// не оставить за собой ни байтов, ни записи.
	//
	// Для уборки C он НЕ ГОДИТСЯ, и это не вопрос вкуса: без метаданных Get
	// вернёт ErrMediaNotFound, objMedia откажет, и клиент получит forbidden
	// вместо unavailable — то есть удалённый объект станет неотличим от
	// никогда не существовавшего (ADR-055, media-transfer.md §5).
	DiscardObject(mediaID string) error

	// DeleteBlob убирает ТОЛЬКО байты, оставляя метаданные надгробием.
	//
	// Надгробие — не остаток и не мусор: именно оно позволяет отвечать
	// unavailable («объект известен, байтов больше нет») вместо forbidden.
	// Состояние объекта этим методом не меняется — его пишет вызывающий
	// ДО удаления байтов (ADR-055, «порядок операций»).
	//
	// Идемпотентен: отсутствие блоба — не ошибка. Прерванный проход уборки
	// оставляет объект в DELETED с байтами на диске, и следующий обязан
	// добить их молча.
	DeleteBlob(mediaID string) error
}

// fsMediaStore — реализация поверх локальной файловой системы.
//
// Каталог живёт ВНЕ каталога бандла плагина, и это измеренное требование, а не
// предпочтение: удаление плагина (`plugin delete`) сносит каталог бандла вместе
// со всем записанным в него, тогда как подкаталог data/ переживает и
// перезапуск, и переустановку, и удаление плагина (ADR-049 §7).
type fsMediaStore struct {
	// cfg читается ЛЕНИВО, а не при конструировании: во-первых, конфигурация
	// Mattermost перечитывается в рантайме и путь может измениться;
	// во-вторых, построение диспетчера не должно требовать доступного API —
	// иначе конструктор начинает зависеть от того, что ему для работы не
	// нужно.
	cfg func() *model.Config
}

// NewFSMediaStore строит хранилище относительно FileSettings.Directory —
// того же корня, что использует сам Mattermost, а не захардкоженного пути.
//
// Каталоги создаются лениво, при первой записи: пустой MediaStore не должен
// оставлять следов на диске.
func NewFSMediaStore(cfg func() *model.Config) *fsMediaStore {
	return &fsMediaStore{cfg: cfg}
}

// root — корень хранилища; вычисляется при каждом обращении.
func (s *fsMediaStore) root() string {
	dir := "./data/"
	if s.cfg != nil {
		if c := s.cfg(); c != nil && c.FileSettings.Directory != nil && *c.FileSettings.Directory != "" {
			dir = *c.FileSettings.Directory
		}
	}
	return filepath.Join(dir, "photon-media")
}

// objectDir — путь к каталогу объекта, с шардированием по первым двум символам.
//
// media_id проверяется как ИДЕНТИФИКАТОР, а не как имя файла: он приходит из
// payload клиента, и без проверки «../» превратил бы Get в чтение произвольного
// файла. Допускаются только буквы, цифры, дефис и подчёркивание.
func (s *fsMediaStore) objectDir(mediaID string) (string, error) {
	if len(mediaID) < 2 || len(mediaID) > 64 {
		return "", ErrMediaInvalidID
	}
	for _, r := range mediaID {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_'
		if !ok {
			return "", ErrMediaInvalidID
		}
	}
	return filepath.Join(s.root(), strings.ToLower(mediaID[:2]), mediaID), nil
}

func (s *fsMediaStore) Get(mediaID string) (*MediaObject, error) {
	dir, err := s.objectDir(mediaID)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrMediaNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("media: read meta: %w", err)
	}
	var m MediaObject
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("media: decode meta: %w", err)
	}
	return &m, nil
}

func (s *fsMediaStore) PutMeta(m *MediaObject) error {
	dir, err := s.objectDir(m.MediaID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("media: mkdir: %w", err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("media: encode meta: %w", err)
	}
	// Запись через временный файл и переименование: частично записанный
	// meta.json выглядел бы как повреждённый объект, а не как отсутствующий.
	tmp := filepath.Join(dir, "meta.json.tmp")
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("media: write meta: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "meta.json")); err != nil {
		return fmt.Errorf("media: commit meta: %w", err)
	}
	return nil
}

// fsBlobWriter — запись во временный файл с переименованием при фиксации.
//
// Тот же приём, что у метаданных, и по той же причине: частично записанное
// тело не должно быть отличимо от отсутствующего — иначе объект выглядел бы
// целым, а его байты были бы обрезаны.
type fsBlobWriter struct {
	f    *os.File
	tmp  string
	dest string
	done bool
}

func (w *fsBlobWriter) Write(p []byte) (int, error) { return w.f.Write(p) }

func (w *fsBlobWriter) Commit() error {
	if w.done {
		return nil
	}
	w.done = true
	if err := w.f.Close(); err != nil {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("media: close blob: %w", err)
	}
	if err := os.Rename(w.tmp, w.dest); err != nil {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("media: commit blob: %w", err)
	}
	return nil
}

func (w *fsBlobWriter) Discard() error {
	if w.done {
		return nil
	}
	w.done = true
	_ = w.f.Close()
	_ = os.Remove(w.tmp)
	return nil
}

func (s *fsMediaStore) CreateBlob(mediaID string) (BlobWriter, error) {
	dir, err := s.objectDir(mediaID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("media: mkdir: %w", err)
	}
	tmp := filepath.Join(dir, "blob.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("media: create blob: %w", err)
	}
	return &fsBlobWriter{f: f, tmp: tmp, dest: filepath.Join(dir, "blob")}, nil
}

func (s *fsMediaStore) OpenBlob(mediaID string) (io.ReadCloser, error) {
	dir, err := s.objectDir(mediaID)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, "blob"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrMediaNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("media: open blob: %w", err)
	}
	return f, nil
}

func (s *fsMediaStore) DeleteBlob(mediaID string) error {
	dir, err := s.objectDir(mediaID)
	if err != nil {
		return err
	}
	// meta.json НЕ трогается — см. контракт выше. Удаляется ровно файл тела.
	if err := os.Remove(filepath.Join(dir, "blob")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("media: delete blob: %w", err)
	}
	return nil
}

func (s *fsMediaStore) DiscardObject(mediaID string) error {
	dir, err := s.objectDir(mediaID)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("media: discard object: %w", err)
	}
	return nil
}
