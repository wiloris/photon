package main

import (
	"encoding/json"
	"errors"
	"fmt"
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
// Чего здесь НЕТ и не должно появиться в первом срезе: возобновляемость,
// диапазоны, дедупликация, планировщик уборки, вытеснение, квоты, удаление по
// удалению поста. Всё это отнесено ADR-048 §14 и ADR-049 §11.

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
// Поля сверх перечисленных там не вводятся.
type MediaObject struct {
	MediaID    string     `json:"media_id"`
	PostID     string     `json:"post_id,omitempty"` // пуст до прикрепления
	ChannelID  string     `json:"channel_id"`
	UploaderID string     `json:"uploader_id"`
	SHA256     string     `json:"sha256"`
	Size       int64      `json:"size"`
	Mime       string     `json:"mime"`
	Name       string     `json:"name"`
	W          int32      `json:"w,omitempty"`
	H          int32      `json:"h,omitempty"`
	State      MediaState `json:"state"`
	CreatedAt  int64      `json:"created_at"`
	ExpiresAt  int64      `json:"expires_at"`
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

// MediaStore — контракт хранилища. Интерфейс, а не структура: ADR-049 §8
// требует, чтобы бэкенд заменялся без изменения протокола.
type MediaStore interface {
	// Get возвращает метаданные объекта или ErrMediaNotFound.
	Get(mediaID string) (*MediaObject, error)
	// PutMeta записывает метаданные объекта.
	PutMeta(m *MediaObject) error
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
