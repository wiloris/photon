package main

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/mattermost/mattermost/server/public/model"
)

// fakeAuthzAPI — подделка той части Plugin API, которой пользуется слой
// авторизации. Написана руками, а не через plugintest/mock: набор методов
// маленький, а поведение HasPermissionToChannel здесь воспроизводит настоящее
// (канал → команда → система, channels/app/authorization.go:290-315) — именно
// оно определяет, отказывает ли правило не-члену, и подменять его моком с
// заранее заданными ответами значило бы проверять собственные ожидания.
type fakeAuthzAPI struct {
	channels   map[string]*model.Channel
	posts      map[string]*model.Post
	files      map[string]*model.FileInfo
	byName     map[string]string // teamID+"/"+name → channelID
	grants     map[string]bool   // см. grantKey
	compliance bool
	// users — пользователи с особым способом входа (ADR-091 §2.4). Субъект
	// не из карты — обычный пользователь Mattermost: так устроены все
	// субъекты матрицы, и правилам, не спрашивающим GetUser, всё равно.
	users     map[string]*model.User
	usersFail bool // GetUser отвечает ошибкой
}

func grantKey(userID, scope, id, perm string) string {
	return fmt.Sprintf("%s|%s|%s|%s", userID, scope, id, perm)
}

func (f *fakeAuthzAPI) grant(userID, scope, id string, perms ...*model.Permission) {
	for _, p := range perms {
		f.grants[grantKey(userID, scope, id, p.Id)] = true
	}
}

func (f *fakeAuthzAPI) HasPermissionTo(userID string, p *model.Permission) bool {
	return f.grants[grantKey(userID, "system", "", p.Id)]
}

func (f *fakeAuthzAPI) HasPermissionToTeam(userID, teamID string, p *model.Permission) bool {
	if f.grants[grantKey(userID, "team", teamID, p.Id)] {
		return true
	}
	return f.HasPermissionTo(userID, p)
}

func (f *fakeAuthzAPI) HasPermissionToChannel(userID, channelID string, p *model.Permission) bool {
	if f.grants[grantKey(userID, "channel", channelID, p.Id)] {
		return true
	}
	if ch, ok := f.channels[channelID]; ok && ch.TeamId != "" {
		return f.HasPermissionToTeam(userID, ch.TeamId, p)
	}
	return f.HasPermissionTo(userID, p)
}

func (f *fakeAuthzAPI) GetChannel(id string) (*model.Channel, *model.AppError) {
	if ch, ok := f.channels[id]; ok {
		return ch, nil
	}
	return nil, model.NewAppError("GetChannel", "not_found", nil, "", 404)
}

func (f *fakeAuthzAPI) GetChannelByName(teamID, name string, _ bool) (*model.Channel, *model.AppError) {
	if id, ok := f.byName[teamID+"/"+name]; ok {
		return f.channels[id], nil
	}
	return nil, model.NewAppError("GetChannelByName", "not_found", nil, "", 404)
}

func (f *fakeAuthzAPI) GetPost(id string) (*model.Post, *model.AppError) {
	if p, ok := f.posts[id]; ok {
		return p, nil
	}
	return nil, model.NewAppError("GetPost", "not_found", nil, "", 404)
}

func (f *fakeAuthzAPI) GetFileInfo(id string) (*model.FileInfo, *model.AppError) {
	if fi, ok := f.files[id]; ok {
		return fi, nil
	}
	return nil, model.NewAppError("GetFileInfo", "not_found", nil, "", 404)
}

func (f *fakeAuthzAPI) GetUser(id string) (*model.User, *model.AppError) {
	if f.usersFail {
		return nil, model.NewAppError("GetUser", "store_error", nil, "", 500)
	}
	if u, ok := f.users[id]; ok {
		return u, nil
	}
	return &model.User{Id: id}, nil
}

func (f *fakeAuthzAPI) GetConfig() *model.Config {
	c := &model.Config{}
	c.SetDefaults()
	c.ComplianceSettings.Enable = model.NewPointer(f.compliance)
	return c
}

// fakeMediaStore — индекс media в памяти. Нужен, потому что objMedia
// резолвит канал через MediaStore, а не через API Mattermost: авторитет для
// media принадлежит хранилищу (ADR-048 §7).
type fakeMediaStore struct {
	// Мьютекс обязателен: к хранилищу обращаются ОДНОВРЕМЕННО горутина
	// обработчика стрима и сам тест. Без него -race падает на первом же
	// сквозном прогоне — так и было найдено.
	mu    sync.Mutex
	objs  map[string]*MediaObject
	blobs map[string][]byte
	// counts — счётчики обращений. Нужны уборке C (ADR-055): «состояние не
	// переписывается повторно» и «пост без media не трогает хранилище» —
	// утверждения о ВЫЗОВАХ, и по конечному состоянию они неразличимы.
	counts storeCalls
}

// storeCalls — сколько раз хранилище звали. Сравнивается целиком, поэтому
// новый метод, о котором тест не знает, тоже попадёт в сравнение.
type storeCalls struct {
	get        int
	putMeta    int
	deleteBlob int
	discard    int
}

func (s *fakeMediaStore) calls() storeCalls {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts
}

func newFakeMediaStore() *fakeMediaStore {
	return &fakeMediaStore{objs: map[string]*MediaObject{}, blobs: map[string][]byte{}}
}

func (s *fakeMediaStore) add(m *MediaObject) *fakeMediaStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objs[m.MediaID] = m
	return s
}

// snapshot — копии для проверок в тесте: читать карты напрямую нельзя.
func (s *fakeMediaStore) blob(id string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blobs[id]
	return append([]byte(nil), b...), ok
}

func (s *fakeMediaStore) objectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objs)
}

func (s *fakeMediaStore) setBlob(id string, b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blobs[id] = b
}

func (s *fakeMediaStore) dropBlob(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blobs, id)
}

func (s *fakeMediaStore) Get(mediaID string) (*MediaObject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.get++
	m, ok := s.objs[mediaID]
	if !ok {
		return nil, ErrMediaNotFound
	}
	// Копия, а не ссылка, — по той же причине, по которой копирует PutMeta,
	// и симметрично ей. Настоящий fsMediaStore декодирует meta.json заново на
	// каждый Get, поэтому правка объекта в памяти НЕ видна хранилищу, пока
	// PutMeta не прошёл. Двойник, отдающий указатель, скрывал бы ровно это:
	// сервис мутирует объект перед записью (Attach, OnPostDeleted), и при
	// отказе записи состояние выглядело бы применённым. Найдено тестом
	// TestFailedStateWriteLeavesBytesAlone.
	c := *m
	return &c, nil
}

func (s *fakeMediaStore) PutMeta(m *MediaObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.putMeta++
	// Копия, а не ссылка: сервис мутирует объект между записями (Attach), и
	// хранилище, отдающее ту же структуру, скрыло бы пропущенный PutMeta.
	c := *m
	s.objs[m.MediaID] = &c
	return nil
}

// ── байты ────────────────────────────────────────────────────────────────

type fakeBlobWriter struct {
	s       *fakeMediaStore
	id      string
	buf     bytes.Buffer
	done    bool
	commits *int
}

func (w *fakeBlobWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *fakeBlobWriter) Commit() error {
	if w.done {
		return nil
	}
	w.done = true
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if w.s.blobs == nil {
		w.s.blobs = map[string][]byte{}
	}
	w.s.blobs[w.id] = append([]byte(nil), w.buf.Bytes()...)
	return nil
}

func (w *fakeBlobWriter) Discard() error {
	w.done = true
	w.buf.Reset()
	return nil
}

func (s *fakeMediaStore) CreateBlob(mediaID string) (BlobWriter, error) {
	return &fakeBlobWriter{s: s, id: mediaID}, nil
}

func (s *fakeMediaStore) OpenBlob(mediaID string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blobs[mediaID]
	if !ok {
		return nil, ErrMediaNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *fakeMediaStore) DiscardObject(mediaID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.discard++
	delete(s.objs, mediaID)
	delete(s.blobs, mediaID)
	return nil
}

// DeleteBlob убирает ТОЛЬКО байты. Метаданные остаются надгробием — на этом
// держится различение unavailable и forbidden (ADR-055).
func (s *fakeMediaStore) DeleteBlob(mediaID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.deleteBlob++
	delete(s.blobs, mediaID)
	return nil
}

// blobEquals — сравнение тела через потокобезопасный доступ.
func blobEquals(s *fakeMediaStore, id string, want []byte) bool {
	got, ok := s.blob(id)
	return ok && bytes.Equal(got, want)
}
