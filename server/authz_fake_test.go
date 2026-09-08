package main

import (
	"bytes"
	"fmt"
	"io"

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
	objs  map[string]*MediaObject
	blobs map[string][]byte
}

func newFakeMediaStore() *fakeMediaStore {
	return &fakeMediaStore{objs: map[string]*MediaObject{}, blobs: map[string][]byte{}}
}

func (s *fakeMediaStore) add(m *MediaObject) *fakeMediaStore {
	s.objs[m.MediaID] = m
	return s
}

func (s *fakeMediaStore) Get(mediaID string) (*MediaObject, error) {
	m, ok := s.objs[mediaID]
	if !ok {
		return nil, ErrMediaNotFound
	}
	return m, nil
}

func (s *fakeMediaStore) PutMeta(m *MediaObject) error {
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
	b, ok := s.blobs[mediaID]
	if !ok {
		return nil, ErrMediaNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *fakeMediaStore) DiscardObject(mediaID string) error {
	delete(s.objs, mediaID)
	delete(s.blobs, mediaID)
	return nil
}
