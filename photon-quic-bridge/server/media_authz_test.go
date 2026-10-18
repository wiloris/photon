package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// Этап 2 первого среза MediaService: MediaStore, objMedia, правила 700-702 и
// единственная точка принятия решения об авторизации.
//
// Кадрирования, потока и клиента здесь ещё нет — они следующими этапами.

// ── MediaStore ───────────────────────────────────────────────────────────────

func tmpStore(t *testing.T) *fsMediaStore {
	t.Helper()
	dir := t.TempDir()
	cfg := &model.Config{}
	cfg.SetDefaults()
	cfg.FileSettings.Directory = model.NewPointer(dir)
	return NewFSMediaStore(func() *model.Config { return cfg })
}

func TestMediaStoreRoundTrip(t *testing.T) {
	s := tmpStore(t)
	in := &MediaObject{
		MediaID: "abc123", ChannelID: "ch1", UploaderID: "u1",
		SHA256: strings.Repeat("a", 64), Size: 42, Mime: "image/png",
		Name: "x.png", State: MediaStored, CreatedAt: 1, ExpiresAt: 2,
	}
	require.NoError(t, s.PutMeta(in))

	got, err := s.Get("abc123")
	require.NoError(t, err)
	require.Equal(t, *in, *got)
}

func TestMediaStoreUnknownIDIsNotFound(t *testing.T) {
	s := tmpStore(t)
	_, err := s.Get("nosuchobject")
	require.ErrorIs(t, err, ErrMediaNotFound)
}

// media_id приходит из payload клиента. Без проверки идентификатора "../"
// превратил бы Get в чтение произвольного файла.
func TestMediaStoreRejectsPathTraversal(t *testing.T) {
	s := tmpStore(t)
	for _, id := range []string{"../etc", "a/b", "..", "a", "", "a/../../x", "a\\b", strings.Repeat("z", 65)} {
		_, err := s.Get(id)
		require.ErrorIsf(t, err, ErrMediaInvalidID, "id %q должен быть отвергнут как идентификатор", id)
		require.ErrorIsf(t, s.PutMeta(&MediaObject{MediaID: id}), ErrMediaInvalidID, "запись по id %q", id)
	}
}

// Частично записанный meta.json выглядел бы как повреждённый объект, а не как
// отсутствующий: запись идёт через временный файл и переименование.
func TestMediaStoreWriteIsAtomic(t *testing.T) {
	s := tmpStore(t)
	require.NoError(t, s.PutMeta(&MediaObject{MediaID: "atomic01", ChannelID: "c", State: MediaStored}))
	dir, err := s.objectDir("atomic01")
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Equal(t, []string{"meta.json"}, names, "временный файл не должен оставаться")
	require.NotContains(t, names, "meta.json.tmp")
	_ = filepath.Join
}

func TestMediaHasBytesDistinguishesLifecycle(t *testing.T) {
	for _, st := range []MediaState{MediaStored, MediaAttached, MediaAvailable} {
		require.True(t, (&MediaObject{State: st}).HasBytes(), string(st))
	}
	for _, st := range []MediaState{MediaExpired, MediaDeleted} {
		require.False(t, (&MediaObject{State: st}).HasBytes(), string(st))
	}
}

// ── objMedia ─────────────────────────────────────────────────────────────────

// Форма objMedia повторяет objFile: идентификатор → канал → право чтения.
// Проверяется той же матрицей субъектов, что и остальные правила.
func TestObjMediaResolvesChannelAndAppliesReadRight(t *testing.T) {
	z := newFixture()
	// A видит приватный канал, B — нет.
	require.NoError(t, z.authorize(uA, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "media_priv"})))
	require.Error(t, z.authorize(uB, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "media_priv"})))
	// Публичный канал доступен обоим.
	require.NoError(t, z.authorize(uA, ActionMediaDownload, pl(t, map[string]string{"media_id": "media_pub"})))
	require.NoError(t, z.authorize(uB, ActionMediaDownload, pl(t, map[string]string{"media_id": "media_pub"})))
}

// Отсутствие объекта даёт ТОТ ЖЕ отказ, что отсутствие права. Различать их
// значило бы позволить пробовать существование чужих media_id (ADR-039).
func TestObjMediaUnknownIDIsIndistinguishableFromNoRight(t *testing.T) {
	z := newFixture()
	missing := z.authorize(uB, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "nosuch"}))
	noRight := z.authorize(uB, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "media_priv"}))
	require.Error(t, missing)
	require.Error(t, noRight)
	require.Equal(t, noRight.Error(), missing.Error(),
		"тексты отказов обязаны совпадать, иначе существование media_id утекает")
}

func TestObjMediaEmptyIDDenied(t *testing.T) {
	z := newFixture()
	require.Error(t, z.authorize(uA, ActionMediaGetInfo, pl(t, map[string]string{"media_id": ""})))
}

// Хранилище может отсутствовать (push-роутер). Тогда objMedia отказывает, а не
// разыменовывает nil: отказ по умолчанию, ADR-039 §7.2.
func TestObjMediaWithoutStoreFailsClosed(t *testing.T) {
	// Хранилища нет вовсе; API берётся из той же фикстуры, чтобы отличие было
	// ровно одно — отсутствие MediaStore.
	z := newFixture()
	z.media = nil
	require.Error(t, z.authorize(uA, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "media_pub"})))
}

// Загрузка авторизуется ПО КАНАЛУ: объекта в этот момент ещё нет.
func TestMediaUploadAuthorizedByChannelNotObject(t *testing.T) {
	z := newFixture()
	require.NoError(t, z.authorize(uB, ActionMediaUpload, pl(t, map[string]string{"channel_id": cPub})))
	require.Error(t, z.authorize(uB, ActionMediaUpload, pl(t, map[string]string{"channel_id": cPriv})))
}

// ── единственная точка принятия решения ──────────────────────────────────────

// СТРУКТУРНАЯ гарантия, а не проверка поведения: authz.authorize обязан
// вызываться ровно из одного места продакшн-кода.
//
// Проверка нужна именно структурная, потому что защищает от будущего: медийный
// путь несёт двоичный хвост и не проходит через Dispatch, и соблазн вызвать
// authorize «ещё раз, у себя» возникнет ровно там. Тест поведения этого не
// поймает — обе реализации могут какое-то время совпадать, а разойтись позже.
func TestAuthorizeIsTheSingleDecisionPoint(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	callSites := map[string]int{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		require.NoError(t, err)
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(line, ".authz.authorize(") {
				callSites[name]++
			}
		}
	}

	total := 0
	for _, n := range callSites {
		total += n
	}
	require.Equalf(t, 1, total,
		"authz.authorize вызывается из %d мест (%v); решение обязано приниматься в одном — Dispatcher.Authorize",
		total, callSites)
	require.Contains(t, callSites, "dispatcher.go",
		"единственный вызов обязан находиться в Dispatcher.Authorize")
}

// Поведенческая пара к структурной: там, где Authorize отказывает, Dispatch
// отказывает тем же текстом и до обработчика не доходит.
//
// Матрица намеренно состоит только из ОТКАЗОВ: разрешённое действие уходит в
// обработчик, а он требует моков, к авторизации отношения не имеющих. Проверять
// здесь надо согласие двух путей, а не работу обработчиков.
func TestDispatchAndAuthorizeAgreeOnDenial(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	cases := []struct {
		name    string
		action  int
		payload string
	}{
		{"чужой приватный канал", ActionGetFeed, `{"channel_id":"` + cPriv + `"}`},
		{"media в чужом канале", ActionMediaGetInfo, `{"media_id":"media_priv"}`},
		{"несуществующая media", ActionMediaGetInfo, `{"media_id":"nosuch"}`},
		{"загрузка в чужой канал", ActionMediaUpload, `{"channel_id":"` + cPriv + `"}`},
		{"неизвестный action", 999, `{}`},
	}
	for _, c := range cases {
		h := HeaderFrame{Action: c.action}
		raw := json.RawMessage(c.payload)

		deny := d.Authorize(uB, h, raw)
		require.NotNilf(t, deny, "%s: Authorize обязан отказать", c.name)

		resp := d.Dispatch(uB, h, raw)
		require.Falsef(t, resp.OK, "%s: Dispatch пропустил то, что Authorize отверг", c.name)
		require.Equalf(t, deny.Error, resp.Error, "%s: разные тексты отказа", c.name)
		require.Nilf(t, resp.Data, "%s: тело не должно доходить до отвергнутого вызывающего", c.name)
	}
}

// Обратная сторона: где Authorize разрешает, он возвращает именно nil, а не
// «пустой отказ». Обработчик при этом не вызывается — здесь проверяется только
// вердикт.
func TestAuthorizeAllowsWhatFixturePermits(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	allowed := []struct {
		name    string
		action  int
		payload string
	}{
		{"публичный канал", ActionGetFeed, `{"channel_id":"` + cPub + `"}`},
		{"media в публичном канале", ActionMediaGetInfo, `{"media_id":"media_pub"}`},
		{"загрузка в публичный канал", ActionMediaUpload, `{"channel_id":"` + cPub + `"}`},
	}
	for _, c := range allowed {
		require.Nilf(t, d.Authorize(uB, HeaderFrame{Action: c.action}, json.RawMessage(c.payload)),
			"%s: Authorize должен разрешить", c.name)
	}
}

// ── MediaHandler ─────────────────────────────────────────────────────────────

func mediaHandler(t *testing.T) (*MediaHandler, *fakeMediaStore) {
	t.Helper()
	api := quietAPI()
	store := newFakeMediaStore()
	return &MediaHandler{plugin: handlersFor(api), media: NewMediaService(store)}, store
}

func TestMediaGetInfoReturnsMetadata(t *testing.T) {
	h, store := mediaHandler(t)
	store.add(&MediaObject{
		MediaID: "m1", ChannelID: "c1", SHA256: strings.Repeat("b", 64),
		Size: 7, Mime: "text/plain", Name: "a.txt", State: MediaAvailable,
	})
	resp := h.Handle("u1", ActionMediaGetInfo, json.RawMessage(`{"media_id":"m1"}`))
	require.True(t, resp.OK, resp.Error)
	info, ok := resp.Data.(MediaInfoResponse)
	require.True(t, ok)
	require.Equal(t, "m1", info.Media.MediaID)
	require.Equal(t, int64(7), info.Media.Size)
	require.Equal(t, "a.txt", info.Media.Name)
}

// unavailable отличим от forbidden: субъект уже имеет право видеть объект и
// узнаёт только, что байтов больше нет (media-transfer.md §5).
func TestMediaGetInfoReportsUnavailableForExpired(t *testing.T) {
	h, store := mediaHandler(t)
	store.add(&MediaObject{MediaID: "gone01", ChannelID: "c1", State: MediaExpired})
	resp := h.Handle("u1", ActionMediaGetInfo, json.RawMessage(`{"media_id":"gone01"}`))
	require.False(t, resp.OK)
	require.Equal(t, "unavailable", resp.Error)
	require.NotEqual(t, errForbidden.Error(), resp.Error)
}

// Загрузка и скачивание требуют потока и на обычном JSON-пути невозможны —
// это постоянный ответ, а не заглушка.
func TestMediaTransferActionsRefusedOnJSONPath(t *testing.T) {
	h, _ := mediaHandler(t)
	for _, a := range []int{ActionMediaUpload, ActionMediaDownload} {
		resp := h.Handle("u1", a, json.RawMessage(`{}`))
		require.Falsef(t, resp.OK, "action %d", a)
		require.Containsf(t, resp.Error, "media stream", "action %d: причина отказа должна называть поток", a)
	}
}

func TestMediaGetInfoRejectsEmptyID(t *testing.T) {
	h, _ := mediaHandler(t)
	resp := h.Handle("u1", ActionMediaGetInfo, json.RawMessage(`{}`))
	require.False(t, resp.OK)
	require.Contains(t, resp.Error, "invalid")
}

// Резолв канала добавляет к проверке прав одно свойство, которого сама
// политика не даёт: КАНАЛ ОБЯЗАН СУЩЕСТВОВАТЬ. Без него объект, ссылающийся на
// удалённый канал, был бы доступен субъекту с правом системного уровня —
// проверка прав по идентификатору несуществующего канала у Mattermost
// проваливается на область системы и отвечает «да».
//
// Тест добавлен по итогам мутации: подстановка канала из метаданных вместо
// резолва проходила зелёной, потому что все прежние случаи отсекались
// отсутствием права. Различающий случай — именно этот.
func TestObjMediaRequiresExistingChannel(t *testing.T) {
	f := &fakeAuthzAPI{
		channels: map[string]*model.Channel{},
		posts:    map[string]*model.Post{},
		files:    map[string]*model.FileInfo{},
		byName:   map[string]string{},
		grants:   map[string]bool{},
	}
	// Субъект с правом чтения на уровне системы: для существующего канала он
	// прошёл бы, и это нормально.
	f.grant(uA, "system", "", model.PermissionReadChannelContent)

	store := newFakeMediaStore().
		add(&MediaObject{MediaID: "ghostch1", ChannelID: "chan_deleted", State: MediaAvailable}).
		add(&MediaObject{MediaID: "nochan01", ChannelID: "", State: MediaAvailable})
	z := newAuthz(f, store)

	require.Error(t, z.authorize(uA, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "ghostch1"})),
		"объект, ссылающийся на несуществующий канал, обязан быть отвергнут")
	require.Error(t, z.authorize(uA, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "nochan01"})),
		"пустой channel_id обязан быть отвергнут — ADR-039 §5")

	// Контроль: тот же субъект и то же право на СУЩЕСТВУЮЩЕМ канале проходят.
	// Без этого тест мог бы быть зелёным просто потому, что всё запрещено.
	f.channels["chan_real"] = &model.Channel{Id: "chan_real", Type: model.ChannelTypeOpen, TeamId: team}
	store.add(&MediaObject{MediaID: "realch01", ChannelID: "chan_real", State: MediaAvailable})
	require.NoError(t, z.authorize(uA, ActionMediaGetInfo, pl(t, map[string]string{"media_id": "realch01"})),
		"контроль: на существующем канале то же право обязано пропускать")
}

// Объект вложения разговора (ADR-124 M1) пустым channel_id отрезан от пути
// 700–702: ни один субъект не получает его по 701/702, права — только 704/705.
func TestObjMediaDeniesConversationObject(t *testing.T) {
	z := newFixture()
	z.media.(*fakeMediaStore).add(&MediaObject{MediaID: "conv_media", ConversationID: "conv1", State: MediaStored})
	for _, subject := range []string{uA, uB} {
		for _, action := range []int{ActionMediaDownload, ActionMediaGetInfo} {
			require.ErrorIs(t, z.authorize(subject, action, pl(t, map[string]string{"media_id": "conv_media"})), errForbidden)
		}
	}
}
