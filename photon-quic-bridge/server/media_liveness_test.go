package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// Живость поста как условие отдачи media (ADR-056).
//
// Слепое место, ради которого всё заведено, найдено live acceptance уборки C:
// при каскадном удалении треда события по ответам не приходят, и их media
// оставалась читаемой. Здесь проверяется ответ НА ЧТЕНИИ, а не уборка:
// состояние и байты этой проверкой не меняются.

// ── оракул: разбор исходов Plugin API ────────────────────────────────────────

// fakePostGetter — ровно то, что оракул спрашивает у хоста.
type fakePostGetter struct {
	post  *model.Post
	err   *model.AppError
	calls int
}

func (f *fakePostGetter) GetPost(string) (*model.Post, *model.AppError) {
	f.calls++
	return f.post, f.err
}

type silentLog struct{ warns, debugs int }

func (l *silentLog) LogDebug(string, ...any) { l.debugs++ }
func (l *silentLog) LogWarn(string, ...any)  { l.warns++ }

func appErr(status int, id string) *model.AppError {
	return &model.AppError{Id: id, StatusCode: status, Message: id}
}

// Четыре исхода Plugin API дают три РАЗНЫХ вердикта. Мутация «любая ошибка =
// PostGone» проходит первые два случая и падает на 5xx.
func TestLivenessOracleDistinguishesGoneFromFailure(t *testing.T) {
	cases := []struct {
		name string
		post *model.Post
		err  *model.AppError
		want PostLiveness
	}{
		{"пост жив", &model.Post{Id: "p1"}, nil, PostAlive},
		{"пост удалён — 404", nil, appErr(http.StatusNotFound, "app.post.get.app_error"), PostGone},
		{"RPC мёртв — ни поста, ни ошибки", nil, nil, PostUnknown},
		{"инфраструктура — 500", nil, appErr(http.StatusInternalServerError, "app.post.get.app_error"), PostUnknown},
		{"инфраструктура — 503", nil, appErr(http.StatusServiceUnavailable, "store.sql.conn"), PostUnknown},
		{"отказ прав — 403", nil, appErr(http.StatusForbidden, "api.context.permissions.app_error"), PostUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			oracle := postLivenessOracle(&fakePostGetter{post: c.post, err: c.err}, &silentLog{})
			require.Equal(t, c.want, oracle("p1"))
		})
	}
}

// Значение по умолчанию — самое безопасное. Мутация, переставляющая
// константы так, что нулевым становится PostGone, падает здесь.
func TestPostUnknownIsTheZeroValue(t *testing.T) {
	var zero PostLiveness
	require.Equal(t, PostUnknown, zero, "забытый или неинициализированный вердикт обязан отдавать объект")
}

// Пустой post_id не повод беспокоить хост.
func TestLivenessOracleSkipsEmptyPostID(t *testing.T) {
	api := &fakePostGetter{post: &model.Post{Id: "p1"}}
	require.Equal(t, PostUnknown, postLivenessOracle(api, &silentLog{})(""))
	require.Equal(t, 0, api.calls, "GetPost не должен вызываться вовсе")
}

// ── сервис: кого гасим, кого нет ─────────────────────────────────────────────

// livenessSvc — сервис с подставным оракулом и счётчиком обращений к нему.
func livenessSvc(t *testing.T, verdict PostLiveness) (*MediaService, *fakeMediaStore, *int) {
	t.Helper()
	store := newFakeMediaStore()
	calls := 0
	s := NewMediaService(store).WithLiveness(func(string) PostLiveness {
		calls++
		return verdict
	})
	return s, store, &calls
}

// Мутация: «не спрашивать живость вовсе» — ровно поведение до ADR-056.
func TestMediaOfDeadPostIsUnavailable(t *testing.T) {
	s, store, calls := livenessSvc(t, PostGone)
	attachedObject(store, "m1", "deadpost", []byte("body"))

	_, err := s.Info("m1")
	require.ErrorIs(t, err, ErrMediaUnavailable)
	require.Equal(t, "unavailable", mediaErrorText(err),
		"forbidden означал бы, что объект неизвестен субъекту — это не так")

	var sink strings.Builder
	_, err = s.Download("m1", &sink)
	require.ErrorIs(t, err, ErrMediaUnavailable)
	require.Empty(t, sink.String(), "ни одного байта мёртвого поста наружу")
	require.Equal(t, 2, *calls, "оракул спрошен на обоих путях")
}

// Живой пост — объект отдаётся, и это не «тест ради теста»: без него мутация
// «гасить всегда» прошла бы предыдущую проверку.
func TestMediaOfLivePostIsServed(t *testing.T) {
	s, store, _ := livenessSvc(t, PostAlive)
	attachedObject(store, "m1", "livepost", []byte("body"))

	obj, err := s.Info("m1")
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, obj.State)

	var sink strings.Builder
	_, err = s.Download("m1", &sink)
	require.NoError(t, err)
	require.Equal(t, "body", sink.String())
}

// Мутация: «PostUnknown считать мёртвым». Отказ транспорта plugin API,
// упавшая база и отсутствие оракула не являются доказательством удаления —
// принцип ADR-054, применённый на сервере.
func TestUnknownLivenessServesTheObject(t *testing.T) {
	s, store, calls := livenessSvc(t, PostUnknown)
	attachedObject(store, "m1", "somepost", []byte("body"))

	obj, err := s.Info("m1")
	require.NoError(t, err, "нет сигнала — объект отдаётся")
	require.NotNil(t, obj)
	require.Equal(t, 1, *calls)

	var sink strings.Builder
	_, err = s.Download("m1", &sink)
	require.NoError(t, err)
	require.Equal(t, "body", sink.String())
}

// Сервис без оракула обязан работать как прежде: он используется там, где
// спрашивать некого.
func TestServiceWithoutOracleServesEverything(t *testing.T) {
	store := newFakeMediaStore()
	s := NewMediaService(store)
	attachedObject(store, "m1", "whatever", []byte("body"))

	_, err := s.Info("m1")
	require.NoError(t, err)
}

// Мутация: «спрашивать живость и для пустого post_id». Неприкреплённый объект
// обязан остаться читаемым — на этом держится N5 живого прогона, а сама
// брошенная загрузка обнаружима по пустому post_id и возрасту (уборка A).
func TestUnattachedMediaIsNeverSilencedByLiveness(t *testing.T) {
	s, store, calls := livenessSvc(t, PostGone)
	store.add(&MediaObject{MediaID: "m1", ChannelID: "c1", State: MediaStored,
		SHA256: strings.Repeat("a", 64), Size: 4})
	store.setBlob("m1", []byte("body"))

	obj, err := s.Info("m1")
	require.NoError(t, err, "STORED без post_id — не orphan, а брошенная загрузка")
	require.Equal(t, MediaStored, obj.State)

	var sink strings.Builder
	_, err = s.Download("m1", &sink)
	require.NoError(t, err)
	require.Equal(t, "body", sink.String())
	require.Equal(t, 0, *calls, "оракул не должен вызываться при пустом post_id")
}

// Мутация: «спрашивать живость ДО HasBytes». Ответ тот же, но объект, уже
// убранный уборкой C, заставлял бы ходить к хосту на каждом чтении.
func TestDeletedObjectDoesNotConsultTheOracle(t *testing.T) {
	s, store, calls := livenessSvc(t, PostGone)
	store.add(&MediaObject{MediaID: "m1", PostID: "p1", ChannelID: "c1", State: MediaDeleted})

	_, err := s.Info("m1")
	require.ErrorIs(t, err, ErrMediaUnavailable)
	require.Equal(t, 0, *calls, "у DELETED ответ известен без опроса хоста")
}

// Прикрепление НЕ ломается проверкой живости: resolveMedia ходит через Info,
// а у неприкреплённого объекта post_id пуст.
func TestAttachFlowSurvivesLivenessOracle(t *testing.T) {
	s, store, _ := livenessSvc(t, PostGone)
	store.add(&MediaObject{MediaID: "m1", ChannelID: "c1", UploaderID: "u1",
		State: MediaStored, SHA256: strings.Repeat("a", 64), Size: 4})
	store.setBlob("m1", []byte("body"))

	obj, err := s.Attach("m1", "newpost")
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, obj.State)
}

// ── путь до клиента: ровно unavailable ───────────────────────────────────────

// Тот же ответ, что у уборки C, и по той же причине: субъект вправе видеть
// объект, байтов ему больше не дают. forbidden сказал бы, что объекта нет.
func TestGetInfoReportsUnavailableForMediaOfDeadPost(t *testing.T) {
	store := newFakeMediaStore()
	svc := NewMediaService(store).WithLiveness(func(string) PostLiveness { return PostGone })
	h := &MediaHandler{plugin: handlersFor(quietAPI()), media: svc}
	attachedObject(store, "m1", "deadpost", []byte("body"))

	// Handler ходит в хранилище напрямую (702 отдаёт метаданные), поэтому
	// проверка живости живёт в сервисе — здесь доказывается, что путь
	// скачивания и путь метаданных отвечают ОДИНАКОВО.
	_, err := svc.Info("m1")
	require.Equal(t, "unavailable", mediaErrorText(err))

	resp := h.Handle("u1", ActionMediaGetInfo, json.RawMessage(`{"media_id":"m1"}`))
	require.False(t, resp.OK)
	require.Equal(t, "unavailable", resp.Error)
}
