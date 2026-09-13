package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// Уборка C: удаление поста делает связанную media недоступной (ADR-055,
// photon-core/docs/media-deletion.md).
//
// Каждый тест здесь заведён под названную мутацию — список в §7 спецификации.
// Проверка без мутации, на которой она падает, в срез не принималась.

// ── оснастка ─────────────────────────────────────────────────────────────────

// delSvc — сервис и хранилище с одним прикреплённым объектом, как после
// нормального createPost: состояние AVAILABLE, post_id проставлен, блоб есть.
func delSvc(t *testing.T) (*MediaService, *fakeMediaStore) {
	t.Helper()
	store := newFakeMediaStore()
	return NewMediaService(store), store
}

func attachedObject(store *fakeMediaStore, mediaID, postID string, body []byte) {
	store.add(&MediaObject{
		MediaID: mediaID, PostID: postID, ChannelID: "c1", UploaderID: "u1",
		SHA256: strings.Repeat("a", 64), Size: int64(len(body)), Mime: "text/plain",
		Name: mediaID + ".txt", State: MediaAvailable,
	})
	store.setBlob(mediaID, body)
}

// deletedPost — пост в том виде, в каком его отдаёт хук: props уже прошли
// через БД, то есть распались на []any и map[string]any.
func deletedPost(postID string, mediaIDs ...string) *model.Post {
	p := &model.Post{Id: postID, ChannelId: "c1", UserId: "u1"}
	if len(mediaIDs) == 0 {
		return p
	}
	refs := make([]any, 0, len(mediaIDs))
	for _, id := range mediaIDs {
		refs = append(refs, map[string]any{
			"media_id": id, "sha256": strings.Repeat("a", 64),
			"size": float64(3), "name": id + ".txt", "mime": "text/plain",
		})
	}
	p.AddProp("photon_media", refs)
	return p
}

// hookPlugin — плагин, собранный ровно настолько, чтобы хук дошёл до сервиса.
// QuicListener строится литералом, а не конструктором: слушателю в этом тесте
// нужно единственное поле — dispatcher.
func hookPlugin(t *testing.T, svc *MediaService) *Plugin {
	t.Helper()
	p := handlersFor(quietAPI())
	p.quicListener = &QuicListener{plugin: p, dispatcher: &Dispatcher{plugin: p, mediaService: svc}}
	return p
}

// ── 1. Обычное удаление ──────────────────────────────────────────────────────

// Мутация: «хук ничего не делает».
func TestPostDeletionMakesMediaUnavailable(t *testing.T) {
	s, store := delSvc(t)
	attachedObject(store, "m1", "post1", []byte("body"))

	res := s.OnPostDeleted("post1", []string{"m1"})
	require.Equal(t, 1, res.Deleted)

	obj, err := store.Get("m1")
	require.NoError(t, err, "метаданные обязаны пережить уборку")
	require.Equal(t, MediaDeleted, obj.State)
	require.False(t, obj.HasBytes())

	_, ok := store.blob("m1")
	require.False(t, ok, "блоб обязан быть удалён")
}

// Надгробие хранит происхождение: post_id не затирается. Мутация «очистить
// post_id заодно» проходит проверку выше и падает здесь.
func TestPostDeletionKeepsTombstoneProvenance(t *testing.T) {
	s, store := delSvc(t)
	attachedObject(store, "m1", "post1", []byte("body"))

	s.OnPostDeleted("post1", []string{"m1"})

	obj, err := store.Get("m1")
	require.NoError(t, err)
	require.Equal(t, "post1", obj.PostID)
	require.Equal(t, "c1", obj.ChannelID)
	require.Equal(t, strings.Repeat("a", 64), obj.SHA256)
}

// ── 8. meta.json не удаляется ────────────────────────────────────────────────

// Мутация: «DiscardObject вместо DeleteBlob». Она уберёт объект целиком, и
// хранилище перестанет о нём знать — а значит клиент получит forbidden вместо
// unavailable (ADR-055, «почему надгробие»).
func TestPostDeletionNeverDiscardsTheObject(t *testing.T) {
	s, store := delSvc(t)
	attachedObject(store, "m1", "post1", []byte("body"))
	require.Equal(t, 1, store.objectCount())

	s.OnPostDeleted("post1", []string{"m1"})

	require.Equal(t, 1, store.objectCount(), "объект обязан остаться в индексе")
	_, err := store.Get("m1")
	require.NoError(t, err)
	require.NotErrorIs(t, err, ErrMediaNotFound,
		"отсутствие метаданных не является способом обозначить DELETED")
}

// ── 2. Пост без media ────────────────────────────────────────────────────────

// Мутация: «разбирать props без проверки». Тихий no-op обязан не трогать
// хранилище вовсе: подавляющее большинство постов вложений не имеет.
func TestPostDeletionWithoutMediaTouchesNothing(t *testing.T) {
	s, store := delSvc(t)
	attachedObject(store, "m1", "post1", []byte("body"))
	before := store.calls()

	res := s.OnPostDeleted("post1", nil)

	require.Equal(t, PostDeletionResult{}, res)
	require.Equal(t, before, store.calls(), "хранилище не должно вызываться вовсе")
}

// Props, которых нет или которые не той формы, не роняют разбор и не выдумывают
// идентификаторов.
func TestMediaIDsFromPostHandlesAbsentAndMalformedProps(t *testing.T) {
	require.Empty(t, mediaIDsFromPost(nil))
	require.Empty(t, mediaIDsFromPost(&model.Post{Id: "p"}))

	junk := &model.Post{Id: "p"}
	junk.AddProp("photon_media", "не список")
	require.Empty(t, mediaIDsFromPost(junk))

	half := &model.Post{Id: "p"}
	half.AddProp("photon_media", []any{map[string]any{"media_id": ""}, map[string]any{"sha256": "x"}})
	require.Empty(t, mediaIDsFromPost(half), "пустые и безымянные записи не дают идентификаторов")

	other := &model.Post{Id: "p"}
	other.AddProp("some_other_plugin", []any{map[string]any{"media_id": "m1"}})
	require.Empty(t, mediaIDsFromPost(other), "чужое пространство имён props не читается")
}

// Форма props, записанная нами же, читается обратно. Писатель и читатель
// канонической формы обязаны сходиться — иначе хук молча не найдёт ничего.
func TestMediaIDsFromPostReadsBackWhatMediaPropsValueWrote(t *testing.T) {
	refs := []MediaRef{
		{MediaID: "m1", SHA256: strings.Repeat("a", 64), Size: 3, Name: "a.txt", Mime: "text/plain"},
		{MediaID: "m2", SHA256: strings.Repeat("b", 64), Size: 4, Name: "b.txt", Mime: "text/plain"},
	}
	value, err := mediaPropsValue(refs)
	require.NoError(t, err)

	post := &model.Post{Id: "post1"}
	post.AddProp("photon_media", value)
	require.Equal(t, []string{"m1", "m2"}, mediaIDsFromPost(post))

	// И та же форма, пережившая сериализацию в БД и обратно.
	raw, err := json.Marshal(post)
	require.NoError(t, err)
	var back model.Post
	require.NoError(t, json.Unmarshal(raw, &back))
	require.Equal(t, []string{"m1", "m2"}, mediaIDsFromPost(&back))
}

// ── 3. Несколько media у одного поста ────────────────────────────────────────

// Мутация: «break после первой».
func TestPostDeletionHandlesEveryAttachment(t *testing.T) {
	s, store := delSvc(t)
	for _, id := range []string{"m1", "m2", "m3"} {
		attachedObject(store, id, "post1", []byte(id))
	}

	res := s.OnPostDeleted("post1", []string{"m1", "m2", "m3"})
	require.Equal(t, 3, res.Deleted)

	for _, id := range []string{"m1", "m2", "m3"} {
		obj, err := store.Get(id)
		require.NoErrorf(t, err, "объект %s", id)
		require.Equalf(t, MediaDeleted, obj.State, "объект %s", id)
		_, ok := store.blob(id)
		require.Falsef(t, ok, "блоб %s", id)
	}
}

// ── 4. Повторное удаление ────────────────────────────────────────────────────

// Мутация: «слепой PutMeta без чтения состояния» — она перепишет состояние
// второй раз, и счётчик записей это поймает.
func TestPostDeletionIsIdempotent(t *testing.T) {
	s, store := delSvc(t)
	attachedObject(store, "m1", "post1", []byte("body"))

	first := s.OnPostDeleted("post1", []string{"m1"})
	require.Equal(t, 1, first.Deleted)
	writes := store.calls().putMeta

	second := s.OnPostDeleted("post1", []string{"m1"})
	require.Equal(t, 0, second.Deleted, "второе удаление не переводит ничего заново")
	require.Equal(t, 1, second.Converged)
	require.Equal(t, writes, store.calls().putMeta, "состояние не переписывается повторно")

	obj, err := store.Get("m1")
	require.NoError(t, err)
	require.Equal(t, MediaDeleted, obj.State)
}

// ── 5. Объект уже DELETED, блоб остался от прерванного прохода ───────────────

// Порядок «сначала состояние, потом байты» (ADR-055) допускает сбой между
// ними: объект уже DELETED, а блоб на диске. Следующий проход обязан добить
// байты, не переписывая состояние.
func TestPostDeletionConvergesAfterInterruptedRun(t *testing.T) {
	s, store := delSvc(t)
	store.add(&MediaObject{
		MediaID: "m1", PostID: "post1", ChannelID: "c1", State: MediaDeleted,
	})
	store.setBlob("m1", []byte("оставшиеся байты"))
	writes := store.calls().putMeta

	res := s.OnPostDeleted("post1", []string{"m1"})

	require.Equal(t, 1, res.Converged)
	require.Equal(t, writes, store.calls().putMeta, "состояние не переписывается")
	_, ok := store.blob("m1")
	require.False(t, ok, "блоб обязан быть добит")
}

// Мутация: «os.Remove без игнорирования отсутствия». Второй вызов по объекту
// без блоба обязан быть тихим.
func TestDeleteBlobIsIdempotent(t *testing.T) {
	store := tmpStore(t)
	require.NoError(t, store.PutMeta(&MediaObject{MediaID: "m1abc", ChannelID: "c1", State: MediaAvailable}))

	w, err := store.CreateBlob("m1abc")
	require.NoError(t, err)
	_, err = w.Write([]byte("body"))
	require.NoError(t, err)
	require.NoError(t, w.Commit())

	require.NoError(t, store.DeleteBlob("m1abc"), "первый вызов")
	require.NoError(t, store.DeleteBlob("m1abc"), "второй вызов по отсутствующему блобу")
	require.NoError(t, store.DeleteBlob("neverexisted"), "объект, которого нет вовсе")

	_, err = store.OpenBlob("m1abc")
	require.ErrorIs(t, err, ErrMediaNotFound)

	// Метаданные на месте — именно этим DeleteBlob отличается от DiscardObject.
	obj, err := store.Get("m1abc")
	require.NoError(t, err)
	require.Equal(t, "c1", obj.ChannelID)
}

// Идентификатор проверяется так же, как в остальных операциях: DeleteBlob не
// должен становиться способом удалить произвольный файл.
func TestDeleteBlobRejectsInvalidID(t *testing.T) {
	store := tmpStore(t)
	for _, id := range []string{"../etc", "a/b", "", strings.Repeat("z", 65)} {
		require.ErrorIsf(t, store.DeleteBlob(id), ErrMediaInvalidID, "id %q", id)
	}
}

// ── 9, 10. Что не трогается ──────────────────────────────────────────────────

// Мутация: «переводить всё, что названо в props». Props удаляемого поста не
// являются доказательством владения: объект может принадлежать живому посту.
func TestPostDeletionSparesObjectsOwnedByAnotherPost(t *testing.T) {
	s, store := delSvc(t)
	attachedObject(store, "mine", "post1", []byte("mine"))
	attachedObject(store, "alien", "post2", []byte("alien"))

	res := s.OnPostDeleted("post1", []string{"mine", "alien"})
	require.Equal(t, 1, res.Deleted)
	require.Equal(t, 1, res.Skipped)

	alien, err := store.Get("alien")
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, alien.State, "чужой живой объект не трогается")
	require.True(t, blobEquals(store, "alien", []byte("alien")))
}

// STORED в том виде, в каком его пишет продакшн: пустой post_id. Такой объект
// отсеивается ПРОВЕРКОЙ ВЛАДЕНИЯ, а не проверкой состояния, — и это стоит
// назвать прямо, потому что первая редакция теста измеряла не то, что
// обещала в имени.
func TestPostDeletionLeavesUnattachedUploadToCleanupA(t *testing.T) {
	s, store := delSvc(t)
	store.add(&MediaObject{MediaID: "m1", ChannelID: "c1", State: MediaStored})
	store.setBlob("m1", []byte("body"))

	res := s.OnPostDeleted("post1", []string{"m1"})
	require.Equal(t, 0, res.Deleted)
	require.Equal(t, 1, res.Skipped)

	obj, err := store.Get("m1")
	require.NoError(t, err)
	require.Equal(t, MediaStored, obj.State)
	require.True(t, blobEquals(store, "m1", []byte("body")))
}

// Мутация: «переводить любое состояние».
//
// Комбинация STORED + непустой post_id СИНТЕТИЧЕСКАЯ: продакшн её не пишет,
// потому что Attach проставляет post_id и ATTACHED одной записью. Тест всё
// равно нужен, и вот почему. Предыдущая проверка от этой мутации не защищает
// — она отсеивает объект раньше, по владению, и проходит одинаково с
// проверкой состояния и без неё. Без синтетического входа ветка «STORED —
// это уборка A» осталась бы непроверяемой, а мутация, снимающая её, —
// невидимой.
func TestPostDeletionRefusesStoredEvenWhenPostMatches(t *testing.T) {
	s, store := delSvc(t)
	store.add(&MediaObject{MediaID: "m1", PostID: "post1", ChannelID: "c1", State: MediaStored})
	store.setBlob("m1", []byte("body"))

	res := s.OnPostDeleted("post1", []string{"m1"})
	require.Equal(t, 0, res.Deleted, "STORED — брошенная загрузка, уборка A, вне этого среза")
	require.Equal(t, 1, res.Skipped)

	obj, err := store.Get("m1")
	require.NoError(t, err)
	require.Equal(t, MediaStored, obj.State)
	require.True(t, blobEquals(store, "m1", []byte("body")), "байты не трогаются")
}

// ATTACHED — не только AVAILABLE: объект, у которого не открылся блоб при
// прикреплении, остаётся в ATTACHED, и правило обязано называть оба состояния.
func TestPostDeletionCoversAttachedAsWellAsAvailable(t *testing.T) {
	s, store := delSvc(t)
	store.add(&MediaObject{MediaID: "m1", PostID: "post1", ChannelID: "c1", State: MediaAttached})
	store.setBlob("m1", []byte("body"))

	res := s.OnPostDeleted("post1", []string{"m1"})
	require.Equal(t, 1, res.Deleted)

	obj, err := store.Get("m1")
	require.NoError(t, err)
	require.Equal(t, MediaDeleted, obj.State)
}

// Объекта нет в индексе — убирать нечего, и это не ошибка.
func TestPostDeletionSkipsUnknownMedia(t *testing.T) {
	s, store := delSvc(t)
	res := s.OnPostDeleted("post1", []string{"nosuch"})
	require.Equal(t, 0, res.Deleted)
	require.Equal(t, 1, res.Skipped)
	require.Equal(t, 0, res.Failed)
	require.Equal(t, 0, store.objectCount())
}

// ── порядок операций ─────────────────────────────────────────────────────────

// brokenMeta — хранилище, у которого отказывает запись метаданных.
type brokenMeta struct {
	*fakeMediaStore
}

func (s *brokenMeta) PutMeta(*MediaObject) error { return errors.New("disk full") }

// Порядок «сначала состояние, потом байты» (ADR-055) проверяется отказом
// посередине: если PutMeta не прошёл, байты трогать НЕЛЬЗЯ — иначе объект
// остался бы AVAILABLE, заявляющим то, чего нет, и Download ответил бы
// forbidden вместо unavailable.
//
// Мутация, которую ловит именно этот тест: «удалять блоб первым» — обратный
// порядок, при котором все остальные проверки остаются зелёными.
func TestFailedStateWriteLeavesBytesAlone(t *testing.T) {
	inner := newFakeMediaStore()
	store := &brokenMeta{fakeMediaStore: inner}
	s := NewMediaService(store)
	attachedObject(inner, "m1", "post1", []byte("body"))

	res := s.OnPostDeleted("post1", []string{"m1"})
	require.Equal(t, 1, res.Failed)
	require.Equal(t, 0, res.Deleted)

	require.True(t, blobEquals(inner, "m1", []byte("body")),
		"состояние не записано — байты обязаны остаться на месте")
	require.Equal(t, 0, inner.calls().deleteBlob, "DeleteBlob не должен вызываться вовсе")

	obj, err := inner.Get("m1")
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, obj.State)
}

// ── 6. Пропущенное событие — граница среза как поведение ─────────────────────

// Хук best-effort, и подметания в этом срезе НЕТ. Тест утверждает границу:
// объект, чьё событие не пришло, остаётся доступным. Он обязан упасть, если
// кто-то тайком добавит в уборку C обход хранилища.
func TestMissedDeletionLeavesOrphanUntouchedInThisSlice(t *testing.T) {
	s, store := delSvc(t)
	attachedObject(store, "orphan", "postGone", []byte("orphan"))
	attachedObject(store, "m1", "post1", []byte("body"))

	// Событие пришло только по post1; про postGone никто не сообщил.
	s.OnPostDeleted("post1", []string{"m1"})

	orphan, err := store.Get("orphan")
	require.NoError(t, err)
	require.Equal(t, MediaAvailable, orphan.State,
		"уборка C не подметает: orphan остаётся до отдельного решения (ADR-055)")
	require.True(t, orphan.HasBytes())
	require.True(t, blobEquals(store, "orphan", []byte("orphan")))
}

// ── 7. Ответ клиенту: ровно unavailable ──────────────────────────────────────

// Мутация: «удалять meta.json вместе с блобом» — она даст forbidden, и
// сравнение точной строки это поймает. Проверка «пришла какая-то ошибка»
// зеленела бы на обеих.
func TestGetInfoAnswersUnavailableAfterPostDeletion(t *testing.T) {
	store := newFakeMediaStore()
	svc := NewMediaService(store)
	h := &MediaHandler{plugin: handlersFor(quietAPI()), media: svc}
	attachedObject(store, "m1", "post1", []byte("body"))

	// Живой объект отвечает нормально — иначе unavailable ниже ничего не
	// измерял бы: сервер, отказывающий на всё, прошёл бы проверку.
	ok := h.Handle("u1", ActionMediaGetInfo, json.RawMessage(`{"media_id":"m1"}`))
	require.True(t, ok.OK, ok.Error)

	svc.OnPostDeleted("post1", []string{"m1"})

	resp := h.Handle("u1", ActionMediaGetInfo, json.RawMessage(`{"media_id":"m1"}`))
	require.False(t, resp.OK)
	require.Equal(t, "unavailable", resp.Error)
	require.NotEqual(t, errForbidden.Error(), resp.Error,
		"forbidden означал бы, что метаданные удалены вместе с байтами")
}

// Тот же ответ на пути скачивания: Info вызывается до кадра ответа, поэтому
// клиент получает причину, а не оборванное тело.
func TestDownloadPathReportsUnavailableAfterPostDeletion(t *testing.T) {
	store := newFakeMediaStore()
	svc := NewMediaService(store)
	attachedObject(store, "m1", "post1", []byte("body"))

	svc.OnPostDeleted("post1", []string{"m1"})

	_, err := svc.Info("m1")
	require.ErrorIs(t, err, ErrMediaUnavailable)
	require.Equal(t, "unavailable", mediaErrorText(err))

	_, err = svc.Download("m1", &strings.Builder{})
	require.ErrorIs(t, err, ErrMediaUnavailable)
	require.Equal(t, "unavailable", mediaErrorText(err))
}

// ── хук ──────────────────────────────────────────────────────────────────────

// Мутация: «хук не подключён» — Mattermost просто не вызовет ничего, и
// объект останется доступным.
func TestMessageHasBeenDeletedHookRunsCleanupC(t *testing.T) {
	svc, store := delSvc(t)
	attachedObject(store, "m1", "post1", []byte("body"))
	p := hookPlugin(t, svc)

	p.MessageHasBeenDeleted(nil, deletedPost("post1", "m1"))

	obj, err := store.Get("m1")
	require.NoError(t, err)
	require.Equal(t, MediaDeleted, obj.State)
	_, ok := store.blob("m1")
	require.False(t, ok)
}

// F7 и его причина: хук не имеет права разыменовать неподнятый listener.
// Прецедент дорогой — первое же сообщение роняло процесс плагина.
func TestMessageHasBeenDeletedHookSurvivesInactiveBridge(t *testing.T) {
	p := handlersFor(quietAPI())
	require.NotPanics(t, func() {
		p.MessageHasBeenDeleted(nil, deletedPost("post1", "m1"))
	})

	p.quicListener = &QuicListener{plugin: p}
	require.NotPanics(t, func() {
		p.MessageHasBeenDeleted(nil, deletedPost("post1", "m1"))
	})

	svc, _ := delSvc(t)
	p.quicListener = &QuicListener{plugin: p, dispatcher: &Dispatcher{plugin: p, mediaService: svc}}
	require.NotPanics(t, func() { p.MessageHasBeenDeleted(nil, nil) })
}

// Пост без вложений не должен доходить до сервиса вовсе.
func TestMessageHasBeenDeletedHookIgnoresPostsWithoutMedia(t *testing.T) {
	svc, store := delSvc(t)
	attachedObject(store, "m1", "post1", []byte("body"))
	p := hookPlugin(t, svc)
	before := store.calls()

	p.MessageHasBeenDeleted(nil, deletedPost("post2"))

	require.Equal(t, before, store.calls())
}
