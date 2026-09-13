package main

import (
	"encoding/json"
	"net"
	"net/rpc"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Props пересекают НАСТОЯЩУЮ границу plugin RPC.
//
// Зачем отдельный файл и настоящий net/rpc: тесты на моках
// (media_attach_test.go) передают *model.Post из горутины в горутину — там
// сериализации нет вообще, и они остаются зелёными при любом типе внутри
// props. Живой прогон Stage 6 упал ровно на этом:
//
//	RPC call to CreatePost API failed: gob: type not registered for interface: []main.MediaRef
//
// Плагин зовёт хост через hashicorp/go-plugin поверх net/rpc с gob-кодеком
// (plugin/client_rpc_generated.go: apiRPCClient.CreatePost шлёт
// Z_CreatePostArgs{post}). Здесь воспроизведена та же пара: gob-кодек
// net/rpc поверх net.Pipe. Типы, известные хосту, регистрирует init пакета
// plugin (client_rpc.go) — он линкуется в этот бинарник, потому что Plugin
// встраивает plugin.MattermostPlugin.

// CreatePostArgs / CreatePostReturns повторяют форму Z_CreatePostArgs и
// Z_CreatePostReturns. Сами они неэкспортируемые, поэтому воспроизведены:
// значение имеет ФОРМА аргумента (указатель на модель в поле структуры),
// а не имя типа.
type CreatePostArgs struct{ A *model.Post }

type CreatePostReturns struct {
	A *model.Post
	B *model.AppError
}

// hostSide — сторона хоста: принимает пост, каким он доехал.
type hostSide struct{ got chan *model.Post }

func (h *hostSide) CreatePost(args *CreatePostArgs, returns *CreatePostReturns) error {
	h.got <- args.A
	returns.A = args.A
	return nil
}

// throughPluginRPC отправляет пост так, как его отправляет plugin API, и
// возвращает то, что РАЗОБРАЛА дальняя сторона. Ошибка кодирования — это и
// есть отказ, который в проде сжигает соединение с хостом навсегда
// (net/rpc: после сбоя WriteRequest поток десинхронизирован, input() ставит
// client.shutdown, дальше всё — ErrShutdown).
func throughPluginRPC(t *testing.T, post *model.Post) (*model.Post, error) {
	t.Helper()

	srvConn, cliConn := net.Pipe()
	host := &hostSide{got: make(chan *model.Post, 1)}

	srv := rpc.NewServer()
	require.NoError(t, srv.RegisterName("Plugin", host))
	go srv.ServeConn(srvConn)

	client := rpc.NewClient(cliConn)
	t.Cleanup(func() { _ = client.Close() })

	var returns CreatePostReturns
	if err := client.Call("Plugin.CreatePost", &CreatePostArgs{A: post}, &returns); err != nil {
		return nil, err
	}
	select {
	case got := <-host.got:
		return got, nil
	case <-time.After(5 * time.Second):
		t.Fatal("дальняя сторона не получила пост")
		return nil, nil
	}
}

// requireJSONCanonical — главный oracle этого файла.
//
// Он запрещает не только []MediaRef, но и «починку» через gob.Register:
// зарегистрированный тип уехал бы и вернулся как та же структура, и проверка
// формы это увидит. Свойство, которое здесь утверждается, сильнее
// кодируемости: дальняя сторона обязана восстановить значение, зная ТОЛЬКО
// типы, которые уже знает, — те же самые, в которых props приходят из БД.
func requireJSONCanonical(t *testing.T, path string, v any) {
	t.Helper()
	switch x := v.(type) {
	case nil, bool, string, float64:
	case []any:
		for i, e := range x {
			requireJSONCanonical(t, path+"[]", e)
			_ = i
		}
	case map[string]any:
		for k, e := range x {
			requireJSONCanonical(t, path+"."+k, e)
		}
	default:
		t.Fatalf("%s: тип %T не JSON-канонический; props обязаны состоять только из "+
			"map[string]any, []any, string, float64, bool и nil", path, v)
	}
}

// postBuiltByHandler прогоняет настоящий createPost и отдаёт пост, каким его
// собрал обработчик, — до всякой сериализации.
func postBuiltByHandler(t *testing.T, refs []MediaRef, message string) *model.Post {
	t.Helper()
	h, store, api := postHandlerWithMedia(t, "u1")
	for _, r := range refs {
		body := mediaBytes(64)
		obj := storedObject(r.MediaID, "c1", "u1", body)
		store.add(obj)
		store.setBlob(r.MediaID, body)
	}

	var captured *model.Post
	api.On("CreatePost", mock.Anything).Run(func(args mock.Arguments) {
		captured = args.Get(0).(*model.Post)
	}).Return(&model.Post{Id: "post1", ChannelId: "c1"}, nil)

	payload, err := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: message, PhotonMedia: refs,
	})
	require.NoError(t, err)
	resp := h.createPost("u1", payload)
	require.True(t, resp.OK, resp.Error)
	require.NotNil(t, captured)
	return captured
}

// Пост с вложением доезжает до хоста и разбирается там без единого
// плагин-локального типа.
func TestCreatePostPropsCrossPluginRPCBoundary(t *testing.T) {
	built := postBuiltByHandler(t, []MediaRef{{MediaID: "mediaobj1"}}, "hi")

	got, err := throughPluginRPC(t, built)
	require.NoError(t, err, "props обязаны кодироваться gob'ом на границе plugin RPC")
	require.NotNil(t, got)

	props := got.GetProps()
	requireJSONCanonical(t, "props", map[string]any(props))

	raw, ok := props["photon_media"]
	require.True(t, ok, "photon_media обязан доехать")

	list, ok := raw.([]any)
	require.Truef(t, ok, "photon_media обязан быть []any, а не %T", raw)
	require.Len(t, list, 1)

	entry, ok := list[0].(map[string]any)
	require.Truef(t, ok, "запись обязана быть map[string]any, а не %T", list[0])
	require.Equal(t, "mediaobj1", entry["media_id"])
	require.Equal(t, float64(64), entry["size"], "число после JSON-канонизации — float64")
	require.Equal(t, "p.png", entry["name"])
	require.Equal(t, "image/png", entry["mime"])

	require.Len(t, props, 1, "Photon пишет только photon_media")
}

// Мутация как тест: типизированная запись ЛОМАЕТ ту же границу.
//
// Это доказательство, что предыдущий тест имеет зубы: он не «проходит всегда»,
// а отличает канонический вид от плагин-локального типа.
func TestTypedMediaRefInPropsBreaksPluginRPC(t *testing.T) {
	post := &model.Post{UserId: "u1", ChannelId: "c1", Message: "hi"}
	post.AddProp("photon_media", []MediaRef{{MediaID: "mediaobj1", Size: 64}})

	_, err := throughPluginRPC(t, post)
	require.Error(t, err, "[]main.MediaRef внутри interface обязан ломать gob")
	require.Contains(t, err.Error(), "gob")
	require.Contains(t, err.Error(), "MediaRef")
}

// Пост БЕЗ вложения: props не появляются, граница проходится, посторонние
// ключи не трогаются.
func TestCreatePostWithoutMediaLeavesPropsUntouched(t *testing.T) {
	built := postBuiltByHandler(t, nil, "без вложений")
	require.NotContains(t, built.GetProps(), "photon_media",
		"без photon_media в запросе ключа не возникает")

	// Посторонний ключ канонического вида обязан пережить границу нетронутым:
	// mediaPropsValue не имеет к чужим ключам никакого отношения.
	built.AddProp("custom_key", map[string]any{"a": float64(1), "b": "два"})

	got, err := throughPluginRPC(t, built)
	require.NoError(t, err)
	props := got.GetProps()
	requireJSONCanonical(t, "props", map[string]any(props))
	require.Equal(t, map[string]any{"a": float64(1), "b": "два"}, props["custom_key"])
	require.NotContains(t, props, "photon_media")
}

// Неудавшийся вызов plugin API возвращает (nil, nil): сгенерированный клиент
// пишет ошибку транспорта в лог и отдаёт нули. Обработчик обязан ответить
// конвертом, а не разыменовать nil — именно это разыменование в Stage 6
// уронило горутину стрима, и клиент получил EOF вместо ответа.
func TestCreatePostHandlesFailedRPCWithoutPanic(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(64)
	store.add(storedObject("mediaobj1", "c1", "u1", body))
	store.setBlob("mediaobj1", body)

	var nilPost *model.Post
	var nilErr *model.AppError
	api.On("CreatePost", mock.Anything).Return(nilPost, nilErr)

	payload, err := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj1"}},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		resp := h.createPost("u1", payload)
		require.False(t, resp.OK, "неудавшийся вызов не может выглядеть успехом")
		require.Contains(t, resp.Error, "create post failed")
	})
}
