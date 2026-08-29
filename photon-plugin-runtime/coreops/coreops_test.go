package coreops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sort"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-core/adapters/aesgcm"
	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	ed25519adapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/ipc"
)

// Тесты идут против НАСТОЯЩИХ адаптеров (bbolt, aesgcm, ed25519), а не
// против моков: закрываемая этой фазой оговорка 🟡 Phase 11 в том и состояла,
// что персистентность была смоделирована общим in-memory store.

const testNS = "plugin-data"

func newHandler(t *testing.T) (*Handler, ports.Storage, func()) {
	t.Helper()
	h, st, _, done := newHandlerAt(t, filepath.Join(t.TempDir(), "test.db"), nil)
	return h, st, done
}

// newHandlerAt открывает хранилище по ЗАДАННОМУ пути и с заданным
// мастер-ключом. Нужен для проверки перезапуска: только так «остановить и
// запустить заново» отличается от «создать второй пустой инстанс».
func newHandlerAt(t *testing.T, path string, master ports.SymmetricKey) (*Handler, ports.Storage, ports.SymmetricKey, func()) {
	t.Helper()
	_ = path
	st, err := bboltadapter.New(path, []bboltadapter.Migration{{
		Version: 1,
		Up: func(tx *bolt.Tx) error {
			_, createErr := tx.CreateBucketIfNotExists([]byte(testNS))
			return createErr
		},
	}})
	if err != nil {
		t.Fatalf("bbolt.New: %v", err)
	}
	if err := st.Migrate(context.Background(), 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	id := ed25519adapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	cr := aesgcm.New()
	if master == nil {
		master, err = cr.GenerateKey(context.Background())
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
	}

	h, err := New(Config{
		Storage:          st,
		StorageNamespace: testNS,
		Identity:         id,
		DeviceKey:        pair.Private,
		Crypto:           cr,
		DeriveKey: func(ctx context.Context, domain string) (ports.SymmetricKey, error) {
			return cr.DeriveKey(ctx, master, domain)
		},
		Log: func(string, string, string) {},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return h, st, master, func() { st.Close(context.Background()) }
}

func call(t *testing.T, h *Handler, pluginID, op string, req any) ([]byte, error) {
	t.Helper()
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return h.Handle(context.Background(), ipc.CallerInfo{PluginID: pluginID}, op, payload)
}

func mustCall(t *testing.T, h *Handler, pluginID, op string, req any) []byte {
	t.Helper()
	out, err := call(t, h, pluginID, op, req)
	if err != nil {
		t.Fatalf("%s(%s): %v", op, pluginID, err)
	}
	return out
}

func opCode(t *testing.T, err error) ipc.ErrorCode {
	t.Helper()
	var opErr *ipc.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("ожидался *ipc.OpError, получено %T: %v", err, err)
	}
	return opErr.Code
}

func listKeys(t *testing.T, out []byte) []string {
	t.Helper()
	var resp struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("unmarshal keys: %v", err)
	}
	sort.Strings(resp.Keys)
	return resp.Keys
}

// ── storage: базовый цикл ────────────────────────────────────────────

func TestStorage_PutGetDelete(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	mustCall(t, h, "com.example.a", "storage.put",
		map[string]any{"namespace": "ns", "key": "k", "value": []byte("v")})

	out := mustCall(t, h, "com.example.a", "storage.get",
		map[string]any{"namespace": "ns", "key": "k"})
	var got struct {
		Value []byte `json:"value"`
	}
	json.Unmarshal(out, &got)
	if string(got.Value) != "v" {
		t.Errorf("value = %q, want v", got.Value)
	}

	mustCall(t, h, "com.example.a", "storage.delete",
		map[string]any{"namespace": "ns", "key": "k"})

	_, err := call(t, h, "com.example.a", "storage.get",
		map[string]any{"namespace": "ns", "key": "k"})
	if code := opCode(t, err); code != ipc.ErrNotFound {
		t.Errorf("get после delete = %s, want NOT_FOUND", code)
	}
}

// ErrNotFound — обычный исход, не сбой ядра: плагин обязан его обрабатывать,
// и отличать от «хранилище не подготовлено» (§9).
func TestStorage_MissingKeyIsNotFoundNotInternal(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	_, err := call(t, h, "com.example.a", "storage.get",
		map[string]any{"namespace": "ns", "key": "нет-такого"})
	if code := opCode(t, err); code != ipc.ErrNotFound {
		t.Fatalf("код = %s, want NOT_FOUND", code)
	}
}

// ── namespace: изоляция ──────────────────────────────────────────────

// Два плагина, у которых ОДИН ИДЕНТИФИКАТОР ЯВЛЯЕТСЯ ПРЕФИКСОМ ДРУГОГО.
// Пара выбрана намеренно: наивная фильтрация strings.HasPrefix(key,
// "plugin.com.a") пропустила бы ключи com.about (§10 п. 6a).
const (
	pluginA     = "com.example.a"
	pluginAbout = "com.example.about"
)

func TestNamespace_GetDoesNotCrossPlugins(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	mustCall(t, h, pluginA, "storage.put",
		map[string]any{"namespace": "ns", "key": "secret", "value": []byte("A")})
	mustCall(t, h, pluginAbout, "storage.put",
		map[string]any{"namespace": "ns", "key": "secret", "value": []byte("ABOUT")})

	out := mustCall(t, h, pluginA, "storage.get",
		map[string]any{"namespace": "ns", "key": "secret"})
	var got struct {
		Value []byte `json:"value"`
	}
	json.Unmarshal(out, &got)
	if string(got.Value) != "A" {
		t.Errorf("плагин %s получил %q — чужое значение", pluginA, got.Value)
	}
}

// Отдельный тест, а не следствие предыдущего: путь другой. get промахивается
// по одному ключу, list обходит множество и фильтрует, и ломается иначе.
func TestNamespace_ListDoesNotCrossPlugins(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	mustCall(t, h, pluginA, "storage.put",
		map[string]any{"namespace": "ns", "key": "own", "value": []byte("1")})
	mustCall(t, h, pluginAbout, "storage.put",
		map[string]any{"namespace": "ns", "key": "foreign", "value": []byte("2")})

	keys := listKeys(t, mustCall(t, h, pluginA, "storage.list",
		map[string]any{"namespace": "ns", "prefix": ""}))

	if len(keys) != 1 || keys[0] != "own" {
		t.Fatalf("list у %s = %v, want [own] — ключи %s не должны быть видны",
			pluginA, keys, pluginAbout)
	}
}

// Три случая префикса разом (§10 п. 6). Первый и третий проверяются здесь
// впервые: пустой префикс не должен показывать чужое, а префикс длиннее
// любого ключа не должен ни падать, ни возвращать лишнее.
func TestNamespace_ListPrefixEdgeCases(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	mustCall(t, h, pluginA, "storage.put",
		map[string]any{"namespace": "ns", "key": "alpha", "value": []byte("1")})
	mustCall(t, h, pluginA, "storage.put",
		map[string]any{"namespace": "ns", "key": "beta", "value": []byte("2")})
	mustCall(t, h, pluginAbout, "storage.put",
		map[string]any{"namespace": "ns", "key": "alpha", "value": []byte("чужое")})

	cases := []struct {
		name   string
		prefix string
		want   []string
	}{
		{"пустой префикс", "", []string{"alpha", "beta"}},
		{"префикс совпадает с началом чужого namespace", "alpha", []string{"alpha"}},
		{"префикс длиннее любого ключа", "alphabetical-and-then-some", []string{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			keys := listKeys(t, mustCall(t, h, pluginA, "storage.list",
				map[string]any{"namespace": "ns", "prefix": c.prefix}))
			if len(keys) != len(c.want) {
				t.Fatalf("keys = %v, want %v", keys, c.want)
			}
			for i := range keys {
				if keys[i] != c.want[i] {
					t.Fatalf("keys = %v, want %v", keys, c.want)
				}
			}
		})
	}
}

// Плагин, пытающийся выйти за свой namespace, получает СВОЙ ключ с таким
// именем — не отказ и не чужие данные (§5: преобразование, не проверка).
func TestNamespace_EscapeAttemptYieldsOwnKey(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	mustCall(t, h, pluginAbout, "storage.put",
		map[string]any{"namespace": "ns", "key": "secret", "value": []byte("чужое")})

	escape := "plugin." + pluginAbout + ".ns|secret"
	_, err := call(t, h, pluginA, "storage.get",
		map[string]any{"namespace": "ns", "key": escape})

	// Ожидается NOT_FOUND по СВОЕМУ ключу с таким именем: ни FORBIDDEN (это
	// была бы проверка), ни успех (это была бы утечка).
	if code := opCode(t, err); code != ipc.ErrNotFound {
		t.Fatalf("побег дал %s; NOT_FOUND означал бы преобразование, "+
			"FORBIDDEN — проверку, успех — утечку", code)
	}

	// И тот же ключ действительно свой: записав его, плагин читает своё.
	mustCall(t, h, pluginA, "storage.put",
		map[string]any{"namespace": "ns", "key": escape, "value": []byte("моё")})
	out := mustCall(t, h, pluginA, "storage.get",
		map[string]any{"namespace": "ns", "key": escape})
	var got struct {
		Value []byte `json:"value"`
	}
	json.Unmarshal(out, &got)
	if string(got.Value) != "моё" {
		t.Errorf("value = %q, want «моё»", got.Value)
	}
}

// Результат list подставляется в get БЕЗ преобразований (§5).
func TestNamespace_ListResultFeedsGetVerbatim(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	mustCall(t, h, pluginA, "storage.put",
		map[string]any{"namespace": "ns", "key": "k1", "value": []byte("v1")})

	keys := listKeys(t, mustCall(t, h, pluginA, "storage.list",
		map[string]any{"namespace": "ns", "prefix": ""}))
	if len(keys) != 1 {
		t.Fatalf("keys = %v", keys)
	}
	if bytes.Contains([]byte(keys[0]), []byte("plugin")) {
		t.Fatalf("ключ %q содержит служебный префикс — плагин узнал про механизм", keys[0])
	}

	out := mustCall(t, h, pluginA, "storage.get",
		map[string]any{"namespace": "ns", "key": keys[0]})
	var got struct {
		Value []byte `json:"value"`
	}
	json.Unmarshal(out, &got)
	if string(got.Value) != "v1" {
		t.Errorf("ключ из list не сработал в get: value = %q", got.Value)
	}
}

// ── crypto ───────────────────────────────────────────────────────────

func TestCrypto_RoundTrip(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	out := mustCall(t, h, pluginA, "crypto.encrypt",
		map[string]any{"plaintext": []byte("тайна")})
	var enc struct {
		Ciphertext []byte `json:"ciphertext"`
	}
	json.Unmarshal(out, &enc)

	out = mustCall(t, h, pluginA, "crypto.decrypt",
		map[string]any{"ciphertext": enc.Ciphertext})
	var dec struct {
		Plaintext []byte `json:"plaintext"`
	}
	json.Unmarshal(out, &dec)
	if string(dec.Plaintext) != "тайна" {
		t.Errorf("plaintext = %q", dec.Plaintext)
	}
}

func TestCrypto_PluginCannotDecryptForeign(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	out := mustCall(t, h, pluginAbout, "crypto.encrypt",
		map[string]any{"plaintext": []byte("чужое")})
	var enc struct {
		Ciphertext []byte `json:"ciphertext"`
	}
	json.Unmarshal(out, &enc)

	_, err := call(t, h, pluginA, "crypto.decrypt",
		map[string]any{"ciphertext": enc.Ciphertext})
	if code := opCode(t, err); code != ipc.ErrInvalidRequest {
		t.Fatalf("чужой шифротекст дал %s, want INVALID_REQUEST", code)
	}
}

// Расшифровка не удалась — не сбой ядра, а данные плагина. Повтор бессмыслен,
// поэтому INVALID_REQUEST, а не INTERNAL_ERROR (§9).
func TestCrypto_DecryptionFailureIsInvalidRequest(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	_, err := call(t, h, pluginA, "crypto.decrypt",
		map[string]any{"ciphertext": []byte("не шифротекст вовсе")})
	if code := opCode(t, err); code != ipc.ErrInvalidRequest {
		t.Fatalf("код = %s, want INVALID_REQUEST", code)
	}
}

// ── identity ─────────────────────────────────────────────────────────

// Подписывается доменно-разделённое представление, а не сырые data
// (ADR-037): проверка сырых даёт false, префиксованных — true.
func TestIdentity_SignUsesDomainPrefix(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	data := []byte("что угодно")
	out := mustCall(t, h, pluginA, "identity.sign", map[string]any{"data": data})
	var s struct {
		Signature []byte `json:"signature"`
	}
	json.Unmarshal(out, &s)

	out = mustCall(t, h, pluginA, "identity.public_key", struct{}{})
	var pk struct {
		PublicKey []byte `json:"public_key"`
	}
	json.Unmarshal(out, &pk)

	// Сырые data проверку НЕ проходят — иначе подпись плагина годилась бы
	// как подпись устройства под произвольным содержимым.
	if verify(t, h, pk.PublicKey, data, s.Signature) {
		t.Fatal("подпись валидна для СЫРЫХ data — доменный префикс не применён")
	}
	if !verify(t, h, pk.PublicKey, pluginSignInput(pluginA, data), s.Signature) {
		t.Fatal("подпись невалидна для префиксованного представления")
	}
}

// Подпись плагина A не проходит как подпись плагина B: plugin_id входит в
// подписываемое (ADR-037).
func TestIdentity_SignaturesAreNotInterchangeableBetweenPlugins(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	data := []byte("одни и те же данные")
	out := mustCall(t, h, pluginA, "identity.sign", map[string]any{"data": data})
	var s struct {
		Signature []byte `json:"signature"`
	}
	json.Unmarshal(out, &s)

	out = mustCall(t, h, pluginA, "identity.public_key", struct{}{})
	var pk struct {
		PublicKey []byte `json:"public_key"`
	}
	json.Unmarshal(out, &pk)

	if verify(t, h, pk.PublicKey, pluginSignInput(pluginAbout, data), s.Signature) {
		t.Fatal("подпись плагина A валидна как подпись плагина B")
	}
}

func verify(t *testing.T, h *Handler, pub, data, sig []byte) bool {
	t.Helper()
	out := mustCall(t, h, pluginA, "identity.verify", map[string]any{
		"public_key": pub, "data": data, "signature": sig,
	})
	var v struct {
		Valid bool `json:"valid"`
	}
	json.Unmarshal(out, &v)
	return v.Valid
}

// ── прочее ───────────────────────────────────────────────────────────

func TestUnknownOperationIsUnsupported(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	_, err := call(t, h, pluginA, "storage.truncate", struct{}{})
	if code := opCode(t, err); code != ipc.ErrUnsupported {
		t.Errorf("код = %s, want UNSUPPORTED", code)
	}
}

// Пустой PluginID означал бы общий namespace для всех — то есть отсутствие
// изоляции. Сессия его таким не отдаёт, но исполнитель обязан отказать сам.
func TestEmptyCallerIsRejected(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	_, err := h.Handle(context.Background(), ipc.CallerInfo{}, "storage.get", []byte(`{}`))
	if code := opCode(t, err); code != ipc.ErrInternal {
		t.Errorf("код = %s, want INTERNAL_ERROR", code)
	}
}

func TestNewRejectsIncompleteConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("пустая конфигурация принята — неполный исполнитель отказывал бы выборочно")
	}
}

// ── перезапуск ───────────────────────────────────────────────────────
//
// Эти два теста закрывают оговорку 🟡 «восстановление после сбоя» из Phase 11.
// Там персистентность была СМОДЕЛИРОВАНА: общий in-memory store переживал
// циклы Start/Stop, что доказывало лишь непомеху со стороны host/ipc/
// permissions. Здесь — настоящий bbolt, закрытый и открытый заново.

func TestRestart_StorageSurvives(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")

	h1, _, _, done1 := newHandlerAt(t, path, nil)
	mustCall(t, h1, pluginA, "storage.put",
		map[string]any{"namespace": "ns", "key": "k", "value": []byte("до перезапуска")})
	done1() // закрываем хранилище — процесс «остановлен»

	h2, _, _, done2 := newHandlerAt(t, path, nil)
	defer done2()

	out := mustCall(t, h2, pluginA, "storage.get",
		map[string]any{"namespace": "ns", "key": "k"})
	var got struct {
		Value []byte `json:"value"`
	}
	json.Unmarshal(out, &got)
	if string(got.Value) != "до перезапуска" {
		t.Fatalf("после перезапуска value = %q", got.Value)
	}
}

// Детерминизм DeriveKey проверяется СКВОЗНЫМ сценарием, а не сравнением двух
// вызовов адаптера (§6): между ними стоит Handler, собирающий домен из
// caller.PluginID, и ошибка там (домен из чего-то, меняющегося между
// запусками) свойством адаптера не ловится.
func TestRestart_PluginDecryptsItsOwnCiphertext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart-crypto.db")

	h1, _, master, done1 := newHandlerAt(t, path, nil)
	out := mustCall(t, h1, pluginA, "crypto.encrypt",
		map[string]any{"plaintext": []byte("переживи перезапуск")})
	var enc struct {
		Ciphertext []byte `json:"ciphertext"`
	}
	json.Unmarshal(out, &enc)
	done1()

	// Мастер-ключ тот же — он принадлежит устройству и переживает перезапуск
	// (§11.1: это зафиксировано независимо от того, откуда он берётся).
	h2, _, _, done2 := newHandlerAt(t, path, master)
	defer done2()

	out = mustCall(t, h2, pluginA, "crypto.decrypt",
		map[string]any{"ciphertext": enc.Ciphertext})
	var dec struct {
		Plaintext []byte `json:"plaintext"`
	}
	json.Unmarshal(out, &dec)
	if string(dec.Plaintext) != "переживи перезапуск" {
		t.Fatalf("после перезапуска plaintext = %q", dec.Plaintext)
	}
}

// Ключ хранилища собирается с ЯВНЫМИ границами полей, а не склейкой через
// разделитель. Эта пара — причина, по которой так:
//
//	плагин "com.example.a"   + namespace "b.c" -> plugin.com.example.a.b.c|k
//	плагин "com.example.a.b" + namespace "c"   -> plugin.com.example.a.b.c|k
//
// При наивной склейке это ОДИН ключ у двух РАЗНЫХ плагинов: второй перетирает
// данные первого и читает их как свои. Оба идентификатора проходят валидацию
// манифеста, и такая пара появляется у одного вендора естественно.
func TestNamespace_KeyEncodingIsUnambiguous(t *testing.T) {
	h, _, done := newHandler(t)
	defer done()

	const (
		shallow = "com.example.a"   // namespace "b.c"
		deep    = "com.example.a.b" // namespace "c"
	)

	mustCall(t, h, shallow, "storage.put",
		map[string]any{"namespace": "b.c", "key": "k", "value": []byte("мелкий")})
	mustCall(t, h, deep, "storage.put",
		map[string]any{"namespace": "c", "key": "k", "value": []byte("глубокий")})

	for _, c := range []struct{ plugin, ns, want string }{
		{shallow, "b.c", "мелкий"},
		{deep, "c", "глубокий"},
	} {
		out := mustCall(t, h, c.plugin, "storage.get",
			map[string]any{"namespace": c.ns, "key": "k"})
		var got struct {
			Value []byte `json:"value"`
		}
		json.Unmarshal(out, &got)
		if string(got.Value) != c.want {
			t.Errorf("%s/%s = %q, want %q — границы полей выведены из содержимого",
				c.plugin, c.ns, got.Value, c.want)
		}
	}
}
