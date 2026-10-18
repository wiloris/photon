package dispatcher

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/wiloris/photon-core/services/session"
)

// 815/816 — передача файла вложения через медиасоединение (ADR-124 M1;
// docs/local-actions.md). Сервер — fakeBridge с обработчиком медийных
// стримов: кадры и тело по тому же правилу границы, что у Bridge.

func hexSHA(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// mediaServer — фиктивный Bridge для 703/704: принятые тела по upload_key и
// объекты для скачивания по media_id.
type mediaServer struct {
	mu       sync.Mutex
	uploads  map[string][]byte
	payloads []map[string]any
	objects  map[string][]byte
	reject   string // непусто — 703/704 отвечают {ok:false, error: reject}
	// corrupt — 704 объявляет хэш, не совпадающий с телом; short — отдаёт
	// меньше байт, чем объявлено.
	corrupt, short bool
}

func newMediaServer() *mediaServer {
	return &mediaServer{uploads: map[string][]byte{}, objects: map[string][]byte{}}
}

func (m *mediaServer) serve(hdr map[string]any, payload json.RawMessage, body io.Reader, w io.Writer) {
	var p map[string]any
	_ = json.Unmarshal(payload, &p)
	m.mu.Lock()
	m.payloads = append(m.payloads, p)
	reject, corrupt, short := m.reject, m.corrupt, m.short
	m.mu.Unlock()
	enc := json.NewEncoder(w)
	switch hdr["action"].(float64) {
	case 703:
		n := int64(p["body_length"].(float64))
		got, _ := io.ReadAll(io.LimitReader(body, n))
		if reject != "" {
			_ = enc.Encode(map[string]any{"ok": false, "error": reject})
			return
		}
		if int64(len(got)) != n || hexSHA(got) != p["sha256"] {
			_ = enc.Encode(map[string]any{"ok": false, "error": "invalid_request"})
			return
		}
		key := p["upload_key"].(string)
		m.mu.Lock()
		m.uploads[key] = got
		m.mu.Unlock()
		_ = enc.Encode(map[string]any{"ok": true, "data": map[string]any{
			"media_id": "m-" + key, "sha256": hexSHA(got), "size": len(got)}})
	case 704:
		if reject != "" {
			_ = enc.Encode(map[string]any{"ok": false, "error": reject})
			return
		}
		id := p["media_id"].(string)
		m.mu.Lock()
		blob, ok := m.objects[id]
		m.mu.Unlock()
		if !ok {
			_ = enc.Encode(map[string]any{"ok": false, "error": "forbidden"})
			return
		}
		sum := hexSHA(blob)
		if corrupt {
			sum = hexSHA([]byte("other"))
		}
		_ = enc.Encode(map[string]any{"ok": true, "body_length": len(blob), "data": map[string]any{
			"media_id": id, "kind": "file", "mime": "application/octet-stream", "name": "f.bin",
			"size": len(blob), "sha256": sum}})
		if short {
			blob = blob[:len(blob)/2]
		}
		_, _ = w.Write(blob)
	}
}

func mediaDispatcher(t *testing.T) (*Dispatcher, *fakeBridge, *mediaServer) {
	t.Helper()
	br := newFakeBridge()
	ms := newMediaServer()
	br.mediaStream = ms.serve
	d := newTargetless(t, br, nil)
	setTarget(t, d, `{"endpoint":"bridge.example:4443","session_token":"tok-1"}`)
	return d, br, ms
}

func writeFile(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "src.bin")
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func mediaCall(t *testing.T, d *Dispatcher, action int32, req any) (map[string]any, error) {
	t.Helper()
	raw, _ := json.Marshal(req)
	out, err := d.SendAction(action, raw)
	if err != nil {
		return nil, err
	}
	var env map[string]any
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatalf("конверт не JSON: %s", out)
	}
	return env, nil
}

func uploadReq815(path, key string) map[string]any {
	return map[string]any{"conversation_id": "conv1", "upload_key": key, "path": path, "name": "src.bin", "kind": "file"}
}

func TestMedia815UploadsFileOverMediaConnection(t *testing.T) {
	d, br, ms := mediaDispatcher(t)
	data := bytes.Repeat([]byte("m1-"), 50_000)
	env, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(writeFile(t, data), "op:0"))
	if err != nil {
		t.Fatalf("815: %v", err)
	}
	if env["ok"] != true {
		t.Fatalf("815: %v", env)
	}
	got := env["data"].(map[string]any)
	if got["media_id"] != "m-op:0" || got["sha256"] != hexSHA(data) || got["size"] != float64(len(data)) {
		t.Fatalf("ответ: %v", got)
	}
	if !bytes.Equal(ms.uploads["op:0"], data) {
		t.Fatal("сервер принял не те байты")
	}
	p := ms.payloads[0]
	if p["conversation_id"] != "conv1" || p["kind"] != "file" || p["name"] != "src.bin" || p["body_length"] != float64(len(data)) {
		t.Fatalf("payload 703: %v", p)
	}
	// Медиасоединение — к цели 814 с её токеном, режим action.
	br.mu.Lock()
	c := br.conns[len(br.conns)-1]
	br.mu.Unlock()
	if c.authToken() != "tok-1" {
		t.Fatalf("токен медиасоединения %q", c.authToken())
	}
	if eps := endpointsOf(br); eps[len(eps)-1] != "bridge.example:4443" {
		t.Fatalf("endpoint %v", eps)
	}
}

func TestMedia815ServerVerdictInEnvelope(t *testing.T) {
	d, _, ms := mediaDispatcher(t)
	ms.reject = "forbidden"
	env, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(writeFile(t, []byte("x")), "k"))
	if err != nil {
		t.Fatalf("вердикт сервера — не отказ вызова: %v", err)
	}
	if env["ok"] != false || env["error"] != "forbidden" {
		t.Fatalf("конверт %v", env)
	}
}

func TestMedia815InvalidArguments(t *testing.T) {
	d, _, _ := mediaDispatcher(t)
	file := writeFile(t, []byte("x"))
	for name, req := range map[string]map[string]any{
		"нет ключа":     {"conversation_id": "c", "path": file, "name": "n", "kind": "file"},
		"вид image":     {"conversation_id": "c", "upload_key": "k", "path": file, "name": "n", "kind": "image"},
		"нет файла":     uploadReq815(filepath.Join(t.TempDir(), "missing"), "k"),
		"каталог":       uploadReq815(t.TempDir(), "k"),
		"нет разговора": {"upload_key": "k", "path": file, "name": "n", "kind": "file"},
	} {
		if _, err := mediaCall(t, d, actionLocalMediaUploadFile, req); !errors.Is(err, session.ErrInvalidPayload) || Classify(err) != ClassInvalidArgument {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestMedia815WithoutTargetNotConnected(t *testing.T) {
	br := newFakeBridge()
	d := newTargetless(t, br, nil)
	_, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(writeFile(t, []byte("x")), "k"))
	if Classify(err) != ClassNotConnected {
		t.Fatalf("без цели 814: %v (%v)", err, Classify(err))
	}
	if br.connectCount() != 0 {
		t.Fatal("без цели сети быть не должно")
	}
}

func download816(t *testing.T, d *Dispatcher, path string) (map[string]any, error) {
	return mediaCall(t, d, actionLocalMediaDownloadFile, map[string]any{"media_id": "obj1", "path": path})
}

func TestMedia816DownloadsAtomically(t *testing.T) {
	d, _, ms := mediaDispatcher(t)
	data := bytes.Repeat([]byte("dl-"), 40_000)
	ms.objects["obj1"] = data
	dst := filepath.Join(t.TempDir(), "out.bin")
	env, err := download816(t, d, dst)
	if err != nil || env["ok"] != true {
		t.Fatalf("816: %v %v", env, err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("файл не совпал: %v", err)
	}
	if _, err := os.Stat(dst + ".part"); !os.IsNotExist(err) {
		t.Fatal("временный файл остался")
	}
	info := env["data"].(map[string]any)
	if info["sha256"] != hexSHA(data) || info["mime"] != "application/octet-stream" {
		t.Fatalf("ответ %v", info)
	}
}

// Прерванная или повреждённая передача никогда не объявляется успешной и не
// оставляет файла ни по пути назначения, ни временного.
func TestMedia816FailuresLeaveNoFile(t *testing.T) {
	for name, tune := range map[string]func(*mediaServer){
		"хэш не совпал":     func(m *mediaServer) { m.corrupt = true },
		"тело короче":       func(m *mediaServer) { m.short = true },
		"вердикт forbidden": func(m *mediaServer) { m.reject = "forbidden" },
	} {
		t.Run(name, func(t *testing.T) {
			d, _, ms := mediaDispatcher(t)
			ms.objects["obj1"] = bytes.Repeat([]byte("z"), 10_000)
			tune(ms)
			dst := filepath.Join(t.TempDir(), "out.bin")
			env, err := download816(t, d, dst)
			if err == nil && env["ok"] == true {
				t.Fatal("отказ объявлен успехом")
			}
			if ms.reject != "" && (err != nil || env["error"] != ms.reject) {
				t.Fatalf("вердикт должен прийти в конверте: %v %v", env, err)
			}
			for _, p := range []string{dst, dst + ".part"} {
				if _, serr := os.Stat(p); !os.IsNotExist(serr) {
					t.Fatalf("остался файл %s", p)
				}
			}
		})
	}
}

func TestMedia816InvalidPath(t *testing.T) {
	d, _, ms := mediaDispatcher(t)
	ms.objects["obj1"] = []byte("x")
	for _, p := range []string{"relative.bin", filepath.Join(t.TempDir(), "no", "dir.bin")} {
		if _, err := download816(t, d, p); Classify(err) != ClassInvalidArgument {
			t.Errorf("%s: %v", p, err)
		}
	}
}

// Медиасоединение одно на цель и переиспользуется; смена цели — новое
// соединение; обрыв — следующий вызов подключается заново.
func TestMediaConnectionLifecycle(t *testing.T) {
	d, br, ms := mediaDispatcher(t)
	ms.objects["obj1"] = []byte("x")
	file := writeFile(t, []byte("data"))
	before := br.connectCount()
	for i := 0; i < 3; i++ {
		if env, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(file, "k"+strings.Repeat("x", i))); err != nil || env["ok"] != true {
			t.Fatalf("815 #%d: %v %v", i, env, err)
		}
	}
	if got := br.connectCount() - before; got != 1 {
		t.Fatalf("на три передачи открыто %d соединений, ожидалось 1", got)
	}

	setTarget(t, d, `{"endpoint":"other.example:4443","session_token":"tok-2"}`)
	if env, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(file, "after")); err != nil || env["ok"] != true {
		t.Fatalf("после смены цели: %v %v", env, err)
	}
	if eps := endpointsOf(br); eps[len(eps)-1] != "other.example:4443" {
		t.Fatalf("медиасоединение не к новой цели: %v", eps)
	}

	n := br.connectCount()
	br.breakLast(io.ErrClosedPipe)
	if _, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(file, "lost")); err == nil {
		t.Fatal("передача по оборванному соединению объявлена успешной")
	}
	if env, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(file, "again")); err != nil || env["ok"] != true {
		t.Fatalf("после обрыва повтор не подключился: %v %v", env, err)
	}
	if br.connectCount() != n+1 {
		t.Fatalf("после обрыва ожидалось одно новое соединение, было %d → %d", n, br.connectCount())
	}

	setTarget(t, d, `{}`)
	if _, err := mediaCall(t, d, actionLocalMediaUploadFile, uploadReq815(file, "cleared")); Classify(err) != ClassNotConnected {
		t.Fatalf("после снятия цели: %v", err)
	}
}
