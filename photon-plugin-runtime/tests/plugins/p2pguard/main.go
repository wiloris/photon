// Command p2pguard — фикстура Plugin Test Laboratory для условия 6A ADR-076.
//
// Доказывает границу домена p2p.* на НАСТОЯЩЕМ процессе и настоящем проводе
// IPC: право, владение handle и освобождение сессий при завершении плагина.
// In-process проверка этих свойств доказывала бы другое утверждение —
// net.Pipe не порождает процесса и не имеет кода возврата (урок Phase 11).
//
// ГОВОРИТ ПО ПРОВОДУ, А НЕ ЧЕРЕЗ SDK, и это не обход ради удобства. Тот же
// приём и та же причина, что у режимов unsupported-op и plugin-id-spoof
// фикстуры permission: типизированный API SDK операций p2p.* не выражает
// (ADR-076 §7 оставил условие 5 без методов SDK), а недоверенный плагин
// пользоваться SDK не обязан вовсе. Для проверки ГРАНИЦЫ провод честнее:
// он проверяет, что отвергает runtime, а не что не умеет отправить клиент.
//
// Режим выбирается PHOTON_TEST_MODE — то же соглашение, что у hello, crash и
// permission.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/wiloris/photon-plugin-runtime/ipc"
)

const defaultPluginID = "com.engram.test.p2pguard"

// p2pOps — все шесть операций домена (ADR-076 §3.3). Порядок фиксирован,
// чтобы вывод был сравним между режимами.
var p2pOps = []string{
	"p2p.create_offer",
	"p2p.accept_offer",
	"p2p.complete",
	"p2p.peer",
	"p2p.send",
	"p2p.close",
}

func pluginID() string {
	if id := os.Getenv("PHOTON_TEST_PLUGIN_ID"); id != "" {
		return id
	}
	return defaultPluginID
}

func main() {
	rw := stdio{}
	if err := rawHandshake(rw); err != nil {
		os.Exit(1)
	}
	c := &conn{rw: rw}

	switch os.Getenv("PHOTON_TEST_MODE") {
	case "probe-all":
		probeAll(c)
	case "own-session":
		ownSession(c)
	case "steal-session":
		stealSession(c)
	default:
		os.Exit(2)
	}
	c.waitForStop()
}

// probeAll дёргает все шесть операций и докладывает КОД ОТКАЗА по каждой.
//
// Один режим на оба сценария — с правом и без — намеренно: разницу обязан
// создавать ТОЛЬКО выданный grant, а не другой код плагина. Если бы сценарии
// отличались ещё и поведением фикстуры, тест не доказывал бы, что решает
// именно право.
func probeAll(c *conn) {
	for _, op := range p2pOps {
		code := c.call(op, probePayload(op))
		c.log("p2p_probe:" + op + "=" + code)
	}
	c.log("p2p_probe:done")
}

// probePayload — минимально валидное тело для каждой операции.
//
// Тело валидно намеренно: отказ обязан приходить от ПРАВА, а не от разбора
// payload. Иначе «отвергнуто» ничего не измеряло бы — тот же дефект, что в
// Phase 13.5, где проверка «отвергнут без сети» проходила бы и на мутации.
func probePayload(op string) any {
	switch op {
	case "p2p.create_offer":
		return map[string]any{"peer_pubkey": []byte("peer-key-probe")}
	case "p2p.accept_offer":
		return map[string]any{"peer_pubkey": []byte("peer-key-probe"), "offer": []byte("OFFER")}
	case "p2p.complete":
		return map[string]any{"negotiation_id": "0000000000000000", "answer": []byte("ANSWER")}
	case "p2p.send":
		return map[string]any{"session_id": "0000000000000000", "payload": []byte("x")}
	default: // p2p.peer, p2p.close
		return map[string]any{"session_id": "0000000000000000"}
	}
}

// ownSession открывает сессию и сообщает её handle наружу.
//
// Handle уходит в журнал: тест берёт его оттуда и отдаёт ВТОРОМУ процессу —
// так проверка владения работает с настоящим, а не выдуманным идентификатором.
func ownSession(c *conn) {
	raw, code := c.callRaw("p2p.accept_offer", map[string]any{
		"peer_pubkey": []byte("peer-of-owner"),
		"offer":       []byte("OFFER"),
	})
	if code != "" {
		c.log("p2p_own:error=" + code)
		return
	}
	var resp struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		c.log("p2p_own:malformed")
		return
	}
	// Своя сессия обязана быть достижима владельцу — иначе проверка чужой
	// ничего не значит.
	c.log("p2p_own:peer=" + c.call("p2p.peer", map[string]any{"session_id": resp.SessionID}))
	c.log("p2p_own:session=" + resp.SessionID)
}

// stealSession пробует работать с чужим handle, полученным извне.
func stealSession(c *conn) {
	victim := os.Getenv("PHOTON_TEST_VICTIM_SESSION")
	if victim == "" {
		c.log("p2p_theft:no-target")
		return
	}
	for _, op := range []string{"p2p.peer", "p2p.send", "p2p.close"} {
		body := map[string]any{"session_id": victim}
		if op == "p2p.send" {
			body["payload"] = []byte("stolen")
		}
		c.log("p2p_theft:" + op + "=" + c.call(op, body))
	}
	c.log("p2p_theft:done")
}

// ── провод ──────────────────────────────────────────────────────────

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }

func rawHandshake(rw io.ReadWriter) error {
	hello, err := ipc.Decode(rw)
	if err != nil {
		return fmt.Errorf("read HELLO: %w", err)
	}
	if hello.Type != ipc.MsgHello {
		return fmt.Errorf("expected HELLO, got %s", hello.Type)
	}
	return ipc.Encode(rw, &ipc.Message{
		Type:                 ipc.MsgHelloAck,
		HelloProtocolVersion: hello.HelloProtocolVersion,
		HelloPluginID:        pluginID(),
	})
}

type conn struct {
	rw   io.ReadWriter
	next uint64
}

// call возвращает код отказа; пустая строка — успех.
func (c *conn) call(op string, body any) string {
	_, code := c.callRaw(op, body)
	return code
}

// callRaw отправляет запрос и дожидается ОТВЕТА ИМЕННО НА НЕГО.
//
// Разбор не может быть «прочитать следующее сообщение»: по тому же каналу
// идут события (MsgEvent, Runtime → Plugin), и p2p.message или p2p.closed
// вполне может прийти между запросом и ответом. Сопоставление по RequestID —
// не педантизм, а условие того, чтобы тест мерил ответ, а не событие.
func (c *conn) callRaw(op string, body any) ([]byte, string) {
	c.next++
	id := c.next
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, "MARSHAL_FAILED"
	}
	if err := ipc.Encode(c.rw, &ipc.Message{
		Type: ipc.MsgRequest, RequestID: id, Op: op, Payload: payload,
	}); err != nil {
		return nil, "WRITE_FAILED"
	}
	for {
		m, err := ipc.Decode(c.rw)
		if err != nil {
			return nil, "READ_FAILED"
		}
		if m.Type != ipc.MsgResponse || m.RequestID != id {
			continue // событие либо чужой ответ
		}
		if m.ErrorCode != "" {
			return nil, string(m.ErrorCode)
		}
		return m.Payload, ""
	}
}

// log идёт через log.write — единственную операцию без права, поэтому сам
// доклад не может провалиться так, как проверяемые операции.
func (c *conn) log(msg string) {
	c.next++
	payload, _ := json.Marshal(map[string]string{"level": "info", "message": msg})
	_ = ipc.Encode(c.rw, &ipc.Message{
		Type: ipc.MsgRequest, RequestID: c.next, Op: "log.write", Payload: payload,
	})
	for {
		m, err := ipc.Decode(c.rw)
		if err != nil || (m.Type == ipc.MsgResponse && m.RequestID == c.next) {
			return
		}
		if m.Type == ipc.MsgStop {
			_ = ipc.Encode(c.rw, &ipc.Message{Type: ipc.MsgStopAck})
			os.Exit(0)
		}
	}
}

// waitForStop повторяет то, что делает Run() в SDK на пути штатной остановки,
// чтобы host классифицировал процесс так же, как любую воспитанную фикстуру.
func (c *conn) waitForStop() {
	for {
		m, err := ipc.Decode(c.rw)
		if err != nil {
			return
		}
		if m.Type == ipc.MsgStop {
			_ = ipc.Encode(c.rw, &ipc.Message{Type: ipc.MsgStopAck})
			return
		}
	}
}
