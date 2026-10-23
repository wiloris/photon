// bridgesim — QUIC-сервер с протоколом Photon для стенда S1–S4 ADR-130.
//
// Не Bridge: нет Mattermost, авторизации и плагина. Совпадает с Bridge там,
// где это важно для транспорта: quic-go v0.59 (поддержка смены адреса
// клиента — NAT rebinding), KeepAlivePeriod 15 с, idle-timeout по умолчанию
// (30 с), ALPN photon-quic-v1, handshake auth → mode → ack, действие —
// заголовок + payload → конверт, push — однонаправленным потоком.
//
// Управление — строками stdin: "normal" (отвечать), "hold" (принять
// действие и не отвечать), "delay <мс>" (ответить с задержкой). О каждом
// принятом действии печатает "ACTION <код>" в stdout; адрес — "LISTEN <addr>".
// Первый аргумент — адрес прослушивания (по умолчанию 127.0.0.1:0): S3-a
// поднимает новый процесс на порту убитого.
package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

var (
	mu      sync.Mutex
	mode    = "normal"
	delayMS = 0
	out     = bufio.NewWriter(os.Stdout)
	outMu   sync.Mutex
)

func say(format string, a ...any) {
	outMu.Lock()
	fmt.Fprintf(out, format+"\n", a...)
	out.Flush()
	outMu.Unlock()
}

func tlsConfig() *tls.Config {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "bridgesim"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{"photon-quic-v1"},
		MinVersion:   tls.VersionTLS13,
	}
}

func main() {
	addr := "127.0.0.1:0"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	ln, err := quic.ListenAddr(addr, tlsConfig(), &quic.Config{KeepAlivePeriod: 15 * time.Second})
	if err != nil {
		say("ERR %v", err)
		os.Exit(1)
	}
	say("LISTEN %s", ln.Addr())
	go control()
	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			return
		}
		go serveConn(conn)
	}
}

func control() {
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 0 {
			continue
		}
		mu.Lock()
		mode = f[0]
		if f[0] == "delay" && len(f) > 1 {
			delayMS, _ = strconv.Atoi(f[1])
		}
		mu.Unlock()
		say("MODE %s", sc.Text())
	}
	os.Exit(0) // stdin закрыт — стенд закончил
}

func serveConn(conn *quic.Conn) {
	first := true
	for {
		st, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		if first {
			first = false
			if handshake(st) == "subscribe" {
				if u, err := conn.OpenUniStream(); err == nil {
					json.NewEncoder(u).Encode(map[string]any{"event": "hello", "data": map[string]any{}})
					u.Close()
				}
			}
			continue
		}
		go serveAction(st)
	}
}

func handshake(st *quic.Stream) string {
	dec := json.NewDecoder(st)
	var auth, m map[string]any
	if dec.Decode(&auth) != nil || dec.Decode(&m) != nil {
		return ""
	}
	md, _ := m["mode"].(string)
	status := "ready"
	if md == "subscribe" {
		status = "subscribed"
	}
	json.NewEncoder(st).Encode(map[string]any{"ok": true, "data": map[string]any{"status": status}})
	st.Close()
	return md
}

func serveAction(st *quic.Stream) {
	dec := json.NewDecoder(st)
	var hdr map[string]any
	var payload json.RawMessage
	if dec.Decode(&hdr) != nil || dec.Decode(&payload) != nil {
		return
	}
	action, _ := hdr["action"].(float64)
	say("ACTION %d", int(action))
	mu.Lock()
	m, d := mode, delayMS
	mu.Unlock()
	switch m {
	case "hold":
		<-st.Context().Done()
		return
	case "delay":
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	json.NewEncoder(st).Encode(map[string]any{"ok": true, "data": map[string]any{"echo": action}})
	st.Close()
	// Как Bridge (quic_listener.go, ADR-050): кредит MAX_STREAMS возвращается,
	// только когда завершены ОБЕ половины потока; без CancelRead соединение
	// переживало бы ровно 100 действий.
	st.CancelRead(0)
}
