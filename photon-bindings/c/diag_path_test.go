package main

import (
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
	"net"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

/*
 * Путь подключения через границу ABI (ADR-130 §2.3, дополнение):
 * Photon_Create с bridge.endpoint на настоящий QUIC-слушатель (127.0.0.1 и
 * [::1]) → Photon_SendAction с сетевым кодом (подключение action) →
 * Photon_SendAction(820) → поля события dial в response_json. Слушатель
 * протокола Photon не знает: вызов кончается таймаутом, но QUIC-подключение
 * устанавливается — его и описывает dial. Новых символов нет
 * (TestABISurfaceUnchanged).
 */

func pathQUICListener(t *testing.T, addr string) string {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "path"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	ln, err := quicgo.ListenAddr(addr, &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{"photon-quic-v1"},
	}, nil)
	if err != nil {
		t.Skipf("слушатель %s недоступен в этом окружении: %v", addr, err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			if _, err := ln.Accept(context.Background()); err != nil {
				return
			}
		}
	}()
	return ln.Addr().String()
}

func TestDiag_DialPathThroughABI(t *testing.T) {
	for _, c := range []struct{ listen, family string }{
		{"127.0.0.1:0", "IPv4"},
		{"[::1]:0", "IPv6"},
	} {
		addr := pathQUICListener(t, c.listen)
		cfg := fmt.Sprintf(`{"bridge":{"endpoint":%q,"session_token":"tok","connect_timeout_ms":2000,"call_timeout_ms":500,"tls":{"insecure_skip_verify":true}}}`, addr)
		h, rc := tsCreate(cstr(t, cfg), false)
		if rc != tsOK || h == nil {
			t.Fatalf("Photon_Create: rc = %d", rc)
		}
		tsSendAction(h, 200, cstr(t, `{}`), false) // подключение action; исход вызова не важен
		var dial map[string]any
		for deadline := time.Now().Add(5 * time.Second); dial == nil && time.Now().Before(deadline); {
			var ev diagEventsC
			json.Unmarshal(sendOK(t, h, actionDiagEventsC, `{"after_seq":0,"limit":256}`), &ev)
			for _, e := range ev.Events {
				if e["kind"] == "dial" && e["conn"] == "action" {
					dial = e
				}
			}
			if dial == nil {
				time.Sleep(20 * time.Millisecond)
			}
		}
		tsDestroy(h)
		if dial == nil {
			t.Fatalf("%s: события dial через 820 нет", c.listen)
		}
		f, _ := dial["fields"].(map[string]any)
		if f["class"] != "ok" || f["transport"] != "quic" || f["family"] != c.family || f["remote"] != addr {
			t.Fatalf("%s: dial через 820: %v (слушатель %s)", c.listen, dial, addr)
		}
	}
}
