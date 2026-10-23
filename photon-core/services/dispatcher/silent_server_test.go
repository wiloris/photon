package dispatcher

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/wiloris/photon-core/ports"
)

// Сервер принял QUIC и молчит на уровне протокола Photon (ack handshake'а не
// приходит). До исправления session.handshakeCancelable Close здесь висел
// бесконечно — нарушение ADR-130 §5.1 (Close ≤ 15 с); найдено сквозным
// тестом пути подключения через ABI. Настоящий adapters/quic, штатный
// ConnectTimeout (15 с): Close обязан прервать подключение, а не ждать его.
func TestCloseWithSilentServerDoesNotHang(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "silent"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	ln, err := quicgo.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{"photon-quic-v1"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			if _, err := ln.Accept(context.Background()); err != nil {
				return
			}
		}
	}()

	d, err := New(Config{Endpoint: ports.Endpoint(ln.Addr().String()), SessionToken: "tok",
		TLS: TLSOptions{InsecureSkipVerify: true}, CallTimeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.SendAction(200, []byte(`{}`)); err == nil {
		t.Fatal("вызов к молчащему серверу удался")
	}
	start := time.Now()
	done := make(chan struct{})
	go func() { d.Close(); close(done) }()
	select {
	case <-done:
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("Close %v — ждал подключение, а не прервал его", el)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Close висит на молчащем сервере (ADR-130 §5.1: Close ≤ 15 с)")
	}
}
