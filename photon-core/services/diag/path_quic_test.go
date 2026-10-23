package diag

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

	quicadapter "github.com/wiloris/photon-core/adapters/quic"
	"github.com/wiloris/photon-core/ports"
)

// Путь подключения на настоящем транспорте ядра (adapters/quic, quic-go
// v0.48) против настоящего QUIC-слушателя на 127.0.0.1 и [::1]: transport —
// от адаптера, remote — адрес слушателя, family — по нему.

func quicListener(t *testing.T, addr string) string {
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
			c, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			_ = c
		}
	}()
	return ln.Addr().String()
}

func TestDialPathRealQUIC(t *testing.T) {
	inner := quicadapter.New(quicadapter.Config{TLSConfig: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"photon-quic-v1"}}})
	defer inner.Close()
	for _, c := range []struct{ listen, family string }{
		{"127.0.0.1:0", "IPv4"},
		{"[::1]:0", "IPv6"},
	} {
		d := newTestDiag(t, 64)
		addr := quicListener(t, c.listen)
		tr := WrapTransport(inner, d, ConnAction)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := tr.Connect(ctx, ports.Endpoint(addr))
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", c.listen, err)
		}
		e := lastDial(t, d)
		if fieldStr(e, KeyTransport) != "quic" || fieldStr(e, KeyFamily) != c.family ||
			fieldStr(e, KeyRemote) != addr || fieldStr(e, KeyClass) != "ok" {
			t.Fatalf("%s: dial %v (слушатель %s)", c.listen, e.Wire(), addr)
		}
		conn.Close()
	}
	// Отказ на настоящем транспорте: никто не слушает — адреса и семейства нет.
	d := newTestDiag(t, 64)
	tr := WrapTransport(inner, d, ConnAction)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := tr.Connect(ctx, "[::1]:1"); err == nil {
		t.Fatal("подключение к [::1]:1 удалось")
	}
	if e := lastDial(t, d); fieldStr(e, KeyTransport) != "quic" || fieldStr(e, KeyClass) == "ok" ||
		has(e, KeyRemote) || has(e, KeyFamily) {
		t.Fatalf("отказ: %v", e.Wire())
	}
}
