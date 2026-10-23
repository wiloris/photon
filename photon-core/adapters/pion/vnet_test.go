package pion

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v3/vnet"
	"github.com/pion/turn/v3"
	edadapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"
)

// Выбор пути самим ICE, без ForceRelay.
//
// CLAUDE.md держал это в «честно не проверено»: принудительный relay
// доказывает работоспособность пути, а не правильность выбора, а сети, где
// прямой путь невозможен, не было. Здесь такая сеть — виртуальная (pion
// transport/vnet): два LAN за NAT и WAN с TURN/STUN-сервером. Код ICE, DTLS,
// SCTP и handshake ADR-041 — настоящий, сеть — модель. Это доказательство
// логики выбора, не замена прогона на настоящей сети.

const (
	vGlobalA = "27.1.1.1"
	vLocalA  = "192.168.0.1"
	vGlobalB = "28.1.1.1"
	vLocalB  = "10.2.0.1"
	vServer  = "1.2.3.4"
	vPort    = 3478
	vUser    = "user"
	vPass    = "pass"
)

type vnetTopology struct {
	wan        *vnet.Router
	netA, netB *vnet.Net
	turn       *turn.Server
}

func (v *vnetTopology) close() {
	_ = v.turn.Close()
	_ = v.wan.Stop()
}

func lan(t *testing.T, wan *vnet.Router, global, local, mask string, nat *vnet.NATType) *vnet.Net {
	t.Helper()
	r, err := vnet.NewRouter(&vnet.RouterConfig{
		StaticIPs:     []string{global},
		CIDR:          local + "/" + mask,
		NATType:       nat,
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	n, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{local}})
	if err != nil {
		t.Fatalf("net: %v", err)
	}
	if err := r.AddNet(n); err != nil {
		t.Fatalf("add net: %v", err)
	}
	if err := wan.AddRouter(r); err != nil {
		t.Fatalf("add router: %v", err)
	}
	return n
}

func buildTopology(t *testing.T, nat *vnet.NATType) *vnetTopology {
	t.Helper()
	wan, err := vnet.NewRouter(&vnet.RouterConfig{CIDR: "0.0.0.0/0", LoggerFactory: logging.NewDefaultLoggerFactory()})
	if err != nil {
		t.Fatalf("wan: %v", err)
	}
	wanNet, err := vnet.NewNet(&vnet.NetConfig{StaticIP: vServer})
	if err != nil {
		t.Fatalf("wan net: %v", err)
	}
	if err := wan.AddNet(wanNet); err != nil {
		t.Fatalf("add wan net: %v", err)
	}
	netA := lan(t, wan, vGlobalA, vLocalA, "24", nat)
	netB := lan(t, wan, vGlobalB, vLocalB, "24", nat)
	if err := wan.Start(); err != nil {
		t.Fatalf("wan start: %v", err)
	}

	pc, err := wanNet.ListenPacket("udp", fmt.Sprintf("%s:%d", vServer, vPort))
	if err != nil {
		t.Fatalf("turn listen: %v", err)
	}
	srv, err := turn.NewServer(turn.ServerConfig{
		Realm: "photon.test",
		AuthHandler: func(user, realm string, _ net.Addr) ([]byte, bool) {
			if user != vUser {
				return nil, false
			}
			return turn.GenerateAuthKey(user, realm, vPass), true
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: pc,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
				RelayAddress: net.ParseIP(vServer),
				Address:      "0.0.0.0",
				Net:          wanNet,
			},
		}},
		LoggerFactory: nopLoggerFactory{},
	})
	if err != nil {
		t.Fatalf("turn server: %v", err)
	}
	v := &vnetTopology{wan: wan, netA: netA, netB: netB, turn: srv}
	t.Cleanup(v.close)
	return v
}

// vnetPeer — тот же адаптер, что в продакшне, с STUN и TURN, БЕЗ ForceRelay:
// путь выбирает ICE.
func vnetPeer(t *testing.T, n *vnet.Net) peerFixture {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{
		Identity: id, Self: pair, EstablishTimeout: 25 * time.Second,
		STUNTURN: []string{fmt.Sprintf("stun:%s:%d", vServer, vPort), fmt.Sprintf("turn:%s:%d?transport=udp", vServer, vPort)},
		TURNUser: vUser, TURNPass: vPass,
		net: n,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return peerFixture{adapter: a, pair: pair}
}

// exchange проверяет, что по выбранному пути идут данные в обе стороны.
func exchange(t *testing.T, sA, sB ports.Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stA, err := sA.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := stA.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	stB, err := sB.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stB, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("B принял %q, %v", buf, err)
	}
	if _, err := stB.Write([]byte("pong")); err != nil {
		t.Fatalf("ответ: %v", err)
	}
	if _, err := io.ReadFull(stA, buf); err != nil || string(buf) != "pong" {
		t.Fatalf("A принял %q, %v", buf, err)
	}
}

// selectedPair — выбранная пара кандидатов в форме "local->remote"
// (ports.Endpoint, который адаптер заполняет из ICE).
func selectedPair(s ports.Session) string { return string(s.RemoteEndpoint()) }

func establishOverVNet(t *testing.T, nat *vnet.NATType) (string, string) {
	t.Helper()
	topo := buildTopology(t, nat)
	a, b := vnetPeer(t, topo.netA), vnetPeer(t, topo.netB)
	sA, sB := connect(t, a, b)
	if sA.Peer() == nil || sB.Peer() == nil {
		t.Fatal("сессия не установилась")
	}
	exchange(t, sA, sB)
	pA, pB := selectedPair(sA), selectedPair(sB)
	_ = sA.Close()
	_ = sB.Close()
	return pA, pB
}

// Прямой путь возможен (NAT с независимым от адресата отображением): ICE
// обязан выбрать его, а не relay, хотя TURN настроен и relay-кандидаты
// собраны.
func TestICEPrefersDirectPathWhenPossible(t *testing.T) {
	pA, pB := establishOverVNet(t, &vnet.NATType{
		MappingBehavior:   vnet.EndpointIndependent,
		FilteringBehavior: vnet.EndpointIndependent,
	})
	t.Logf("A: %s", pA)
	t.Logf("B: %s", pB)
	for _, p := range []string{pA, pB} {
		if strings.Contains(p, "relay") {
			t.Fatalf("при возможном прямом пути выбран relay: %s", p)
		}
		if !strings.Contains(p, "srflx") && !strings.Contains(p, "prflx") {
			t.Fatalf("ожидалась пара через внешние адреса NAT (srflx/prflx): %s", p)
		}
	}
}

// Прямой путь невозможен (симметричный NAT с обеих сторон: отображение и
// фильтрация зависят от адресата). ICE обязан САМ перейти на TURN relay —
// ForceRelay выключен.
func TestICEFallsBackToTURNWhenDirectImpossible(t *testing.T) {
	pA, pB := establishOverVNet(t, &vnet.NATType{
		MappingBehavior:   vnet.EndpointAddrPortDependent,
		FilteringBehavior: vnet.EndpointAddrPortDependent,
	})
	t.Logf("A: %s", pA)
	t.Logf("B: %s", pB)
	if !strings.Contains(pA, "relay") && !strings.Contains(pB, "relay") {
		t.Fatalf("за симметричным NAT выбран не relay: A %s, B %s", pA, pB)
	}
}

// Контроль модели: за симметричным NAT БЕЗ TURN соединение невозможно. Без
// этого контроля прохождение предыдущего теста не доказывало бы, что прямой
// путь там действительно закрыт.
func TestSymmetricNATWithoutTURNCannotConnect(t *testing.T) {
	topo := buildTopology(t, &vnet.NATType{
		MappingBehavior:   vnet.EndpointAddrPortDependent,
		FilteringBehavior: vnet.EndpointAddrPortDependent,
	})
	mk := func(n *vnet.Net) peerFixture {
		id := edadapter.New()
		pair, err := id.Generate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		a, err := New(Config{
			Identity: id, Self: pair, EstablishTimeout: 8 * time.Second,
			STUNTURN: []string{fmt.Sprintf("stun:%s:%d", vServer, vPort)},
			net:      n,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = a.Close() })
		return peerFixture{adapter: a, pair: pair}
	}
	a, b := mk(topo.netA), mk(topo.netB)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	offer, err := a.adapter.CreateOffer(ctx, b.pair.Public)
	if err != nil {
		t.Fatal(err)
	}
	answer, sB, err := b.adapter.AcceptOffer(ctx, a.pair.Public, offer)
	if err != nil {
		t.Fatal(err)
	}
	sA, err := a.adapter.CompleteSession(ctx, b.pair.Public, answer)
	if err != nil {
		t.Fatal(err)
	}
	if sA.Peer() != nil || sB.Peer() != nil {
		t.Fatalf("за симметричным NAT без TURN соединение установилось: %s / %s — модель не закрывает прямой путь",
			selectedPair(sA), selectedPair(sB))
	}
}
