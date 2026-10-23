package pion

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/transport/v3/vnet"
	edadapter "github.com/wiloris/photon-core/adapters/ed25519"
)

// Кратковременная потеря пакетов — не потеря собеседника.
//
// Обработчик состояния ICE (pion.go) рвёт сессию только на Failed. Переход
// Connected → Disconnected → Connected (туннель, лифт, смена соты) бывает
// у живого собеседника постоянно; если считать Disconnected потерей, Chat
// Roulette обрывал бы разговор на каждой секундной просадке сети. Мутация
// «Failed || Disconnected» выживала все тесты (мутационная проверка среза Chat Roulette, ADR-116):
// ни один не проводил сессию через Disconnected и обратно. Здесь — проводит.

const (
	blipLoss = 6 * time.Second // PeerLossTimeout: disconnected через 2 с тишины, failed ещё через 4 с
	blipDrop = 3500 * time.Millisecond
)

// vnetPeerWithLoss — vnetPeer с явным сроком потери собеседника.
func vnetPeerWithLoss(t *testing.T, n *vnet.Net, loss time.Duration) peerFixture {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{
		Identity: id, Self: pair, EstablishTimeout: 25 * time.Second, PeerLossTimeout: loss,
		STUNTURN: []string{fmt.Sprintf("stun:%s:%d", vServer, vPort)},
		net:      n,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return peerFixture{adapter: a, pair: pair}
}

func TestBriefPacketLossDoesNotEndSession(t *testing.T) {
	topo := buildTopology(t, &vnet.NATType{
		MappingBehavior:   vnet.EndpointIndependent,
		FilteringBehavior: vnet.EndpointIndependent,
	})
	// Фильтр на WAN: пока drop, ни один пакет между LAN не проходит.
	var drop atomic.Bool
	var dropped atomic.Int64
	topo.wan.AddChunkFilter(func(vnet.Chunk) bool {
		if drop.Load() {
			dropped.Add(1)
			return false
		}
		return true
	})

	a, b := vnetPeerWithLoss(t, topo.netA, blipLoss), vnetPeerWithLoss(t, topo.netB, blipLoss)
	sA, sB := connect(t, a, b)
	if sA.Peer() == nil || sB.Peer() == nil {
		t.Fatal("сессия не установилась")
	}
	exchange(t, sA, sB)

	// Тишина дольше порога Disconnected (loss/3), но короче срока потери.
	drop.Store(true)
	time.Sleep(blipDrop)
	drop.Store(false)
	if dropped.Load() == 0 {
		t.Fatal("за время просадки не отброшено ни одного пакета — просадки не было, тест ничего не проверяет")
	}
	t.Logf("отброшено пакетов: %d за %s", dropped.Load(), blipDrop)

	// Связь вернулась: ICE снова Connected, данные идут по той же сессии.
	time.Sleep(2 * time.Second)
	exchange(t, sA, sB)
	_ = sA.Close()
	_ = sB.Close()
}
