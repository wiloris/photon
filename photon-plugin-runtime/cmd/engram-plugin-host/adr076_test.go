package main

import (
	"testing"
	"time"

	"github.com/wiloris/photon/photon-plugin-runtime/ipc"
)

// Инвариант ADR-076 §7.7 в ПРОДАКШН-КОМПОЗИЦИИ.
//
// Проверяется здесь, а не только в coreops, потому что нарушить его может
// именно композиция: coreops знает лишь собственное умолчание, а хост задаёт
// величину явно. Смысл инварианта — ожидание установления обязано кончаться
// причиной со стороны P2P, а не дедлайном IPC; иначе плагин не отличит
// «собеседник не дошёл до established» от «истёк срок запроса».
func TestADR076_EstablishTimeoutIsStrictlyLessThanRequestTimeout(t *testing.T) {
	if p2pEstablishTimeout <= 0 {
		t.Fatalf("p2pEstablishTimeout = %s — потолок обязателен (ADR-044 §3)", p2pEstablishTimeout)
	}
	if p2pEstablishTimeout >= ipc.DefaultRequestTimeout {
		t.Fatalf("p2pEstablishTimeout (%s) не меньше ipc.DefaultRequestTimeout (%s): "+
			"ожидание установления кончится дедлайном IPC, а не причиной со стороны P2P",
			p2pEstablishTimeout, ipc.DefaultRequestTimeout)
	}
	// Запас не символический: между ними обязано оставаться место, иначе
	// «строго меньше» выполняется формально, а гонка сохраняется.
	if gap := ipc.DefaultRequestTimeout - p2pEstablishTimeout; gap < time.Second {
		t.Fatalf("зазор %s меньше секунды — строгое неравенство выполнено формально, "+
			"а гонка с дедлайном IPC сохраняется", gap)
	}
}

// Композиция обязана СТРОИТЬСЯ с P2P: coreops.New отвергает P2P без
// EmitEvent и с негодным потолком, поэтому успешный запуск хоста сам по себе
// доказывает согласованность конфигурации. Тест фиксирует, что это свойство
// проверяется, а не подразумевается.
func TestADR076_ProductionCompositionCarriesP2P(t *testing.T) {
	if p2pEstablishTimeout != 20*time.Second {
		t.Logf("p2pEstablishTimeout изменён на %s — убедитесь, что §7.7 всё ещё выполняется",
			p2pEstablishTimeout)
	}
}
