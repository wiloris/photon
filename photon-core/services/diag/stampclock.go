package diag

// Часы времени записи (`at_mono_ms`) у Record — выбор при создании журнала
// (решение владельца 2026-10-23, ADR-130 §5.4).
//
// Что выбирается. Record времени не измеряет — только ставит метку с
// точностью до миллисекунды. Поэтому ей годятся грубые монотонные часы
// (CLOCK_MONOTONIC_COARSE), если их разрешение не хуже 1 мс: та же шкала, что
// у monotime, но без обращения к аппаратному таймеру. Длительности (вызовы,
// подключения, транзакции) и время их событий по-прежнему — точные monotime.
//
// Почему выбор, а не замена. Замеры (IMPLEMENTATION_LOG, контроль p99):
//   - хост с HPET: грубые — p99 ≈ 1.0–1.2 мкс, точные — 1.5–2.7 мкс, разрешение
//     грубых 1 мс → грубые выгоднее;
//   - Redmi (arm64): разрешение грубых 4 мс (HZ=250) — не годится по
//     точности, и системный вызов (p99 0.9–1.3 мкс) дороже точных через vDSO
//     (0.3–0.7 мкс) → точные;
//   - TSC: точные через vDSO — десятки наносекунд → точные.
// Слепая замена ухудшила бы две платформы из трёх.
//
// Правило: грубые — только если их разрешение ≤ 1 мс И их чтение заметно
// (≥ 1.25×) дешевле точного на этой машине; иначе точные.

const (
	stampFine   = "monotime"
	stampCoarse = "monotonic_coarse"
)

// pickCoarse — чистое правило выбора; стоимости — средние нс на чтение.
func pickCoarse(available bool, resolutionNS int64, fineNS, coarseNS float64) bool {
	return available && resolutionNS > 0 && resolutionNS <= 1_000_000 && coarseNS*1.25 <= fineNS
}

// costPerRead — средняя цена одного чтения часов, нс: минимум по трём
// пачкам по 64 чтения (минимум — устойчив к вытеснению). ≈ 0.2–1 мс на
// инстанс при дорогих часах; один раз, при создании.
func costPerRead(read func() int64) float64 {
	best := -1.0
	for b := 0; b < 3; b++ {
		t0 := monotime()
		for i := 0; i < 64; i++ {
			sinkStamp = read()
		}
		per := float64(monotime()-t0) / 64
		if best < 0 || per < best {
			best = per
		}
	}
	return best
}

var sinkStamp int64

// chooseStamp выбирает часы метки записи для этой машины.
func chooseStamp() (read func() int64, name string) {
	if !coarseAvailable {
		return monotime, stampFine
	}
	res := coarseResolution()
	if res <= 0 || res > 1_000_000 {
		return monotime, stampFine
	}
	if pickCoarse(true, res, costPerRead(monotime), costPerRead(coarseMono)) {
		return coarseMono, stampCoarse
	}
	return monotime, stampFine
}
