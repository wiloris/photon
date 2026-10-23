package dispatcher

import (
	"os"
	"testing"
)

// BenchmarkSendActionRejected — вызов с кодом, который отсекается без сети
// (тот же зонд, что у PerfLab): цена наблюдения вызова диагностикой.
// Диагностика включена при PHOTON_BENCH_DIAG=1, иначе выключена; сравнение —
// чередующимися прогонами (ADR-130 §5.6). Неудачный вызов пишет call_failed
// — это худший случай для журнала: успешный вызов записи не делает.
func BenchmarkSendActionRejected(b *testing.B) {
	d, err := New(Config{Diagnostics: DiagnosticsConfig{Disabled: os.Getenv("PHOTON_BENCH_DIAG") != "1"}})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	payload := []byte(`{}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.SendAction(424242, payload); err == nil {
			b.Fatal("неизвестный код принят")
		}
	}
}
