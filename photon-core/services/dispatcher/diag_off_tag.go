//go:build photon_diag_off

package dispatcher

// Сборка с тегом photon_diag_off выключает диагностику у всех инстансов:
// весь набор тестов прогоняется дважды — с диагностикой и без (ADR-130 §5.6).
func init() { diagForcedOff = true }
