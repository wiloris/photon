//go:build !linux

package diag

// Вне Linux грубых часов нет — время записи всегда по monotime.
const coarseAvailable = false

func coarseMono() int64       { return monotime() }
func coarseResolution() int64 { return 0 }
