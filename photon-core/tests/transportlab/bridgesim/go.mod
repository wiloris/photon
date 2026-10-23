module github.com/wiloris/photon-core/tests/transportlab/bridgesim

// Отдельный модуль: сервер стенда S1–S4 на том же quic-go, что Bridge
// (v0.59, photon-quic-bridge/go.mod), и тем же toolchain. Ядро — v0.48;
// общий модуль у них невозможен (CLAUDE.md, «Расхождение окружений»).
go 1.24.6

toolchain go1.24.7

require github.com/quic-go/quic-go v0.59.0

require (
	golang.org/x/crypto v0.43.0 // indirect
	golang.org/x/net v0.46.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
)
