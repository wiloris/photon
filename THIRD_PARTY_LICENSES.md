# Сторонние компоненты

Исходники сторонних библиотек в репозиторий **не скопированы**: их загружают
инструменты сборки Go и Gradle по версиям из `go.mod`/`go.sum` и
`build.gradle.kts`. Ниже — то, что компилируется в публикуемые модули
(`go list -deps`), и лицензии по файлам LICENSE этих версий. Все они
разрешительные (MIT, BSD-3-Clause, Apache-2.0) и совместимы с GNU AGPL
версии 3, под которой распространяется сам Photon (`AGPL-3.0-only`):
собранная работа, включающая эти компоненты, распространяется под
`AGPL-3.0-only`, а сами компоненты сохраняют свои
лицензии. При распространении собранных бинарников (`.so`, `.jar`,
исполняемые файлы) тексты лицензий этих компонентов нужно прикладывать к
бинарникам вместе с исходным кодом Photon по условиям `AGPL-3.0-only`.

## Go

| модуль | версия | лицензия | где |
|---|---|---|---|
| github.com/quic-go/quic-go | v0.48.2 | MIT | транспорт QUIC ядра |
| github.com/pion/dtls/v2, ice/v3, logging, mdns/v2, randutil, sctp, stun/v2, transport/v2, transport/v3, turn/v3 | см. `photon-core/go.mod` | MIT | P2P-адаптер (`adapters/pion`) |
| go.etcd.io/bbolt | v1.3.11 | MIT | хранилище (`adapters/bbolt`) |
| google.golang.org/protobuf | v1.34.2 | BSD-3-Clause | `photon.proto` |
| github.com/wlynxg/anet | v0.0.3 | BSD-3-Clause | перечисление интерфейсов на Android (`//go:build android`) |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause | зависимость pion |
| golang.org/x/crypto, x/net, x/sys, x/exp | см. `go.mod` | BSD-3-Clause | стандартные расширения Go |

Только тестовый QUIC-сервер `photon-core/tests/transportlab/bridgesim`
(отдельный `go.mod`): `quic-go v0.59.0` (MIT) и `golang.org/x/*` (BSD-3-Clause).

## JVM

| артефакт | версия | лицензия | где |
|---|---|---|---|
| org.jetbrains.kotlin:kotlin-stdlib | 2.0.21 | Apache-2.0 | `photon-sdk`, `photon-devtools-core`, `photon-developer-tools` |
| org.jetbrains.kotlinx:kotlinx-serialization-json | 1.7.3 | Apache-2.0 | `photon-sdk` |
| com.google.protobuf:protobuf-java | 3.21.12 | BSD-3-Clause | `photon-developer-tools` (Protobuf Inspector) |

## Сгенерированный код

`photon-core/protobuf/photon.pb.go` сгенерирован `protoc-gen-go` из
`photon.proto` этого репозитория; лицензия генератора на результат не
распространяется.
