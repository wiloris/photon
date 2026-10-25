# Photon

[English](README.md) · **Русский**

Photon — платформа для клиентских приложений: ядро на Go с хранилищем,
идентичностью, криптографией, транспортом QUIC, P2P и встроенной
диагностикой; единая граница наружу — небольшой C ABI; поверх неё — JVM SDK,
инструменты разработчика и среда исполнения плагинов.

> **Статус: ранний публичный релиз.** Интерфейсы ядра стабилизированы
> (C ABI 1.0), но протокол ядра пока привязан к первой серверной реализации,
> а самостоятельного сервера в репозитории нет — см. «Ограничения».

## Что есть

| компонент | каталог | что делает |
|---|---|---|
| Photon Core | `photon-core/` | порты и адаптеры: QUIC и TCP, хранилище bbolt, AES-GCM, Ed25519, OIDC, P2P (ICE + DTLS + SCTP на pion); сервисы сессии, диспетчеризации действий, журнала диагностики, локальной ленты, очереди отправки, медиа |
| C ABI | `photon-bindings/c/` | 12 функций `Photon_*`, ABI 1.0 (`photon_core.h`) |
| JNI-мост | `photon-bindings/jvm/` | Java/Kotlin поверх C ABI |
| JVM SDK | `photon-sdk/` | `dev.photon.sdk`: `PhotonClient`, конфигурация, события, локальная лента, `diagnostics()` |
| Плагины | `photon-plugin-runtime/`, `photon-plugin-sdk/` | плагины — отдельные процессы с IPC, манифестом и правами; SDK плагина без зависимостей |
| Инструменты | `photon-devtools-core/`, `photon-developer-tools/` | Inspector Live, путь подключения, журнал ядра; консольные лаборатории сети, хранилища, криптографии, плагинов |

Подробно — [`ARCHITECTURE.md`](ARCHITECTURE.md). Спецификации —
`photon-core/docs/`, `photon-bindings/docs/`, `photon-plugin-runtime/docs/`.

## Сборка и проверка

Требования: Go 1.22 (ядро, bindings, плагины), JDK 21 и Gradle (JVM-модули),
C-компилятор для cgo (`photon-bindings/c`), `protoc` — для
`photon-developer-tools`. Тестовому серверу `photon-core/tests/transportlab/bridgesim`
нужен Go ≥ 1.24 — его подставляет `GOTOOLCHAIN=auto`.

```sh
(cd photon-core && go build ./... && go test ./...)
(cd photon-bindings/c && go test .)
(cd photon-plugin-sdk && go test ./...)
(cd photon-plugin-runtime && go test -timeout 600s ./...)
(cd photon-sdk && gradle test)
(cd photon-devtools-core && gradle test)
(cd photon-developer-tools && gradle test)   # собирает .so ядра и JNI
```

## Использование

С ядром работают через C ABI (`photon-bindings/c/photon_core.h`,
`photon-bindings/c/README.md`) или из JVM через SDK
(`photon-bindings/docs/sdk-api.md`). Плагины — по
`photon-plugin-runtime/docs/plugin-development.md` и примерам
`photon-plugin-runtime/examples/`.

## Ограничения первого релиза

- **Нет самостоятельного сервера.** Сетевой протокол ядра сложился вокруг
  первой серверной реализации — плагина Mattermost, который в этот
  репозиторий не входит; в `photon.proto` есть сущности Mattermost и
  действия первого приложения на платформе. Для тестов есть только тестовый
  сервер `bridgesim`.
- **Транспорт в рабочем пути — только QUIC.** Autodial (лестница IPv6/IPv4 ×
  QUIC/TCP → маскировка) реализован, но не подключён; серверной стороны TCP
  нет; транспорта с маскировкой нет.
- **Изоляция плагинов — только Linux.**
- **Проверенная платформа — Linux x86-64.** Android, macOS и Windows — в
  планах (ниже).

## Направления развития (не реализовано)

- **Android** — упаковка ядра (AAR) и среда Android-расширений.
- **Linux, macOS** — собранные и проверенные C ABI и SDK для настольных ОС
  (сейчас проверен Linux; под macOS ядро только компилируется).
- **Photon Server** — собственный сервер протокола; предпосылка —
  протокол, нейтральный к серверу.
- **Транспорт** — Autodial в рабочем пути, TCP-сервер, маскировка.

## Участие в разработке

Перед слиянием pull request автор принимает [CLA](CLA.md) — порядок описан в
[`CONTRIBUTING.md`](CONTRIBUTING.md). Об уязвимостях сообщайте не в issues, а
по [`SECURITY.md`](SECURITY.md).

## Пути Go-модулей

| модуль | тег версии | зависит от |
|---|---|---|
| `github.com/wiloris/photon/photon-core` | `photon-core/vX.Y.Z` | — |
| `github.com/wiloris/photon/photon-plugin-sdk` | `photon-plugin-sdk/vX.Y.Z` | — (зависимостей нет вовсе) |
| `github.com/wiloris/photon/photon-plugin-runtime` | `photon-plugin-runtime/vX.Y.Z` | `photon-core`, `photon-plugin-sdk` |
| `github.com/wiloris/photon/photon-bindings/c` | `photon-bindings/c/vX.Y.Z` | `photon-core` |

Каждый модуль версионируется отдельно тегом с префиксом своего каталога:
`go get github.com/wiloris/photon/photon-core@v0.1.0` находит тег
`photon-core/v0.1.0`. В `require` между модулями записаны выпущенные версии.
Внутри репозитория модули дополнительно связаны относительными `replace`, чтобы
правка `photon-core` собиралась и проверялась вместе с модулями, которые его
используют; `replace` действует только при сборке в этом репозитории и
игнорируется, когда модуль подключён как зависимость. Следствие:
`go install <команда>@<версия>` отказывает командам из `photon-plugin-runtime`
(например, `cmd/engram-plugin-host`), потому что в их модуле есть `replace`;
подключите модуль в свой `go.mod` и соберите `go build` либо соберите из клона.

`photon-core/tests/transportlab/bridgesim` — отдельный модуль только потому,
что ему нужны более новые Go и quic-go; это тестовый сервер, версий у него нет.

Как выбирается версия, что обязан содержать выпуск и что запрещено делать с
опубликованной версией — `VERSIONING.md`.

## Лицензия

GNU Affero General Public License версии 3, только эта версия
(`AGPL-3.0-only`): более поздние версии лицензии не предлагаются.
Правообладатель — wiloris. Полный текст — [`LICENSE`](LICENSE), уведомление —
[`NOTICE`](NOTICE).

Коротко о том, что это значит (текст лицензии имеет приоритет над этим
пересказом):

- распространяя Photon или работу на его основе — в том числе приложение,
  которое включает собранное ядро (`libphoton_core.so`), SDK или SDK
  плагинов, — нужно предоставить получателям полный соответствующий
  исходный код под `AGPL-3.0-only`;
- если изменённая версия даёт пользователям работать с ней по сети, этим
  пользователям нужно предложить её исходный код (§13);
- производные работы распространяются только под `AGPL-3.0-only`; отдельных
  исключений для каких-либо продуктов нет.

Сторонние компоненты и их лицензии — [`THIRD_PARTY_LICENSES.md`](THIRD_PARTY_LICENSES.md).

Ссылки вида `ADR-NNN` в коде и документах указывают на внутренний журнал
решений проекта, который в этот релиз не входит.
