# REPOSITORY.md

## Структура репозиториев Photon Ecosystem

Этот документ описывает физическое расположение кода. Если непонятно, куда положить новый файл — сначала свериться с `ARCHITECTURE.md` (что это за код по смыслу), потом найти соответствующую папку здесь.

---

## Обзор экосистемы

```
Photon Ecosystem
├── photon-core              (Go ядро, AGPL)
├── photon-sdk                (API для разработчиков приложений)
├── photon-bindings           (C/JNI/Desktop мосты)
├── photon-plugin-runtime     (исполнение плагинов)
├── photon-plugin-sdk         (SDK для разработчиков плагинов)
├── photon-developer-tools    (Photon Developer Tools, Linux Desktop; ранее photon-debug-client)
├── photon-quic-bridge        (закрытый сервер)
├── engram-desktop
├── engram-android
└── enstor
```

Каждый репозиторий — отдельная единица версионирования и релиза. Между собой они связаны через опубликованные интерфейсы (ports, protobuf-схемы), а не через прямые импорты внутренних пакетов.

**Фактическое состояние отличается от этой схемы, и это зафиксировано намеренно (ADR-051).** Сегодня `photon-core`, `photon-bindings`, `photon-plugin-runtime`, `photon-plugin-sdk` и `photon-developer-tools` — **каталоги одного репозитория** `photon`: вложенных `.git` нет, submodules нет. Отдельными репозиториями живут только `photon-quic-bridge` (своя история) и ещё не созданные `engram-*` / `enstor`. Схема выше — целевая, а не описание диска; читать её как описание файловой системы нельзя.

**Критерий выделения `photon-developer-tools` в отдельный репозиторий** (ADR-051): появление второго независимого потребителя Kotlin-обёртки — Android-клиента или ENGRAM Desktop. Тогда же решается вопрос о публикуемом артефакте, и решается один раз для обоих.

---

## photon-core/

Ядро платформы. Единственный репозиторий, где допустима бизнес-логика уровня "как устроена система".

```
photon-core/
├── ports/          — интерфейсы. Только контракты, ноль реализаций.
│   ├── storage.go
│   ├── transport.go
│   ├── identity.go
│   ├── crypto.go
│   ├── media.go
│   ├── p2p.go
│   ├── plugin.go
│   └── notification.go
│
├── adapters/       — реализации портов
│   ├── bbolt/          (Storage)
│   ├── ed25519/        (Identity)
│   ├── quic/            (Transport)
│   ├── tcp/              (Transport, резерв)
│   └── pion/             (P2P: ICE/STUN/TURN)

Последняя ступень цепочки autodial — транспорт с маскировкой (ARCHITECTURE.md,
раздел 5) — каталога здесь не имеет: адаптера нет, и это роль, а не
запланированный продукт. Появится — встанет рядом с `quic/` и `tcp/`.
│
├── services/       — бизнес-логика, оркестрирующая порты/адаптеры
│   ├── autodial/        (выбор маршрута подключения)
│   └── dispatcher/      (маршрутизация action code → handler → response)
│
├── protobuf/       — Photon Protocol: схема протокола и сгенерированный код.
│   │                 Общий контракт для Core и всех потребителей (включая
│   │                 photon-quic-bridge) — см. ARCHITECTURE.md раздел 7.6.
│   │                 Bridge импортирует эти определения, а не объявляет
│   │                 свои собственные action-коды.
│   ├── photon.proto
│   └── photon.pb.go
│
├── security/       — см. ADR-029, ADR-030: большая часть ответственности
│                     оказалась уже покрыта другими пакетами, каталог
│                     сохраняется под будущее содержимое уровня ядра
│   ├── permissions/      ⚠ НЕ реализация Authorizer — enforcement принадлежит
│   │                       photon-plugin-runtime; в ядре только контракт
│   │                       ports.Authorizer (ADR-030)
│   ├── signatures/       ⚠ НЕ создаётся — ответственность Identity
│   │                       (ports/identity.go + adapters/ed25519), ADR-029
│   └── trust/            ⚠ НЕ создаётся — Device Trust Model (ADR-021)
│                           реализована в services/account, ADR-029
│
│   sandbox/ УДАЛЁН из структуры photon-core (ADR-030): изолирует процесс
│   плагина, а плагин по ADR-005 не живёт внутри ядра. Место sandbox —
│   photon-plugin-runtime/sandbox/
│
├── tests/          — интеграционные и e2e тесты ядра
│   └── plugins/          (тестовые плагины: hello, crash, permission, storage, stress)
│
└── docs/           — документация ядра (помимо корневых .md файлов)
```

**Правило:** файл никогда не попадает в `services/`, если его можно выразить как адаптер к существующему порту. Новый порт заводится только тогда, когда существующие семь (`storage`, `transport`, `identity`, `crypto`, `media`, `p2p`, `plugin`, `notification`) действительно не покрывают потребность — и это архитектурное решение, фиксируемое в `ARCHITECTURE.md`.

---

## photon-bindings/

```
photon-bindings/
├── c/          — C ABI поверх ядра (cgo, c-shared). Единственная публичная граница.
│   └── photon_core.h   — официальный ABI-контракт, см. ARCHITECTURE.md раздел 7
└── jvm/        — общий JVM-мост (JNI + Kotlin): Desktop и будущий Android
```

Отдельного `desktop/` нет и не заводится (**ADR-051**): маршалинг у платформ
один и тот же — тот же `photon_jni.c` собирается и `cc`, и NDK. Каталог
назывался `android/` до ADR-051. Платформенные различия — например, хранение
приватного ключа средствами ОС (ADR-021) — получат свой подкаталог тогда,
когда для них появится код, а не заранее.

Здесь не должно быть бизнес-логики — только маршалинг данных между Go-ядром и клиентским языком/платформой.

`photon_core.h` — не пример, а официальный ABI, вокруг которого строится `jvm/`. Любое изменение сигнатуры в этом файле — breaking change ABI и требует увеличения `PHOTON_ABI_VERSION` (см. ARCHITECTURE.md, раздел 7.5).

---

## photon-plugin-runtime/

**Ещё не создан.** По ADR-030 сначала спецификации (threat model → lifecycle →
manifest → IPC), затем скелет репозитория — тот же порядок, что сработал с
`adapters/tcp` (`WIRE_FORMAT.md` до кода).

```
photon-plugin-runtime/
├── manifest/    — парсинг и валидация манифеста плагина
├── permissions/ — реализация ports.Authorizer, политика прав
├── sandbox/     — изоляция процесса плагина (перенесено из photon-core, ADR-030)
├── ipc/         — протокол ядро ↔ host
├── host/        — процесс исполнения плагина
├── supervisor/  — запуск, обработка падения, перезапуск
└── lifecycle/   — discover → validate → authorize → start → running → stop
```

Плагин физически никогда не находится в этом репозитории — здесь только
инфраструктура для его исполнения.

**Runtime ≠ SDK** (ADR-030). Разделение принципиально, потому что определяет, кто
кому доверяет:

```
Plugin SDK          ← библиотека для автора плагина
    ↓
  Plugin            ← сторонний код, НЕдоверенный
    ↓ IPC
Plugin Runtime      ← часть платформы, пользователь ей доверяет
    ↓
Photon Core
```

Код SDK исполняется внутри недоверенного процесса плагина; код Runtime — снаружи,
и именно он принимает решения о правах. Смешивать нельзя.

**Направление зависимости** (ADR-005, ADR-030): `photon-plugin-runtime` реализует
`ports.Authorizer` из `photon-core`, но `photon-core` о runtime не знает и никогда
не импортирует его.

---

## photon-plugin-sdk/

```
photon-plugin-sdk/
├── manifest/   — формат и валидация manifest.json
├── api/        — Storage(), Events(), Identity(), Network()
├── examples/   — примеры плагинов для документации
└── docs/
```

---

## photon-developer-tools/ (ранее photon-debug-client)

Compose Desktop приложение, стек: `Compose Desktop → JNI/C → Photon Core`. Полноценная среда разработки платформы, а не только отладочный клиент.

```
photon-developer-tools/
├── dashboard/          — состояние ядра
├── network-lab/        — тестирование транспорта: маршруты, RTT
├── storage-viewer/     — тестирование Storage, просмотр локальной базы
├── crypto-lab/         — ключи и подписи
├── plugin-lab/         — тестирование Plugin Runtime: установка/права/запуск
├── protobuf-inspector/ — инспектор сообщений протокола
├── logging/            — централизованный просмотр логов ядра
└── performance/        — профилирование, нагрузочное тестирование
```

---

## photon-quic-bridge/ (закрытый репозиторий)

```
photon-quic-bridge/
├── listener/     — QUIC listener
├── mattermost/   — интеграция с Mattermost API
├── push/         — push-уведомления
└── handlers/     — getUsers, searchUsers, getReactions и т.д.
```

Не публикуется под AGPL — серверная инфраструктура физически отделена от открытого ядра.

**Известный технический долг (до заморозки ABI v1):** action-коды и wire-структуры (`protocol.go`) сейчас объявлены непосредственно в этом репозитории. Целевое состояние — bridge импортирует их из `photon-core/protobuf/` (Photon Protocol) как один из потребителей, наравне с Core, а не как источник истины для протокола. См. ARCHITECTURE.md раздел 7.6.

---

## photon-sdk/

```
photon-sdk/
├── client/     — высокоуровневый клиентский API для приложений поверх Photon
├── examples/
└── docs/
```

---

## engram-desktop/

```
engram-desktop/
├── ui/
├── modules/      — Wallet, Chat, Media и т.д. (фаза 21+)
└── integration/  — связка с Photon Core + Plugin Runtime + ENSTOR
```

---

## engram-android/

```
engram-android/
├── app/
│   ├── lite/
│   ├── standard/
│   └── full/
└── integration/   — связка с Photon SDK
```

---

## enstor/

```
enstor/
├── plugins/
├── extensions/
├── themes/
├── apps/
└── backend/    — подписи, версии, рейтинги, обновления
```

---

## Общее правило для всех репозиториев

Если новая папка не описана в этом документе — перед созданием она добавляется сюда осознанно, а не появляется по ходу написания фичи. Это тот же принцип, что и для `ROADMAP.md`: структура фиксируется отдельно от текущей задачи, а не как её побочный эффект.
