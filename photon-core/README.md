# photon-core

Ядро платформы Photon. Go, AGPL.

Перед тем как читать код — прочитай, в этом порядке:

1. [`ARCHITECTURE.md`](./ARCHITECTURE.md) — устройство системы: философия, Ports & Adapters, Plugin Runtime, C ABI, безопасность.
2. [`DECISIONS.md`](./DECISIONS.md) — почему принято именно так (ADR).
3. [`ROADMAP.md`](./ROADMAP.md) — текущая фаза и что дальше.
4. [`REPOSITORY.md`](./REPOSITORY.md) — куда физически класть файлы.

## Текущая фаза

См. `ROADMAP.md` → Phase 1 (Core interfaces). Первый порт: [`ports/storage.go`](./ports/storage.go).

## Правило для контрибьюторов

`ports/` содержит только интерфейсы. Ни одной реализации. Реализации живут в `adapters/`.
