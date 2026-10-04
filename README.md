# Photon / ENGRAM

**Photon** — платформа: ядро, транспорт, плагины, безопасность.
**ENGRAM** — приложение поверх неё. Разделение принципиально (ADR-003): что
может понадобиться другому приложению — в Photon, что специфично для ENGRAM —
в ENGRAM или в плагине.

## С чего начать

1. [`CLAUDE.md`](CLAUDE.md) — точка входа: метод работы, границы, окружение,
   команды.
2. [`AGENT_WORKFLOW.md`](AGENT_WORKFLOW.md) — как работают Claude Code и
   Claude Session: роли, цикл «задание → реализация → приёмка», проверки,
   известные падения, правила коммитов.
3. [`CURRENT_TASK.md`](CURRENT_TASK.md) — текущее состояние и следующий срез.
4. [`DECISIONS.md`](DECISIONS.md) — журнал ADR; действуют только принятые.

## Состав

| каталог | что |
|---|---|
| `photon-core/` | ядро: порты, адаптеры, сервисы, `protobuf/photon.proto` |
| `photon-plugin-runtime/`, `photon-plugin-sdk/` | среда исполнения плагинов и SDK |
| `photon-bindings/` | C ABI и JNI |
| `photon-sdk/`, `photon-developer-tools/` | JVM SDK и Linux Desktop клиент разработчика |
| `photon-quic-bridge/` | плагин Mattermost (Photon QUIC Bridge); `docs/` — спецификации групповых разговоров |
| `tools/pgtest/` | временный PostgreSQL для контрактных тестов Bridge (только тесты) |

Архитектура — [`ARCHITECTURE.md`](ARCHITECTURE.md), фазы и история —
[`ROADMAP.md`](ROADMAP.md), структура репозиториев —
[`REPOSITORY.md`](REPOSITORY.md).

Секреты (пароли, токены, ключи) в репозиторий не попадают; live-оснастка
принимает их флагами или переменными окружения.
