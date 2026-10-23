# photon-core

Ядро платформы Photon (Go). Лицензия — GNU AGPL-3.0-only, см. `../LICENSE` и `../NOTICE`.
Общее описание — [`../README.md`](../README.md) и
[`../ARCHITECTURE.md`](../ARCHITECTURE.md).

| каталог | что это |
|---|---|
| `ports/` | контракты ядра — только интерфейсы (`docs/ports.md`) |
| `adapters/` | реализации портов: `quic`, `tcp`, `bbolt`, `aesgcm`, `ed25519`, `googleoidc`, `pion` |
| `services/` | логика поверх портов: `session`, `dispatcher`, `diag`, `localfeed`, `outbox`, `media`, `account`, `p2phost`, `autodial` |
| `protobuf/` | каталог действий протокола (`photon.proto`) |
| `docs/` | спецификации сессии, маршрутизации действий, доставки событий, P2P, плагинов |
| `tests/` | стенды транспорта (`transportlab` с тестовым сервером `bridgesim`), P2P и ручные прогоны против сервера протокола (`livecheck`, `eventcheck`, `p2pcheck`) — адреса и учётные данные передаются флагами |
| `cmd/` | вспомогательные программы проверки вертикальных срезов |

## Проверка

```sh
go build ./... && go test ./... && go vet ./... && gofmt -l .
go test ./adapters/tcp/... -fuzz=FuzzDecodeFrame -fuzztime=30s   # фаззинг кадров TCP
```

Тесты, которым нужна сеть или долгие таймауты (`tests/transportlab`), по
умолчанию пропускаются и включаются переменными окружения — см. комментарии в
их исходниках.

## Правило для изменений

`ports/` содержит только интерфейсы; реализации — в `adapters/`. Порты
меняются только отдельным решением, после проверки, что задачу нельзя решить
внутри адаптера.
