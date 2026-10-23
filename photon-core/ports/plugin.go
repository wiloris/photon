// Package ports — см. storage.go для общих правил пакета.
package ports

import "context"

// Permission — именованный тип поверх строки (см. Endpoint в
// transport.go, PhotonID в ADR-018) — тот же стиль, что и у других
// платформенных идентификаторов пакета. Значения — плоские строки
// вида "storage", "network" (формат уже установлен манифестом
// плагина, см. ARCHITECTURE.md); константы для известных значений
// добавляются по мере необходимости, не меняя сигнатур.
type Permission string

// Authorizer — порт проверки прав вызывающей стороны.
// Первая реализация будет определена при реализации Plugin Runtime
// (см. ROADMAP.md, Phase 9 — Plugin Architecture).
//
// Файл называется plugin.go по номенклатуре ROADMAP.md, но интерфейс
// сознательно не назван Plugin: Core не знает про процессы, IPC,
// загрузку или манифесты (ADR-005 — Plugin Runtime отдельный процесс,
// вызывающий Core через уже существующий Photon Protocol, а не
// наоборот). Для Core вызывающая сторона — caller, опознанный
// PublicKey (identity.go), как и везде в системе; Authorizer можно
// реализовать поверх Plugin Runtime, встроенной политики, тестового
// адаптера — вообще без каких-либо плагинов.
//
// Не описывает жизненный цикл (Start/Stop/Load/Unload — это Plugin
// Runtime, не Core) и не содержит IPC-вокабуляра (pipe, socket, RPC,
// stdin/stdout — способ доставки вызова этому порту не принадлежит).
// Проверка подписи вызывающего — уже задача Identity.Verify выше по
// пайплайну диспетчера; Authorizer проверяет право уже
// аутентифицированного вызова, не переизобретает аутентификацию.
type Authorizer interface {
	// Authorize проверяет право caller'а на permission.
	// (false, nil) — доступ запрещён, ожидаемый отрицательный
	// результат. (true, nil) — разрешено. error — решение принять
	// невозможно (политика не загружена, повреждена база разрешений
	// и т.п.) — та же семантика, что у Identity.Verify: ожидаемый
	// отрицательный результат не смешивается с технической ошибкой.
	Authorize(ctx context.Context, caller PublicKey, permission Permission) (bool, error)
}
