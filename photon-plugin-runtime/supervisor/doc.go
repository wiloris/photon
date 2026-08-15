// Package supervisor — жизненный цикл плагинов.
//
// Спецификация: docs/lifecycle.md.
//
// Центральный координатор: связывает manifest, permissions, host, sandbox
// и ipc. Владеет state machine из 7 состояний (STOPPED, STARTING,
// RUNNING, STOPPING, TERMINATING, FAILED, DISABLED).
//
// Планируемое разделение:
//
//	lifecycle — переходы состояний, handshake, shutdown
//	restart   — классификация завершений (6 типов, не один ERROR),
//	            экспоненциальный backoff с обязательным jitter,
//	            disable policy
//	registry  — установленные плагины, persistent state поверх
//	            ports.Storage (namespace plugin.<id>.*)
//
// Ключевые правила из спецификации:
//   - protocol_violation НЕ перезапускается автоматически, в отличие от
//     crash: сбой переживается перезапуском, нарушение протокола
//     повторится и может быть попыткой атаки;
//   - счётчик перезапусков сбрасывается после стабильной работы;
//   - Runtime restart != Plugin identity reset;
//   - один экземпляр на plugin_id (multi объявлен, но отклоняется).
package supervisor
