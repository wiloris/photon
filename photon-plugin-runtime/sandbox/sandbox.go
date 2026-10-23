// Package sandbox — изоляция процесса плагина.
//
// Спецификация: docs/sandbox.md. Свойства зафиксированы ДО реализации,
// потому что механизмы платформо-зависимы: seccomp — способ достичь
// свойства на Linux, а не само требование.
//
// Sandbox НЕ управляет жизненным циклом (docs/sandbox.md, раздел 1):
// нет Start, Restart, IsAlive. Он применяет ограничения к процессу,
// который запускает host, и освобождает ресурсы после завершения.
// Появление здесь метода управления означало бы дублирование
// supervisor.
package sandbox

import (
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

// Property — свойство изоляции (docs/sandbox.md, раздел 2).
type Property string

const (
	// PropSeparateProcess (P1) — плагин исполняется вне адресного
	// пространства Runtime.
	PropSeparateProcess Property = "separate_process"

	// PropMemoryIsolation (P2) — нет доступа к памяти Runtime и других
	// плагинов. Следствие P1.
	PropMemoryIsolation Property = "memory_isolation"

	// PropFilesystemLimit (P3) — нет произвольного доступа к ФС хоста.
	PropFilesystemLimit Property = "filesystem_limit"

	// PropNetworkLimit (P4) — сеть выдаётся как capability, не по
	// умолчанию.
	PropNetworkLimit Property = "network_limit"

	// PropResourceLimit (P5) — CPU/RAM/дескрипторы ограничены.
	PropResourceLimit Property = "resource_limit"

	// PropSingleChannel (P6) — IPC единственный путь наружу.
	PropSingleChannel Property = "single_channel"

	// PropTerminable (P7) — процесс завершаем за ограниченное время.
	PropTerminable Property = "terminable"

	// PropNoEscalation (P8) — нет привилегий выше, чем у Runtime.
	PropNoEscalation Property = "no_escalation"
)

// mandatoryProperties — без них изоляции нет вообще
// (docs/sandbox.md, раздел 6). Недостижимость → отказ запуска.
var mandatoryProperties = []Property{
	PropSeparateProcess,
	PropSingleChannel,
	PropTerminable,
}

// bestEffortProperties — применяются, если платформа позволяет.
// Недостижимость → запуск с явным предупреждением, но НЕ молча.
var bestEffortProperties = []Property{
	PropMemoryIsolation,
	PropFilesystemLimit,
	PropNetworkLimit,
	PropResourceLimit,
	PropNoEscalation,
}

// IsMandatory сообщает, обязательно ли свойство.
func IsMandatory(p Property) bool {
	for _, m := range mandatoryProperties {
		if m == p {
			return true
		}
	}
	return false
}

// Spec — запрашиваемая изоляция.
type Spec struct {
	// PackageRoot — корень пакета плагина; за его пределы файловый
	// доступ не выдаётся.
	PackageRoot string

	// WorkDir — рабочий каталог процесса. Пустое значение = PackageRoot.
	WorkDir string

	// AllowNetwork — выдана ли плагину сетевая capability.
	//
	// Sandbox не решает это сам: право приходит от Authorizer
	// (docs/sandbox.md, раздел 5 — sandbox не подменяет authorization).
	AllowNetwork bool

	// MaxMemoryBytes — предел памяти. 0 = без предела.
	MaxMemoryBytes uint64

	// MaxFileDescriptors — предел дескрипторов. 0 = без предела.
	MaxFileDescriptors uint64

	// MaxCPUSeconds — предел процессорного времени. 0 = без предела.
	MaxCPUSeconds uint64
}

// Applied — что РЕАЛЬНО применено.
//
// Возвращается наружу намеренно: без этого честная деградация
// невозможна — вызывающий не отличил бы "изоляция применена" от
// "изоляция пропущена молча" (docs/sandbox.md, раздел 7).
type Applied struct {
	// Properties — свойство -> применено ли.
	Properties map[Property]bool

	// Warnings — что не удалось и почему. Пустой список означает, что
	// всё запрошенное применено.
	Warnings []string

	// Platform — имя реализации ("linux", "none").
	Platform string
}

// Has сообщает, применено ли свойство.
func (a Applied) Has(p Property) bool { return a.Properties[p] }

// MissingMandatory возвращает обязательные свойства, которые не
// применены.
func (a Applied) MissingMandatory() []Property {
	var missing []Property
	for _, p := range mandatoryProperties {
		if !a.Properties[p] {
			missing = append(missing, p)
		}
	}
	return missing
}

// String — читаемая сводка для логов и Developer Tools.
func (a Applied) String() string {
	var on, off []string
	for p, ok := range a.Properties {
		if ok {
			on = append(on, string(p))
		} else {
			off = append(off, string(p))
		}
	}
	sort.Strings(on)
	sort.Strings(off)

	var b strings.Builder
	fmt.Fprintf(&b, "sandbox[%s] applied=%v", a.Platform, on)
	if len(off) > 0 {
		fmt.Fprintf(&b, " missing=%v", off)
	}
	for _, w := range a.Warnings {
		fmt.Fprintf(&b, "\n  warning: %s", w)
	}
	return b.String()
}

// ErrMandatoryPropertyUnavailable — обязательное свойство недостижимо.
//
// Запуск в этом случае запрещён: без P1/P6/P7 изоляции нет вообще, и
// продолжать означало бы создать ложное впечатление защищённости.
var ErrMandatoryPropertyUnavailable = errors.New("sandbox: mandatory property unavailable")

// Sandbox применяет изоляцию к процессу плагина.
type Sandbox interface {
	// Prepare настраивает cmd ДО запуска и сообщает, что реально
	// применено.
	//
	// Возвращает ошибку, если недостижимо ОБЯЗАТЕЛЬНОЕ свойство.
	// Недостижимость best-effort свойства — не ошибка, но обязана
	// попасть в Applied.Warnings: тихая деградация запрещена
	// (docs/sandbox.md, раздел 6).
	Prepare(cmd *exec.Cmd, spec Spec) (Applied, error)

	// Cleanup освобождает ресурсы после завершения процесса.
	// Идемпотентен.
	Cleanup() error
}

// Verify проверяет, что применённая изоляция допускает запуск.
//
// Вызывается host'ом после Prepare. Отдельная функция, а не проверка
// внутри Prepare: решение "запускать или нет" принимает вызывающий, а
// sandbox лишь сообщает факты.
func Verify(a Applied) error {
	missing := a.MissingMandatory()
	if len(missing) == 0 {
		return nil
	}
	names := make([]string, len(missing))
	for i, p := range missing {
		names[i] = string(p)
	}
	return fmt.Errorf("%w: %s", ErrMandatoryPropertyUnavailable, strings.Join(names, ", "))
}
