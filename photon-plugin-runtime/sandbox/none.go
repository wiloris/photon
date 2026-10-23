package sandbox

import "os/exec"

// None — отсутствие OS-изоляции.
//
// Имя говорит прямо: это НЕ sandbox. Заглушка, названная нейтрально
// ("default", "basic"), создавала бы ложное впечатление защищённости —
// именно то, что запрещает docs/sandbox.md, раздел 6.
//
// Layer 1 (отдельный процесс, единственный канал, завершаемость)
// обеспечивается самим фактом запуска через host и присутствует здесь.
// Layer 2 (ФС, сеть, ресурсы, эскалация) отсутствует полностью и
// честно отражается в Applied.Warnings.
//
// Допустимо для разработки и тестов. Использование в проде означает
// Layer 1 без Layer 2 — осознанное решение вызывающего, а не молчаливый
// умолчальный режим.
type None struct{}

func NewNone() *None { return &None{} }

var _ Sandbox = (*None)(nil)

func (None) Prepare(cmd *exec.Cmd, spec Spec) (Applied, error) {
	if spec.WorkDir != "" {
		cmd.Dir = spec.WorkDir
	} else if spec.PackageRoot != "" {
		cmd.Dir = spec.PackageRoot
	}

	return Applied{
		Platform: "none",
		Properties: map[Property]bool{
			// Layer 1 — обеспечивается host'ом, не этим пакетом.
			PropSeparateProcess: true,
			PropSingleChannel:   true,
			PropTerminable:      true,
			PropMemoryIsolation: true, // следствие отдельного процесса

			// Layer 2 — отсутствует.
			PropFilesystemLimit: false,
			PropNetworkLimit:    false,
			PropResourceLimit:   false,
			PropNoEscalation:    false,
		},
		Warnings: []string{
			"no OS-level isolation: filesystem, network, resources and privilege escalation are NOT restricted",
			"suitable for development and tests only",
		},
	}, nil
}

func (None) Cleanup() error { return nil }
