package manifest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Validator отвечает на вопрос:
//
//	«Является ли этот манифест допустимым и безопасным?»
//
// В отличие от парсера, работает с уже разобранной структурой.

var (
	ErrMissingField        = errors.New("manifest: required field is missing")
	ErrUnsupportedVersion  = errors.New("manifest: unsupported version")
	ErrInvalidPluginID     = errors.New("manifest: invalid plugin_id")
	ErrInvalidVersion      = errors.New("manifest: invalid version")
	ErrInvalidEntrypoint   = errors.New("manifest: invalid entrypoint")
	ErrEntrypointEscape    = errors.New("manifest: entrypoint escapes package root")
	ErrEntrypointNotFound  = errors.New("manifest: entrypoint does not exist")
	ErrEntrypointNotFile   = errors.New("manifest: entrypoint is not a regular file")
	ErrUnsupportedInstance = errors.New("manifest: unsupported instance_policy")
	ErrInvalidPermission   = errors.New("manifest: invalid permission name")
	ErrInvalidCapability   = errors.New("manifest: invalid capability name")
	ErrDuplicateEntry      = errors.New("manifest: duplicate entry")
)

// pluginIDPattern — обратный доменный формат: "com.engram.ssh".
// Минимум два сегмента: односоставный идентификатор ("ssh") слишком
// легко занять чужой.
var pluginIDPattern = regexp.MustCompile(`^[a-z0-9]+(\.[a-z0-9][a-z0-9-]*)+$`)

// semverPattern — упрощённый semantic version: MAJOR.MINOR.PATCH с
// необязательным пре-релизом.
var semverPattern = regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$`)

// capabilityPattern — "домен.действие": chat.send, storage.read.
//
// Валидатор проверяет только СИНТАКСИС. Существует ли такое право и
// можно ли его выдать — решает permission policy Runtime
// (docs/manifest.md): манифест не выдаёт права, поэтому и знать
// registry известных прав ему не нужно. Хардкод списка прав здесь
// связал бы формат пакета с конкретной политикой конкретной сборки.
var capabilityPattern = regexp.MustCompile(`^[a-z0-9]+(\.[a-z0-9]+)+$`)

// Validate проверяет манифест целиком.
//
// packageRoot — корень установленного пакета; относительно него
// разрешается entrypoint. Пустая строка отключает проверки файловой
// системы (полезно для валидации манифеста до распаковки пакета), но
// структурные проверки пути выполняются всё равно.
func Validate(m *Manifest, packageRoot string) error {
	if m == nil {
		return fmt.Errorf("%w: manifest is nil", ErrMissingField)
	}

	if err := validateVersions(m); err != nil {
		return err
	}
	if err := validateIdentity(m.Identity); err != nil {
		return err
	}
	if err := validateInstancePolicy(m.Runtime.InstancePolicy); err != nil {
		return err
	}
	if err := validateNames(m); err != nil {
		return err
	}
	// Проверка пути — последней среди структурных, потому что
	// единственная обращается к файловой системе.
	return validateEntrypoint(m.Runtime.Entrypoint, packageRoot)
}

func validateVersions(m *Manifest) error {
	if m.ManifestVersion == 0 {
		return fmt.Errorf("%w: manifest_version", ErrMissingField)
	}
	if m.ManifestVersion != SupportedManifestVersion {
		return fmt.Errorf("%w: manifest_version %d (supported: %d)",
			ErrUnsupportedVersion, m.ManifestVersion, SupportedManifestVersion)
	}
	if m.Runtime.ProtocolVersion == 0 {
		return fmt.Errorf("%w: runtime.protocol_version", ErrMissingField)
	}
	if m.Runtime.ProtocolVersion != SupportedProtocolVersion {
		return fmt.Errorf("%w: protocol_version %d (supported: %d)",
			ErrUnsupportedVersion, m.Runtime.ProtocolVersion, SupportedProtocolVersion)
	}
	return nil
}

func validateIdentity(id Identity) error {
	if id.PluginID == "" {
		return fmt.Errorf("%w: identity.plugin_id", ErrMissingField)
	}
	if !pluginIDPattern.MatchString(id.PluginID) {
		return fmt.Errorf("%w: %q (expected reverse-domain form, e.g. com.example.plugin)",
			ErrInvalidPluginID, id.PluginID)
	}
	if id.Version == "" {
		return fmt.Errorf("%w: identity.version", ErrMissingField)
	}
	if !semverPattern.MatchString(id.Version) {
		return fmt.Errorf("%w: %q (expected MAJOR.MINOR.PATCH)", ErrInvalidVersion, id.Version)
	}
	return nil
}

func validateInstancePolicy(p InstancePolicy) error {
	switch p {
	case "", InstanceSingle:
		return nil
	case InstanceMulti:
		// Отклоняется явно, а не запускается тихо в single-режиме:
		// расхождение между заявленным и фактическим поведением хуже
		// честного отказа.
		return fmt.Errorf("%w: %q is declared in the format but not implemented by this runtime",
			ErrUnsupportedInstance, p)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedInstance, p)
	}
}

func validateNames(m *Manifest) error {
	seenPerm := make(map[string]bool, len(m.Permissions.Requested))
	for _, p := range m.Permissions.Requested {
		if !capabilityPattern.MatchString(p) {
			return fmt.Errorf("%w: %q (expected domain.action)", ErrInvalidPermission, p)
		}
		if seenPerm[p] {
			return fmt.Errorf("%w: permission %q listed twice", ErrDuplicateEntry, p)
		}
		seenPerm[p] = true
	}

	seenCap := make(map[string]bool, len(m.Capabilities.Provided))
	for _, c := range m.Capabilities.Provided {
		if !capabilityPattern.MatchString(c) {
			return fmt.Errorf("%w: %q (expected domain.action)", ErrInvalidCapability, c)
		}
		if seenCap[c] {
			return fmt.Errorf("%w: capability %q listed twice", ErrDuplicateEntry, c)
		}
		seenCap[c] = true
	}
	return nil
}

// validateEntrypoint — самая важная проверка манифеста.
//
// Без неё манифест становится способом заставить Runtime исполнить
// произвольный файл на хосте: путь обхода sandbox, созданный
// собственными руками.
//
// filepath.Join(root, entrypoint) НЕДОСТАТОЧНО. Join очищает "..", но
// ничего не знает о символических ссылках: bin/plugin -> /etc/evil
// пройдёт такую проверку и запустит совсем не то, что лежит в пакете.
// Поэтому проверяются ОБА случая, и проверка symlink выполняется через
// разрешение реального пути.
func validateEntrypoint(entrypoint, packageRoot string) error {
	if entrypoint == "" {
		return fmt.Errorf("%w: runtime.entrypoint", ErrMissingField)
	}
	if filepath.IsAbs(entrypoint) {
		return fmt.Errorf("%w: %q is absolute; must be relative to the package root",
			ErrInvalidEntrypoint, entrypoint)
	}
	// Windows-style абсолютные пути и UNC на любой платформе.
	if strings.HasPrefix(entrypoint, `\`) || strings.Contains(entrypoint, `:\`) {
		return fmt.Errorf("%w: %q looks like an absolute path", ErrInvalidEntrypoint, entrypoint)
	}

	// Структурная проверка выхода за корень — работает и без доступа к
	// файловой системе.
	cleaned := filepath.Clean(entrypoint)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: %q traverses outside the package root",
			ErrEntrypointEscape, entrypoint)
	}

	if packageRoot == "" {
		// Проверки файловой системы отключены — структурных достаточно.
		return nil
	}

	// Разрешаем корень пакета: он сам может быть за symlink (например,
	// /var -> /private/var), и без этого сравнение путей ниже давало бы
	// ложные срабатывания.
	realRoot, err := filepath.EvalSymlinks(packageRoot)
	if err != nil {
		return fmt.Errorf("manifest: resolve package root: %w", err)
	}

	target := filepath.Join(realRoot, cleaned)

	// EvalSymlinks разрешает ВСЮ цепочку ссылок — именно это ловит
	// bin/plugin -> /etc/evil.
	realTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %q", ErrEntrypointNotFound, entrypoint)
		}
		return fmt.Errorf("manifest: resolve entrypoint: %w", err)
	}

	// Финальная проверка: реальный файл обязан лежать внутри реального
	// корня. Сравнение через Rel, а не через HasPrefix со строками:
	// "/pkg-evil" имеет префикс "/pkg", но лежит вне его.
	rel, err := filepath.Rel(realRoot, realTarget)
	if err != nil {
		return fmt.Errorf("%w: cannot relate %q to package root", ErrEntrypointEscape, entrypoint)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: %q resolves to %q, outside the package root",
			ErrEntrypointEscape, entrypoint, realTarget)
	}

	info, err := os.Stat(realTarget)
	if err != nil {
		return fmt.Errorf("manifest: stat entrypoint: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("%w: %q is a directory", ErrEntrypointNotFile, entrypoint)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %q is not a regular file", ErrEntrypointNotFile, entrypoint)
	}
	return nil
}
