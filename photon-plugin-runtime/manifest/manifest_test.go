package manifest

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Манифест — НЕдоверенный вход (часть пакета плагина). Поэтому тесты
// security-oriented: happy path здесь наименее интересная часть.

const validManifestJSON = `{
  "manifest_version": 1,
  "identity": {"plugin_id": "com.engram.ssh", "version": "1.4.2"},
  "runtime": {"protocol_version": 1, "entrypoint": "bin/plugin", "instance_policy": "single"},
  "permissions": {"requested": ["network.connect", "storage.read"]},
  "capabilities": {"provided": ["ssh.session"]}
}`

// newPackage создаёт временный пакет с исполняемым файлом внутри.
func newPackage(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "plugin"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}
	return root
}

func mustParse(t *testing.T, s string) *Manifest {
	t.Helper()
	m, err := ParseBytes([]byte(s))
	if err != nil {
		t.Fatalf("ParseBytes: %v", err)
	}
	return m
}

// ── Parser: только разбор ───────────────────────────────────────────

func TestParse_ValidManifest(t *testing.T) {
	m := mustParse(t, validManifestJSON)

	if m.Identity.PluginID != "com.engram.ssh" {
		t.Fatalf("PluginID = %q", m.Identity.PluginID)
	}
	if m.Runtime.ProtocolVersion != 1 {
		t.Fatalf("ProtocolVersion = %d", m.Runtime.ProtocolVersion)
	}
	if len(m.Permissions.Requested) != 2 {
		t.Fatalf("Requested = %v", m.Permissions.Requested)
	}
	if len(m.Capabilities.Provided) != 1 {
		t.Fatalf("Provided = %v", m.Capabilities.Provided)
	}
}

func TestParse_MalformedJSON(t *testing.T) {
	for _, in := range []string{
		`{not json`,
		`{"manifest_version": }`,
		``,
		`[]`,
		`"string"`,
		`{"manifest_version": "not a number"}`,
	} {
		if _, err := ParseBytes([]byte(in)); err == nil {
			t.Fatalf("input %q: expected an error", in)
		}
	}
}

func TestParse_UnknownOptionalFieldIsIgnored(t *testing.T) {
	// Forward compatibility: неизвестное НЕобязательное поле не мешает.
	in := `{
	  "manifest_version": 1,
	  "identity": {"plugin_id": "com.engram.ssh", "version": "1.0.0"},
	  "runtime": {"protocol_version": 1, "entrypoint": "bin/plugin"},
	  "future_optional_setting": {"anything": true}
	}`
	if _, err := ParseBytes([]byte(in)); err != nil {
		t.Fatalf("unknown optional field must be ignored, got: %v", err)
	}
}

func TestParse_UnknownRequiredFieldIsRejected(t *testing.T) {
	// Ключевое отличие от предыдущего теста: автор пометил поле как
	// обязательное. Тихо запустить плагин без режима безопасности,
	// который автор счёл обязательным, — хуже отказа.
	in := `{
	  "manifest_version": 1,
	  "identity": {"plugin_id": "com.engram.ssh", "version": "1.0.0"},
	  "runtime": {"protocol_version": 1, "entrypoint": "bin/plugin"},
	  "required_new_security_mode": "strict-isolation"
	}`
	_, err := ParseBytes([]byte(in))
	if !errors.Is(err, ErrUnknownRequiredField) {
		t.Fatalf("expected ErrUnknownRequiredField, got %v", err)
	}
}

func TestParse_OversizedInputRejected(t *testing.T) {
	// Вход недоверенный: без предела файл на гигабайты был бы прочитан
	// целиком в память.
	big := make([]byte, maxManifestSize+1)
	for i := range big {
		big[i] = ' '
	}
	if _, err := ParseBytes(big); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}

	if _, err := Parse(bytes.NewReader(big)); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("Parse: expected ErrManifestTooLarge, got %v", err)
	}
}

func TestParse_DoesNotValidate(t *testing.T) {
	// Разделение ответственности: манифест с опасным entrypoint обязан
	// УСПЕШНО разобраться и быть отвергнутым валидатором — иначе
	// нарушение безопасности выглядело бы как "parse error".
	in := `{
	  "manifest_version": 1,
	  "identity": {"plugin_id": "com.engram.ssh", "version": "1.0.0"},
	  "runtime": {"protocol_version": 1, "entrypoint": "../../evil"}
	}`
	m, err := ParseBytes([]byte(in))
	if err != nil {
		t.Fatalf("parser must not reject a dangerous but syntactically valid manifest: %v", err)
	}
	if err := Validate(m, ""); !errors.Is(err, ErrEntrypointEscape) {
		t.Fatalf("validator must reject it: got %v", err)
	}
}

func TestParse_GrantedPermissionsInjectionIsIgnored(t *testing.T) {
	// Попытка протащить выданные права через манифест. Поля нет в
	// структуре, поэтому оно молча игнорируется — и это правильно:
	// невозможно "выдать себе права", записав их в свой же пакет.
	in := `{
	  "manifest_version": 1,
	  "identity": {"plugin_id": "com.engram.ssh", "version": "1.0.0"},
	  "runtime": {"protocol_version": 1, "entrypoint": "bin/plugin"},
	  "permissions": {"requested": ["storage.read"], "granted": ["storage.write", "network.connect"]},
	  "granted_permissions": ["admin.everything"]
	}`
	m := mustParse(t, in)

	if len(m.Permissions.Requested) != 1 || m.Permissions.Requested[0] != "storage.read" {
		t.Fatalf("Requested = %v", m.Permissions.Requested)
	}
	// Структура физически не имеет места для granted — проверяем, что
	// его и не появилось окольным путём.
	if strings.Contains(m.Permissions.Requested[0], "write") {
		t.Fatal("granted permissions leaked into requested")
	}
}

// ── Validator: версии ───────────────────────────────────────────────

func TestValidate_Valid(t *testing.T) {
	root := newPackage(t)
	if err := Validate(mustParse(t, validManifestJSON), root); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
}

func TestValidate_MissingRequiredFields(t *testing.T) {
	cases := map[string]string{
		"manifest_version": `{"identity":{"plugin_id":"com.a.b","version":"1.0.0"},"runtime":{"protocol_version":1,"entrypoint":"bin/plugin"}}`,
		"plugin_id":        `{"manifest_version":1,"identity":{"version":"1.0.0"},"runtime":{"protocol_version":1,"entrypoint":"bin/plugin"}}`,
		"version":          `{"manifest_version":1,"identity":{"plugin_id":"com.a.b"},"runtime":{"protocol_version":1,"entrypoint":"bin/plugin"}}`,
		"protocol_version": `{"manifest_version":1,"identity":{"plugin_id":"com.a.b","version":"1.0.0"},"runtime":{"entrypoint":"bin/plugin"}}`,
		"entrypoint":       `{"manifest_version":1,"identity":{"plugin_id":"com.a.b","version":"1.0.0"},"runtime":{"protocol_version":1}}`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			m := mustParse(t, in)
			err := Validate(m, "")
			if err == nil {
				t.Fatalf("missing %s: expected an error", name)
			}
			if !errors.Is(err, ErrMissingField) && !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("missing %s: unexpected error kind: %v", name, err)
			}
		})
	}
}

func TestValidate_UnsupportedManifestVersion(t *testing.T) {
	in := strings.Replace(validManifestJSON, `"manifest_version": 1`, `"manifest_version": 99`, 1)
	if err := Validate(mustParse(t, in), ""); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
}

func TestValidate_UnsupportedProtocolVersion(t *testing.T) {
	in := strings.Replace(validManifestJSON, `"protocol_version": 1`, `"protocol_version": 42`, 1)
	if err := Validate(mustParse(t, in), ""); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
}

// ── Validator: identity ─────────────────────────────────────────────

func TestValidate_InvalidPluginID(t *testing.T) {
	for _, id := range []string{
		"ssh",                    // односоставный: слишком легко занять чужой
		"com.engram.SSH",         // верхний регистр
		"com..engram",            // пустой сегмент
		".com.engram",            // ведущая точка
		"com.engram.",            // хвостовая точка
		"com engram ssh",         // пробелы
		"com.engram.ssh/../evil", // попытка протащить путь
		"",
	} {
		in := strings.Replace(validManifestJSON, `"plugin_id": "com.engram.ssh"`,
			`"plugin_id": "`+id+`"`, 1)
		m, err := ParseBytes([]byte(in))
		if err != nil {
			continue // некоторые варианты ломают JSON — это тоже отказ
		}
		if err := Validate(m, ""); err == nil {
			t.Fatalf("plugin_id %q was accepted", id)
		}
	}
}

func TestValidate_InvalidVersion(t *testing.T) {
	for _, v := range []string{"1.4", "v1.4.2", "1.4.2.3", "latest", "", "1.x.2"} {
		in := strings.Replace(validManifestJSON, `"version": "1.4.2"`, `"version": "`+v+`"`, 1)
		m, err := ParseBytes([]byte(in))
		if err != nil {
			continue
		}
		if err := Validate(m, ""); err == nil {
			t.Fatalf("version %q was accepted", v)
		}
	}
}

func TestValidate_PreReleaseVersionAccepted(t *testing.T) {
	in := strings.Replace(validManifestJSON, `"version": "1.4.2"`, `"version": "2.0.0-beta.1"`, 1)
	if err := Validate(mustParse(t, in), ""); err != nil {
		t.Fatalf("pre-release version rejected: %v", err)
	}
}

// ── Validator: instance policy ──────────────────────────────────────

func TestValidate_InstancePolicyMultiRejected(t *testing.T) {
	// Объявлен в формате, но не реализован. Отклоняется явно, а не
	// запускается тихо в single-режиме.
	in := strings.Replace(validManifestJSON, `"instance_policy": "single"`,
		`"instance_policy": "multi"`, 1)
	err := Validate(mustParse(t, in), "")
	if !errors.Is(err, ErrUnsupportedInstance) {
		t.Fatalf("expected ErrUnsupportedInstance, got %v", err)
	}
}

func TestValidate_InstancePolicyUnknownRejected(t *testing.T) {
	in := strings.Replace(validManifestJSON, `"instance_policy": "single"`,
		`"instance_policy": "whatever"`, 1)
	if err := Validate(mustParse(t, in), ""); !errors.Is(err, ErrUnsupportedInstance) {
		t.Fatalf("expected ErrUnsupportedInstance, got %v", err)
	}
}

func TestValidate_InstancePolicyEmptyDefaultsToSingle(t *testing.T) {
	in := strings.Replace(validManifestJSON, `, "instance_policy": "single"`, ``, 1)
	if err := Validate(mustParse(t, in), ""); err != nil {
		t.Fatalf("empty instance_policy must default to single: %v", err)
	}
}

// ── Validator: имена прав и capabilities ────────────────────────────

func TestValidate_InvalidPermissionNames(t *testing.T) {
	for _, p := range []string{"network", "Network.Connect", "network connect", "", "network..connect"} {
		in := strings.Replace(validManifestJSON, `["network.connect", "storage.read"]`,
			`["`+p+`"]`, 1)
		m, err := ParseBytes([]byte(in))
		if err != nil {
			continue
		}
		if err := Validate(m, ""); !errors.Is(err, ErrInvalidPermission) {
			t.Fatalf("permission %q: expected ErrInvalidPermission, got %v", p, err)
		}
	}
}

func TestValidate_DuplicatePermissions(t *testing.T) {
	in := strings.Replace(validManifestJSON, `["network.connect", "storage.read"]`,
		`["storage.read", "storage.read"]`, 1)
	if err := Validate(mustParse(t, in), ""); !errors.Is(err, ErrDuplicateEntry) {
		t.Fatalf("expected ErrDuplicateEntry, got %v", err)
	}
}

func TestValidate_DuplicateCapabilities(t *testing.T) {
	in := strings.Replace(validManifestJSON, `["ssh.session"]`,
		`["ssh.session", "ssh.session"]`, 1)
	if err := Validate(mustParse(t, in), ""); !errors.Is(err, ErrDuplicateEntry) {
		t.Fatalf("expected ErrDuplicateEntry, got %v", err)
	}
}

func TestValidate_EmptyPermissionsAllowed(t *testing.T) {
	in := `{
	  "manifest_version": 1,
	  "identity": {"plugin_id": "com.engram.quiet", "version": "1.0.0"},
	  "runtime": {"protocol_version": 1, "entrypoint": "bin/plugin"}
	}`
	if err := Validate(mustParse(t, in), ""); err != nil {
		t.Fatalf("a plugin requesting nothing must be valid: %v", err)
	}
}

// ── Validator: entrypoint — самая важная часть ──────────────────────

func TestValidate_AbsoluteEntrypointRejected(t *testing.T) {
	for _, ep := range []string{"/bin/sh", "/home/user/anything", `\windows\system32\cmd.exe`, `C:\evil.exe`} {
		in := strings.Replace(validManifestJSON, `"entrypoint": "bin/plugin"`,
			`"entrypoint": "`+strings.ReplaceAll(ep, `\`, `\\`)+`"`, 1)
		m, err := ParseBytes([]byte(in))
		if err != nil {
			t.Fatalf("entrypoint %q: parse: %v", ep, err)
		}
		if err := Validate(m, ""); !errors.Is(err, ErrInvalidEntrypoint) {
			t.Fatalf("absolute entrypoint %q was accepted (err=%v)", ep, err)
		}
	}
}

func TestValidate_TraversalEntrypointRejected(t *testing.T) {
	for _, ep := range []string{"../../evil", "../evil", "bin/../../evil", ".."} {
		in := strings.Replace(validManifestJSON, `"entrypoint": "bin/plugin"`,
			`"entrypoint": "`+ep+`"`, 1)
		if err := Validate(mustParse(t, in), ""); !errors.Is(err, ErrEntrypointEscape) {
			t.Fatalf("traversal entrypoint %q was accepted (err=%v)", ep, err)
		}
	}
}

func TestValidate_SymlinkEscapeRejected(t *testing.T) {
	// Ключевой тест: filepath.Join очищает "..", но НИЧЕГО не знает о
	// символических ссылках. bin/plugin -> /etc/passwd прошёл бы
	// наивную проверку и запустил совсем не то, что лежит в пакете.
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "evil")
	if err := os.WriteFile(outside, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	link := filepath.Join(binDir, "plugin")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	err := Validate(mustParse(t, validManifestJSON), root)
	if !errors.Is(err, ErrEntrypointEscape) {
		t.Fatalf("symlink escaping the package root was accepted (err=%v)", err)
	}
}

func TestValidate_SymlinkInsidePackageAllowed(t *testing.T) {
	// Ссылка ВНУТРИ пакета — законный приём (bin/plugin -> bin/plugin-v2).
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	real := filepath.Join(binDir, "plugin-v2")
	if err := os.WriteFile(real, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink(real, filepath.Join(binDir, "plugin")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if err := Validate(mustParse(t, validManifestJSON), root); err != nil {
		t.Fatalf("symlink inside the package must be allowed: %v", err)
	}
}

func TestValidate_EntrypointNotFound(t *testing.T) {
	root := t.TempDir() // пусто, bin/plugin отсутствует
	if err := Validate(mustParse(t, validManifestJSON), root); !errors.Is(err, ErrEntrypointNotFound) {
		t.Fatalf("expected ErrEntrypointNotFound, got %v", err)
	}
}

func TestValidate_EntrypointIsDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bin", "plugin"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := Validate(mustParse(t, validManifestJSON), root); !errors.Is(err, ErrEntrypointNotFile) {
		t.Fatalf("expected ErrEntrypointNotFile, got %v", err)
	}
}

func TestValidate_SiblingDirectoryPrefixNotConfused(t *testing.T) {
	// "/tmp/pkg-evil" имеет строковый префикс "/tmp/pkg", но лежит ВНЕ
	// него. Проверка через strings.HasPrefix дала бы ложный пропуск —
	// поэтому в валидаторе используется filepath.Rel.
	base := t.TempDir()
	root := filepath.Join(base, "pkg")
	sibling := filepath.Join(base, "pkg-evil")
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(sibling, 0o755); err != nil {
		t.Fatalf("mkdir sibling: %v", err)
	}
	evil := filepath.Join(sibling, "evil")
	if err := os.WriteFile(evil, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink(evil, filepath.Join(root, "bin", "plugin")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if err := Validate(mustParse(t, validManifestJSON), root); !errors.Is(err, ErrEntrypointEscape) {
		t.Fatalf("sibling directory with a shared prefix was accepted (err=%v)", err)
	}
}

// ── Integrity ───────────────────────────────────────────────────────

func TestValidate_IntegrityIsStructuralOnly(t *testing.T) {
	// Поля разбираются, но подпись НЕ проверяется: семантика отложена
	// до спецификации trust/distribution policy.
	in := strings.Replace(validManifestJSON, `"capabilities": {"provided": ["ssh.session"]}`,
		`"capabilities": {"provided": ["ssh.session"]}, "integrity": {"signature": "deadbeef", "signing_key_id": "key-1"}`, 1)
	m := mustParse(t, in)
	if m.Integrity == nil || m.Integrity.Signature != "deadbeef" {
		t.Fatalf("integrity not parsed: %+v", m.Integrity)
	}
	root := newPackage(t)
	if err := Validate(m, root); err != nil {
		t.Fatalf("integrity must not be verified at this stage: %v", err)
	}
}

func TestValidate_MissingIntegrityAllowedForNow(t *testing.T) {
	root := newPackage(t)
	m := mustParse(t, validManifestJSON) // без integrity
	if err := Validate(m, root); err != nil {
		t.Fatalf("missing integrity must not fail validation yet: %v", err)
	}
}

// ── Fuzzing ─────────────────────────────────────────────────────────

func FuzzParseBytes(f *testing.F) {
	f.Add([]byte(validManifestJSON))
	f.Add([]byte(`{"manifest_version":1}`))
	f.Add([]byte(`{"required_x":1}`))
	f.Add([]byte(`{`))
	f.Add([]byte(``))
	f.Add([]byte(`{"runtime":{"entrypoint":"../../../etc/passwd"}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := ParseBytes(data)
		if err != nil {
			return // отказ — нормальный результат
		}
		if m == nil {
			t.Fatal("ParseBytes returned nil manifest with nil error")
		}
		// Если разобралось — валидатор не должен паниковать ни на чём.
		// Результат не важен, важно отсутствие паники.
		_ = Validate(m, "")
	})
}
