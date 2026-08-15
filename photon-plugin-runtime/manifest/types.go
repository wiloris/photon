package manifest

// Manifest — разобранный манифест плагина.
//
// Отражает ТОЛЬКО содержимое пакета (docs/manifest.md). Runtime state
// сюда не попадает и попасть не может: выданные права, PID, состояние
// процесса, счётчик перезапусков, идентификатор запуска Runtime — всё
// это принадлежит supervisor/registry и хранится отдельно.
//
// Причина строгости не стилистическая: если бы granted permissions
// оказались полем этой структуры, при следующей загрузке плагин
// предъявлял бы Runtime собственные полномочия — то есть недоверенная
// сторона определяла бы свои права (threat model, раздел 3).
type Manifest struct {
	// ManifestVersion — версия формата самого манифеста. Одна из трёх
	// независимых осей версионирования (вместе с ProtocolVersion и
	// Version).
	ManifestVersion int `json:"manifest_version"`

	Identity     Identity     `json:"identity"`
	Runtime      RuntimeSpec  `json:"runtime"`
	Permissions  Permissions  `json:"permissions"`
	Capabilities Capabilities `json:"capabilities"`
	Integrity    *Integrity   `json:"integrity,omitempty"`
}

// Identity — кто этот плагин.
type Identity struct {
	// PluginID — стабильный идентификатор в обратном доменном формате
	// ("com.engram.ssh"). Не зависит от имени файла и пути установки.
	// Ключ single-instance policy и ключ persistent-состояния.
	PluginID string `json:"plugin_id"`

	// Version — версия плагина ("1.4.2"). Обновление версии НЕ создаёт
	// новую identity: те же выданные права, то же состояние, тот же
	// trust level. Иначе каждое обновление сбрасывало бы решения
	// пользователя о правах.
	Version string `json:"version"`
}

// RuntimeSpec — как запускать и на каком протоколе говорить.
type RuntimeSpec struct {
	// ProtocolVersion — версия IPC-протокола Runtime <-> Plugin.
	// НЕ версия плагина: плагин 2.7.0 может говорить по протоколу 1.
	// Позволяет обновлять Runtime и SDK независимо от плагинов.
	ProtocolVersion int `json:"protocol_version"`

	// Entrypoint — путь к исполняемому файлу ОТНОСИТЕЛЬНО корня
	// пакета. Абсолютные пути, "..", symlink за пределы пакета
	// отвергаются валидатором: без этого манифест становится способом
	// заставить Runtime исполнить произвольный файл хоста.
	Entrypoint string `json:"entrypoint"`

	// InstancePolicy — сколько экземпляров допустимо. Пустое значение
	// трактуется как InstanceSingle.
	InstancePolicy InstancePolicy `json:"instance_policy,omitempty"`
}

// Permissions — что плагин ЗАПРАШИВАЕТ. Не то, что ему выдано.
type Permissions struct {
	// Requested — запрошенные права. Выданные (granted) хранятся
	// Runtime отдельно и в манифест не записываются никогда.
	Requested []string `json:"requested,omitempty"`
}

// Capabilities — что плагин УМЕЕТ предоставлять.
type Capabilities struct {
	// Provided сверяется с capabilities из HELLO_ACK при handshake:
	// плагин не может заявить в рантайме больше, чем декларировал при
	// установке.
	Provided []string `json:"provided,omitempty"`
}

// Integrity — метаданные подписи пакета.
//
// Поля объявлены с версии формата 1, чтобы их использование позже не
// потребовало менять ManifestVersion. Семантика проверки ОТЛОЖЕНА до
// спецификации trust/distribution policy (docs/manifest.md): не решено,
// кто вправе подписывать плагины, каким ключам Runtime доверяет и как
// отзывается скомпрометированный ключ.
//
// Валидатор проверяет только структурную корректность этих полей и
// НЕ проверяет саму подпись.
type Integrity struct {
	Signature    string `json:"signature,omitempty"`
	SigningKeyID string `json:"signing_key_id,omitempty"`
}

// InstancePolicy — политика количества экземпляров.
type InstancePolicy string

const (
	// InstanceSingle — один экземпляр на PluginID. Значение по
	// умолчанию.
	InstanceSingle InstancePolicy = "single"

	// InstanceMulti объявлен в формате, но текущий Runtime обязан
	// отклонять такой манифест как неподдерживаемый — вместо тихого
	// запуска в single-режиме, который создал бы расхождение между
	// заявленным и фактическим поведением.
	//
	// Реализация требует ответов, которых пока нет: чьё
	// persistent-состояние авторитетно, как делятся права, какой
	// экземпляр получает событие (docs/lifecycle.md, раздел 10).
	InstanceMulti InstancePolicy = "multi"
)

// SupportedManifestVersion — версия формата, поддерживаемая этим
// Runtime.
const SupportedManifestVersion = 1

// SupportedProtocolVersion — версия IPC-протокола, поддерживаемая этим
// Runtime.
const SupportedProtocolVersion = 1
