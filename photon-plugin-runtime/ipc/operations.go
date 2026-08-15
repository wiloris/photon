package ipc

// Соответствие операций и прав (docs/sdk-boundary.md, раздел 4).
//
// Таблица живёт ЗДЕСЬ, в runtime, а не в SDK: маппинг на стороне
// недоверенного процесса позволил бы плагину объявить, что
// storage.put требует storage.read.
//
// Проверка по точному имени, без вычислений и иерархий: storage.read
// не даёт storage.write.

// opPermissions — операция -> требуемое право.
//
// Отсутствие операции в этой карте означает, что runtime её не
// поддерживает: запрос отклоняется как UNSUPPORTED, а не выполняется
// с каким-нибудь правом по умолчанию.
var opPermissions = map[string]string{
	// storage.* — поверх ports.Storage
	"storage.get":    "storage.read",
	"storage.put":    "storage.write",
	"storage.delete": "storage.write",
	"storage.list":   "storage.read",

	// identity.* — поверх ports.Identity.
	// Приватный ключ не выдаётся ни одной операцией (ADR-021).
	"identity.public_key": "identity.read",
	"identity.sign":       "identity.sign",
	"identity.verify":     "identity.read",

	// crypto.* — поверх ports.Crypto.
	// Ключ выводит runtime по домену plugin.<id>; плагин его не видит.
	"crypto.encrypt": "crypto.use",
	"crypto.decrypt": "crypto.use",
}

// opsWithoutPermission — операции, не требующие права.
//
// Единственная такая операция: плагин должен уметь сообщить о
// проблеме, даже не получив никаких прав (docs/sdk-boundary.md,
// раздел 3). Объём ограничивается runtime, иначе это DoS через диск.
var opsWithoutPermission = map[string]bool{
	"log.write": true,
}

// permissionForOp возвращает право, требуемое операцией.
//
// Три различимых случая:
//
//	(perm, true,  true)  — операция известна, требует права perm
//	("",   true,  false) — операция известна, права не требует
//	("",   false, false) — операция неизвестна этому runtime
//
// Последний случай обязан приводить к UNSUPPORTED, а не к отказу в
// правах: «я не знаю такой операции» и «тебе нельзя» — разные ответы,
// и SDK должен их различать (docs/ipc.md, раздел 5).
func permissionForOp(op string) (permission string, known bool, requiresPermission bool) {
	if perm, ok := opPermissions[op]; ok {
		return perm, true, true
	}
	if opsWithoutPermission[op] {
		return "", true, false
	}
	return "", false, false
}
