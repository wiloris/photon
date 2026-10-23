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

	// p2p.* — поверх ports.P2P (ADR-076). Право ОДНО на весь домен:
	// «согласовать, но не отправлять» не является осмысленным состоянием,
	// и пара прав создала бы различие, за которым нет ни одного сценария
	// (ADR-076 §3.5).
	//
	// СОСТОЯНИЕ: исполнителя ещё нет. Это условие 4 из шести
	// (docs/sdk-boundary.md, «Порядок добавления домена операций»), и оно
	// требует вносить имена в каталог и в документ ОДНОВРЕМЕННО — иначе
	// расхождение спецификации и кода, ровно то, что нашёл E2E Phase 10.
	// Пока условие 5 не выполнено, эти операции проходят проверку права и
	// получают UNSUPPORTED от coreops. Такое состояние ГРОМКОЕ и временное;
	// почему оно не нарушает принцип отбора §3 — там же, в sdk-boundary.md.
	"p2p.create_offer": "p2p.connect",
	"p2p.accept_offer": "p2p.connect",
	"p2p.complete":     "p2p.connect",
	"p2p.peer":         "p2p.connect",
	"p2p.send":         "p2p.connect",
	"p2p.close":        "p2p.connect",
}

// opsWithoutPermission — операции, не требующие права.
//
// Две такие операции, и обе по одной причине: плагин обязан оставаться
// наблюдаемым и управляемым, даже не получив ни одного права.
//
//	log.write     — сообщить о проблеме (docs/sdk-boundary.md, раздел 3).
//	                Объём ограничивается runtime, иначе это DoS через диск.
//	work.complete — доложить, что текущая единица работы закончена
//	                (ADR-072). Право на этот доклад превратило бы «право не
//	                выдано» в «хост не дождётся завершения», то есть в
//	                зависание вместо отказа.
var opsWithoutPermission = map[string]bool{
	"log.write":     true,
	"work.complete": true,
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
