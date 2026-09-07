// Package ports содержит только интерфейсы ядра Photon. Реализации
// живут в adapters/ (см. ARCHITECTURE.md, раздел 3; ADR-002).
package ports

import "context"

// Storage — порт персистентного хранения ядра.
// Первая реализация — adapters/bbolt (см. ROADMAP.md, Phase 2).
//
// Namespace вместо bucket, схема только через Migrate — см. ADR-010.
// Транзакции — closure-style (View/Update), не специфика bbolt —
// см. ADR-010. Проверка на утечку реализации — ADR-011/013.
type Storage interface {
	// View выполняет транзакцию только на чтение.
	View(ctx context.Context, fn func(tx ReadTx) error) error

	// Update выполняет транзакцию на чтение и запись. Ошибка внутри
	// fn откатывает все изменения транзакции.
	Update(ctx context.Context, fn func(tx WriteTx) error) error

	// Migrate приводит схему к targetVersion; должна быть
	// идемпотентной. Единственное место, где namespace начинает
	// существовать — см. ErrNamespaceNotFound ниже.
	Migrate(ctx context.Context, targetVersion int) error

	// Close освобождает ресурсы хранилища (файловые дескрипторы,
	// фоновые горутины).
	Close(ctx context.Context) error
}

// ReadTx — операции чтения внутри транзакции.
type ReadTx interface {
	// Get возвращает значение по ключу. ErrNotFound — ключа нет;
	// ErrNamespaceNotFound — namespace не подготовлен Migrate.
	Get(namespace string, key []byte) ([]byte, error)

	// ForEach обходит все пары namespace в порядке, определяемом
	// адаптером.
	ForEach(namespace string, fn func(key, value []byte) error) error

	// ForEachPrefix — как ForEach, но только пары с ключом,
	// начинающимся на prefix (см. использование в Engram: листинг по
	// префиксу).
	ForEachPrefix(namespace string, prefix []byte, fn func(key, value []byte) error) error

	// ForEachPrefixFrom — как ForEachPrefix, но обход начинается СТРОГО
	// ПОСЛЕ start. Множество выдачи задаётся одной формулой:
	//
	//	{ k : k > start И HasPrefix(k, prefix) }, в возрастающем
	//	байтовом порядке ключей
	//
	// Из неё следует всё: запись с ключом == start не выдаётся; первая
	// выданная строго больше start; отсутствие самого start в хранилище
	// на результат не влияет (курсорная запись могла быть удалена между
	// страницами); обход не выходит за границу prefix.
	//
	// Два правила поверх формулы:
	//   - пустой start ЭКВИВАЛЕНТЕН ForEachPrefix (обход с начала
	//     префикса) — композиционное свойство, проверяемое сравнением
	//     выдач, а не краевой случай;
	//   - непустой start, не принадлежащий prefix, — ОШИБКА вызывающего
	//     (ErrCursorOutOfPrefix), а не пустая выдача: такой start
	//     возникает ровно одним способом — курсор чужого префикса, и
	//     тихая пустая выдача выглядела бы как «данные закончились».
	//
	// ВАЖНО: метод осмыслен ТОЛЬКО при упорядоченном обходе. ForEach и
	// ForEachPrefix документированы как обход «в порядке, определяемом
	// адаптером»; здесь порядок — часть контракта, и адаптер обязан его
	// обеспечить. Реализация, выполнившая сигнатуру без порядка,
	// выполнила её формально и не выполнила по смыслу.
	//
	// Зачем нужен (ADR-047): без него N-я страница стоит O(N·limit) —
	// предыдущие приходится пройти и выбросить. Измерено: страница №100
	// просматривала 5000 записей ради 50.
	ForEachPrefixFrom(namespace string, prefix, start []byte, fn func(key, value []byte) error) error
}

// WriteTx — операции чтения и записи внутри Update.
type WriteTx interface {
	ReadTx

	// Put записывает значение. ErrNamespaceNotFound, если namespace
	// не подготовлен Migrate — порт не создаёт namespace неявно.
	Put(namespace string, key, value []byte) error

	// Delete удаляет ключ. Удаление отсутствующего ключа — не ошибка.
	Delete(namespace string, key []byte) error
}
