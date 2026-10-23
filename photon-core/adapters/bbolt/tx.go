package bbolt

import (
	"bytes"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-core/ports"
)

// readTx реализует ports.ReadTx. namespace маппится на top-level
// bbolt bucket один в один — это деталь именно этого адаптера (см.
// ADR-010: порт не специфицирует, как namespace устроен физически).
type readTx struct {
	tx *bolt.Tx
}

var _ ports.ReadTx = (*readTx)(nil)

func (r *readTx) Get(namespace string, key []byte) ([]byte, error) {
	b := r.tx.Bucket([]byte(namespace))
	if b == nil {
		return nil, ports.ErrNamespaceNotFound
	}
	v := b.Get(key)
	if v == nil {
		return nil, ports.ErrNotFound
	}
	// Значение, возвращённое bbolt, валидно только на время жизни
	// транзакции — копируем перед тем, как отдать вызывающему коду,
	// который может использовать его после завершения View/Update.
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (r *readTx) ForEach(namespace string, fn func(key, value []byte) error) error {
	b := r.tx.Bucket([]byte(namespace))
	if b == nil {
		return ports.ErrNamespaceNotFound
	}
	// Копируем key/value перед передачей в fn — та же причина, что в
	// Get: срезы bbolt валидны только на время жизни транзакции.
	// Семантика ForEach обязана совпадать с Get (единообразие важнее
	// экономии на копировании), иначе вызывающий код должен помнить,
	// какой метод порта безопасно использовать после транзакции, а
	// какой — нет.
	return b.ForEach(func(k, v []byte) error {
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		return fn(keyCopy, valCopy)
	})
}

func (r *readTx) ForEachPrefix(namespace string, prefix []byte, fn func(key, value []byte) error) error {
	b := r.tx.Bucket([]byte(namespace))
	if b == nil {
		return ports.ErrNamespaceNotFound
	}
	c := b.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		if err := fn(keyCopy, valCopy); err != nil {
			return err
		}
	}
	return nil
}

// ForEachPrefixFrom — обход строго после start (ADR-047).
//
// Позиционирование выполняется РОДНЫМ Cursor.Seek за O(log n) по B-дереву:
// линейного пропуска записей до курсора здесь нет и быть не должно — ради
// этого метод и заводился. Единственный шаг Next() после точного попадания в
// start обеспечивает строгую исключаемость и линейным пропуском не является.
func (r *readTx) ForEachPrefixFrom(namespace string, prefix, start []byte, fn func(key, value []byte) error) error {
	b := r.tx.Bucket([]byte(namespace))
	if b == nil {
		return ports.ErrNamespaceNotFound
	}

	// Пустой start — обход с начала префикса, то есть в точности
	// ForEachPrefix. Композиционное свойство контракта, а не краевой случай:
	// проверяется сравнением выдач.
	if len(start) == 0 {
		return r.ForEachPrefix(namespace, prefix, fn)
	}

	// Непустой start обязан принадлежать префиксу. Иначе это курсор чужого
	// префикса — ошибка вызывающего, и молчать о ней нельзя (ADR-047 §2).
	if !bytes.HasPrefix(start, prefix) {
		return ports.ErrCursorOutOfPrefix
	}

	c := b.Cursor()
	k, v := c.Seek(start)
	// Seek встаёт на первый ключ >= start. Если это ровно start — шагаем
	// один раз: контракт требует СТРОГО больше.
	if k != nil && bytes.Equal(k, start) {
		k, v = c.Next()
	}
	for ; k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		// Копирование по той же причине, что в Get и ForEach: срезы bbolt
		// валидны только на время жизни транзакции.
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		if err := fn(keyCopy, valCopy); err != nil {
			return err
		}
	}
	return nil
}

// writeTx реализует ports.WriteTx, встраивая readTx для операций
// чтения — тот же bbolt.Tx используется для чтения и записи внутри
// одной транзакции Update.
type writeTx struct {
	*readTx
}

var _ ports.WriteTx = (*writeTx)(nil)

func (w *writeTx) Put(namespace string, key, value []byte) error {
	b := w.tx.Bucket([]byte(namespace))
	if b == nil {
		return ports.ErrNamespaceNotFound
	}
	return b.Put(key, value)
}

func (w *writeTx) Delete(namespace string, key []byte) error {
	b := w.tx.Bucket([]byte(namespace))
	if b == nil {
		return ports.ErrNamespaceNotFound
	}
	return b.Delete(key)
}
