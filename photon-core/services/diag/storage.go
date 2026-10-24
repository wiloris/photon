package diag

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// Storage — наблюдающий декоратор поверх ports.Storage (ADR-130 §2.2, §2.6).
//
// Наблюдение без вмешательства — инварианты, каждый проверен тестом на
// настоящем bbolt с декоратором и без (storage_test.go):
//   - ошибка, которую вернул адаптер, возвращается ТА ЖЕ — без обёртки и
//     без замены (== для sentinel, errors.Is/As по той же цепочке);
//   - fn исполняется ровно столько раз, сколько вызвал бы адаптер: декоратор
//     не повторяет транзакций и не открывает дополнительных;
//   - паника в fn не перехватывается: она проходит наружу, адаптер
//     откатывает транзакцию, как и без декоратора;
//   - сбой диагностики (переполненный журнал, выключенная диагностика) на
//     исход транзакции не влияет — записи ошибок не возвращают.
//
// Три измерения каждой транзакции, раздельно:
//   - ожидание: от вызова до входа в fn — захват писательской блокировки
//     bbolt и начало транзакции (у View — начало читающей);
//   - выполнение: от входа в fn до её возврата;
//   - фиксация: от возврата fn до возврата Update — commit с fsync либо
//     откат. У View фиксации нет.
type Storage struct {
	inner ports.Storage
	d     *Diag
	stats func() BoltStats // nil — статистики файла нет (подставленное хранилище)

	open      atomic.Bool
	schemaMax atomic.Int64 // наибольшая подтверждённая целевая версия Migrate

	viewN, updateN     atomic.Uint64
	viewErr, updateErr atomic.Uint64
	rollbacks, slowTxN atomic.Uint64
	viewWait, viewExec Histogram
	updWait, updExec   Histogram
	updCommit          Histogram
}

var _ ports.Storage = (*Storage)(nil)

// BoltStats — статистика файла bbolt для снимка. Заполняет владелец
// хранилища (читает (*bbolt.Storage).Stats() без транзакции и без
// писательской блокировки); пакет diag от адаптера не зависит.
type BoltStats struct {
	FileSize      int64 `json:"file_size"`
	FreePages     int   `json:"free_pages"`
	PendingPages  int   `json:"pending_pages"`
	FreeAlloc     int   `json:"free_alloc"`
	FreelistInuse int   `json:"freelist_inuse"`
	ReadTxStarted int   `json:"read_tx_started"`
	ReadTxOpen    int   `json:"read_tx_open"`
	PageCount     int64 `json:"page_count"`
	PageAlloc     int64 `json:"page_alloc"`
	Writes        int64 `json:"writes"`
	WriteTimeUS   int64 `json:"write_time_us"`
}

// WrapStorage возвращает inner под наблюдением d. На выключенной
// диагностике (d == nil) возвращает inner как есть — без декоратора, и
// поведение неотличимо от прежнего построением, а не проверкой.
func WrapStorage(inner ports.Storage, d *Diag, stats func() BoltStats) ports.Storage {
	if d == nil || inner == nil {
		return inner
	}
	s := &Storage{inner: inner, d: d, stats: stats}
	s.open.Store(true)
	d.attachStorage(s)
	return s
}

// StorageOpened записывает исход открытия хранилища. Открытие происходит
// ДО декоратора (его нечем обернуть, пока хранилища нет), поэтому событие
// пишет владелец: class — "ok", "locked" (файл занят другим владельцем,
// истёк таймаут) или "failed".
// start — отметка Mono() перед открытием.
func (d *Diag) StorageOpened(start int64, class string) {
	if d == nil {
		return
	}
	end := monotime()
	d.recordMono(end, CompStorage, KindStorageOpen, ConnNone, Num(KeyDurUS, (end-start)/1e3), Str(KeyClass, class))
}

const (
	txView   = "view"
	txUpdate = "update"
)

// View — см. ports.Storage.
func (s *Storage) View(ctx context.Context, fn func(tx ports.ReadTx) error) error {
	t0 := monotime()
	var tIn, tOut int64
	var fnErr error
	entered := false
	err := s.inner.View(ctx, func(tx ports.ReadTx) error {
		tIn = monotime()
		entered = true
		fnErr = fn(tx)
		tOut = monotime()
		return fnErr
	})
	// У View фиксации нет: после fn — только откат читающей транзакции.
	// Концом транзакции служит то же чтение часов, что и концом выполнения
	// (одним чтением HPET меньше на каждое чтение хранилища); если fn не
	// вызывалась — часы читаются здесь.
	t1 := tOut
	if !entered {
		t1 = monotime()
	}

	s.viewN.Add(1)
	if entered {
		s.viewWait.Observe(time.Duration(tIn - t0))
		s.viewExec.Observe(time.Duration(tOut - tIn))
	}
	if err != nil {
		s.viewErr.Add(1)
		s.d.recordMono(t1, CompStorage, KindTxFailed, ConnNone,
			Str(KeyTx, txView), Str(KeyClass, txErrorClass(err, fnErr, entered)),
			Bool(KeyStarted, entered), Num(KeyDurUS, time.Duration(t1-t0).Microseconds()))
	}
	if entered && time.Duration(t1-t0) >= s.d.SlowTx() {
		s.slowTxN.Add(1)
		s.d.recordMono(t1, CompStorage, KindTxSlow, ConnNone, Str(KeyTx, txView),
			Num(KeyWaitUS, time.Duration(tIn-t0).Microseconds()), Num(KeyExecUS, time.Duration(tOut-tIn).Microseconds()))
	}
	return err
}

// Update — см. ports.Storage.
func (s *Storage) Update(ctx context.Context, fn func(tx ports.WriteTx) error) error {
	t0 := monotime()
	var tIn, tOut int64
	var fnErr error
	entered := false
	err := s.inner.Update(ctx, func(tx ports.WriteTx) error {
		tIn = monotime()
		entered = true
		fnErr = fn(tx)
		tOut = monotime()
		return fnErr
	})
	t1 := monotime()

	s.updateN.Add(1)
	if entered {
		s.updWait.Observe(time.Duration(tIn - t0))
		s.updExec.Observe(time.Duration(tOut - tIn))
		s.updCommit.Observe(time.Duration(t1 - tOut))
	}
	if err != nil {
		// Откат = Update вернул ошибку после начала транзакции: bbolt
		// откатывает сам, декоратор только отмечает исход.
		s.updateErr.Add(1)
		if entered {
			s.rollbacks.Add(1)
		}
		s.d.recordMono(t1, CompStorage, KindTxFailed, ConnNone,
			Str(KeyTx, txUpdate), Str(KeyClass, txErrorClass(err, fnErr, entered)),
			Bool(KeyStarted, entered), Bool(KeyRolledBack, entered),
			Num(KeyDurUS, time.Duration(t1-t0).Microseconds()))
	}
	if entered && time.Duration(t1-t0) >= s.d.SlowTx() {
		s.slowTxN.Add(1)
		s.d.recordMono(t1, CompStorage, KindTxSlow, ConnNone, Str(KeyTx, txUpdate),
			Num(KeyWaitUS, time.Duration(tIn-t0).Microseconds()), Num(KeyExecUS, time.Duration(tOut-tIn).Microseconds()),
			Num(KeyCommitUS, time.Duration(t1-tOut).Microseconds()))
	}
	return err
}

// Migrate — см. ports.Storage. Версия «откуда» декоратору не видна: порт её
// не отдаёт, а читать служебный bucket адаптера в обход порта значило бы
// менять адаптер ради диагностики. В событии и снимке — подтверждённая
// целевая версия.
func (s *Storage) Migrate(ctx context.Context, targetVersion int) error {
	t0 := monotime()
	err := s.inner.Migrate(ctx, targetVersion)
	t1 := monotime()
	if err == nil {
		for {
			cur := s.schemaMax.Load()
			if int64(targetVersion) <= cur || s.schemaMax.CompareAndSwap(cur, int64(targetVersion)) {
				break
			}
		}
	}
	s.d.recordMono(t1, CompStorage, KindStorageMigrate, ConnNone,
		Num(KeyTo, int64(targetVersion)), Num(KeyDurUS, (t1-t0)/1e3),
		Str(KeyClass, outcomeClass(err)))
	return err
}

// Close — см. ports.Storage.
func (s *Storage) Close(ctx context.Context) error {
	t0 := monotime()
	err := s.inner.Close(ctx)
	t1 := monotime()
	s.open.Store(false)
	s.d.recordMono(t1, CompStorage, KindStorageClose, ConnNone,
		Num(KeyDurUS, (t1-t0)/1e3), Str(KeyClass, outcomeClass(err)))
	return err
}

// StorageSnapshot — секция хранилища в снимке (ADR-130 §2.5).
type StorageSnapshot struct {
	Open          bool   `json:"open"`
	SchemaVersion int64  `json:"schema_version"`
	ViewTx        uint64 `json:"view_tx"`
	UpdateTx      uint64 `json:"update_tx"`
	ViewErrors    uint64 `json:"view_errors"`
	UpdateErrors  uint64 `json:"update_errors"`
	Rollbacks     uint64 `json:"rollbacks"`
	SlowTx        uint64 `json:"slow_tx"`

	ViewWait     HistogramSnapshot `json:"view_wait"`
	ViewExec     HistogramSnapshot `json:"view_exec"`
	UpdateWait   HistogramSnapshot `json:"update_wait"`
	UpdateExec   HistogramSnapshot `json:"update_exec"`
	UpdateCommit HistogramSnapshot `json:"update_commit"`

	Bolt *BoltStats `json:"bolt,omitempty"`
}

func (s *Storage) snapshot() StorageSnapshot {
	ss := StorageSnapshot{
		Open:          s.open.Load(),
		SchemaVersion: s.schemaMax.Load(),
		ViewTx:        s.viewN.Load(),
		UpdateTx:      s.updateN.Load(),
		ViewErrors:    s.viewErr.Load(),
		UpdateErrors:  s.updateErr.Load(),
		Rollbacks:     s.rollbacks.Load(),
		SlowTx:        s.slowTxN.Load(),
		ViewWait:      s.viewWait.snapshot(),
		ViewExec:      s.viewExec.snapshot(),
		UpdateWait:    s.updWait.snapshot(),
		UpdateExec:    s.updExec.snapshot(),
		UpdateCommit:  s.updCommit.snapshot(),
	}
	// Статистика файла — только у открытого хранилища: у закрытого bbolt
	// путь и счётчики уже не про живую базу.
	if s.stats != nil && ss.Open {
		b := s.stats()
		ss.Bolt = &b
	}
	return ss
}

// txErrorClass — класс ошибки транзакции. Только класс и имя известного
// sentinel-а, без текста ошибки: текст ошибки fn принадлежит приложению и
// может нести его данные, а текст ошибки bbolt — путь к файлу (ADR-130 §2.8).
//
//   - not_started — транзакция не началась (ctx отменён до старта и т. п.);
//   - fn — ошибку вернула fn вызывающего;
//   - commit — fn вернула nil, а Update — ошибку: сбой фиксации.
//
// Известный sentinel порта или context добавляется через двоеточие:
// "fn:not_found", "not_started:canceled".
func txErrorClass(err, fnErr error, entered bool) string {
	base := "commit"
	switch {
	case !entered:
		base = "not_started"
	case fnErr != nil && errors.Is(err, fnErr):
		base = "fn"
	}
	if name := sentinelName(err); name != "" {
		return base + ":" + name
	}
	return base
}

// outcomeClass — исход служебной операции хранилища: ok либо класс ошибки.
func outcomeClass(err error) string {
	if err == nil {
		return "ok"
	}
	if name := sentinelName(err); name != "" {
		return name
	}
	return "error"
}

var knownSentinels = []struct {
	err  error
	name string
}{
	{ports.ErrNotFound, "not_found"},
	{ports.ErrNamespaceNotFound, "namespace_not_found"},
	{ports.ErrCursorOutOfPrefix, "cursor_out_of_prefix"},
	{context.Canceled, "canceled"},
	{context.DeadlineExceeded, "deadline"},
}

func sentinelName(err error) string {
	for _, k := range knownSentinels {
		if errors.Is(err, k.err) {
			return k.name
		}
	}
	return ""
}
