// Package localfeed — локальное хранилище ленты одного канала.
//
// ЭТО НЕ ПОРТ И НЕ ФРЕЙМВОРК СИНХРОНИЗАЦИИ. Внутренняя механика первого
// вертикального среза Phase 12, живущая в services/ ровно по правилу ADR-046
// §6.1: пока паттерн доступа не наблюдался, синхронизация не превращается в
// публичный контракт. Сервис переписывается свободно; порт — нет.
//
// Что здесь НАМЕРЕННО отсутствует и появиться не должно до отдельного решения:
// обобщение на другие сущности, вытеснение, квоты, шифрование, слияние
// конфликтов, подписка на события. Срез отвечает на восемь вопросов
// CURRENT_TASK.md, а не строит кеш.
//
// Доменных типов ENGRAM здесь нет: запись хранится как непрозрачные байты,
// а ключ порядка сообщает вызывающий (вариант C, ADR-046).
package localfeed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/wiloris/photon-core/ports"
)

// Namespace хранилища. Один на срез: обобщать рано.
const Namespace = "photon.localfeed"

// SchemaVersion — версия схемы для Migrate. Namespace не создаётся неявно
// (ADR-010), поэтому версия нужна с самого начала, а не «когда понадобится».
const SchemaVersion = 1

var ErrEmptyChannel = errors.New("localfeed: channel id is required")

// Record — единица синхронизации среза.
//
// Тело непрозрачно: Core не разбирает и не валидирует его как домен. Порядок
// и идентичность сообщает вызывающий — в этом и состоит вариант C.
type Record struct {
	// ID — идентификатор записи у источника. Второй компонент ключа порядка:
	// OrderKey в миллисекундах НЕ уникален, и без ID две записи одной
	// миллисекунды схлопнулись бы в одну.
	ID string

	// OrderKey — ключ порядка, сообщённый вызывающим (для Mattermost это
	// create_at). Core не знает, что это время, и не обязан знать.
	OrderKey int64

	// Version — версия записи у источника (для Mattermost update_at).
	// Нужна, чтобы отличить «не менялось» от «менялось незаметно»: без неё
	// повторная выдача той же записи неотличима от обновления.
	Version int64

	// Body — непрозрачные байты. Core их не разбирает.
	Body []byte
}

// Store — локальное хранилище ленты поверх ports.Storage.
//
// Никаких новых портов: используется существующий контракт как есть, включая
// его ограничения. Наблюдение за тем, где эти ограничения начинают мешать, —
// главный результат среза.
type Store struct {
	st ports.Storage

	// stats — журнал фактического access pattern. Не диагностика «на всякий
	// случай»: по условию Phase 12 это основной результат среза.
	stats *Stats
}

func New(st ports.Storage, stats *Stats) *Store {
	if stats == nil {
		stats = &Stats{}
	}
	return &Store{st: st, stats: stats}
}

// Prepare подготавливает namespace. Отдельный шаг, потому что ports.Storage
// не создаёт namespace неявно (ADR-010).
func (s *Store) Prepare(ctx context.Context) error {
	return s.st.Migrate(ctx, SchemaVersion)
}

// key кодирует порядок прямо в ключе — единственный способ получить
// упорядоченное чтение через ForEachPrefix, потому что порт порядок обхода
// НЕ гарантирует («в порядке, определяемом адаптером»).
//
// Формат: <channel> 0x00 <inverted OrderKey, 8 байт BE> <ID>
//
// OrderKey инвертируется (MaxInt64 - x), чтобы САМЫЕ СВЕЖИЕ записи шли
// первыми при возрастающем обходе. Это прямое следствие ограничения:
// ForEachPrefix умеет идти только от начала префикса, обратного обхода нет.
// Открытие канала показывает свежее — значит свежее обязано лежать в начале.
//
// Big-endian обязателен: только он сохраняет числовой порядок в байтовом
// сравнении. Little-endian дал бы порядок, не имеющий отношения к времени.
func key(channelID string, orderKey int64, id string) []byte {
	inv := ^uint64(0) - uint64(orderKey)
	b := make([]byte, 0, len(channelID)+1+8+len(id))
	b = append(b, channelID...)
	b = append(b, 0x00)
	var ord [8]byte
	binary.BigEndian.PutUint64(ord[:], inv)
	b = append(b, ord[:]...)
	b = append(b, id...)
	return b
}

func prefix(channelID string) []byte {
	b := make([]byte, 0, len(channelID)+1)
	b = append(b, channelID...)
	b = append(b, 0x00)
	return b
}

// Put сохраняет записи. Идемпотентен: ключ детерминирован, повторная запись
// той же версии не создаёт дубля — свойство 3 из шести обязательных (ADR-046 §2).
//
// Put НИЧЕГО НЕ УТВЕРЖДАЕТ О НЕПРЕРЫВНОСТИ: он кладёт записи и только. Для
// догрузки после offline нужен Ingest — он записывает ещё и то, какой участок
// ленты после этого известен полностью (docs/feed-catchup.md §4.1). Разделение
// намеренное: «положить пять записей» и «эти пять — всё, что было между X и Y»
// разные утверждения, и Put второго сделать не может, потому что не знает
// запроса, породившего записи.
func (s *Store) Put(ctx context.Context, channelID string, recs []Record) error {
	if channelID == "" {
		return ErrEmptyChannel
	}
	return s.st.Update(ctx, func(tx ports.WriteTx) error {
		for _, r := range recs {
			v, err := encode(r)
			if err != nil {
				return err
			}
			if err := tx.Put(Namespace, key(channelID, r.OrderKey, r.ID), v); err != nil {
				return err
			}
			s.stats.countWrite()
		}
		return nil
	})
}

// IngestResult — что стало известно после блока.
type IngestResult struct {
	// Stored — сколько записей записано.
	Stored int
	// Segments — сколько непрерывных участков у канала ПОСЛЕ слияния.
	// Уменьшение означает закрытый пропуск; рост — новый.
	Segments int
	// Updated — записей, у которых пришла версия СТАРШЕ локальной; тело
	// перезаписано.
	Updated int

	// Unchanged — версия совпала с локальной. Запись всё равно
	// перезаписывается: тело при равной версии обязано совпадать, а если не
	// совпало — источник нарушил собственный контракт, и локальная копия не
	// авторитетнее пришедшей.
	Unchanged int

	// Stale — пришла версия МЛАДШЕ локальной. Такая запись НЕ применяется:
	// доставка может переупорядочиться, и «откат» записи к прежнему телу был
	// бы потерей уже известного обновления.
	Stale int

	// Vanished — записей, которые лежат локально внутри диапазона блока, но
	// в самом блоке отсутствуют.
	//
	// Это ЕДИНСТВЕННЫЙ способ увидеть удаление: живой прогон показал, что
	// удалённый пост просто исчезает из ленты — надгробия нет, delete_at в
	// выдаче не приходит. Сегмент утверждает полноту «на момент выборки»,
	// поэтому запись, попавшая в покрытый диапазон и не пришедшая, у
	// источника отсутствует.
	//
	// Поле СЧИТАЕТ, но ничего не удаляет: политика удаления — отдельное
	// решение, и принимать его этим срезом рано (feed-reconcile.md §8).
	Vanished int

	// GapsClosed — на сколько пропусков стало МЕНЬШЕ.
	//
	// ОТРИЦАТЕЛЬНОЕ значение законно и означает противоположное: блок не
	// сомкнулся с известным и породил НОВЫЙ пропуск. Именно так выглядит
	// первый шаг возвращения после offline — самые свежие записи повисают
	// отдельным островом над старым куском.
	//
	// Прежняя редакция этого комментария утверждала, что отрицательным
	// значение не бывает. Первый же прогон против живого Bridge вернул -1;
	// рассуждение было неверным, и записано это здесь, а не исправлено
	// молча.
	GapsClosed int
}

// Ingest записывает блок и то, что этот блок ДОКАЗАЛ о полноте ленты.
//
// Записи и сегменты пишутся ОДНОЙ транзакцией. Порознь их писать нельзя, и
// это не педантизм: записи без сегмента — область, которую читатель считает
// неизвестной, хотя данные есть; сегмент без записей — заявленная полнота
// поверх пустоты, то есть тихая дыра ровно того вида, против которого весь
// механизм и заведён. Атомарность даёт ports.Storage.Update, поэтому
// дополнительной машинерии не нужно.
func (s *Store) Ingest(ctx context.Context, channelID string, b Block) (IngestResult, error) {
	if channelID == "" {
		return IngestResult{}, ErrEmptyChannel
	}

	var res IngestResult
	err := s.st.Update(ctx, func(tx ports.WriteTx) error {
		before, err := loadSegments(tx, channelID)
		if err != nil {
			return err
		}

		after, err := applyBlock(before, b, b.Records)
		if err != nil {
			return err
		}

		// Согласование по версии. До четвёртого среза Ingest клал вслепую:
		// существующую запись не читал и версий не сравнивал, поэтому
		// переупорядоченная доставка молча откатывала бы запись к прежнему
		// телу. Version при этом уже существовал и уже заполнялся из
		// update_at — не читался нигде.
		var upd, same, stale int
		for _, r := range b.Records {
			k := key(channelID, r.OrderKey, r.ID)
			prev, err := tx.Get(Namespace, k)
			if err != nil && !errors.Is(err, ports.ErrNotFound) {
				return err
			}
			if prev != nil {
				old, derr := decode(prev)
				if derr != nil {
					return derr
				}
				switch {
				case r.Version < old.Version:
					stale++
					continue // откат запрещён
				case r.Version == old.Version:
					same++
				default:
					upd++
				}
			}
			v, err := encode(r)
			if err != nil {
				return err
			}
			if err := tx.Put(Namespace, k, v); err != nil {
				return err
			}
			s.stats.countWrite()
		}

		vanished, err := countVanished(tx, channelID, b)
		if err != nil {
			return err
		}

		enc, err := encodeSegments(after)
		if err != nil {
			return err
		}
		if err := tx.Put(Namespace, segmentsKey(channelID), enc); err != nil {
			return err
		}

		res = IngestResult{
			Stored:     len(b.Records) - stale,
			Updated:    upd,
			Unchanged:  same,
			Stale:      stale,
			Vanished:   vanished,
			Segments:   len(after),
			GapsClosed: gapCount(before) - gapCount(after),
		}
		return nil
	})
	if err != nil {
		return IngestResult{}, fmt.Errorf("localfeed: ingest: %w", err)
	}
	return res, nil
}

// gapCount — число пропусков между сегментами. Ноль сегментов — тоже ноль
// пропусков: ничего не известно, но и дыр не заявлено.
func gapCount(segs []Segment) int {
	if len(segs) == 0 {
		return 0
	}
	return len(segs) - 1
}

// PageResult — страница и то, ЧТО ЗА НЕЙ.
//
// Отдельный тип, а не []Record, потому что «дальше ничего нет» и «дальше я не
// знаю» — разные ответы, а выглядят одинаково: пустое продолжение. Именно на
// этом различии держится весь механизм сегментов.
type PageResult struct {
	Records []Record

	// GapAfter — за последней отданной записью следует ПРОПУСК: ниже есть
	// известные записи, но между ними и этой лежит неизвестное. Читатель
	// обязан показать это как разрыв, а не склеить.
	//
	// Ниже САМОГО СТАРОГО сегмента GapAfter не ставится: там не пропуск, а
	// граница уже загруженного — её закрывает обычная догрузка назад
	// (before), и путать эти два случая нельзя.
	GapAfter bool
}

// Page читает страницу в порядке «свежие первыми».
//
// afterKey/afterID — позиция, ПОСЛЕ которой продолжать (для «загрузить ещё»).
// Пустой afterID означает «с начала», то есть с самых свежих.
//
// С ADR-047 позиционирование выполняет сам порт: ForEachPrefixFrom встаёт на
// первый ключ строго после курсора родным Seek за O(log n). Прежняя редакция
// проходила и выбрасывала все предыдущие страницы вручную — стоимость N-й
// страницы была O(N·limit), что и было измерено (страница №100 просматривала
// 5000 записей ради 50).
//
// Счётчик skipped сохранён намеренно: он обязан остаться НУЛЁМ. Ненулевое
// значение означало бы, что линейный пропуск вернулся, — журнал ловит это
// без отдельного теста.
//
// ОБХОД ОСТАНАВЛИВАЕТСЯ НА ГРАНИЦЕ СЕГМЕНТА. Прежняя сигнатура возвращала
// []Record и о пропусках сообщить не могла; она удалена, а не оставлена
// рядом, — вариант, не умеющий сказать «дальше неизвестно», это ровно тот
// вызов, которым тихая дыра попадает на экран.
//
// Канал без сегментов (данные легли через Put, а не Ingest) читается как
// раньше: разрывов не заявляется. Это не потеря строгости, а честность —
// утверждать пропуск там, где о полноте ничего не известно, значило бы
// выдумывать знание.
func (s *Store) Page(ctx context.Context, channelID string, afterKey int64, afterID string, limit int) (PageResult, error) {
	if channelID == "" {
		return PageResult{}, ErrEmptyChannel
	}
	if limit <= 0 {
		limit = 30
	}

	var (
		res     PageResult
		scanned int
	)

	// Пустой курсор — с начала префикса. Порт трактует пустой start именно
	// так, поэтому отдельной ветки здесь не нужно.
	var start []byte
	if afterID != "" {
		start = key(channelID, afterKey, afterID)
	}

	errStop := errors.New("stop")
	errGap := errors.New("gap")
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		segs, e := loadSegments(tx, channelID)
		if e != nil {
			return e
		}

		curSeg := -2 // -2 = ещё не определён; -1 = запись вне сегментов
		e = tx.ForEachPrefixFrom(Namespace, prefix(channelID), start, func(k, v []byte) error {
			scanned++
			r, derr := decode(v)
			if derr != nil {
				return derr
			}
			if len(segs) > 0 {
				idx := segmentIndex(segs, Position{OrderKey: r.OrderKey, ID: r.ID})
				if curSeg == -2 {
					curSeg = idx
				} else if idx != curSeg {
					// Пересекли границу: запись НЕ отдаём, страница
					// заканчивается здесь.
					return errGap
				}
			}
			if len(res.Records) >= limit {
				// Страница уже набрана. Эта запись прочитана ТОЛЬКО ради
				// проверки выше — не начинается ли за границей страницы
				// другой сегмент. В выдачу она не идёт.
				//
				// Заглядывание на одну запись вперёд обязательно. Без него
				// пропуск не виден, когда сегмент кончается ровно на
				// границе страницы: limit достигался раньше проверки, и
				// разрыв молча склеивался. Найдено живым прогоном —
				// при page-size 3 и сегменте из 3 записей разрыв исчезал,
				// а при page-size 30 на тех же данных был виден.
				return errStop
			}
			res.Records = append(res.Records, r)
			if len(segs) == 0 && len(res.Records) >= limit {
				// Сегментов нет — смотреть вперёд незачем, и лишнюю запись
				// читать тоже: стоимость чтения не меняется.
				return errStop
			}
			return nil
		})
		switch {
		case errors.Is(e, errGap):
			res.GapAfter = true
		case e != nil && !errors.Is(e, errStop):
			return e
		}
		return nil
	})
	s.stats.countRead(scanned, 0, len(res.Records))
	if err != nil {
		return PageResult{}, fmt.Errorf("localfeed: page: %w", err)
	}
	return res, nil
}

// segmentIndex — номер сегмента, содержащего позицию, или -1.
//
// Линейный поиск сознателен: сегментов единицы, и бинарный поиск здесь
// сложнее для чтения при выигрыше, который нечем измерить. Если число
// сегментов начнёт расти — это само по себе находка, и журнал её покажет.
func segmentIndex(segs []Segment, p Position) int {
	for i, sg := range segs {
		if sg.contains(p) {
			return i
		}
	}
	return -1
}

// Count — сколько записей канала лежит локально.
func (s *Store) Count(ctx context.Context, channelID string) (int, error) {
	n := 0
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		return tx.ForEachPrefix(Namespace, prefix(channelID), func([]byte, []byte) error {
			n++
			return nil
		})
	})
	return n, err
}
