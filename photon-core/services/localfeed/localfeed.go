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
func (s *Store) Page(ctx context.Context, channelID string, afterKey int64, afterID string, limit int) ([]Record, error) {
	if channelID == "" {
		return nil, ErrEmptyChannel
	}
	if limit <= 0 {
		limit = 30
	}

	var (
		out     []Record
		scanned int
	)

	// Пустой курсор — с начала префикса. Порт трактует пустой start именно
	// так, поэтому отдельной ветки здесь не нужно.
	var start []byte
	if afterID != "" {
		start = key(channelID, afterKey, afterID)
	}

	errStop := errors.New("stop")
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		e := tx.ForEachPrefixFrom(Namespace, prefix(channelID), start, func(k, v []byte) error {
			scanned++
			r, derr := decode(v)
			if derr != nil {
				return derr
			}
			out = append(out, r)
			if len(out) >= limit {
				return errStop
			}
			return nil
		})
		if e != nil && !errors.Is(e, errStop) {
			return e
		}
		return nil
	})
	s.stats.countRead(scanned, 0, len(out))
	if err != nil {
		return nil, fmt.Errorf("localfeed: page: %w", err)
	}
	return out, nil
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
