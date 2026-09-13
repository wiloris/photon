package localfeed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/wiloris/photon-core/ports"
)

// Сегменты — знание о ПОЛНОТЕ, а не о содержимом.
//
// До этого файла локальная лента была множеством записей, и вопрос «между
// этими двумя сообщениями действительно ничего не было или я просто не
// знаю?» ответа не имел. Для пролистывания назад он и не возникал: любое
// подмножество читается одинаково. Он возникает при догрузке после offline —
// см. docs/feed-catchup.md.
//
// Сегмент утверждает: между Oldest и Newest включительно локально лежит
// КАЖДАЯ запись, которая была у сервера на момент выборки. Между соседними
// сегментами — пропуск.

// Position — позиция записи в ленте.
type Position struct {
	OrderKey int64  `json:"order_key"`
	ID       string `json:"id"`
}

// newerThan — порядок, ОБЯЗАННЫЙ совпадать с порядком обхода ключей.
//
// Ключ записи — <channel> 0x00 <inv(OrderKey) BE> <ID>. Возрастающий байтовый
// обход даёт OrderKey по убыванию, а при равном OrderKey — ID по
// ВОЗРАСТАНИЮ. Значит среди одинакового OrderKey более свежим считается
// МЕНЬШИЙ ID. Инверсия не описка: другое сравнение разошлось бы с обходом
// молча, и сегменты перестали бы соответствовать тому, что читается.
func (p Position) newerThan(q Position) bool {
	if p.OrderKey != q.OrderKey {
		return p.OrderKey > q.OrderKey
	}
	return p.ID < q.ID
}

func (p Position) equals(q Position) bool { return p.OrderKey == q.OrderKey && p.ID == q.ID }

func newest(a, b Position) Position {
	if a.newerThan(b) {
		return a
	}
	return b
}

func oldest(a, b Position) Position {
	if a.newerThan(b) {
		return b
	}
	return a
}

// Segment — непрерывный участок ленты, обе границы включительно.
type Segment struct {
	Oldest Position `json:"oldest"`
	Newest Position `json:"newest"`
}

// contains — позиция лежит внутри сегмента (границы включительно).
func (s Segment) contains(p Position) bool {
	return !p.newerThan(s.Newest) && !s.Oldest.newerThan(p)
}

// overlaps — сегменты пересекаются хотя бы одной позицией.
func (s Segment) overlaps(o Segment) bool {
	return s.contains(o.Newest) || s.contains(o.Oldest) ||
		o.contains(s.Newest) || o.contains(s.Oldest)
}

func (s Segment) union(o Segment) Segment {
	return Segment{Oldest: oldest(s.Oldest, o.Oldest), Newest: newest(s.Newest, o.Newest)}
}

// Direction — чем был запрос, породивший блок.
type Direction int

const (
	// DirNewest — «самые свежие» без курсора.
	DirNewest Direction = iota
	// DirAfter — записи НОВЕЕ якоря.
	DirAfter
	// DirBefore — записи СТАРШЕ якоря.
	DirBefore
)

// Block — результат ОДНОГО ответа сервера вместе с тем, что он доказал.
//
// Записи сами по себе не говорят о полноте: одни и те же пять записей могут
// быть «всё, что есть между X и Y» и «пять из ста». Разницу несёт запрос, а
// не ответ, — поэтому Anchor и Dir обязательны, а не удобны.
type Block struct {
	Records []Record

	// Anchor — курсор запроса. nil для DirNewest.
	Anchor *Position

	Dir Direction

	// AtEnd — сервер вернул МЕНЬШЕ запрошенного, значит блок дошёл до края.
	// Для DirAfter это «выше ничего нет» и единственный случай, меняющий
	// алгебру сегментов: всё, что лежит выше, оказывается частью той же
	// непрерывной верхушки (docs/feed-catchup.md §4.1).
	AtEnd bool
}

var (
	// ErrAnchorRequired — DirAfter/DirBefore без якоря. Молча трактовать
	// такой блок как «самые свежие» нельзя: он объявил бы непрерывным
	// участок, который сервер не подтверждал.
	ErrAnchorRequired = errors.New("localfeed: anchor is required for DirAfter/DirBefore")

	// ErrEmptyBlock — блок без записей и без якоря не доказывает ничего.
	ErrEmptyBlock = errors.New("localfeed: block without records and without anchor proves nothing")
)

// segmentsKey — ключ списка сегментов канала.
//
// Разделитель 0x01 против 0x00 у записей: пространства не пересекаются, и
// ForEachPrefix по записям сегменты не видит. Один ключ на канал, а не ключ на
// сегмент — операция всегда затрагивает список целиком (слияние), и одна
// запись даёт атомарность без согласования нескольких ключей.
func segmentsKey(channelID string) []byte {
	b := make([]byte, 0, len(channelID)+1)
	b = append(b, channelID...)
	b = append(b, 0x01)
	return b
}

func encodeSegments(segs []Segment) ([]byte, error) { return json.Marshal(segs) }

func decodeSegments(v []byte) ([]Segment, error) {
	var segs []Segment
	if err := json.Unmarshal(v, &segs); err != nil {
		return nil, fmt.Errorf("localfeed: decode segments: %w", err)
	}
	return segs, nil
}

// loadSegments читает список внутри уже открытой транзакции.
func loadSegments(tx ports.ReadTx, channelID string) ([]Segment, error) {
	v, err := tx.Get(Namespace, segmentsKey(channelID))
	if errors.Is(err, ports.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeSegments(v)
}

// normalize приводит список к каноническому виду: свежие первыми,
// пересекающиеся слиты. Вызывается после КАЖДОГО изменения — инвариант
// «непересекающиеся и упорядоченные» держится структурно, а не по дисциплине
// вызывающих.
func normalize(segs []Segment) []Segment {
	if len(segs) == 0 {
		return nil
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].Newest.newerThan(segs[j].Newest) })
	out := []Segment{segs[0]}
	for _, s := range segs[1:] {
		last := &out[len(out)-1]
		if last.overlaps(s) {
			*last = last.union(s)
			continue
		}
		out = append(out, s)
	}
	return out
}

// coverage — какой участок доказал блок.
func coverage(b Block, recs []Record) (Segment, error) {
	switch b.Dir {
	case DirAfter, DirBefore:
		if b.Anchor == nil {
			return Segment{}, ErrAnchorRequired
		}
	default:
		if len(recs) == 0 {
			return Segment{}, ErrEmptyBlock
		}
	}

	var seg Segment
	if len(recs) > 0 {
		first := Position{OrderKey: recs[0].OrderKey, ID: recs[0].ID}
		seg = Segment{Oldest: first, Newest: first}
		for _, r := range recs[1:] {
			p := Position{OrderKey: r.OrderKey, ID: r.ID}
			seg.Newest = newest(seg.Newest, p)
			seg.Oldest = oldest(seg.Oldest, p)
		}
	}

	if b.Anchor != nil {
		// Якорь включается в покрытие: сервер отсчитывал ОТ него, значит
		// непрерывность доказана начиная с самого якоря. Без этого блок и
		// сегмент якоря не пересеклись бы, и слияние не состоялось бы —
		// пропуск не закрывался бы никогда.
		if len(recs) == 0 {
			seg = Segment{Oldest: *b.Anchor, Newest: *b.Anchor}
		} else {
			seg = seg.union(Segment{Oldest: *b.Anchor, Newest: *b.Anchor})
		}
	}
	return seg, nil
}

// applyBlock — алгебра слияния. Отделена от хранилища намеренно: свойство
// проверяется на структурах, без bbolt и без сети.
func applyBlock(segs []Segment, b Block, recs []Record) ([]Segment, error) {
	seg, err := coverage(b, recs)
	if err != nil {
		return nil, err
	}

	next := append([]Segment(nil), segs...)
	next = append(next, seg)

	// DirAfter + AtEnd: сервер заявил, что новее блока нет ничего. Значит
	// всё, что лежит выше, — часть той же непрерывной верхушки. Это
	// единственное место, где непрерывность выводится из ОТСУТСТВИЯ записей,
	// и без него пустой ответ на закрытие пропуска ничего бы не менял.
	if b.Dir == DirAfter && b.AtEnd {
		merged := seg
		for _, s := range segs {
			if s.Newest.newerThan(seg.Newest) {
				merged = merged.union(s)
			}
		}
		next = append(next, merged)
	}

	return normalize(next), nil
}

// Segments — копия списка сегментов канала, свежие первыми.
func (s *Store) Segments(ctx context.Context, channelID string) ([]Segment, error) {
	var segs []Segment
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		var e error
		segs, e = loadSegments(tx, channelID)
		return e
	})
	return segs, err
}

// GapAnchor — позиция, которую надо передать как after, чтобы закрыть САМЫЙ
// ВЕРХНИЙ пропуск.
//
// Это Newest сегмента, лежащего ПОД пропуском, а не самая свежая локальная
// запись: та лежит НАД пропуском, и запрос от неё вернул бы уже известное,
// оставив дыру навсегда. Ошибка настолько правдоподобная, что вынесена в
// отдельный метод, а не оставлена вызывающему.
func (s *Store) GapAnchor(ctx context.Context, channelID string) (Position, bool, error) {
	segs, err := s.Segments(ctx, channelID)
	if err != nil || len(segs) < 2 {
		return Position{}, false, err
	}
	return segs[1].Newest, true, nil
}

// Watermark — самая свежая локальная позиция. Отвечает на другой вопрос, чем
// GapAnchor: «докуда я дочитал сверху», а не «откуда закрывать дыру».
func (s *Store) Watermark(ctx context.Context, channelID string) (Position, bool, error) {
	if channelID == "" {
		return Position{}, false, ErrEmptyChannel
	}
	var (
		pos   Position
		found bool
	)
	stop := errors.New("stop")
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		e := tx.ForEachPrefix(Namespace, prefix(channelID), func(_, v []byte) error {
			r, derr := decode(v)
			if derr != nil {
				return derr
			}
			pos = Position{OrderKey: r.OrderKey, ID: r.ID}
			found = true
			return stop
		})
		if e != nil && !errors.Is(e, stop) {
			return e
		}
		return nil
	})
	return pos, found, err
}

// countVanished — сколько локальных записей внутри диапазона блока в самом
// блоке ОТСУТСТВУЕТ.
//
// Единственный способ увидеть удаление. Живой прогон четвёртого среза
// показал: удалённая запись просто ИСЧЕЗАЕТ из ленты — надгробия нет,
// delete_at в выдаче не приходит, и по самой записи узнать об удалении
// нечем. Но сегмент утверждает полноту «на момент выборки»: значит запись,
// попавшая в покрытый блоком диапазон и не пришедшая в нём, у источника
// отсутствует.
//
// Функция СЧИТАЕТ и ничего не удаляет: политика удаления — отдельное
// решение (feed-reconcile.md §8, ADR-054).
//
// ОБХОД ИДЁТ ПО ДИАПАЗОНУ, А НЕ ПО ВСЕЙ ЛЕНТЕ. Прежняя редакция шла
// ForEachPrefix от начала префикса и отбрасывала лишнее уже внутри callback.
// Это было измерено (feed-reconcile.md §10): просмотр равнялся всей ленте
// ВСЕГДА — 137 мс и 44 МБ мусора на заливку при 100 000 записей, 86 % времени
// Ingest, — а поскольку лента читается страницами, полная догрузка выходила
// квадратичной по её размеру.
//
// Здесь ничего не изобретается: порядок обхода ключей СОВПАДАЕТ с порядком
// Position.newerThan (это инвариант раскладки ключа, см. localfeed.go), значит
// покрытый диапазон — непрерывный участок обхода, и достаточно встать на его
// верхнюю границу через ForEachPrefixFrom (ADR-047) и остановиться, миновав
// нижнюю. Новых возможностей у ports.Storage не просят: ADR-047 заводился
// ровно под «продолжить чтение с известной позиции».
//
// Семантика Vanished не меняется (ADR-053/054): диапазон по-прежнему задаётся
// замороженным сравнением позиций, исчезнувшей считается локальная запись
// внутри покрытого диапазона, которой нет в блоке, и функция по-прежнему
// только считает.
func countVanished(tx ports.ReadTx, channelID string, b Block) (int, error) {
	if len(b.Records) == 0 {
		return 0, nil
	}

	present := make(map[string]struct{}, len(b.Records))
	lo := Position{OrderKey: b.Records[0].OrderKey, ID: b.Records[0].ID}
	hi := lo
	for _, r := range b.Records {
		present[r.ID] = struct{}{}
		p := Position{OrderKey: r.OrderKey, ID: r.ID}
		lo = oldest(lo, p)
		hi = newest(hi, p)
	}

	// stopScan — не ошибка, а выход из обхода: контракт ForEachPrefix* иного
	// способа прерваться не даёт. Тот же приём, что в Watermark.
	stopScan := errors.New("stop")

	pfx := prefix(channelID)
	var n int
	err := tx.ForEachPrefixFrom(Namespace, pfx, orderBound(channelID, hi.OrderKey), func(k, _ []byte) error {
		p, perr := positionFromKey(pfx, k)
		if perr != nil {
			return perr
		}
		if lo.newerThan(p) {
			// Ниже нижней границы. Дальше по обходу только ещё старее —
			// смотреть больше нечего.
			return stopScan
		}
		if p.newerThan(hi) {
			// Тот же OrderKey, что у hi, но ID меньше, то есть НОВЕЕ hi:
			// граница по OrderKey грубее границы по позиции, и такие записи
			// обход захватывает первыми. Их число ограничено числом записей
			// одной миллисекунды, а не длиной ленты.
			return nil
		}
		if _, ok := present[p.ID]; !ok {
			n++
		}
		return nil
	})
	if err != nil && !errors.Is(err, stopScan) {
		return 0, err
	}
	return n, nil
}
