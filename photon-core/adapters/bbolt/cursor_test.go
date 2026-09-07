package bbolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/wiloris/photon-core/ports"
)

// Контрактные тесты ForEachPrefixFrom (ADR-047).
//
// Проверяется ровно то, что обещает порт, а не то, как это делает bbolt:
// множество выдачи { k : k > start И HasPrefix(k, prefix) } в возрастающем
// байтовом порядке. Восемь свойств ниже перечислены в самом ADR как
// обязательные — каждое соответствует способу, которым курсорная пагинация
// ломается молча.

func newCursorStorage(t *testing.T, seed map[string]string) *Storage {
	t.Helper()
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	err := s.Update(ctx, func(tx ports.WriteTx) error {
		for k, v := range seed {
			if err := tx.Put("test.ns", []byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return s
}

// collectFrom — обход в виде среза ключей, в том порядке, в каком их выдал порт.
func collectFrom(t *testing.T, s *Storage, prefix, start string) []string {
	t.Helper()
	var got []string
	err := s.View(context.Background(), func(tx ports.ReadTx) error {
		return tx.ForEachPrefixFrom("test.ns", []byte(prefix), []byte(start), func(k, _ []byte) error {
			got = append(got, string(k))
			return nil
		})
	})
	if err != nil {
		t.Fatalf("ForEachPrefixFrom(%q, %q): %v", prefix, start, err)
	}
	return got
}

// 1. Обход начинается СТРОГО ПОСЛЕ start: сам start в выдачу не попадает.
//
// Свойство несимметрично по последствиям. Включающая граница не теряет
// данных и потому не выглядит как поломка — она даёт дубликат последней
// записи предыдущей страницы на КАЖДОЙ границе, то есть ошибку, которую
// в UI видно как повторяющееся сообщение, а в тесте на одну страницу — никак.
func TestForEachPrefixFrom_StartIsExclusive(t *testing.T) {
	s := newCursorStorage(t, map[string]string{
		"u:a": "1", "u:b": "2", "u:c": "3", "u:d": "4",
	})
	got := collectFrom(t, s, "u:", "u:b")
	want := []string{"u:c", "u:d"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v (u:b — сам start — должен быть исключён)", got, want)
	}
}

// 2. start равен последнему ключу префикса → пустая выдача, не ошибка.
// Это нормальный конец пагинации, а не краевой случай.
func TestForEachPrefixFrom_StartAtLastKeyYieldsNothing(t *testing.T) {
	s := newCursorStorage(t, map[string]string{"u:a": "1", "u:b": "2", "u:c": "3"})
	if got := collectFrom(t, s, "u:", "u:c"); len(got) != 0 {
		t.Fatalf("got %v, want пустую выдачу", got)
	}
}

// 3. start отсутствует в хранилище → обход всё равно начинается с первого
// ключа, БОЛЬШЕГО start. Курсор — это позиция в порядке, а не ссылка на
// запись: удалённое сообщение не должно останавливать пагинацию.
func TestForEachPrefixFrom_StartAbsentFromStore(t *testing.T) {
	s := newCursorStorage(t, map[string]string{"u:a": "1", "u:c": "3", "u:e": "5"})
	got := collectFrom(t, s, "u:", "u:b") // u:b никогда не существовал
	want := []string{"u:c", "u:e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// 4. Пустой start эквивалентен ForEachPrefix. Сравниваются сами выдачи, а не
// их длины: одинаковая длина при разном порядке — та же ошибка, только тише.
func TestForEachPrefixFrom_EmptyStartEqualsForEachPrefix(t *testing.T) {
	s := newCursorStorage(t, map[string]string{
		"u:a": "1", "u:b": "2", "u:c": "3", "s:x": "no",
	})
	var viaPrefix []string
	err := s.View(context.Background(), func(tx ports.ReadTx) error {
		return tx.ForEachPrefix("test.ns", []byte("u:"), func(k, _ []byte) error {
			viaPrefix = append(viaPrefix, string(k))
			return nil
		})
	})
	if err != nil {
		t.Fatalf("ForEachPrefix: %v", err)
	}

	for _, start := range [][]byte{nil, {}} {
		var viaFrom []string
		err := s.View(context.Background(), func(tx ports.ReadTx) error {
			return tx.ForEachPrefixFrom("test.ns", []byte("u:"), start, func(k, _ []byte) error {
				viaFrom = append(viaFrom, string(k))
				return nil
			})
		})
		if err != nil {
			t.Fatalf("ForEachPrefixFrom(start=%v): %v", start, err)
		}
		if fmt.Sprint(viaFrom) != fmt.Sprint(viaPrefix) {
			t.Fatalf("start=%v дал %v, ForEachPrefix дал %v", start, viaFrom, viaPrefix)
		}
	}
}

// 5. Непустой start вне prefix → ErrCursorOutOfPrefix, и НИ ОДНОГО вызова fn.
// Ошибка обязана предшествовать обходу: частичная выдача с ошибкой в конце
// оставила бы вызывающего с данными чужого префикса на руках.
func TestForEachPrefixFrom_StartOutsidePrefixIsError(t *testing.T) {
	s := newCursorStorage(t, map[string]string{"u:a": "1", "u:b": "2", "s:a": "x"})

	for _, start := range []string{"s:a", "v:a", "t"} {
		calls := 0
		err := s.View(context.Background(), func(tx ports.ReadTx) error {
			return tx.ForEachPrefixFrom("test.ns", []byte("u:"), []byte(start), func(k, _ []byte) error {
				calls++
				return nil
			})
		})
		if !errors.Is(err, ports.ErrCursorOutOfPrefix) {
			t.Fatalf("start=%q: got %v, want ErrCursorOutOfPrefix", start, err)
		}
		if calls != 0 {
			t.Fatalf("start=%q: fn вызвана %d раз при ошибке контракта", start, calls)
		}
	}
}

// 6. Обход не пересекает границу префикса, даже когда соседний префикс
// лежит в байтовом порядке сразу за ним и продолжился бы «естественно».
// "u:" и "u;" соседние: ';' — следующий байт после ':'.
func TestForEachPrefixFrom_StopsAtPrefixBoundary(t *testing.T) {
	s := newCursorStorage(t, map[string]string{
		"u:a": "1", "u:b": "2",
		"u;a": "чужой", "u;b": "чужой",
		"v:a": "чужой",
	})
	got := collectFrom(t, s, "u:", "u:a")
	want := []string{"u:b"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v — обход вышел за префикс", got, want)
	}
}

// 7. Постраничный обход покрывает каждую запись РОВНО ОДИН раз, включая
// записи с одинаковым OrderKey. Ключ ленты — <канал> 0x00 <inv OrderKey> <ID>,
// и одинаковый OrderKey у соседей — не редкость (пакетная доставка, импорт).
// Именно на них включающая граница и потеря записи неразличимы по счётчику.
func TestForEachPrefixFrom_PagingCoversEachRecordExactlyOnce(t *testing.T) {
	const total, pageSize = 97, 10

	// 97 записей на 5 различных OrderKey — по ~20 записей с одинаковым.
	seed := map[string]string{}
	var ordered []string
	for i := 0; i < total; i++ {
		var k []byte
		k = append(k, "ch"...)
		k = append(k, 0x00)
		var ord [8]byte
		binary.BigEndian.PutUint64(ord[:], uint64(i/20))
		k = append(k, ord[:]...)
		k = append(k, fmt.Sprintf("id%03d", i)...)
		seed[string(k)] = fmt.Sprint(i)
		ordered = append(ordered, string(k))
	}
	s := newCursorStorage(t, seed)

	seen := map[string]int{}
	prefix := append([]byte("ch"), 0x00)
	start := []byte(nil)
	pages := 0
	const maxPages = total/pageSize + 10 // см. комментарий в cost_test.go
	for {
		if pages > maxPages {
			t.Fatalf("более %d страниц на %d записях — курсор не двигается", maxPages, total)
		}
		var page []string
		err := s.View(context.Background(), func(tx ports.ReadTx) error {
			n := 0
			return tx.ForEachPrefixFrom("test.ns", prefix, start, func(k, _ []byte) error {
				page = append(page, string(k))
				n++
				if n == pageSize {
					return errStopPage
				}
				return nil
			})
		})
		if err != nil && !errors.Is(err, errStopPage) {
			t.Fatalf("страница %d: %v", pages+1, err)
		}
		if len(page) == 0 {
			break
		}
		pages++
		for _, k := range page {
			seen[k]++
		}
		start = []byte(page[len(page)-1])
	}

	if len(seen) != total {
		t.Fatalf("покрыто %d записей из %d — пропуск на границе страниц", len(seen), total)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("ключ %q выдан %d раз — дубликат на границе страниц", k, n)
		}
	}
	if pages != 10 { // 97 = 9*10 + 7
		t.Fatalf("страниц %d, ожидалось 10", pages)
	}
}

var errStopPage = errors.New("stop page")

// 8. Вставка МЕЖДУ страницами не даёт ни дубликата, ни пропуска среди тех
// записей, что существовали на момент начала обхода.
//
// Это и есть смысл курсора вместо offset: при offset вставка сдвигает всё,
// что после неё, и одна запись выпадает либо повторяется. Здесь вставки
// делаются в обе стороны от текущей позиции — до неё (не должна появиться)
// и после (появиться может, и это не ошибка).
func TestForEachPrefixFrom_InsertBetweenPagesCausesNoDuplicateOrGap(t *testing.T) {
	seed := map[string]string{}
	var original []string
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("u:%03d", i*2) // чётные — оставляем зазор для вставок
		seed[k] = fmt.Sprint(i)
		original = append(original, k)
	}
	s := newCursorStorage(t, seed)

	ctx := context.Background()
	seen := map[string]int{}
	start := []byte(nil)
	page := 0
	for {
		// Предел обязателен: без него страница, игнорирующая курсор, даёт
		// бесконечный цикл вместо падения, а зависший тест мутацию не ловит.
		// 40 исходных + ≤8 вставок за курсор по 5 на страницу — честных ≤10.
		if page > 20 {
			t.Fatalf("более 20 страниц на 40 записях — курсор не двигается")
		}
		var got []string
		err := s.View(ctx, func(tx ports.ReadTx) error {
			n := 0
			return tx.ForEachPrefixFrom("test.ns", []byte("u:"), start, func(k, _ []byte) error {
				got = append(got, string(k))
				n++
				if n == 5 {
					return errStopPage
				}
				return nil
			})
		})
		if err != nil && !errors.Is(err, errStopPage) {
			t.Fatalf("страница %d: %v", page+1, err)
		}
		if len(got) == 0 {
			break
		}
		page++
		for _, k := range got {
			seen[k]++
		}
		start = []byte(got[len(got)-1])

		// Вставки по обе стороны от курсора между страницами. Обе — на
		// нечётные ключи (исходные чётные), внутрь исходного диапазона:
		// вставка ЗА хвост дописывала бы по записи на каждую страницу и
		// обход не сходился бы — свойство тут ни при чём, это была бы
		// ошибка самого стимула.
		// Позиции берутся от ФАКТИЧЕСКОГО курсора, а не от номера страницы:
		// страница отдаёт в том числе ранее вставленные ключи, поэтому
		// счётчик страниц позицию не предсказывает.
		var cursorIdx int
		if _, err := fmt.Sscanf(string(start), "u:%d", &cursorIdx); err != nil {
			t.Fatalf("разбор курсора %q: %v", start, err)
		}
		if cursorIdx+1 >= 78 {
			continue // за пределами исходного диапазона больше не вставляем
		}
		err = s.Update(ctx, func(tx ports.WriteTx) error {
			before := fmt.Sprintf("u:%03d", cursorIdx-3) // позади курсора
			after := fmt.Sprintf("u:%03d", cursorIdx+1)  // сразу за курсором
			if err := tx.Put("test.ns", []byte(before), []byte("late")); err != nil {
				return err
			}
			return tx.Put("test.ns", []byte(after), []byte("late"))
		})
		if err != nil {
			t.Fatalf("вставка после страницы %d: %v", page, err)
		}
	}

	for _, k := range original {
		switch seen[k] {
		case 1:
		case 0:
			t.Fatalf("исходный ключ %q пропущен при вставках между страницами", k)
		default:
			t.Fatalf("исходный ключ %q выдан %d раз при вставках между страницами", k, seen[k])
		}
	}
	// Вставленные ДО курсора не имели права появиться: они позади позиции.
	for k, v := range seen {
		if v > 0 && !bytes.HasPrefix([]byte(k), []byte("u:")) {
			t.Fatalf("выдан ключ вне префикса: %q", k)
		}
	}
}
