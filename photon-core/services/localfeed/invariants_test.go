package localfeed

import (
	"context"
	"errors"
	"testing"
)

/*
 * Структурные инварианты блока и записи — ADR-065 п. 3.6–3.9.
 *
 * Все четыре проверяются БЕЗ знания домена и без обращения к серверу: они
 * ловят утверждение, противоречащее самому себе, а не ложное утверждение о
 * сервере. Второго Core сделать не может, и этот файл его не изображает —
 * см. ADR-065 п. 4.
 *
 * Тесты failure-oriented: каждый инвариант проверяется нарушением, и рядом
 * стоит контрольный случай, который обязан ПРОХОДИТЬ. Без контрольного случая
 * проверка «всё отвергается» выглядела бы так же зелено.
 *
 * Отказ обязан быть полным: ни записей, ни сегментов. Это проверяется
 * отдельно, потому что «ошибку вернул» и «ничего не записал» — разные
 * утверждения, а испорченную ленту чинить нечем (ADR-054, ADR-060).
 */

// assertEmpty — в канале нет ни записей, ни сегментов.
func assertEmpty(t *testing.T, s *Store, ch, why string) {
	t.Helper()
	ctx := context.Background()
	n, err := s.Count(ctx, ch)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 0 {
		t.Fatalf("%s: записей %d, ожидалось 0 — отказ не был полным", why, n)
	}
	segs, err := s.Segments(ctx, ch)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 0 {
		t.Fatalf("%s: сегментов %d, ожидалось 0 — заявлена полнота поверх отказа", why, len(segs))
	}
}

// ── 3.6: непустой идентификатор записи ───────────────────────────────────

func TestADR065_PutRejectsEmptyRecordID(t *testing.T) {
	s, _ := newStore(t)
	err := s.Put(context.Background(), "ch", []Record{rec("a", 100), {ID: "", OrderKey: 200}})
	if !errors.Is(err, ErrEmptyRecordID) {
		t.Fatalf("Put: %v, ожидалось ErrEmptyRecordID", err)
	}
	// Первая запись блока законна — но блок отвергнут целиком, и её тоже нет.
	assertEmpty(t, s, "ch", "Put с пустым ID")
}

func TestADR065_IngestRejectsEmptyRecordID(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.Ingest(context.Background(), "ch", Block{
		Records: []Record{{ID: "", OrderKey: 100}}, Dir: DirNewest,
	})
	if !errors.Is(err, ErrEmptyRecordID) {
		t.Fatalf("Ingest: %v, ожидалось ErrEmptyRecordID", err)
	}
	assertEmpty(t, s, "ch", "Ingest с пустым ID")
}

// Контрольный случай: непустой ID проходит обе операции.
//
// Без него проверка выше прошла бы и на validateRecords, отвергающем ВСЁ.
func TestADR065_NonEmptyRecordIDPasses(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, "ch", []Record{rec("a", 100)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Ingest(ctx, "ch", Block{Records: []Record{rec("b", 200)}, Dir: DirNewest}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	n, _ := s.Count(ctx, "ch")
	if n != 2 {
		t.Fatalf("записей %d, ожидалось 2", n)
	}
}

// ── 3.7: известное направление ───────────────────────────────────────────

func TestADR065_UnknownDirectionRejected(t *testing.T) {
	// Direction — int, и опечатка вызывающего приходит числом. До ADR-065
	// ветка default трактовала любое такое число как DirNewest, то есть
	// превращала опечатку в утверждение о полноте.
	for _, dir := range []Direction{Direction(3), Direction(7), Direction(-1)} {
		s, _ := newStore(t)
		_, err := s.Ingest(context.Background(), "ch", Block{
			Records: []Record{rec("a", 100)}, Dir: dir,
		})
		if !errors.Is(err, ErrUnknownDirection) {
			t.Fatalf("Dir=%d: %v, ожидалось ErrUnknownDirection", dir, err)
		}
		assertEmpty(t, s, "ch", "неизвестное направление")
	}
}

func TestADR065_KnownDirectionsAccepted(t *testing.T) {
	anchor := pos(100, "a")
	cases := []struct {
		name string
		blk  Block
	}{
		{"newest", Block{Records: []Record{rec("a", 100)}, Dir: DirNewest}},
		{"after", Block{Records: []Record{rec("b", 200)}, Dir: DirAfter, Anchor: &anchor}},
		{"before", Block{Records: []Record{rec("c", 50)}, Dir: DirBefore, Anchor: &anchor}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := newStore(t)
			if _, err := s.Ingest(context.Background(), "ch", c.blk); err != nil {
				t.Fatalf("Ingest: %v — известное направление отвергнуто", err)
			}
		})
	}
}

// ── 3.8: якорь запрещён при DirNewest ────────────────────────────────────

func TestADR065_AnchorNotAllowedForNewest(t *testing.T) {
	// «Самые свежие» — запрос БЕЗ курсора. Якорь при нём раньше молча
	// расширял покрытие до своей позиции: сегмент объявлял полноту участка,
	// о котором блок не говорил ничего.
	anchor := pos(1000, "z")
	s, _ := newStore(t)
	_, err := s.Ingest(context.Background(), "ch", Block{
		Records: []Record{rec("a", 100)}, Dir: DirNewest, Anchor: &anchor,
	})
	if !errors.Is(err, ErrAnchorNotAllowed) {
		t.Fatalf("Ingest: %v, ожидалось ErrAnchorNotAllowed", err)
	}
	assertEmpty(t, s, "ch", "якорь при DirNewest")
}

// Отказ не должен подменить собой прежний: пустой блок без якоря — всё ещё
// ErrEmptyBlock, а не ErrAnchorNotAllowed.
func TestADR065_EmptyNewestBlockStillErrEmptyBlock(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.Ingest(context.Background(), "ch", Block{Dir: DirNewest})
	if !errors.Is(err, ErrEmptyBlock) {
		t.Fatalf("Ingest: %v, ожидалось ErrEmptyBlock", err)
	}
}

// ── 3.9: направленная согласованность ────────────────────────────────────

func TestADR065_AfterRejectsRecordOlderThanAnchor(t *testing.T) {
	anchor := pos(500, "m")
	s, _ := newStore(t)
	_, err := s.Ingest(context.Background(), "ch", Block{
		// Вторая запись СТАРШЕ якоря: сервер на after такого не вернул бы.
		Records: []Record{rec("new", 600), rec("old", 400)},
		Dir:     DirAfter, Anchor: &anchor,
	})
	if !errors.Is(err, ErrRecordCrossesAnchor) {
		t.Fatalf("Ingest: %v, ожидалось ErrRecordCrossesAnchor", err)
	}
	assertEmpty(t, s, "ch", "after с записью старше якоря")
}

func TestADR065_BeforeRejectsRecordNewerThanAnchor(t *testing.T) {
	anchor := pos(500, "m")
	s, _ := newStore(t)
	_, err := s.Ingest(context.Background(), "ch", Block{
		Records: []Record{rec("old", 400), rec("new", 600)},
		Dir:     DirBefore, Anchor: &anchor,
	})
	if !errors.Is(err, ErrRecordCrossesAnchor) {
		t.Fatalf("Ingest: %v, ожидалось ErrRecordCrossesAnchor", err)
	}
	assertEmpty(t, s, "ch", "before с записью новее якоря")
}

// Перепутанные местами before/after — ровно та ошибка, ради которой проверка
// и заведена. Один и тот же блок законен в одну сторону и незаконен в другую.
func TestADR065_SwappedDirectionIsCaught(t *testing.T) {
	anchor := pos(500, "m")
	older := []Record{rec("o1", 400), rec("o2", 300)}

	s1, _ := newStore(t)
	if _, err := s1.Ingest(context.Background(), "ch", Block{
		Records: older, Dir: DirBefore, Anchor: &anchor,
	}); err != nil {
		t.Fatalf("законный DirBefore отвергнут: %v", err)
	}

	s2, _ := newStore(t)
	if _, err := s2.Ingest(context.Background(), "ch", Block{
		Records: older, Dir: DirAfter, Anchor: &anchor,
	}); !errors.Is(err, ErrRecordCrossesAnchor) {
		t.Fatalf("тот же блок с DirAfter: %v, ожидалось ErrRecordCrossesAnchor", err)
	}
}

// Равная якорю запись законна В ОБОИХ направлениях.
//
// Не послабление, а защита работающего приёма: cmd/feedslice.runSeedOld
// кладёт сам якорь в блок, потому что иначе сегмент обещал бы полноту до
// позиции, записи которой в хранилище нет. Строгое сравнение сломало бы его,
// ничего не поймав взамен.
func TestADR065_AnchorItselfMayBeInBlock(t *testing.T) {
	anchor := pos(500, "m")
	for _, c := range []struct {
		name string
		blk  Block
	}{
		{"before", Block{Records: []Record{rec("m", 500), rec("o", 400)},
			Dir: DirBefore, Anchor: &anchor}},
		{"after", Block{Records: []Record{rec("n", 600), rec("m", 500)},
			Dir: DirAfter, Anchor: &anchor}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _ := newStore(t)
			if _, err := s.Ingest(context.Background(), "ch", c.blk); err != nil {
				t.Fatalf("Ingest: %v — якорь в блоке законен", err)
			}
		})
	}
}

// Сравнение берётся ЗАМОРОЖЕННОЕ (ADR-052): при равном OrderKey более свежей
// считается запись с МЕНЬШИМ ID. Проверка обязана пользоваться именно им, а
// не сравнением одних лишь OrderKey — иначе она разойдётся с порядком обхода
// ключей, то есть с тем, как лента читается.
func TestADR065_AnchorSideUsesFrozenIDInversion(t *testing.T) {
	anchor := pos(500, "m")

	// Тот же OrderKey, БОЛЬШИЙ ID ⇒ запись СТАРШЕ якоря ⇒ для after незаконна.
	s1, _ := newStore(t)
	if _, err := s1.Ingest(context.Background(), "ch", Block{
		Records: []Record{rec("z", 500)}, Dir: DirAfter, Anchor: &anchor,
	}); !errors.Is(err, ErrRecordCrossesAnchor) {
		t.Fatalf("after + равный OrderKey, больший ID: %v, ожидалось ErrRecordCrossesAnchor", err)
	}

	// Тот же OrderKey, МЕНЬШИЙ ID ⇒ запись НОВЕЕ якоря ⇒ для after законна.
	s2, _ := newStore(t)
	if _, err := s2.Ingest(context.Background(), "ch", Block{
		Records: []Record{rec("a", 500)}, Dir: DirAfter, Anchor: &anchor,
	}); err != nil {
		t.Fatalf("after + равный OrderKey, меньший ID отвергнут: %v", err)
	}
}

// Пустой блок с якорем остаётся законным: «за якорем ничего нет» — ответ, на
// котором держится закрытие пропуска (feed-catchup.md §4.1). Проверка сторон
// не имеет права его отвергнуть, потому что сторон в нём нет.
func TestADR065_EmptyBlockWithAnchorStillAccepted(t *testing.T) {
	anchor := pos(500, "m")
	s, _ := newStore(t)
	if _, err := s.Ingest(context.Background(), "ch", Block{
		Records: nil, Dir: DirAfter, Anchor: &anchor, AtEnd: true,
	}); err != nil {
		t.Fatalf("Ingest: %v — пустой блок с якорем законен", err)
	}
}
