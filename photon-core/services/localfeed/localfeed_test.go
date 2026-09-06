package localfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
)

// Тесты идут против НАСТОЯЩЕГО adapters/bbolt, а не фиктивного хранилища.
// Причина та же, по которой Phase 13 проверялась против настоящих адаптеров:
// вопрос среза — «достаточно ли существующего ForEachPrefix», и фиктивное
// хранилище, обходящее ключи в удобном порядке, ответил бы на него ложно.

func newStore(t *testing.T) (*Store, *Stats) {
	t.Helper()
	dir := t.TempDir()
	st, err := bboltadapter.New(filepath.Join(dir, "feed.db"), feedMigrations())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { st.Close(context.Background()) })
	stats := &Stats{}
	s := New(st, stats)
	if err := s.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return s, stats
}

// feedMigrations — миграция среза. Namespace не создаётся неявно (ADR-010),
// поэтому шаг обязателен, а не «когда понадобится».
func feedMigrations() []bboltadapter.Migration {
	return []bboltadapter.Migration{
		{Version: SchemaVersion, Up: func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(Namespace))
			return err
		}},
	}
}

func rec(id string, order int64) Record {
	return Record{ID: id, OrderKey: order, Version: order,
		Body: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}
}

// Порядок «свежие первыми» — свойство, ради которого ключ кодируется
// инвертированным OrderKey. Порт порядок обхода НЕ гарантирует, поэтому он
// обязан следовать из кодирования, а не из поведения адаптера.
func TestOrderNewestFirst(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	// Кладём вперемешку — порядок записи не должен влиять на порядок чтения.
	if err := s.Put(ctx, "ch", []Record{rec("b", 200), rec("a", 100), rec("c", 300)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	want := []string{"c", "b", "a"}
	if len(got) != 3 {
		t.Fatalf("получено %d записей", len(got))
	}
	for i, w := range want {
		if got[i].ID != w {
			t.Fatalf("порядок нарушен: %v", ids(got))
		}
	}
}

// Одинаковый OrderKey — реальный случай: create_at в миллисекундах не
// уникален. Без ID во втором компоненте ключа записи схлопнулись бы в одну.
func TestSameOrderKeyKeepsBothRecords(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, "ch", []Record{rec("x", 100), rec("y", 100)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("записи с одинаковым OrderKey схлопнулись: %v", ids(got))
	}
}

// Идемпотентность (свойство 3 из ADR-046 §2): повторная запись той же
// записи не создаёт дубля.
func TestPutIsIdempotent(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	r := rec("a", 100)
	for i := 0; i < 3; i++ {
		if err := s.Put(ctx, "ch", []Record{r}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	n, err := s.Count(ctx, "ch")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 1 {
		t.Fatalf("повторная запись создала дубли: %d", n)
	}
}

// Обновление версии заменяет запись, а не добавляет вторую: ключ не содержит
// версии намеренно.
func TestVersionUpdateReplacesRecord(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, "ch", []Record{{ID: "a", OrderKey: 100, Version: 1, Body: json.RawMessage(`{"v":1}`)}}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, "ch", []Record{{ID: "a", OrderKey: 100, Version: 2, Body: json.RawMessage(`{"v":2}`)}}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("обновление создало дубль: %d", len(got))
	}
	if got[0].Version != 2 || string(got[0].Body) != `{"v":2}` {
		t.Fatalf("обновление не применилось: version=%d body=%s", got[0].Version, got[0].Body)
	}
}

// Постраничный обход курсором отдаёт каждую запись ровно один раз и в
// правильном порядке — без пропусков и дублей на стыках страниц.
func TestPagingCoversEveryRecordExactlyOnce(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	const total = 50
	var recs []Record
	for i := 0; i < total; i++ {
		recs = append(recs, rec(fmt.Sprintf("p%03d", i), int64(1000+i)))
	}
	if err := s.Put(ctx, "ch", recs); err != nil {
		t.Fatalf("Put: %v", err)
	}

	seen := map[string]int{}
	var order []string
	var curKey int64
	var curID string
	for {
		page, err := s.Page(ctx, "ch", curKey, curID, 7)
		if err != nil {
			t.Fatalf("Page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			seen[r.ID]++
			order = append(order, r.ID)
		}
		last := page[len(page)-1]
		curKey, curID = last.OrderKey, last.ID
	}
	if len(seen) != total {
		t.Fatalf("покрыто %d записей из %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("запись %s отдана %d раз — дубль на стыке страниц", id, n)
		}
	}
	for i := 1; i < len(order); i++ {
		if order[i-1] <= order[i] {
			t.Fatalf("порядок нарушен на позиции %d: %s перед %s", i, order[i-1], order[i])
		}
	}
}

// Каналы изолированы префиксом: чтение одного не задевает другой.
func TestChannelsAreIsolated(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, "ch1", []Record{rec("a", 100)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, "ch2", []Record{rec("b", 100)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Page(ctx, "ch1", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("утечка между каналами: %v", ids(got))
	}
}

// Данные переживают закрытие и повторное открытие хранилища — предпосылка
// offline reopen. Тот же приём, что в Phase 13.
func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feed.db")
	migs := feedMigrations()
	ctx := context.Background()

	st, err := bboltadapter.New(path, migs)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s := New(st, nil)
	if err := s.Prepare(ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := s.Put(ctx, "ch", []Record{rec("a", 100), rec("b", 200)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st.Close(ctx)

	st2, err := bboltadapter.New(path, migs)
	if err != nil {
		t.Fatalf("повторное открытие: %v", err)
	}
	defer st2.Close(ctx)
	s2 := New(st2, nil)
	got, err := s2.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page после переоткрытия: %v", err)
	}
	if len(got) != 2 || got[0].ID != "b" {
		t.Fatalf("данные не пережили перезапуск: %v", ids(got))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("файла БД нет: %v", err)
	}
}

// Битое тело отвергается при записи, а не при чтении: иначе испорченная
// запись обнаружилась бы через сутки и не там, где возникла.
func TestInvalidBodyRejectedOnWrite(t *testing.T) {
	s, _ := newStore(t)
	if err := s.Put(context.Background(), "ch",
		[]Record{{ID: "a", OrderKey: 1, Body: []byte("{oops")}}); err == nil {
		t.Fatal("битое тело принято")
	}
}

func ids(rs []Record) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.ID)
	}
	return out
}
