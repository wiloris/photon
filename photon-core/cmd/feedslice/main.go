// feedslice — первый вертикальный срез Phase 12.
//
// Сценарий: «открыть один канал и пролистать его историю назад».
// Полный путь: server → cursor pagination → local storage → ordered local read
// → load more → offline reopen.
//
// ЭТО НЕ КЛИЕНТ И НЕ КЕШ. Срез существует ради наблюдения: журнал фактического
// access pattern и измерение стоимости существующего Storage API — основной
// результат, а работающий экран — средство (CURRENT_TASK.md, Phase 12).
//
// Композиционный корень здесь МИНИМАЛЬНЫЙ и намеренно не обобщается: он
// открывает хранилище и связывает диспетчер с localfeed. Identity устройства
// не заводится — срезу она не нужна (аутентификация идёт сессией Bridge), а
// заводить её «про запас» значило бы расширять scope за пределы необходимого.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/dispatcher"
	"github.com/wiloris/photon-core/services/localfeed"
)

const actionGetFeed = 101 // ACTION_GET_FEED

var (
	addr      = flag.String("addr", "138.124.91.123.sslip.io:4443", "адрес Bridge")
	sessionID = flag.String("session", "", "Session ID Mattermost")
	channelID = flag.String("channel", "", "канал")
	dbPath    = flag.String("db", "", "файл локального хранилища")
	pages     = flag.Int("pages", 5, "сколько страниц пролистать назад")
	pageSize  = flag.Int("page-size", 30, "размер страницы")
	offline   = flag.Bool("offline", false, "только локальное чтение, сети не касаться")
)

// post — ровно те поля, которые нужны СРЕЗУ: идентичность, порядок, версия.
// Доменной сущности ENGRAM здесь нет и в photon-core она не попадает: разбор
// живёт в этой программе, а в хранилище тело уходит непрозрачным.
type post struct {
	ID       string `json:"id"`
	CreateAt int64  `json:"create_at"`
	UpdateAt int64  `json:"update_at"`
	Message  string `json:"message"`
	DeleteAt int64  `json:"delete_at"`
}

type feedResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	Data  struct {
		Order []string        `json:"order"`
		Posts map[string]post `json:"posts"`
	} `json:"data"`
}

func main() {
	flag.Parse()
	if *channelID == "" || *dbPath == "" {
		fmt.Fprintln(os.Stderr, "нужны -channel и -db")
		os.Exit(2)
	}
	if !*offline && *sessionID == "" {
		fmt.Fprintln(os.Stderr, "нужен -session (или -offline)")
		os.Exit(2)
	}

	ctx := context.Background()
	stats := &localfeed.Stats{}

	// ── композиционный корень: хранилище ────────────────────────────────
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o700); err != nil {
		fail("каталог хранилища: %v", err)
	}
	st, err := bboltadapter.New(*dbPath, []bboltadapter.Migration{
		{Version: localfeed.SchemaVersion, Up: func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
			return err
		}},
	})
	if err != nil {
		fail("открытие хранилища: %v", err)
	}
	defer st.Close(ctx)

	feed := localfeed.New(st, stats)
	if err := feed.Prepare(ctx); err != nil {
		fail("подготовка namespace: %v", err)
	}

	fmt.Printf("\n═══ feedslice: канал %s ═══\n", *channelID)
	fmt.Printf("хранилище: %s\n", *dbPath)

	if *offline {
		runOffline(ctx, feed, stats)
		return
	}
	runOnline(ctx, feed, stats)
}

// runOnline — server → cursor → local storage.
func runOnline(ctx context.Context, feed *localfeed.Store, stats *localfeed.Stats) {
	d, err := dispatcher.New(dispatcher.Config{
		Endpoint: ports.Endpoint(*addr), SessionToken: *sessionID,
		ConnectTimeout: 15 * time.Second, CallTimeout: 30 * time.Second,
	})
	if err != nil {
		fail("dispatcher: %v", err)
	}
	defer d.Close()

	fmt.Println("\n── этап 1: загрузка с сервера курсорной пагинацией ──")
	cursor := "" // пусто = самые свежие
	totalFetched := 0
	for i := 1; i <= *pages; i++ {
		payload := map[string]any{"channel_id": *channelID, "limit": *pageSize}
		if cursor != "" {
			payload["before"] = cursor
		}
		raw, _ := json.Marshal(payload)

		t0 := time.Now()
		resp, err := d.SendAction(actionGetFeed, raw)
		if err != nil {
			fail("SendAction: %v", err)
		}
		var fr feedResponse
		if err := json.Unmarshal(resp, &fr); err != nil {
			fail("разбор ответа: %v", err)
		}
		if !fr.OK {
			fail("сервер отверг: %s", fr.Error)
		}
		if len(fr.Data.Order) == 0 {
			fmt.Printf("  стр. %d: пусто — история закончилась\n", i)
			break
		}

		// order сервера — свежие первыми. Курсор для следующей страницы —
		// последний (самый старый) элемент текущей.
		recs := make([]localfeed.Record, 0, len(fr.Data.Order))
		for _, id := range fr.Data.Order {
			p := fr.Data.Posts[id]
			body, _ := json.Marshal(p)
			recs = append(recs, localfeed.Record{
				ID: p.ID, OrderKey: p.CreateAt, Version: p.UpdateAt, Body: body,
			})
		}
		if err := feed.Put(ctx, *channelID, recs); err != nil {
			fail("сохранение: %v", err)
		}
		totalFetched += len(recs)
		cursor = fr.Data.Order[len(fr.Data.Order)-1]
		fmt.Printf("  стр. %d: получено %d, курсор → %s, за %s\n",
			i, len(recs), short(cursor), time.Since(t0).Round(time.Millisecond))
	}
	fmt.Printf("  всего загружено: %d\n", totalFetched)

	readLocal(ctx, feed, stats, "этап 2: локальное упорядоченное чтение + load more")
	report(ctx, feed, stats)
}

// runOffline — чтение без сети: доказывает offline reopen.
func runOffline(ctx context.Context, feed *localfeed.Store, stats *localfeed.Stats) {
	fmt.Println("\n── offline reopen: сети не касаемся ──")
	readLocal(ctx, feed, stats, "локальное чтение после перезапуска")
	report(ctx, feed, stats)
}

// readLocal — ordered local read + load more.
func readLocal(ctx context.Context, feed *localfeed.Store, stats *localfeed.Stats, title string) {
	fmt.Printf("\n── %s ──\n", title)
	var curKey int64
	var curID string
	page := 0
	var firstID, lastID string
	var prevKey int64 = 1<<63 - 1
	// Предел: курсор, не двигающийся с места, обязан завершиться отказом,
	// а не бесконечной печатью страниц. Тот же принцип, что в тестах.
	const maxPages = 10000
	for {
		if page > maxPages {
			fail("более %d страниц — курсор не двигается", maxPages)
		}
		t0 := time.Now()
		recs, err := feed.Page(ctx, *channelID, curKey, curID, *pageSize)
		if err != nil {
			fail("локальное чтение: %v", err)
		}
		if len(recs) == 0 {
			break
		}
		page++
		// Порядок проверяется, а не предполагается: сверяем убывание OrderKey.
		for _, r := range recs {
			if r.OrderKey > prevKey {
				fail("ПОРЯДОК НАРУШЕН: %d после %d", r.OrderKey, prevKey)
			}
			prevKey = r.OrderKey
		}
		if firstID == "" {
			firstID = recs[0].ID
		}
		lastID = recs[len(recs)-1].ID
		fmt.Printf("  стр. %d: %d записей за %s\n", page, len(recs), time.Since(t0).Round(time.Microsecond))
		last := recs[len(recs)-1]
		curKey, curID = last.OrderKey, last.ID
	}
	fmt.Printf("  порядок «свежие первыми» соблюдён на всём проходе\n")
	fmt.Printf("  самая свежая: %s, самая старая: %s\n", short(firstID), short(lastID))
}

func report(ctx context.Context, feed *localfeed.Store, stats *localfeed.Stats) {
	n, _ := feed.Count(ctx, *channelID)
	fmt.Printf("\n── журнал access pattern (основной результат среза) ──\n")
	fmt.Printf("записей в локальном хранилище: %d\n", n)
	fmt.Print(stats.Report())
}

func short(s string) string {
	if len(s) > 10 {
		return s[:10] + "…"
	}
	return s
}

func fail(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "❌ "+f+"\n", a...)
	os.Exit(1)
}
