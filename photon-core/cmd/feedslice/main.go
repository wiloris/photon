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

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/dispatcher"
	"github.com/wiloris/photon/photon-core/services/localfeed"
)

const actionGetFeed = 101 // ACTION_GET_FEED

var (
	addr      = flag.String("addr", "bridge.example.net:4443", "адрес Bridge")
	sessionID = flag.String("session", "", "Session ID Mattermost")
	channelID = flag.String("channel", "", "канал")
	dbPath    = flag.String("db", "", "файл локального хранилища")
	pages     = flag.Int("pages", 5, "сколько страниц пролистать назад")
	pageSize  = flag.Int("page-size", 30, "размер страницы")
	offline   = flag.Bool("offline", false, "только локальное чтение, сети не касаться")
	catchup   = flag.Bool("catchup", false, "догрузка после offline: самые свежие + закрытие пропусков")
	maxFill   = flag.Int("max-fill", 20, "предел итераций закрытия пропуска")
	seedOld   = flag.Int("seed-old", 0, "подготовка: пролистать N страниц назад и сохранить ТОЛЬКО последнюю — имитация прошлой сессии, оставившей старый кусок")
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

	switch {
	case *seedOld > 0:
		runSeedOld(ctx, feed)
	case *offline:
		runOffline(ctx, feed, stats)
	case *catchup:
		runCatchup(ctx, feed, stats)
	default:
		runOnline(ctx, feed, stats)
	}
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

// fetch — один запрос getFeed. Возвращает записи в порядке сервера
// (свежие первыми) и время ОТВЕТА СЕТИ отдельно от всего остального:
// смешивать сетевую задержку с локальной стоимостью нельзя, иначе ни одно из
// двух чисел ничего не значит.
func fetch(d *dispatcher.Dispatcher, payload map[string]any) ([]localfeed.Record, []string, time.Duration) {
	raw, _ := json.Marshal(payload)
	t0 := time.Now()
	resp, err := d.SendAction(actionGetFeed, raw)
	rtt := time.Since(t0)
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
	recs, missing := recordsFrom(fr.Data.Order, fr.Data.Posts)
	for _, id := range missing {
		fmt.Printf("  ПРОПУЩЕН: id %s есть в order, но нет в posts — запись не создана\n", short(id))
	}
	return recs, fr.Data.Order, rtt
}

// recordsFrom переводит пару (order, posts) ответа сервера в записи ленты.
//
// Порядок задаёт ТОЛЬКО order: обход map в Go случаен, и запись из него дала
// бы разный порядок от прогона к прогону при одинаковом ответе.
//
// Идентификатор из order, которого нет в posts, ЗАПИСЬЮ НЕ СТАНОВИТСЯ.
// Прежде такой случай молча давал localfeed.Record с пустым ID и OrderKey 0:
// Ingest пустой ID не проверяет, и в ленте появлялась бы запись, которая
// всегда самая старая и ни к какому посту не относится, — то есть локальное
// хранилище портилось бы тихо. Пропущенные идентификаторы возвращаются
// вызывающему, чтобы он о них сообщил, а не проглотил.
func recordsFrom(order []string, posts map[string]post) (recs []localfeed.Record, missing []string) {
	recs = make([]localfeed.Record, 0, len(order))
	for _, id := range order {
		p, ok := posts[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		body, _ := json.Marshal(p)
		recs = append(recs, localfeed.Record{
			ID: p.ID, OrderKey: p.CreateAt, Version: p.UpdateAt, Body: body,
		})
	}
	return recs, missing
}

// runSeedOld готовит состояние «прошлая сессия оставила старый кусок».
//
// Нужен, чтобы пропуск возник на НАСТОЯЩИХ данных, а не был подстроен записью
// в базу мимо кода: сервер листается назад обычным before, и в хранилище
// кладётся только последняя страница. Всё, что выше, остаётся неизвестным —
// ровно то, что бывает, когда приложение долго не открывали.
//
// Блок объявляется DirBefore с якорем на посте, ОТ которого сервер отсчитывал
// последнюю страницу, и этот пост входит в блок. Иначе сегмент утверждал бы
// полноту до позиции, записи которой нет в хранилище.
func runSeedOld(ctx context.Context, feed *localfeed.Store) {
	d, err := dispatcher.New(dispatcher.Config{
		Endpoint: ports.Endpoint(*addr), SessionToken: *sessionID,
		ConnectTimeout: 15 * time.Second, CallTimeout: 30 * time.Second,
	})
	if err != nil {
		fail("dispatcher: %v", err)
	}
	defer d.Close()

	fmt.Printf("\n── подготовка: листаем %d страниц назад, сохраняем только последнюю ──\n", *seedOld)

	var (
		cursor     string
		anchorRec  *localfeed.Record
		lastBlock  []localfeed.Record
		lastAnchor *localfeed.Record
	)
	for i := 1; i <= *seedOld; i++ {
		payload := map[string]any{"channel_id": *channelID, "limit": *pageSize}
		if cursor != "" {
			payload["before"] = cursor
		}
		recs, order, rtt := fetch(d, payload)
		if len(recs) == 0 {
			fmt.Printf("  стр. %d: пусто — история закончилась раньше\n", i)
			break
		}
		fmt.Printf("  стр. %d: %d записей за %s (не сохраняем, кроме последней)\n",
			i, len(recs), rtt.Round(time.Millisecond))
		lastBlock = recs
		lastAnchor = anchorRec
		last := recs[len(recs)-1]
		cursor = order[len(order)-1]
		anchorRec = &last
	}
	if len(lastBlock) == 0 {
		fail("нечего сохранять")
	}

	block := localfeed.Block{Records: lastBlock, Dir: localfeed.DirNewest}
	if lastAnchor != nil {
		// Якорь входит в блок: сегмент не должен обещать полноту до позиции,
		// записи которой в хранилище нет.
		block.Records = append([]localfeed.Record{*lastAnchor}, lastBlock...)
		p := localfeed.Position{OrderKey: lastAnchor.OrderKey, ID: lastAnchor.ID}
		block.Anchor = &p
		block.Dir = localfeed.DirBefore
	}
	res, err := feed.Ingest(ctx, *channelID, block)
	if err != nil {
		fail("Ingest(seed): %v", err)
	}
	fmt.Printf("  сохранено %d записей, сегментов: %d\n", res.Stored, res.Segments)
	segs, _ := feed.Segments(ctx, *channelID)
	printSegments(segs)
}

// runCatchup — сценарий «вернуться в канал после offline».
//
// Полный путь: самые свежие → обнаружение пропуска → закрытие пропуска
// запросами after → локальное чтение, показывающее разрыв, если он остался.
// Спецификация: docs/feed-catchup.md §5.
func runCatchup(ctx context.Context, feed *localfeed.Store, stats *localfeed.Stats) {
	d, err := dispatcher.New(dispatcher.Config{
		Endpoint: ports.Endpoint(*addr), SessionToken: *sessionID,
		ConnectTimeout: 15 * time.Second, CallTimeout: 30 * time.Second,
	})
	if err != nil {
		fail("dispatcher: %v", err)
	}
	defer d.Close()

	before, _ := feed.Segments(ctx, *channelID)
	fmt.Printf("\n── состояние до догрузки ──\n")
	printSegments(before)

	var netTotal time.Duration
	var netCalls int

	// ── шаг 1: самые свежие ──────────────────────────────────────────────
	fmt.Println("\n── шаг 1: самые свежие (как при открытии экрана) ──")
	recs, order, rtt := fetch(d, map[string]any{"channel_id": *channelID, "limit": *pageSize})
	netTotal += rtt
	netCalls++
	res, err := feed.Ingest(ctx, *channelID, localfeed.Block{
		Records: recs, Dir: localfeed.DirNewest, AtEnd: len(recs) < *pageSize,
	})
	if err != nil {
		fail("Ingest(newest): %v", err)
	}
	fmt.Printf("  получено %d за %s | сегментов: %d | пропусков закрыто: %d\n",
		len(recs), rtt.Round(time.Millisecond), res.Segments, res.GapsClosed)
	if len(order) > 0 {
		fmt.Printf("  верх канала: %s … низ блока: %s\n", short(order[0]), short(order[len(order)-1]))
	}

	// ── шаг 2: закрытие пропусков ────────────────────────────────────────
	fmt.Println("\n── шаг 2: закрытие пропусков запросами after ──")
	fills := 0
	for {
		anchor, ok, err := feed.GapAnchor(ctx, *channelID)
		if err != nil {
			fail("GapAnchor: %v", err)
		}
		if !ok {
			fmt.Println("  пропусков нет — лента непрерывна")
			break
		}
		if fills >= *maxFill {
			fmt.Printf("  предел %d итераций исчерпан, пропуск остался — это НЕ ошибка,\n", *maxFill)
			fmt.Printf("  а честная граница: закрытие продолжится при следующем запуске\n")
			break
		}
		fills++

		recs, order, rtt := fetch(d, map[string]any{
			"channel_id": *channelID, "after": anchor.ID, "limit": *pageSize,
		})
		netTotal += rtt
		netCalls++

		// ГЛАВНЫЙ ВОПРОС ЖИВОГО ПРОГОНА (docs/feed-catchup.md §7): с какого
		// конца сервер отдаёт записи после якоря. Сходимость требует
		// БЛИЖАЙШИХ к якорю; ответ виден по тому, насколько отданное
		// примыкает к якорю по времени.
		if len(recs) > 0 {
			nearest, farthest := recs[len(recs)-1], recs[0]
			fmt.Printf("  заливка %d: якорь order=%d | ближний край блока order=%d (Δ=%d мс) | дальний order=%d\n",
				fills, anchor.OrderKey, nearest.OrderKey, nearest.OrderKey-anchor.OrderKey, farthest.OrderKey)
			_ = order
		}

		res, err := feed.Ingest(ctx, *channelID, localfeed.Block{
			Records: recs, Anchor: &anchor, Dir: localfeed.DirAfter,
			AtEnd: len(recs) < *pageSize,
		})
		if err != nil {
			fail("Ingest(after): %v", err)
		}
		fmt.Printf("    получено %d за %s | сегментов: %d | пропусков закрыто: %d\n",
			len(recs), rtt.Round(time.Millisecond), res.Segments, res.GapsClosed)

		// Цикл обязан сходиться: якорь либо продвинулся, либо пропуск закрыт.
		// Отсутствие движения — ошибка, а не повод для ещё одной итерации.
		after, ok2, _ := feed.GapAnchor(ctx, *channelID)
		if ok2 && after.ID == anchor.ID && res.GapsClosed == 0 && len(recs) > 0 {
			fail("якорь не сдвинулся и пропуск не закрылся — цикл не сходится")
		}
	}

	after, _ := feed.Segments(ctx, *channelID)
	fmt.Printf("\n── состояние после догрузки ──\n")
	printSegments(after)

	readLocal(ctx, feed, stats, "локальное чтение с маркерами пропусков")
	report(ctx, feed, stats)

	fmt.Printf("\n── сеть, ОТДЕЛЬНО от локальной стоимости ──\n")
	fmt.Printf("запросов к Bridge: %d | суммарно %s | в среднем %s на запрос\n",
		netCalls, netTotal.Round(time.Millisecond),
		(netTotal / time.Duration(max(netCalls, 1))).Round(time.Millisecond))
}

func printSegments(segs []localfeed.Segment) {
	if len(segs) == 0 {
		fmt.Println("  сегментов нет — о полноте ленты ничего не известно")
		return
	}
	fmt.Printf("  сегментов: %d, пропусков между ними: %d\n", len(segs), len(segs)-1)
	for i, sg := range segs {
		fmt.Printf("    [%d] свежий край order=%d id=%s … старый край order=%d id=%s\n",
			i, sg.Newest.OrderKey, short(sg.Newest.ID), sg.Oldest.OrderKey, short(sg.Oldest.ID))
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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
		pg, err := feed.Page(ctx, *channelID, curKey, curID, *pageSize)
		if err != nil {
			fail("локальное чтение: %v", err)
		}
		recs := pg.Records
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
		if pg.GapAfter {
			// Именно это и есть результат среза: разрыв ПОКАЗАН, а не
			// склеен молча. Читатель видит границу известного.
			fmt.Printf("  ╌╌ ПРОПУСК: ниже есть записи, но что между ними — неизвестно ╌╌\n")
		}
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
