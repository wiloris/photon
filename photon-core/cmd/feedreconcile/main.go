// feedreconcile — РУЧНОЙ лабораторный прогон четвёртого вертикального среза
// Phase 12: «запись изменилась на сервере, и клиент обязан согласовать её у
// себя».
//
// Спецификация до кода — photon-core/docs/feed-reconcile.md. Единственная
// цель: дать наблюдение вопросу №5 среза — какая семантика Version нужна.
// Сегодня Record.Version объявлен и сериализуется, но не читается нигде.
//
// Не автотест и им не станет: требует живого Bridge и действующей сессии.
// Package main поэтому: `go test ./...` его не подхватывает, `go build ./...`
// собирает, так что расхождение с API пакетов ломает сборку.
//
// Режимы:
//
//	-observe    что происходит НА ПРОВОДЕ при правке и удалении: меняется ли
//	            create_at, что приходит в update_at и delete_at, и виден ли
//	            удалённый пост в ленте. Локального хранилища не касается
//	-reconcile  полный сценарий §5 спецификации против локального хранилища
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	quicadapter "github.com/wiloris/photon-core/adapters/quic"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/dispatcher"
	"github.com/wiloris/photon-core/services/localfeed"
)

var (
	addr      = flag.String("addr", "138.124.91.123.sslip.io:4443", "адрес Bridge")
	sessionID = flag.String("session", "", "Session ID Mattermost")
	channelID = flag.String("channel", "", "канал")
	dbPath    = flag.String("db", "", "файл локального хранилища (для -reconcile)")
	observe   = flag.Bool("observe", false, "только наблюдение провода, без хранилища")
	reconcile = flag.Bool("reconcile", false, "полный сценарий согласования")
	pageSize  = flag.Int("page-size", 10, "размер страницы при чтении окна")
	keep      = flag.Bool("keep", false, "не удалять созданный пост в конце -observe")
)

const (
	actionCreatePost = 100
	actionGetFeed    = 101
	actionDeletePost = 102
	actionUpdatePost = 103
)

type post struct {
	ID       string `json:"id"`
	Message  string `json:"message"`
	CreateAt int64  `json:"create_at"`
	UpdateAt int64  `json:"update_at"`
	DeleteAt int64  `json:"delete_at"`
	EditAt   int64  `json:"edit_at"`
}

type feedData struct {
	Order []string        `json:"order"`
	Posts map[string]post `json:"posts"`
}

type envelope struct {
	OK    bool            `json:"ok"`
	Error string          `json:"error"`
	Data  json.RawMessage `json:"data"`
}

func main() {
	flag.Parse()
	if *sessionID == "" || *channelID == "" {
		fmt.Fprintln(os.Stderr, "нужны -session и -channel")
		os.Exit(2)
	}
	if !*observe && !*reconcile {
		fmt.Fprintln(os.Stderr, "нужен -observe или -reconcile")
		os.Exit(2)
	}
	if *reconcile && *dbPath == "" {
		fmt.Fprintln(os.Stderr, "-reconcile требует -db")
		os.Exit(2)
	}

	ctx := context.Background()
	d := connect()
	defer d.Close()

	if *observe {
		runObserve(d)
		return
	}
	runReconcile(ctx, d)
}

func connect() *dispatcher.Dispatcher {
	tr := quicadapter.New(quicadapter.Config{
		TLSConfig:        &tls.Config{MinVersion: tls.VersionTLS13},
		HandshakeTimeout: 10 * time.Second,
	})
	d, err := dispatcher.New(dispatcher.Config{
		Transport:    tr,
		Endpoint:     ports.Endpoint(*addr),
		SessionToken: *sessionID,
		CallTimeout:  20 * time.Second,
	})
	if err != nil {
		fail("dispatcher: %v", err)
	}
	return d
}

func action(d *dispatcher.Dispatcher, code int32, payload map[string]any) json.RawMessage {
	raw, _ := json.Marshal(payload)
	resp, err := d.SendAction(code, raw)
	if err != nil {
		fail("SendAction(%d): %v", code, err)
	}
	var e envelope
	if err := json.Unmarshal(resp, &e); err != nil {
		fail("разбор ответа (%d): %v", code, err)
	}
	if !e.OK {
		fail("сервер отверг %d: %s", code, e.Error)
	}
	return e.Data
}

func onePost(data json.RawMessage) post {
	var p post
	if err := json.Unmarshal(data, &p); err != nil {
		fail("разбор поста: %v", err)
	}
	return p
}

func window(d *dispatcher.Dispatcher) ([]localfeed.Record, feedData) {
	data := action(d, actionGetFeed, map[string]any{
		"channel_id": *channelID, "limit": *pageSize,
	})
	var fd feedData
	if err := json.Unmarshal(data, &fd); err != nil {
		fail("разбор ленты: %v", err)
	}
	recs := make([]localfeed.Record, 0, len(fd.Order))
	for _, id := range fd.Order {
		p := fd.Posts[id]
		body, _ := json.Marshal(p)
		recs = append(recs, localfeed.Record{
			ID: p.ID, OrderKey: p.CreateAt, Version: p.UpdateAt, Body: body,
		})
	}
	return recs, fd
}

func show(title string, p post) {
	fmt.Printf("  %-22s id=%s\n", title, p.ID)
	fmt.Printf("      create_at=%d  update_at=%d  edit_at=%d  delete_at=%d\n",
		p.CreateAt, p.UpdateAt, p.EditAt, p.DeleteAt)
	fmt.Printf("      message=%q\n", p.Message)
}

// runObserve отвечает на вопросы §3 спецификации, которые про ПРОВОД:
// меняется ли create_at при правке (вопрос 2) и как выглядит удаление
// (вопрос 3). Локального хранилища не касается намеренно: сначала факты.
func runObserve(d *dispatcher.Dispatcher) {
	fmt.Printf("\n═══ feedreconcile -observe: канал %s ═══\n\n", *channelID)

	created := onePost(action(d, actionCreatePost, map[string]any{
		"channel_id": *channelID, "message": "reconcile-probe v1",
	}))
	show("1. создан", created)

	time.Sleep(1200 * time.Millisecond) // чтобы update_at заведомо отличался

	edited := onePost(action(d, actionUpdatePost, map[string]any{
		"post_id": created.ID, "message": "reconcile-probe v2 ОТРЕДАКТИРОВАН",
	}))
	fmt.Println()
	show("2. после правки", edited)

	fmt.Println("\n  ── что изменилось ──")
	fmt.Printf("      create_at: %d → %d  %s\n", created.CreateAt, edited.CreateAt,
		verdict(created.CreateAt == edited.CreateAt, "НЕ изменился", "ИЗМЕНИЛСЯ"))
	fmt.Printf("      update_at: %d → %d  %s\n", created.UpdateAt, edited.UpdateAt,
		verdict(edited.UpdateAt > created.UpdateAt, "вырос", "НЕ вырос"))
	fmt.Printf("      edit_at:   %d → %d\n", created.EditAt, edited.EditAt)

	_, fd := window(d)
	inFeed, fromFeed := findIn(fd, created.ID)
	fmt.Printf("\n  3. в ленте после правки: %s\n", verdict(inFeed, "присутствует", "ОТСУТСТВУЕТ"))
	if inFeed {
		fmt.Printf("      версия в ленте update_at=%d %s\n", fromFeed.UpdateAt,
			verdict(fromFeed.UpdateAt == edited.UpdateAt, "= версии из ответа", "≠ версии из ответа"))
	}

	action(d, actionDeletePost, map[string]any{"post_id": created.ID})
	_, fd2 := window(d)
	inFeed2, fromFeed2 := findIn(fd2, created.ID)
	fmt.Printf("\n  4. после удаления запись в ленте: %s\n",
		verdict(inFeed2, "ПРИСУТСТВУЕТ (надгробие)", "отсутствует (исчезла)"))
	if inFeed2 {
		show("      надгробие", fromFeed2)
	}

	fmt.Println("\n  ── ответы на вопросы спецификации ──")
	fmt.Printf("      №2 позиция при правке: %s\n",
		verdict(created.CreateAt == edited.CreateAt,
			"НЕ двигается — OrderKey остаётся create_at",
			"ДВИГАЕТСЯ — задевает замороженный ADR-052, §7"))
	fmt.Printf("      №3 форма удаления:     %s\n",
		verdict(inFeed2, "надгробие в ленте", "исчезновение из ленты"))
}

func findIn(fd feedData, id string) (bool, post) {
	p, ok := fd.Posts[id]
	if !ok {
		return false, post{}
	}
	for _, o := range fd.Order {
		if o == id {
			return true, p
		}
	}
	// Пост есть в posts, но не в order — тоже форма ответа, и её надо видеть.
	return false, p
}

func verdict(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}

// runReconcile — сценарий §5 спецификации против настоящего хранилища.
func runReconcile(ctx context.Context, d *dispatcher.Dispatcher) {
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

	stats := &localfeed.Stats{}
	feed := localfeed.New(st, stats)
	if err := feed.Prepare(ctx); err != nil {
		fail("подготовка namespace: %v", err)
	}

	fmt.Printf("\n═══ feedreconcile -reconcile: канал %s ═══\n", *channelID)
	fmt.Printf("хранилище: %s\n\n", *dbPath)

	// 1. Опорные записи вокруг пробной.
	//
	// Без них удаление пробной записи оставляет окно ПУСТЫМ, блок не
	// покрывает никакого диапазона, и исчезновение проверить нечем: первый
	// живой прогон именно на это и наткнулся. Опоры держат диапазон
	// покрытым, чтобы пропажа была наблюдаема.
	anchorOld := onePost(action(d, actionCreatePost, map[string]any{
		"channel_id": *channelID, "message": "reconcile anchor OLD",
	}))
	created := onePost(action(d, actionCreatePost, map[string]any{
		"channel_id": *channelID, "message": "reconcile v1",
	}))
	anchorNew := onePost(action(d, actionCreatePost, map[string]any{
		"channel_id": *channelID, "message": "reconcile anchor NEW",
	}))
	fmt.Printf("\n  опоры: старая %s, новая %s\n", short(anchorOld.ID), short(anchorNew.ID))

	// 2. КЛИЕНТ ПРОЧИТАЛ окно и записал его локально — ДО правки.
	//
	// Порядок существен и записан в §5 спецификации: если читать окно до
	// создания записи, правка придёт первой встречей, и путь обновления не
	// будет пройден вовсе. Первый прогон именно так и получился.
	recs0, _ := window(d)
	ingest(ctx, feed, recs0, "1. окно ДО правки")
	// 3. Теперь правка: запись уже лежит локально, версия обязана вырасти.
	time.Sleep(1200 * time.Millisecond)
	edited := onePost(action(d, actionUpdatePost, map[string]any{
		"post_id": created.ID, "message": "reconcile v2 ОТРЕДАКТИРОВАН",
	}))
	fmt.Printf("\n  запись %s: create_at=%d, версия %d → %d\n",
		created.ID, created.CreateAt, created.UpdateAt, edited.UpdateAt)

	// 3. Перечитать окно и согласовать.
	recs2, _ := window(d)
	res2 := ingest(ctx, feed, recs2, "2. окно ПОСЛЕ правки")

	// 4. Что лежит локально.
	fmt.Printf("      путь обновления пройден: %s\n",
		verdict(res2.Updated >= 1, "да", "НЕТ — правка пришла первой встречей"))
	local, ok := findLocal(ctx, feed, created.ID)
	fmt.Printf("\n  локально после согласования: ")
	if !ok {
		fmt.Println("записи НЕТ")
	} else {
		fmt.Printf("версия %d, тело %q\n", local.Version, messageOf(local))
		fmt.Printf("      тело обновилось:  %s\n",
			verdict(messageOf(local) == edited.Message, "да", "НЕТ — согласование не сработало"))
		fmt.Printf("      позиция прежняя:  %s\n",
			verdict(local.OrderKey == created.CreateAt, "да", "НЕТ — задет ADR-052"))
	}

	// 5. Удаление: опоры остаются, диапазон блока по-прежнему покрывает
	//    позицию удалённой записи — только так исчезновение и наблюдаемо.
	action(d, actionDeletePost, map[string]any{"post_id": created.ID})
	recs3, _ := window(d)
	res3 := ingest(ctx, feed, recs3, "3. окно после удаления")
	local2, ok2 := findLocal(ctx, feed, created.ID)
	fmt.Printf("\n  исчезновений замечено: %d %s\n", res3.Vanished,
		verdict(res3.Vanished >= 1, "← удаление увидено через диапазон блока",
			"← НЕ увидено: проверить покрытие диапазона"))
	fmt.Printf("  локально запись: ")
	if ok2 {
		fmt.Printf("ОСТАЛАСЬ, версия %d — срез считает, но не удаляет\n", local2.Version)
	} else {
		fmt.Println("удалена — этого срез делать не должен")
	}

	// Уборка опор: следов после прогона не остаётся.
	for _, id := range []string{anchorOld.ID, anchorNew.ID} {
		action(d, actionDeletePost, map[string]any{"post_id": id})
	}

	fmt.Println("\n" + stats.Report())
}

func ingest(ctx context.Context, feed *localfeed.Store, recs []localfeed.Record, title string) localfeed.IngestResult {
	if len(recs) == 0 {
		fmt.Printf("  %s: пусто\n", title)
		return localfeed.IngestResult{}
	}
	// Окно читается БЕЗ курсора («самые свежие»), поэтому блок объявляется
	// DirNewest и якоря не имеет: Anchor обязателен только для
	// DirBefore/DirAfter, иначе блок объявил бы непрерывным участок, который
	// сервер не подтверждал (feed-catchup.md §4.1).
	b := localfeed.Block{
		Records: recs,
		Dir:     localfeed.DirNewest,
		AtEnd:   len(recs) < *pageSize,
	}
	res, err := feed.Ingest(ctx, *channelID, b)
	if err != nil {
		fail("Ingest: %v", err)
	}
	fmt.Printf("  %s: применено %d (обновлено %d, без изменений %d, отвергнуто устаревших %d), "+
		"исчезло %d, сегментов %d, GapsClosed %d\n",
		title, res.Stored, res.Updated, res.Unchanged, res.Stale, res.Vanished,
		res.Segments, res.GapsClosed)
	return res
}

func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func findLocal(ctx context.Context, feed *localfeed.Store, id string) (localfeed.Record, bool) {
	page, err := feed.Page(ctx, *channelID, 0, "", 200)
	if err != nil {
		fail("Page: %v", err)
	}
	for _, r := range page.Records {
		if r.ID == id {
			return r, true
		}
	}
	return localfeed.Record{}, false
}

func messageOf(r localfeed.Record) string {
	var p post
	_ = json.Unmarshal(r.Body, &p)
	return p.Message
}

func fail(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "\n"+f+"\n", a...)
	os.Exit(1)
}
