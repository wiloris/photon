// eventcheck — ручной прогон доставки событий против НАСТОЯЩЕГО Bridge.
//
// Не автотест. В `go test ./...` не участвует и участвовать не должен:
// требует живого сервера и валидной сессии Mattermost. Та же форма и та же
// причина, что у tests/livecheck и tests/p2pcheck.
//
// Юнит-тесты доставки идут на net.Pipe и доказывают ЛОГИКУ. Здесь проверяется
// то, чего они не проверяют: что подписка поднимается против настоящего QUIC,
// что событие сервера доходит целиком, что обрыв на уровне транспорта
// классифицируется как обрыв, а не как отмена, и что восстановление работает
// на настоящей сети.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/dispatcher"
)

var (
	addr      = flag.String("addr", "bridge.example.net:4443", "адрес Bridge")
	sessionID = flag.String("session", "", "Session ID Mattermost (обязателен)")
	channelID = flag.String("channel", "", "канал для тестовых постов (обязателен)")
	insecure  = flag.Bool("insecure", false, "не проверять сертификат")
	only      = flag.String("only", "", "выполнить только указанную проверку")
	p2pRole   = flag.String("p2p-role", "", "сквозной P2P-сценарий: a | b")
	peerUser  = flag.String("peer-user", "", "user_id собеседника: включает discovery через чужой профиль (разные аккаунты)")
)

const actionCreatePost = 100

var failures int

func ok(f string, a ...any)   { fmt.Printf("  ✅ "+f+"\n", a...) }
func fail(f string, a ...any) { fmt.Printf("  ❌ "+f+"\n", a...); failures++ }
func note(f string, a ...any) { fmt.Printf("  ·  "+f+"\n", a...) }

// recorder — потребитель событий. Считает, сохраняет порядок и ловит
// параллельные вызовы.
type recorder struct {
	mu         sync.Mutex
	events     []string
	inFlight   int
	concurrent bool
	hold       chan struct{} // если не nil, колбэк держится до закрытия
	entered    chan struct{}
}

func (r *recorder) sink(ev []byte) {
	r.mu.Lock()
	r.inFlight++
	if r.inFlight > 1 {
		r.concurrent = true
	}
	hold, entered := r.hold, r.entered
	r.mu.Unlock()

	if hold != nil {
		if entered != nil {
			select {
			case <-entered:
			default:
				close(entered)
			}
		}
		<-hold
	}

	r.mu.Lock()
	r.inFlight--
	r.events = append(r.events, string(ev))
	r.mu.Unlock()
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recorder) countType(t string) int {
	n := 0
	for _, e := range r.all() {
		if typeOf(e) == t {
			n++
		}
	}
	return n
}

func (r *recorder) waitType(t string, n int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if r.countType(t) >= n {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func typeOf(ev string) string {
	var p struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal([]byte(ev), &p)
	return p.Type
}

// newDispVia — инстанс, ходящий через переданный адрес. ServerName берётся из
// настоящего имени Bridge: при работе через прокси адрес — 127.0.0.1, и без
// явного ServerName сертификат не сойдётся.
func newDispVia(endpoint string, tune func(*dispatcher.Config)) (*dispatcher.Dispatcher, error) {
	host := *addr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return newDispCfg(endpoint, host, tune)
}

func newDisp(tune func(*dispatcher.Config)) (*dispatcher.Dispatcher, error) {
	return newDispCfg(*addr, "", tune)
}

func newDispCfg(endpoint, serverName string, tune func(*dispatcher.Config)) (*dispatcher.Dispatcher, error) {
	cfg := dispatcher.Config{
		Endpoint:        ports.Endpoint(endpoint),
		SessionToken:    *sessionID,
		ConnectTimeout:  15 * time.Second,
		CallTimeout:     30 * time.Second,
		EventBackoffMin: time.Second,
		EventBackoffMax: 5 * time.Second,
	}
	cfg.TLS.InsecureSkipVerify = *insecure
	if serverName != "" {
		cfg.TLS.ServerName = serverName
	}
	if tune != nil {
		tune(&cfg)
	}
	return dispatcher.New(cfg)
}

func createPost(d *dispatcher.Dispatcher, text string) error {
	payload, _ := json.Marshal(map[string]string{"channel_id": *channelID, "message": text})
	raw, err := d.SendAction(actionCreatePost, payload)
	if err != nil {
		return err
	}
	var env struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if uerr := json.Unmarshal(raw, &env); uerr != nil {
		return fmt.Errorf("конверт неразбираем: %s", raw)
	}
	if !env.OK {
		return fmt.Errorf("сервер отверг: %s", env.Error)
	}
	return nil
}

func run(name string, fn func()) {
	if *only != "" && *only != name {
		return
	}
	fmt.Printf("\n── %s ───────────────────────────────────\n", name)
	fn()
}

func main() {
	flag.Parse()
	if *sessionID == "" || *channelID == "" {
		fmt.Fprintln(os.Stderr, "нужны -session и -channel")
		os.Exit(2)
	}
	fmt.Printf("\n═══ eventcheck: %s ═══\n", *addr)

	if *p2pRole != "" {
		runP2P(*p2pRole)
		report()
		return
	}

	run("whoami", checkWhoami)
	run("cleanup-mine", checkCleanupMine)
	run("1-connected", check1Connected)
	run("2-message", check2Message)
	run("3-order", check3Order)
	run("4-disconnect", check4Disconnect)
	run("5-independence", check5Independence)
	run("8-two-reconnects", check8TwoReconnects)
	run("close-during-callback", checkCloseDuringCallback)
	run("close-during-backoff", checkCloseDuringBackoff)
	run("close-waiting-push", checkCloseWaitingPush)
	run("no-events-after-close", checkNoEventsAfterClose)
	run("buffer-overflow", checkBufferOverflow)

	report()
}

func report() {
	fmt.Println()
	if failures > 0 {
		fmt.Printf("ИТОГ: нарушенных ожиданий — %d\n\n", failures)
		os.Exit(1)
	}
	fmt.Printf("ИТОГ: все ожидания выполнены\n\n")
}

// whoami — под каким аккаунтом работает переданная сессия. Печатает только
// username и id: сессия в вывод не попадает.
func checkWhoami() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()
	raw, err := d.SendAction(actionGetMe, []byte(`{}`))
	if err != nil {
		fail("getMe: %v", err)
		return
	}
	var env struct {
		OK   bool `json:"ok"`
		Data struct {
			ID       string `json:"id"`
			Username string `json:"username"`
		} `json:"data"`
	}
	if uerr := json.Unmarshal(raw, &env); uerr != nil || !env.OK {
		fail("getMe вернул: %s", raw)
		return
	}
	ok("аккаунт: username=%s id=%s", env.Data.Username, env.Data.ID)
}

// cleanup-mine убирает следы прогона от имени ТЕКУЩЕЙ сессии: временные
// props photon_p2p_* и собственные служебные посты. Нужен там, где REST
// недоступен — есть только сессия Bridge, а не bearer-токен.
//
// Остальные props сохраняются: аккаунт может быть настоящим, и затирать его
// настройки ради уборки за тестом недопустимо.
func checkCleanupMine() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()

	raw, err := d.SendAction(actionGetMe, []byte(`{}`))
	if err != nil {
		fail("getMe: %v", err)
		return
	}
	var me struct {
		OK   bool `json:"ok"`
		Data struct {
			ID    string            `json:"id"`
			Props map[string]string `json:"props"`
		} `json:"data"`
	}
	if uerr := json.Unmarshal(raw, &me); uerr != nil || !me.OK {
		fail("getMe вернул: %s", raw)
		return
	}
	// patchMe на Bridge СЛИВАЕТ props, а не заменяет их:
	// handler_user.go делает `for k, v := range p.Props { user.Props[k] = v }`.
	// Значит удалить ключ через Photon нельзя — можно только обнулить
	// значение. Остальные props при этом не трогаются, что здесь и требуется.
	var temp int
	blank := map[string]string{}
	for k := range me.Data.Props {
		if strings.HasPrefix(k, "photon_p2p_") {
			temp++
			blank[k] = ""
		}
	}
	if temp > 0 {
		payload, _ := json.Marshal(map[string]any{"props": blank})
		if _, perr := d.SendAction(actionPatchMe, payload); perr != nil {
			fail("patchMe: %v", perr)
		} else {
			ok("временных props обнулено: %d (удалить ключ через Bridge нельзя — patchMe сливает)", temp)
		}
	} else {
		ok("временных props нет")
	}

	feed, err := d.SendAction(101, []byte(`{"channel_id":"`+*channelID+`"}`)) // ACTION_GET_FEED
	if err != nil {
		fail("getFeed: %v", err)
		return
	}
	// Форма ленты — order + КАРТА posts, а не массив: так отдаёт Mattermost,
	// и Bridge её не перекладывает.
	var fr struct {
		OK   bool `json:"ok"`
		Data struct {
			Order []string `json:"order"`
			Posts map[string]struct {
				ID       string `json:"id"`
				UserID   string `json:"user_id"`
				Message  string `json:"message"`
				DeleteAt int64  `json:"delete_at"`
			} `json:"posts"`
		} `json:"data"`
	}
	if uerr := json.Unmarshal(feed, &fr); uerr != nil || !fr.OK {
		fail("getFeed вернул: %s", head(feed))
		return
	}
	var deleted int
	for _, id := range fr.Data.Order {
		p := fr.Data.Posts[id]
		if p.UserID != me.Data.ID || p.DeleteAt != 0 {
			continue
		}
		if !strings.HasPrefix(p.Message, "PHOTONSIG1") && !strings.HasPrefix(p.Message, "eventcheck-") {
			continue
		}
		payload, _ := json.Marshal(map[string]string{"post_id": p.ID})
		if _, derr := d.SendAction(102, payload); derr == nil { // ACTION_DELETE_POST
			deleted++
		}
	}
	ok("собственных служебных постов удалено: %d", deleted)
}

func head(b []byte) string {
	if len(b) > 200 {
		return string(b[:200])
	}
	return string(b)
}

// 1. Подписка поднимается против настоящего Bridge и даёт CONNECTED.
func check1Connected() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()
	r := &recorder{}
	t0 := time.Now()
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("CONNECTED не пришёл за 30 с")
		return
	}
	ok("CONNECTED получен за %s", time.Since(t0).Round(time.Millisecond))
}

// 2. Пост, созданный через ОТДЕЛЬНОЕ action-соединение, приходит подпиской.
func check2Message() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()
	r := &recorder{}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("подписка не поднялась")
		return
	}

	marker := fmt.Sprintf("eventcheck-msg-%d", time.Now().UnixNano())
	if err := createPost(d, marker); err != nil {
		fail("создание поста: %v", err)
		return
	}
	ok("пост создан через action-соединение")

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range r.all() {
			if typeOf(e) == dispatcher.EventMessage && strings.Contains(e, marker) {
				ok("MESSAGE доставлен подпиской, маркер найден")
				var probe struct {
					Event  string          `json:"event"`
					UserID string          `json:"user_id"`
					Data   json.RawMessage `json:"data"`
				}
				_ = json.Unmarshal([]byte(e), &probe)
				ok("event=%q, user_id=%q (пусто — как описано в §8.2), data=%d байт",
					probe.Event, probe.UserID, len(probe.Data))
				if probe.Event != "posted" {
					fail("имя события на проводе: %q, ожидалось posted", probe.Event)
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fail("MESSAGE с маркером не пришёл за 30 с")
}

// 3. Несколько событий подряд: порядок сохраняется, колбэки не параллельны.
func check3Order() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()
	r := &recorder{}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("подписка не поднялась")
		return
	}

	const n = 5
	base := fmt.Sprintf("eventcheck-ord-%d", time.Now().UnixNano())
	for i := 0; i < n; i++ {
		if err := createPost(d, fmt.Sprintf("%s-%d", base, i)); err != nil {
			fail("создание поста %d: %v", i, err)
			return
		}
	}
	ok("создано %d постов подряд", n)

	deadline := time.Now().Add(45 * time.Second)
	var seen []int
	for time.Now().Before(deadline) && len(seen) < n {
		seen = seen[:0]
		for _, e := range r.all() {
			for i := 0; i < n; i++ {
				if strings.Contains(e, fmt.Sprintf("%s-%d", base, i)) {
					seen = append(seen, i)
				}
			}
		}
		if len(seen) < n {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if len(seen) < n {
		fail("получено %d из %d событий: %v", len(seen), n, seen)
		return
	}
	for i := 0; i < n; i++ {
		if seen[i] != i {
			fail("порядок нарушен: %v", seen)
			return
		}
	}
	ok("порядок сохранён: %v", seen)

	r.mu.Lock()
	conc := r.concurrent
	r.mu.Unlock()
	if conc {
		fail("колбэки выполнялись параллельно")
	} else {
		ok("параллельных вызовов колбэка не было")
	}
}

// 4. Настоящий обрыв подписки на уровне транспорта.
//
// Обрыв вызывается РАЗРЫВОМ ПУТИ через локальный UDP-ретранслятор, а не
// имитацией ошибки. Первая редакция пыталась добиться обрыва отодвинутым
// keep-alive и провалилась: keep-alive шлёт сам сервер (Phase 12.5), поэтому
// подписка простоем не умирает. Провал записан как находка, а не переформулирован.
func check4Disconnect() {
	px, err := newUDPProxy(*addr)
	if err != nil {
		fail("прокси: %v", err)
		return
	}
	defer px.close()

	d, err := newDispVia(px.addr(), nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()
	r := &recorder{}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("подписка не поднялась через прокси")
		return
	}
	ok("подписка поднята через ретранслятор %s", px.addr())

	px.breakPath()
	note("путь оборван — ждём обнаружения по idle-timeout QUIC")

	if !r.waitType(dispatcher.EventNetworkChanged, 1, 90*time.Second) {
		fail("NETWORK_CHANGED не пришёл за 90 с: обрыв не обнаружен")
		return
	}
	ok("NETWORK_CHANGED получен — обрыв классифицирован как обрыв, не как отмена")

	px.healPath()
	if !r.waitType(dispatcher.EventConnected, 2, 60*time.Second) {
		fail("повторный CONNECTED не пришёл: восстановление не сработало")
		return
	}
	ok("CONNECTED после восстановления пути — reconnect работает")
}

// 5. Action-соединение живёт независимо от подписки.
func check5Independence() {
	px, err := newUDPProxy(*addr)
	if err != nil {
		fail("прокси: %v", err)
		return
	}
	defer px.close()

	// Подписка идёт через прокси, действия — напрямую: так обрыв касается
	// РОВНО подписки, и независимость проверяется, а не предполагается.
	sub, err := newDispVia(px.addr(), nil)
	if err != nil {
		fail("New(sub): %v", err)
		return
	}
	defer sub.Close()
	act, err := newDisp(nil)
	if err != nil {
		fail("New(act): %v", err)
		return
	}
	defer act.Close()

	r := &recorder{}
	if err := sub.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("подписка не поднялась")
		return
	}
	px.breakPath()
	if !r.waitType(dispatcher.EventNetworkChanged, 1, 90*time.Second) {
		fail("обрыв подписки не наступил")
		return
	}
	ok("подписка оборвана")

	if err := createPost(act, fmt.Sprintf("eventcheck-indep-%d", time.Now().UnixNano())); err != nil {
		fail("action не работает при оборванной подписке: %v", err)
		return
	}
	ok("action выполнен при оборванной подписке — соединения независимы")
}

// 8. Два последовательных восстановления.
func check8TwoReconnects() {
	px, err := newUDPProxy(*addr)
	if err != nil {
		fail("прокси: %v", err)
		return
	}
	defer px.close()

	d, err := newDispVia(px.addr(), nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()
	r := &recorder{}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	for cycle := 1; cycle <= 2; cycle++ {
		if !r.waitType(dispatcher.EventConnected, cycle, 60*time.Second) {
			fail("цикл %d: CONNECTED не пришёл", cycle)
			return
		}
		px.breakPath()
		if !r.waitType(dispatcher.EventNetworkChanged, cycle, 90*time.Second) {
			fail("цикл %d: NETWORK_CHANGED не пришёл", cycle)
			return
		}
		px.healPath()
		ok("цикл %d: обрыв обнаружен и путь восстановлен", cycle)
	}
	if !r.waitType(dispatcher.EventConnected, 3, 60*time.Second) {
		fail("третий CONNECTED не пришёл: %v", typesOf(r.all()))
		return
	}
	ok("два последовательных восстановления: %v", typesOf(r.all()))
}

// Close во время исполнения колбэка обязан дождаться его возврата.
func checkCloseDuringCallback() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	r := &recorder{hold: make(chan struct{}), entered: make(chan struct{})}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	select {
	case <-r.entered:
	case <-time.After(30 * time.Second):
		fail("колбэк не начался")
		d.Close()
		return
	}
	ok("колбэк удерживается")

	closed := make(chan struct{})
	go func() { d.Close(); close(closed) }()
	select {
	case <-closed:
		fail("Close вернулся, пока колбэк исполнялся")
		close(r.hold)
		return
	case <-time.After(2 * time.Second):
		ok("Close ждёт возврата из колбэка")
	}
	close(r.hold)
	select {
	case <-closed:
		ok("Close вернулся после завершения колбэка")
	case <-time.After(30 * time.Second):
		fail("Close не вернулся после завершения колбэка")
	}
}

// Close во время паузы backoff возвращается сразу.
func checkCloseDuringBackoff() {
	px, err := newUDPProxy(*addr)
	if err != nil {
		fail("прокси: %v", err)
		return
	}
	defer px.close()

	d, err := newDispVia(px.addr(), func(c *dispatcher.Config) {
		c.EventBackoffMin = 60 * time.Second
		c.EventBackoffMax = 60 * time.Second
	})
	if err != nil {
		fail("New: %v", err)
		return
	}
	r := &recorder{}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("подписка не поднялась")
		d.Close()
		return
	}
	px.breakPath()
	if !r.waitType(dispatcher.EventNetworkChanged, 1, 90*time.Second) {
		fail("обрыв не наступил — паузу backoff застать нечем")
		d.Close()
		return
	}
	time.Sleep(3 * time.Second) // насос в паузе 60 с
	t0 := time.Now()
	d.Close()
	if el := time.Since(t0); el > 10*time.Second {
		fail("Close ждал паузу backoff: %s", el)
		return
	}
	ok("Close во время backoff вернулся за %s", time.Since(t0).Round(time.Millisecond))
}

// Close во время ожидания push возвращается сразу.
func checkCloseWaitingPush() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	r := &recorder{}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("подписка не поднялась")
		d.Close()
		return
	}
	time.Sleep(2 * time.Second) // насос стоит в AcceptPush
	t0 := time.Now()
	d.Close()
	if el := time.Since(t0); el > 10*time.Second {
		fail("Close в ожидании push занял %s", el)
		return
	}
	ok("Close в ожидании push вернулся за %s", time.Since(t0).Round(time.Millisecond))
}

// После Close событий больше не приходит.
func checkNoEventsAfterClose() {
	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	r := &recorder{}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("подписка не поднялась")
		d.Close()
		return
	}

	// Отдельный инстанс — чтобы было чем создать пост после закрытия первого.
	other, err := newDisp(nil)
	if err != nil {
		fail("второй инстанс: %v", err)
		d.Close()
		return
	}
	defer other.Close()

	d.Close()
	before := len(r.all())

	if err := createPost(other, fmt.Sprintf("eventcheck-after-close-%d", time.Now().UnixNano())); err != nil {
		fail("создание поста вторым инстансом: %v", err)
		return
	}
	time.Sleep(10 * time.Second)
	if after := len(r.all()); after != before {
		fail("после Close пришло %d новых событий", after-before)
		return
	}
	ok("после Close новых событий нет (ждали 10 с при заведомо созданном посте)")
}

// Наблюдение за переполнением буфера Bridge (event-delivery.md §9.3).
//
// Не «проверка», а ИЗМЕРЕНИЕ известного ограничения: PushRegistry сервера
// пишет в канал на 64 неблокирующим select с пустым default и роняет событие
// молча. Здесь фиксируется наблюдаемое поведение, а не утверждается, что
// доставка без потерь.
//
// Метод: колбэк удерживается, поэтому клиент перестаёт читать из сети; очередь
// входящих uni-стримов QUIC упирается в лимит, сервер блокируется на
// OpenUniStreamSync, его буфер на 64 наполняется, дальше начинаются потери.
func checkBufferOverflow() {
	const posts = 180

	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()

	r := &recorder{hold: make(chan struct{}), entered: make(chan struct{})}
	if err := d.StartEvents(r.sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	select {
	case <-r.entered:
	case <-time.After(30 * time.Second):
		fail("подписка не поднялась")
		return
	}
	ok("подписка поднята, колбэк удерживается — клиент перестал читать из сети")

	base := fmt.Sprintf("eventcheck-ovf-%d", time.Now().UnixNano())
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	var sent int64
	var mu sync.Mutex
	for i := 0; i < posts; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := createPost(d, fmt.Sprintf("%s-%d", base, i)); err == nil {
				mu.Lock()
				sent++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	note("отправлено постов: %d из %d (action-соединение работало при удержанном колбэке)", sent, posts)

	time.Sleep(5 * time.Second) // дать серверу упереться и начать ронять
	close(r.hold)               // отпускаем клиента

	deadline := time.Now().Add(60 * time.Second)
	seen := map[int]bool{}
	for time.Now().Before(deadline) {
		for _, e := range r.all() {
			for i := 0; i < posts; i++ {
				if !seen[i] && strings.Contains(e, fmt.Sprintf("%s-%d", base, i)) {
					seen[i] = true
				}
			}
		}
		if len(seen) >= int(sent) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	got := len(seen)
	note("доставлено уникальных событий: %d из %d отправленных", got, sent)
	switch {
	case int64(got) == sent:
		note("потерь не наблюдалось — порога переполнения не достигли")
	case got < int(sent):
		note("ПОТЕРЯНО %d событий — наблюдаемое поведение PushRegistry при переполнении", int(sent)-got)
		note("доставка событий НЕ является lossless; это известное ограничение сервера")
	}
	ok("наблюдение зафиксировано (проверка не выносит вердикта PASS/FAIL)")
}

func typesOf(events []string) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, typeOf(e))
	}
	return out
}
