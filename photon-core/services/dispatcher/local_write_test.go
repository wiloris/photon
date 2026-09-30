package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/wiloris/photon-core/services/localfeed"
	"github.com/wiloris/photon-core/services/session"
)

/*
 * Действия записи — ADR-065, третий срез.
 *
 * Проверяется не «ответ разобрался», а четыре свойства, каждое из которых
 * легко потерять правкой: локальное действие не доходит до Bridge; int64 не
 * проезжает через float64; отказ приходит как ошибка ВЫЗОВА нужного класса, а
 * не пустым успехом; счётчики Ingest отдаются все и со знаком.
 */

// ── вспомогательное ─────────────────────────────────────────────────────

type ingestData struct {
	Stored     int `json:"stored"`
	Segments   int `json:"segments"`
	Updated    int `json:"updated"`
	Unchanged  int `json:"unchanged"`
	Stale      int `json:"stale"`
	Vanished   int `json:"vanished"`
	GapsClosed int `json:"gaps_closed"`
}

func okData(t *testing.T, resp []byte, dst any) {
	t.Helper()
	var env struct {
		OK    bool            `json:"ok"`
		Data  json.RawMessage `json:"data"`
		Error string          `json:"error"`
	}
	if err := json.Unmarshal(resp, &env); err != nil {
		t.Fatalf("конверт не разбирается: %v (%s)", err, resp)
	}
	if !env.OK {
		t.Fatalf("ok:false — %s (%s)", env.Error, resp)
	}
	if dst != nil {
		if err := json.Unmarshal(env.Data, dst); err != nil {
			t.Fatalf("data не разбирается: %v (%s)", err, env.Data)
		}
	}
}

// ── маршрутизация ───────────────────────────────────────────────────────

func TestLocalWrite_NeverReachesBridge(t *testing.T) {
	// То же свойство, что у 800, и проверяется так же: счётчик подключений
	// фиктивного транспорта. Исчезнет локальная ветка — тест упадёт здесь, а
	// не на форме ответа.
	st := openTestStorage(t)
	d, br := localDispatcher(t, st)

	calls := []struct {
		action  int32
		payload string
	}{
		{actionLocalFeedPut, `{"channel_id":"ch","records":[{"id":"a","order_key":100,"version":100,"body":{"n":1}}]}`},
		{actionLocalFeedIngest, `{"channel_id":"ch","records":[{"id":"b","order_key":200,"version":200,"body":{"n":2}}],"dir":"newest"}`},
		{actionLocalFeedGapAnchor, `{"channel_id":"ch"}`},
	}
	for _, c := range calls {
		if _, err := d.SendAction(c.action, []byte(c.payload)); err != nil {
			t.Fatalf("SendAction(%d): %v", c.action, err)
		}
	}
	if n := br.connectCount(); n != 0 {
		t.Fatalf("запись ушла в Bridge: подключений %d, ожидалось 0", n)
	}
}

func TestLocalWrite_ResourceForIsTheArbiter(t *testing.T) {
	// Арбитр один (ADR-064 п. 2.3): диапазон называет ResourceFor, а не
	// сравнение с числом внутри диспетчера.
	for _, action := range []int32{actionLocalFeedPut, actionLocalFeedIngest, actionLocalFeedGapAnchor} {
		res, err := session.ResourceFor(action)
		if err != nil {
			t.Fatalf("ResourceFor(%d): %v", action, err)
		}
		if res != session.ResourceLocalFeed {
			t.Fatalf("ResourceFor(%d) = %q, ожидался %q", action, res, session.ResourceLocalFeed)
		}
	}
}

func TestLocalWrite_WithoutStorageFailsExplicitly(t *testing.T) {
	// Инстанс без секции storage законен (ADR-064 п. 1.5); запись на нём
	// обязана получить явный отказ, а не пустой успех.
	br := newFakeBridge()
	d, err := New(Config{Endpoint: "fake:4443", SessionToken: "tok", Transport: br})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	_, err = d.SendAction(actionLocalFeedPut, []byte(`{"channel_id":"ch","records":[]}`))
	if !errors.Is(err, ErrNoStorage) {
		t.Fatalf("SendAction: %v, ожидалось ErrNoStorage", err)
	}
	if got := Classify(err); got != ClassNotConnected {
		t.Fatalf("класс %v, ожидался ClassNotConnected", got)
	}
}

// ── сквозной путь: запись видна чтением ─────────────────────────────────

func TestLocalWrite_PutThenPageRoundTrip(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	body := `{"message":"привет","attachments":[1,2]}`
	_, err := d.SendAction(actionLocalFeedPut, []byte(
		`{"channel_id":"ch","records":[{"id":"p1","order_key":1757000000123,"version":1757000000456,"body":`+body+`}]}`))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := d.SendAction(actionLocalFeedPage, []byte(`{"channel_id":"ch","limit":10}`))
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	var page struct {
		Records []struct {
			ID       string          `json:"id"`
			OrderKey int64           `json:"order_key"`
			Version  int64           `json:"version"`
			Body     json.RawMessage `json:"body"`
		} `json:"records"`
	}
	okData(t, resp, &page)

	if len(page.Records) != 1 {
		t.Fatalf("записей %d, ожидалась 1", len(page.Records))
	}
	r := page.Records[0]
	// Миллисекунды эпохи проходят круг без искажения. Само по себе это НЕ
	// доказывает типизированного разбора: 1.7e12 меньше 2^53, и float64
	// представил бы его точно. Доказывает следующий тест.
	if r.OrderKey != 1757000000123 || r.Version != 1757000000456 {
		t.Fatalf("int64 искажён: order_key=%d version=%d", r.OrderKey, r.Version)
	}
	if string(r.Body) != body {
		t.Fatalf("тело изменилось:\n  было:  %s\n  стало: %s", body, r.Body)
	}
}

// Значение ВЫШЕ 2^53 — то, на чём float64 ломается, а int64 нет.
//
// Тест заведён после мутации: разбор был заменён на float64, и проверка
// round-trip выше её НЕ убила, потому что миллисекунды эпохи (1.7e12) float64
// представляет точно. Зелёный тест, проходящий на мутации, хуже
// отсутствующего — поэтому здесь стоит число, которое разошлось бы: 2^53+1
// округляется float64 до 2^53.
//
// Ни один create_at такого размера не встречается, и это не довод против:
// проверяется КОНТРАКТ разбора, а не диапазон сегодняшних данных. Контракт
// сказан в ADR-065 п. 2.2 — int64, а не «int64, пока значения малы».
func TestLocalWrite_Int64SurvivesAbove2Pow53(t *testing.T) {
	const big = int64(9007199254740993) // 2^53 + 1; float64 даст 9007199254740992

	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	if _, err := d.SendAction(actionLocalFeedPut, []byte(
		`{"channel_id":"ch","records":[{"id":"big","order_key":9007199254740993,"version":9007199254740993,"body":{}}]}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := d.SendAction(actionLocalFeedPage, []byte(`{"channel_id":"ch","limit":10}`))
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	var page struct {
		Records []struct {
			OrderKey int64 `json:"order_key"`
			Version  int64 `json:"version"`
		} `json:"records"`
	}
	okData(t, resp, &page)
	if len(page.Records) != 1 {
		t.Fatalf("записей %d, ожидалась 1", len(page.Records))
	}
	if page.Records[0].OrderKey != big || page.Records[0].Version != big {
		t.Fatalf("значение искажено разбором: order_key=%d version=%d, ожидалось %d",
			page.Records[0].OrderKey, page.Records[0].Version, big)
	}
}

func TestLocalWrite_PutReportsStoredCount(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	resp, err := d.SendAction(actionLocalFeedPut, []byte(
		`{"channel_id":"ch","records":[
			{"id":"a","order_key":100,"version":100,"body":{}},
			{"id":"b","order_key":200,"version":200,"body":{}}]}`))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	var out struct {
		Stored int `json:"stored"`
	}
	okData(t, resp, &out)
	if out.Stored != 2 {
		t.Fatalf("stored=%d, ожидалось 2", out.Stored)
	}
}

// Put сегментов НЕ создаёт: он ничего не утверждает о непрерывности
// (ADR-065 п. 5.1). Проверяется через 803 — после Put пропуска нет, потому
// что нет и сегментов.
func TestLocalWrite_PutCreatesNoSegments(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	if _, err := d.SendAction(actionLocalFeedPut, []byte(
		`{"channel_id":"ch","records":[{"id":"a","order_key":100,"version":100,"body":{}}]}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	segs, err := localfeed.New(st, nil).Segments(context.Background(), "ch")
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 0 {
		t.Fatalf("сегментов %d, ожидалось 0 — Put объявил полноту", len(segs))
	}
}

// ── Ingest: счётчики ────────────────────────────────────────────────────

func TestLocalWrite_IngestReturnsAllCounters(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	first := `{"channel_id":"ch","dir":"newest","records":[
		{"id":"a","order_key":100,"version":100,"body":{}},
		{"id":"b","order_key":200,"version":200,"body":{}}]}`
	resp, err := d.SendAction(actionLocalFeedIngest, []byte(first))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	var got ingestData
	okData(t, resp, &got)
	if got.Stored != 2 || got.Segments != 1 {
		t.Fatalf("первый блок: %+v", got)
	}

	// Повтор того же блока: версии совпали — Unchanged, а не Updated.
	// Идемпотентность (ADR-065 п. 12.2) наблюдается именно здесь.
	resp, err = d.SendAction(actionLocalFeedIngest, []byte(first))
	if err != nil {
		t.Fatalf("Ingest(повтор): %v", err)
	}
	okData(t, resp, &got)
	if got.Unchanged != 2 || got.Updated != 0 || got.Stale != 0 {
		t.Fatalf("повтор: %+v, ожидалось unchanged=2", got)
	}

	// Версия младше локальной не применяется (ADR-053).
	resp, err = d.SendAction(actionLocalFeedIngest, []byte(
		`{"channel_id":"ch","dir":"newest","records":[{"id":"a","order_key":100,"version":50,"body":{}}]}`))
	if err != nil {
		t.Fatalf("Ingest(stale): %v", err)
	}
	okData(t, resp, &got)
	if got.Stale != 1 || got.Stored != 0 {
		t.Fatalf("stale-блок: %+v, ожидалось stale=1 stored=0", got)
	}
}

// gaps_closed ЗНАКОВЫЙ: отрицательное значение законно и означает новый
// пропуск. Кодирование в unsigned или «нормализация» нулём потеряла бы ровно
// тот случай, ради которого поле и заведено.
func TestLocalWrite_GapsClosedMayBeNegative(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	// Старый кусок.
	if _, err := d.SendAction(actionLocalFeedIngest, []byte(
		`{"channel_id":"ch","dir":"newest","records":[{"id":"o1","order_key":100,"version":100,"body":{}}]}`)); err != nil {
		t.Fatalf("Ingest(old): %v", err)
	}
	// Свежий остров, не смыкающийся со старым, — появляется пропуск.
	resp, err := d.SendAction(actionLocalFeedIngest, []byte(
		`{"channel_id":"ch","dir":"before","anchor":{"order_key":9000,"id":"n0"},
		  "records":[{"id":"n1","order_key":8000,"version":8000,"body":{}}]}`))
	if err != nil {
		t.Fatalf("Ingest(island): %v", err)
	}
	var got ingestData
	okData(t, resp, &got)
	if got.GapsClosed >= 0 {
		t.Fatalf("gaps_closed=%d, ожидалось отрицательное (появился пропуск): %+v", got.GapsClosed, got)
	}
}

// ── 803: позиция пропуска ───────────────────────────────────────────────

func TestLocalWrite_GapAnchorFoundAndNotFound(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	type anchorOut struct {
		Found  bool `json:"found"`
		Anchor *struct {
			OrderKey int64  `json:"order_key"`
			ID       string `json:"id"`
		} `json:"anchor"`
	}

	// Пропуска нет — found:false, и это НЕ ошибка и не пустой ответ.
	resp, err := d.SendAction(actionLocalFeedGapAnchor, []byte(`{"channel_id":"ch"}`))
	if err != nil {
		t.Fatalf("GapAnchor(пусто): %v", err)
	}
	var out anchorOut
	okData(t, resp, &out)
	if out.Found || out.Anchor != nil {
		t.Fatalf("на пустом канале found=%v anchor=%+v, ожидалось found:false без якоря", out.Found, out.Anchor)
	}

	// Два несмежных сегмента — пропуск появляется.
	for _, p := range []string{
		`{"channel_id":"ch","dir":"newest","records":[{"id":"o1","order_key":100,"version":100,"body":{}}]}`,
		`{"channel_id":"ch","dir":"before","anchor":{"order_key":9000,"id":"n0"},"records":[{"id":"n1","order_key":8000,"version":8000,"body":{}}]}`,
	} {
		if _, err := d.SendAction(actionLocalFeedIngest, []byte(p)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	resp, err = d.SendAction(actionLocalFeedGapAnchor, []byte(`{"channel_id":"ch"}`))
	if err != nil {
		t.Fatalf("GapAnchor: %v", err)
	}
	out = anchorOut{}
	okData(t, resp, &out)
	if !out.Found || out.Anchor == nil {
		t.Fatalf("пропуск есть, а якоря нет: %+v", out)
	}
	// Якорь — Newest сегмента ПОД пропуском, а не самая свежая запись: запрос
	// от неё вернул бы уже известное и оставил дыру навсегда.
	if out.Anchor.ID != "o1" || out.Anchor.OrderKey != 100 {
		t.Fatalf("якорь %+v, ожидался нижний сегмент (o1/100)", *out.Anchor)
	}
}

// ── malformed payload и отказы ──────────────────────────────────────────

func TestLocalWrite_MalformedPayloadRejected(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	cases := []struct {
		name    string
		action  int32
		payload string
	}{
		{"put: не json", actionLocalFeedPut, `не json`},
		{"put: без channel_id", actionLocalFeedPut, `{"records":[]}`},
		{"put: пустой id записи", actionLocalFeedPut,
			`{"channel_id":"ch","records":[{"id":"","order_key":100,"body":{}}]}`},
		{"put: order_key строкой", actionLocalFeedPut,
			`{"channel_id":"ch","records":[{"id":"a","order_key":"100","body":{}}]}`},
		{"put: order_key дробный", actionLocalFeedPut,
			`{"channel_id":"ch","records":[{"id":"a","order_key":100.5,"body":{}}]}`},
		{"ingest: не json", actionLocalFeedIngest, `{`},
		{"ingest: без channel_id", actionLocalFeedIngest, `{"dir":"newest","records":[]}`},
		{"ingest: dir отсутствует", actionLocalFeedIngest,
			`{"channel_id":"ch","records":[{"id":"a","order_key":100,"body":{}}]}`},
		{"ingest: dir неизвестен", actionLocalFeedIngest,
			`{"channel_id":"ch","dir":"NEWEST","records":[{"id":"a","order_key":100,"body":{}}]}`},
		{"ingest: after без якоря", actionLocalFeedIngest,
			`{"channel_id":"ch","dir":"after","records":[{"id":"a","order_key":100,"body":{}}]}`},
		{"ingest: якорь при newest", actionLocalFeedIngest,
			`{"channel_id":"ch","dir":"newest","anchor":{"order_key":50,"id":"z"},"records":[{"id":"a","order_key":100,"body":{}}]}`},
		{"ingest: запись не по ту сторону якоря", actionLocalFeedIngest,
			`{"channel_id":"ch","dir":"after","anchor":{"order_key":500,"id":"m"},"records":[{"id":"a","order_key":100,"body":{}}]}`},
		{"ingest: пустой блок без якоря", actionLocalFeedIngest,
			`{"channel_id":"ch","dir":"newest","records":[]}`},
		{"gap_anchor: без channel_id", actionLocalFeedGapAnchor, `{}`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := d.SendAction(c.action, []byte(c.payload))
			if err == nil {
				t.Fatalf("payload принят, ожидался отказ")
			}
			// Класс обязан вести вызывающего к исправлению payload, а не к
			// повтору и не к поиску причины в хранилище.
			if got := Classify(err); got != ClassInvalidArgument {
				t.Fatalf("класс %v, ожидался ClassInvalidArgument (ошибка: %v)", got, err)
			}
		})
	}
}

// Отказ обязан быть ПОЛНЫМ: ни одной записи после отвергнутого блока.
// «Ошибку вернул» и «ничего не записал» — разные утверждения.
func TestLocalWrite_RejectedBlockWritesNothing(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	_, err := d.SendAction(actionLocalFeedIngest, []byte(
		`{"channel_id":"ch","dir":"after","anchor":{"order_key":500,"id":"m"},"records":[
			{"id":"good","order_key":600,"version":600,"body":{}},
			{"id":"bad","order_key":100,"version":100,"body":{}}]}`))
	if err == nil {
		t.Fatal("блок с записью не по ту сторону якоря принят")
	}
	n, cerr := localfeed.New(st, nil).Count(context.Background(), "ch")
	if cerr != nil {
		t.Fatalf("Count: %v", cerr)
	}
	if n != 0 {
		t.Fatalf("записей %d после отказа, ожидалось 0", n)
	}
}

// Сохранение исходного sentinel'а — не украшение: класс берётся из
// существующей classTable, а причина остаётся различима errors.Is. Потеря
// одного из двух не видна без этой проверки.
func TestLocalWrite_ErrorKeepsBothClassAndSentinel(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	_, err := d.SendAction(actionLocalFeedPut, []byte(
		`{"channel_id":"ch","records":[{"id":"","order_key":100,"body":{}}]}`))
	if !errors.Is(err, session.ErrInvalidPayload) {
		t.Fatalf("класс потерян: %v", err)
	}
	if !errors.Is(err, localfeed.ErrEmptyRecordID) {
		t.Fatalf("исходный sentinel потерян: %v", err)
	}
}

// Неизвестное поле в payload игнорируется — то же правило, по которому ядро
// читает config_json. Отвергать payload из-за поля, которого ядро не знает,
// значило бы ломать клиента при каждом расширении формы.
func TestLocalWrite_UnknownPayloadFieldIgnored(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	if _, err := d.SendAction(actionLocalFeedPut, []byte(
		`{"channel_id":"ch","provenance":"whatever","records":[{"id":"a","order_key":100,"version":100,"body":{}}]}`)); err != nil {
		t.Fatalf("неизвестное поле отвергнуто: %v", err)
	}
}

// ── полнота классификации ошибок localfeed ──────────────────────────────

// TestLocalWrite_EveryLocalfeedSentinelIsClassified — машинная проверка полноты.
//
// wrapFeedWriteError относит ошибку к классу по списку. Новый sentinel в
// localfeed, не попавший ни в feedWriteArgErrors, ни в список «не аргумент»
// ниже, уехал бы в ClassInternal молча — и вызывающий пошёл бы искать причину
// в хранилище вместо своего payload.
//
// Тест читает исходники соседнего пакета тем же приёмом, что
// TestT8_ClassifyCoversEverySentinel: иначе «полнота» была бы утверждением о
// намерении, а не о коде.
func TestLocalWrite_EveryLocalfeedSentinelIsClassified(t *testing.T) {
	// Ошибки localfeed, которые аргументом НЕ являются. Пусто: все восемь
	// сегодняшних — ошибки вызывающего. Список существует, чтобы появление
	// такой ошибки было решением, а не умолчанием.
	notArgument := map[string]bool{}

	arg := map[string]bool{
		"ErrEmptyChannel":        true,
		"ErrEmptyRecordID":       true,
		"ErrAnchorRequired":      true,
		"ErrEmptyBlock":          true,
		"ErrUnknownDirection":    true,
		"ErrAnchorNotAllowed":    true,
		"ErrRecordCrossesAnchor": true,
		"ErrEmptyCursor":         true,
	}

	var names []string
	for _, f := range []string{"../localfeed/localfeed.go", "../localfeed/segments.go", "../localfeed/catalog.go"} {
		names = append(names, sentinelNames(t, f)...)
	}
	if len(names) == 0 {
		t.Fatal("в localfeed не найдено ни одного sentinel — разбор исходников сломан")
	}

	for _, n := range names {
		if !arg[n] && !notArgument[n] {
			t.Fatalf("sentinel localfeed.%s не классифицирован.\n"+
				"Добавьте его в feedWriteArgErrors (local.go) и в arg (этот файл), "+
				"либо в notArgument с объяснением — иначе он молча станет ClassInternal.", n)
		}
	}

	// Обратная сторона: в arg не должно быть имён, которых в исходниках уже
	// нет, — иначе список начнёт описывать несуществующие ошибки.
	inSource := map[string]bool{}
	for _, n := range names {
		inSource[n] = true
	}
	for n := range arg {
		if !inSource[n] {
			t.Fatalf("arg содержит %s, которого нет в localfeed — удалите строку", n)
		}
	}

	// И то же для самого списка в production-коде: число совпадает с числом
	// имён, объявленных аргументными.
	if len(feedWriteArgErrors) != len(arg) {
		t.Fatalf("feedWriteArgErrors содержит %d ошибок, а аргументными объявлено %d",
			len(feedWriteArgErrors), len(arg))
	}
}

// Ошибка хранилища НЕ становится ошибкой аргумента: вызывающему в payload
// исправлять нечего, и класс обязан вести его не к правке, а к диагностике.
func TestLocalWrite_StorageFailureIsInternal(t *testing.T) {
	err := wrapFeedWriteError(errors.New("bbolt: database not open"))
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("%v, ожидалось ErrStorageUnavailable", err)
	}
	if got := Classify(err); got != ClassInternal {
		t.Fatalf("класс %v, ожидался ClassInternal", got)
	}
	if strings.Contains(err.Error(), "invalid") {
		t.Fatalf("сбой хранилища описан как ошибка аргумента: %v", err)
	}
}
