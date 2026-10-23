package dispatcher

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/wiloris/photon-core/services/diag"
	"github.com/wiloris/photon-core/services/localfeed"
	"github.com/wiloris/photon-core/services/outbox"
	"github.com/wiloris/photon-core/services/session"
)

// T-8: полнота таблицы классификации.
//
// Тест структурный, а не поведенческий, и в этом суть: он читает ИСХОДНИКИ
// session/errors.go и dispatcher/errors.go, собирает объявленные там
// sentinel'ы и требует, чтобы каждый был явно классифицирован. Новый sentinel,
// добавленный без строки в classTable, роняет этот тест — вместо того чтобы
// молча уехать в PHOTON_ERR_INTERNAL и всплыть у клиента невнятным «internal
// error».
//
// Проверка «сломай правило и убедись, что тест падает» проведена: удаление
// любой строки из classTable роняет его, добавление нового sentinel'а в
// session/errors.go — тоже (см. IMPLEMENTATION_LOG.md, «Проверка мутациями»).
//
// known — единственное место, где имя из исходника связывается со значением.
// Дублирование здесь намеренное: именно оно заставляет человека, добавившего
// sentinel, прийти сюда и решить, каким классом тот становится на границе ABI.
var known = map[string]error{
	"ErrSessionClosed":       session.ErrSessionClosed,
	"ErrNotReady":            session.ErrNotReady,
	"ErrConnectInProgress":   session.ErrConnectInProgress,
	"ErrHandshakeRejected":   session.ErrHandshakeRejected,
	"ErrProtocolVersion":     session.ErrProtocolVersion,
	"ErrModeNotAccepted":     session.ErrModeNotAccepted,
	"ErrUnknownAction":       session.ErrUnknownAction,
	"ErrNoResourceForAction": session.ErrNoResourceForAction,
	"ErrLocalAction":         session.ErrLocalAction,
	"ErrInvalidPayload":      session.ErrInvalidPayload,
	"ErrMalformedFrame":      session.ErrMalformedFrame,
	"ErrConnectionLost":      session.ErrConnectionLost,

	"ErrNoEndpoint":         ErrNoEndpoint,
	"ErrClosed":             ErrClosed,
	"ErrNoStorage":          ErrNoStorage,
	"ErrStorageUnavailable": ErrStorageUnavailable,

	// Потоковая передача (DoStream).
	"ErrStreamDirection":    session.ErrStreamDirection,
	"ErrBodyLengthRequired": session.ErrBodyLengthRequired,
	"ErrBodyTruncated":      session.ErrBodyTruncated,
	"ErrBodyLengthInvalid":  session.ErrBodyLengthInvalid,

	// Инварианты локальной ленты (ADR-065). Четыре нижних добавлены самим
	// ADR-065; три верхних существовали с первого среза Phase 12 и были
	// неклассифицированы всё это время — их не «прихватили по дороге», а
	// внесли потому, что оставить три из семи значило бы объявить полноту,
	// которой нет.
	"ErrEmptyChannel":        localfeed.ErrEmptyChannel,
	"ErrAnchorRequired":      localfeed.ErrAnchorRequired,
	"ErrEmptyBlock":          localfeed.ErrEmptyBlock,
	"ErrEmptyRecordID":       localfeed.ErrEmptyRecordID,
	"ErrUnknownDirection":    localfeed.ErrUnknownDirection,
	"ErrAnchorNotAllowed":    localfeed.ErrAnchorNotAllowed,
	"ErrRecordCrossesAnchor": localfeed.ErrRecordCrossesAnchor,
	"ErrEmptyCursor":         localfeed.ErrEmptyCursor,
	"ErrInvalidChannelID":    localfeed.ErrInvalidChannelID,

	// Исходящие (ADR-093, docs/local-actions.md).
	"ErrInvalidQueue": outbox.ErrInvalidQueue,
	"ErrInvalidBody":  outbox.ErrInvalidBody,
	"ErrInvalidLease": outbox.ErrInvalidLease,
	"ErrUnknownOp":    outbox.ErrUnknownOp,

	// Диагностика (ADR-130 §2.7).
	"ErrInvalidCursor":   diag.ErrInvalidCursor,
	"ErrInvalidLimit":    diag.ErrInvalidLimit,
	"ErrInvalidCapacity": diag.ErrInvalidCapacity,
	"ErrStateConflict":   outbox.ErrStateConflict,
}

// resultNotError — sentinel'ы, которые наружу выходят РЕЗУЛЬТАТОМ действия,
// а не ошибкой вызова (807: {"leased":false,"reason":…}). В classTable их
// нет намеренно; что они не утекают ошибкой — TestOutbox_LeaseOutcomesAreResults.
var resultNotError = map[string]error{
	"ErrQueueEmpty": outbox.ErrQueueEmpty,
	"ErrHeadLeased": outbox.ErrHeadLeased,
}

func TestT8_ClassifyCoversEverySentinel(t *testing.T) {
	names := sentinelNames(t, "../session/errors.go")
	names = append(names, sentinelNames(t, "errors.go")...)

	// localfeed добавлен с ADR-065: его инварианты доходят до границы ABI так
	// же, как ошибки сессии, и sentinel без строки в classTable уехал бы в
	// PHOTON_ERR_INTERNAL ровно с тем же результатом — невнятное «internal
	// error» у клиента, которому на самом деле надо чинить payload.
	names = append(names, sentinelNames(t, "../localfeed/localfeed.go")...)
	names = append(names, sentinelNames(t, "../localfeed/segments.go")...)
	names = append(names, sentinelNames(t, "../localfeed/catalog.go")...)
	names = append(names, sentinelNames(t, "../outbox/outbox.go")...)
	// diag — с ADR-130 §2.7: курсор и лимит 820 доходят до границы ABI.
	names = append(names, sentinelNames(t, "../diag/diag.go")...)

	for _, name := range names {
		if _, isResult := resultNotError[name]; isResult {
			continue
		}
		err, ok := known[name]
		if !ok {
			t.Fatalf("sentinel %s объявлен в исходниках, но не известен этому тесту.\n"+
				"Добавьте строку в classTable (dispatcher/errors.go) и в known (этот файл): "+
				"без неё ошибка уедет в ClassInternal молча.", name)
		}
		if _, classified := classifyKnown(err); !classified {
			t.Fatalf("sentinel %s не классифицирован: нет строки в classTable", name)
		}
	}

	// Обратная сторона: в known не должно быть имён, которых в исходниках уже
	// нет, — иначе таблица начнёт описывать несуществующие ошибки.
	inSource := map[string]bool{}
	for _, n := range names {
		inSource[n] = true
	}
	for name := range resultNotError {
		if !inSource[name] {
			t.Fatalf("resultNotError содержит %s, которого нет в исходниках", name)
		}
	}
	for name := range known {
		if !inSource[name] {
			t.Fatalf("known содержит %s, которого нет в исходниках — удалите строку", name)
		}
	}
}

// sentinelNames возвращает имена пакетных переменных вида Err*, объявленных в
// файле.
func sentinelNames(t *testing.T, path string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("разбор %s: %v", path, err)
	}

	var out []string
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, n := range vs.Names {
				if len(n.Name) > 3 && n.Name[:3] == "Err" && ast.IsExported(n.Name) {
					out = append(out, n.Name)
				}
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("в %s не найдено ни одного Err* — тест перестал проверять что-либо", path)
	}
	return out
}

// Классы, назначенные каждому sentinel'у. Отдельно от полноты: полнота
// требует, чтобы строка БЫЛА, этот тест — чтобы она была ПРАВИЛЬНОЙ.
func TestClassifyMapping(t *testing.T) {
	cases := []struct {
		err  error
		want Class
	}{
		{session.ErrUnknownAction, ClassInvalidAction},
		{session.ErrNoResourceForAction, ClassInvalidAction},
		{session.ErrLocalAction, ClassInvalidAction},
		{session.ErrInvalidPayload, ClassInvalidArgument},
		{ErrNoEndpoint, ClassNotConnected},
		{session.ErrNotReady, ClassNotConnected},
		{session.ErrConnectionLost, ClassNotConnected},
		{ErrClosed, ClassInvalidArgument},
		{session.ErrSessionClosed, ClassInvalidArgument},
		{session.ErrHandshakeRejected, ClassPermissionDenied},
		{session.ErrProtocolVersion, ClassInternal},
		{session.ErrModeNotAccepted, ClassInternal},
		{session.ErrMalformedFrame, ClassInternal},
		{session.ErrConnectInProgress, ClassInternal},
	}
	for _, tc := range cases {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("Classify(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}

	if got := Classify(nil); got != ClassOK {
		t.Errorf("Classify(nil) = %v, want ClassOK", got)
	}
	if got := Classify(errUnknownForTest{}); got != ClassInternal {
		t.Errorf("неизвестная ошибка → %v, want ClassInternal", got)
	}
	// Ноль обязан быть безопасным значением: незаполненный Class — не успех.
	if Class(0) != ClassInternal {
		t.Error("нулевое значение Class должно быть ClassInternal")
	}
}

type errUnknownForTest struct{}

func (errUnknownForTest) Error() string { return "нечто своё" }

// ── Инварианты локальной ленты на границе ABI (ADR-065 п. 8) ────────────

// Класс каждого sentinel'а localfeed.
//
// Отдельным тестом, а не строками в TestClassifyMapping: у этих ошибок общий
// довод — все они означают «вызывающий прислал противоречивый блок», — и
// проверять его лучше там, где он записан один раз.
func TestClassifyLocalfeedSentinels(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"пустой канал", localfeed.ErrEmptyChannel, ClassInvalidArgument},
		{"пустой id записи", localfeed.ErrEmptyRecordID, ClassInvalidArgument},
		{"нет якоря для after/before", localfeed.ErrAnchorRequired, ClassInvalidArgument},
		{"пустой блок без якоря", localfeed.ErrEmptyBlock, ClassInvalidArgument},
		{"неизвестное направление", localfeed.ErrUnknownDirection, ClassInvalidArgument},
		{"якорь при newest", localfeed.ErrAnchorNotAllowed, ClassInvalidArgument},
		{"запись не по ту сторону якоря", localfeed.ErrRecordCrossesAnchor, ClassInvalidArgument},
		{"пустой курсор", localfeed.ErrEmptyCursor, ClassInvalidArgument},
		{"channel_id с 0x00", localfeed.ErrInvalidChannelID, ClassInvalidArgument},
		{"неверная очередь", outbox.ErrInvalidQueue, ClassInvalidArgument},
		{"тело не JSON", outbox.ErrInvalidBody, ClassInvalidArgument},
		{"аренда без срока", outbox.ErrInvalidLease, ClassInvalidArgument},
		{"неизвестный op_id", outbox.ErrUnknownOp, ClassInvalidArgument},
		{"запрещённый переход", outbox.ErrStateConflict, ClassInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Fatalf("Classify(%v) = %v, want %v", tc.err, got, tc.want)
			}
			// Полнота отдельно от правильности: строка обязана быть НАЙДЕНА,
			// а не совпасть с ClassInternal по умолчанию. Без этой проверки
			// удаление строки прошло бы незамеченным для всех семи, потому
			// что ClassInternal и «не нашлось» неразличимы по одному классу.
			if _, found := classifyKnown(tc.err); !found {
				t.Fatalf("%v не найден в classTable — класс совпал случайно", tc.err)
			}
		})
	}
}

// Ошибка, дошедшая через localfeed.Ingest, обёрнута им самим
// (fmt.Errorf("localfeed: ingest: %w", err)). Классификация обязана пережить
// обёртку: она идёт по errors.Is, а не по тексту (ADR-033).
func TestClassifyLocalfeedSentinelThroughIngestWrap(t *testing.T) {
	err := fmt.Errorf("localfeed: ingest: %w", localfeed.ErrRecordCrossesAnchor)
	if got := Classify(err); got != ClassInvalidArgument {
		t.Fatalf("класс %v, ожидался ClassInvalidArgument", got)
	}
}

// Оборачивание среза 3 сохраняет ОБА: класс и исходный sentinel.
//
// Проверяется на настоящем wrapFeedWriteError, а не на собранной здесь копии
// его формата: копия разошлась бы с оригиналом молча, и тест продолжил бы
// зеленеть, проверяя сам себя.
func TestClassifyWrappedFeedErrorKeepsSentinel(t *testing.T) {
	for _, sentinel := range []error{
		localfeed.ErrEmptyChannel,
		localfeed.ErrEmptyRecordID,
		localfeed.ErrAnchorRequired,
		localfeed.ErrEmptyBlock,
		localfeed.ErrUnknownDirection,
		localfeed.ErrAnchorNotAllowed,
		localfeed.ErrRecordCrossesAnchor,
		localfeed.ErrEmptyCursor,
		localfeed.ErrInvalidChannelID,
	} {
		wrapped := wrapFeedWriteError(sentinel)
		if !errors.Is(wrapped, sentinel) {
			t.Fatalf("после оборачивания errors.Is не находит %v: %v", sentinel, wrapped)
		}
		if got := Classify(wrapped); got != ClassInvalidArgument {
			t.Fatalf("класс обёрнутой %v = %v, ожидался ClassInvalidArgument", sentinel, got)
		}
	}
}

// Порядок строк значим, и вот случай, ради которого он выбран.
//
// Если новый sentinel localfeed забудут внести в feedWriteArgErrors, ошибка
// придёт обёрнутой как сбой хранилища, сохранив исходную причину внутри.
// Такая ошибка Is-совместима СРАЗУ С ДВУМЯ строками таблицы, и побеждать
// обязана конкретная причина: вызывающий должен узнать, что чинить payload, а
// не диск.
func TestClassifyLocalfeedSentinelWinsOverStorageWrapper(t *testing.T) {
	both := fmt.Errorf("%w: %w", ErrStorageUnavailable, localfeed.ErrUnknownDirection)

	if !errors.Is(both, ErrStorageUnavailable) || !errors.Is(both, localfeed.ErrUnknownDirection) {
		t.Fatal("стенд построен неверно: ошибка обязана совпадать с обеими строками")
	}
	if got := Classify(both); got != ClassInvalidArgument {
		t.Fatalf("класс %v, ожидался ClassInvalidArgument — общая обёртка победила конкретную причину", got)
	}
}

// Поведение для непредусмотренной ошибки не изменилось: новые строки его не
// затронули. Проверяется и класс, и признак «не найдено» — без второго
// ClassInternal по умолчанию неотличим от ClassInternal по таблице.
func TestClassifyUnknownErrorUnchangedAfterLocalfeedRows(t *testing.T) {
	unknown := errors.New("localfeed: нечто, чего в таблице нет")

	if got, found := classifyKnown(unknown); found || got != ClassInternal {
		t.Fatalf("classifyKnown = (%v, %v), ожидалось (ClassInternal, false)", got, found)
	}
	if got := Classify(unknown); got != ClassInternal {
		t.Fatalf("Classify = %v, ожидался ClassInternal", got)
	}
	// Сбой хранилища без узнаваемой причины остаётся внутренней ошибкой.
	if got := Classify(wrapFeedWriteError(unknown)); got != ClassInternal {
		t.Fatalf("обёрнутая неизвестная ошибка → %v, ожидался ClassInternal", got)
	}
}
