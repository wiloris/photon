package dispatcher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

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
	"ErrInvalidPayload":      session.ErrInvalidPayload,
	"ErrMalformedFrame":      session.ErrMalformedFrame,
	"ErrConnectionLost":      session.ErrConnectionLost,

	"ErrNoEndpoint": ErrNoEndpoint,
	"ErrClosed":     ErrClosed,

	// Потоковая передача (DoStream).
	"ErrStreamDirection":    session.ErrStreamDirection,
	"ErrBodyLengthRequired": session.ErrBodyLengthRequired,
	"ErrBodyTruncated":      session.ErrBodyTruncated,
	"ErrBodyLengthInvalid":  session.ErrBodyLengthInvalid,
}

func TestT8_ClassifyCoversEverySentinel(t *testing.T) {
	names := sentinelNames(t, "../session/errors.go")
	names = append(names, sentinelNames(t, "errors.go")...)

	for _, name := range names {
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
