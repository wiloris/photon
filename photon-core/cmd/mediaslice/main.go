// mediaslice — РУЧНОЙ сквозной прогон медийного среза против живого Bridge.
//
// Не автотест и им не станет: требует развёрнутого сервера и действующего
// Session ID. Тест, требующий внешнего сервера, либо делает прогон
// недетерминированным, либо тихо пропускается — и тогда он хуже
// отсутствующего (CLAUDE.md, «Метод работы»). Package main именно поэтому:
// `go test ./...` его не подхватывает, `go build ./...` — собирает, так что
// расхождение с API пакетов ломает сборку, а не остаётся незамеченным.
//
// Локальные тесты рядом (runner_test.go) проверяют ДРУГОЕ — что сам сценарий
// ловит нарушения: против внутрипроцессного поддельного Bridge, без сети. Без
// них прогон, зеленеющий и на исправном сервере, и на сломанном, ничем не
// отличался бы от распечатки.
//
// Что доказывает прогон (полный список — в отчёте, по идентификаторам):
//
//	P1-P4  подготовка: субъект, канал самопереписки, файл, независимый sha256
//	U1-U2  700 upload реального двоичного файла, media_id получен
//	I1-I4  702 info: размер и sha256 совпадают с независимо посчитанными
//	A1-A6  createPost с photon_media; props АВТОРИТЕТНЫ; getFeed; повтор отвергнут
//	D1-D4  701 download в файл; независимая сверка размера и sha256
//	N1-N7  отказы: чужой, несуществующий, неприкреплённый, недоступный
//	S1     дедлайн зависшего стрима
//	C1     кредит MAX_STREAMS не деградирует после ста стримов
//	E1-E3  байты мимо JSON и отсутствие буферизации
//
// Запуск:
//
//	go run ./cmd/mediaslice -session <Session ID A>
//	go run ./cmd/mediaslice -session <A> -session-b <B>     # + проверка чужого объекта
//	go run ./cmd/mediaslice -session <A> -only upload,download
//
// Session ID — это Session.Id, а НЕ токен из /users/login: имя поля
// session_token на проводе историческое (bridge-session.md §2.1).
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	quicadapter "github.com/wiloris/photon-core/adapters/quic"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/session"
)

var (
	addr     = flag.String("addr", "138.124.91.123.sslip.io:4443", "адрес Bridge")
	sessionA = flag.String("session", "", "Session ID субъекта A (обязателен)")
	sessionB = flag.String("session-b", "",
		"Session ID постороннего субъекта B; без него проверка чужого media_id НЕ ВЫПОЛНЯЕТСЯ")
	insecure = flag.Bool("insecure", false, "не проверять сертификат сервера")

	size = flag.Int64("size", 12<<20,
		"размер тестового файла. По умолчанию больше окна flow control одного стрима — "+
			"передача заведомо не укладывается в одно окно")
	streams = flag.Int("streams", 130,
		"сколько последовательных стримов гонять в проверке кредита; должно превышать 100")
	stallWait = flag.Duration("stall-wait", 90*time.Second,
		"внешняя граница ожидания в проверке дедлайна; должна заметно превышать дедлайн сервера")
	stallDeadline = flag.Duration("stall-deadline", 30*time.Second,
		"дедлайн брошенного стрима на СЕРВЕРЕ (mediaStreamDeadline). Порог проверки S1: "+
			"ответ раньше половины этого времени означает отказ, а не освобождение стрима")
	callTimeout = flag.Duration("call-timeout", 120*time.Second, "предел одного вызова")

	workDir = flag.String("workdir", "", "каталог для файлов прогона; пусто — временный")
	keep    = flag.Bool("keep", false, "не удалять созданные посты")
	only    = flag.String("only", "",
		"выполнить только перечисленные секции: prep,upload,info,attach,download,negative,deadline,streams,evidence")
)

func main() {
	flag.Parse()
	if *sessionA == "" {
		fmt.Fprintln(os.Stderr, "нужен -session <Session ID>")
		os.Exit(2)
	}
	// Граница ожидания обязана превышать серверный дедлайн: иначе S1 измерял
	// бы наше терпение и не мог бы отличить освобождение стрима от его
	// отсутствия ни в одну сторону.
	if *stallWait <= *stallDeadline {
		fmt.Fprintf(os.Stderr, "-stall-wait (%s) обязан превышать -stall-deadline (%s)\n",
			*stallWait, *stallDeadline)
		os.Exit(2)
	}

	wd := *workDir
	if wd == "" {
		var err error
		wd, err = os.MkdirTemp("", "mediaslice-")
		if err != nil {
			fmt.Fprintf(os.Stderr, "временный каталог: %v\n", err)
			os.Exit(1)
		}
		defer os.RemoveAll(wd)
	} else if err := os.MkdirAll(wd, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "каталог %s: %v\n", wd, err)
		os.Exit(1)
	}

	tr := quicadapter.New(quicadapter.Config{
		TLSConfig: &tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: *insecure, //nolint:gosec
		},
		HandshakeTimeout: 10 * time.Second,
	})
	defer tr.Close()

	ctx := context.Background()

	a := connect(ctx, tr, *sessionA, "A")
	defer a.Close()

	r := &Runner{
		a: a,
		opts: Options{
			Size:          *size,
			Streams:       *streams,
			StallWait:     *stallWait,
			StallDeadline: *stallDeadline,
			CallTimeout:   *callTimeout,
			WorkDir:       wd,
			Keep:          *keep,
			Only:          parseOnly(*only),
		},
		rep: &report{},
	}
	if *sessionB != "" {
		b := connect(ctx, tr, *sessionB, "B")
		defer b.Close()
		r.b = b
	}

	fmt.Printf("mediaslice → %s\n", *addr)
	fmt.Printf("  файл %d байт, стримов %d, каталог %s\n", *size, *streams, wd)

	err := r.Run(ctx)
	r.rep.print(os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n", err)
		os.Exit(1)
	}
}

// connect поднимает action-сессию. Режим подписки здесь не нужен: медийный
// срез push не порождает, а смешивать их на одном соединении протокол
// запрещает (bridge-session.md §2.4).
func connect(ctx context.Context, tr ports.Transport, token, who string) *session.Session {
	s, err := session.New(session.Config{
		Transport:    tr,
		Endpoint:     ports.Endpoint(*addr),
		SessionToken: token,
		Mode:         session.ModeAction,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "session.New (%s): %v\n", who, err)
		os.Exit(1)
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := s.Connect(cctx); err != nil {
		fmt.Fprintf(os.Stderr, "Connect (%s): %v\n", who, err)
		os.Exit(1)
	}
	return s
}

func parseOnly(s string) map[string]bool {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	out := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out[p] = true
		}
	}
	return out
}
