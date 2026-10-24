package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	photonpb "github.com/wiloris/photon/photon-core/protobuf"
	"github.com/wiloris/photon/photon-core/services/media"
	"github.com/wiloris/photon/photon-core/services/session"
)

// Actor — то, что прогону нужно от субъекта, и ничего сверх.
//
// Интерфейс, а не *session.Session, по той же причине, по которой узок
// media.Streamer: сценарий проверяется без сети, против внутрипроцессного
// поддельного Bridge, и это единственный способ доказать, что проверки
// ЛОВЯТ нарушение. Прогон, который зеленеет и на исправном сервере, и на
// сломанном, ничего не измеряет.
type Actor interface {
	Do(ctx context.Context, action int32, payload []byte) (session.Response, error)
	DoStream(ctx context.Context, action int32, payload []byte,
		body io.Reader, sink io.Writer) (session.Response, error)
}

// Options — параметры прогона.
type Options struct {
	Size      int64         // размер основного тестового файла
	Streams   int           // сколько последовательных стримов в проверке кредита
	StallWait time.Duration // внешняя граница ожидания в проверке дедлайна
	// StallDeadline — дедлайн БРОШЕННОГО стрима на стороне сервера. Не
	// украшение отчёта, а порог проверки S1: без него «освобождён за 0 с»
	// считалось бы успехом, хотя мгновенный ответ означает ровно обратное —
	// стрим не был принят, и зависание не проверялось.
	StallDeadline time.Duration
	CallTimeout   time.Duration // предел одного обычного вызова
	WorkDir       string
	Keep          bool            // не удалять созданные посты
	Only          map[string]bool // пустая карта — выполнять всё
	// OrphanMedia — media_id объекта, осиротевшего ДО появления проверки
	// живости (N9). Флагом, а не константой: идентификатор принадлежит
	// конкретному стенду, и зашивать его в прогон значило бы сделать
	// проверку непереносимой.
	OrphanMedia string
}

// Runner — сценарий сквозного среза.
type Runner struct {
	a Actor // субъект A: он загружает, прикрепляет и скачивает
	b Actor // субъект B: посторонний. nil — часть проверок не выполняется

	opts Options
	rep  *report

	userID    string
	channelID string

	srcPath string
	srcSize int64
	srcHash string // посчитан ЗДЕСЬ, не клиентом media

	mediaID     string // то, что вернула загрузка
	feedMediaID string // то, что взято из ленты; скачивание идёт по нему
	postID      string

	created  []string // созданные посты — удалить за собой
	heapPeak uint64   // максимальный прирост пика по всем передачам
}

func (r *Runner) want(section string) bool {
	if len(r.opts.Only) == 0 {
		return true
	}
	return r.opts.Only[section]
}

// Run выполняет сценарий. Возвращает ошибку, если хоть одна проверка нарушена.
func (r *Runner) Run(ctx context.Context) error {
	defer r.cleanup(ctx)

	if !r.prep(ctx) {
		return r.rep.result()
	}
	if r.want("upload") {
		r.upload(ctx)
	}
	if r.want("info") {
		r.info(ctx)
	}
	if r.want("attach") {
		r.attach(ctx)
	}
	if r.want("download") {
		r.download(ctx)
	}
	if r.want("negative") {
		r.negative(ctx)
	}
	if r.want("deadline") {
		r.deadline(ctx)
	}
	if r.want("streams") {
		r.credit(ctx)
	}
	if r.want("evidence") {
		r.evidence()
	}
	return r.rep.result()
}

// ── 1. Подготовка ───────────────────────────────────────────────────

func (r *Runner) prep(ctx context.Context) bool {
	fmt.Println("\n── подготовка ─────────────────────────────────")

	var me struct {
		ID string `json:"id"`
	}
	if err := r.actionJSON(ctx, r.a, photonpb.Action_ACTION_GET_ME, nil, &me); err != nil {
		r.rep.fail("P1", "getMe: %v", err)
		return false
	}
	if me.ID == "" {
		r.rep.fail("P1", "getMe вернул пустой id")
		return false
	}
	r.userID = me.ID
	r.rep.pass("P1", "субъект A определён: %s", me.ID)

	// Самопереписка: прогон не должен касаться посторонних. Тот же приём, что
	// в livecheck/trigger.go.
	var ch struct {
		ID string `json:"id"`
	}
	payload := []byte(fmt.Sprintf(`{"target_user_id":%q}`, me.ID))
	if err := r.actionJSON(ctx, r.a, photonpb.Action_ACTION_GET_OR_CREATE_DIRECT_CHANNEL, payload, &ch); err != nil {
		r.rep.fail("P2", "getOrCreateDirectChannel: %v", err)
		return false
	}
	if ch.ID == "" {
		r.rep.fail("P2", "канал самопереписки не получен")
		return false
	}
	r.channelID = ch.ID
	r.rep.pass("P2", "канал самопереписки: %s", ch.ID)

	r.srcPath = filepath.Join(r.opts.WorkDir, "source.bin")
	if err := generateSource(r.srcPath, r.opts.Size); err != nil {
		r.rep.fail("P3", "генерация файла: %v", err)
		return false
	}
	if err := verifySourceMarkers(r.srcPath); err != nil {
		r.rep.fail("P3", "врезки в файле: %v", err)
		return false
	}
	r.rep.pass("P3", "файл %d байт создан; врезки на месте (все 256 значений байта, невалидный UTF-8, NUL, псевдоконверт)", r.opts.Size)

	sum, n, err := hashFile(r.srcPath)
	if err != nil {
		r.rep.fail("P4", "независимый sha256 исходника: %v", err)
		return false
	}
	if n != r.opts.Size {
		r.rep.fail("P4", "на диске %d байт при заказанных %d", n, r.opts.Size)
		return false
	}
	r.srcHash, r.srcSize = sum, n
	r.rep.pass("P4", "независимый sha256 исходника: %s (%d байт)", sum, n)
	return true
}

// ── 2. Загрузка (700) ───────────────────────────────────────────────

func (r *Runner) upload(ctx context.Context) {
	fmt.Println("\n── 700 upload ─────────────────────────────────")
	if r.srcHash == "" {
		r.rep.skip("U1", "загрузка", "подготовка не выполнена")
		return
	}

	f, err := os.Open(r.srcPath)
	if err != nil {
		r.rep.fail("U1", "открыть исходник: %v", err)
		return
	}
	defer f.Close()

	cl := media.New(r.a)
	cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
	defer cancel()

	sampler := startHeapSampler(50 * time.Millisecond)
	start := time.Now()
	ref, err := cl.Upload(cctx, media.UploadRequest{
		ChannelID: r.channelID,
		Name:      "mediaslice.bin",
		Mime:      "application/octet-stream",
		SHA256:    r.srcHash,
		Size:      r.srcSize,
	}, f)
	took := time.Since(start)
	r.noteHeap(sampler.finish())

	if err != nil {
		r.rep.fail("U1", "700 upload: %v", err)
		return
	}
	r.mediaID = ref.MediaID
	r.rep.pass("U1", "700 upload за %s: media_id=%s", took.Round(time.Millisecond), ref.MediaID)
	r.rep.measure("загрузка %d байт за %s (%.1f МиБ/с)", r.srcSize, took.Round(time.Millisecond),
		float64(r.srcSize)/(1<<20)/took.Seconds())

	// Отдельной проверки «сервер подтвердил size и sha256» здесь НЕТ, и это
	// осознанно. Клиент media сверяет подтверждение с объявленным сам и
	// отказал бы при расхождении — то есть такая проверка не могла бы
	// покраснеть ни при каком поведении сервера. Зелёная всегда, она была бы
	// хуже отсутствующей. Подтверждение измеряется там, где у него есть
	// различающий случай: I3/I4 (702) и D2/D3 (независимая сверка файлов).
	r.rep.measure("сервер подтвердил size=%d sha256=%s (сверено клиентом media при приёме ответа)",
		ref.Size, ref.SHA256)
}

// ── 3. Метаданные (702) ─────────────────────────────────────────────

func (r *Runner) info(ctx context.Context) {
	fmt.Println("\n── 702 info ───────────────────────────────────")
	if r.mediaID == "" {
		r.rep.skip("I1", "702 info", "media_id не получен")
		return
	}
	got, err := r.mediaInfo(ctx, r.a, r.mediaID)
	if err != nil {
		r.rep.fail("I1", "702 info: %v", err)
		return
	}
	r.rep.pass("I1", "702 info выполнен")

	if got.MediaID != r.mediaID {
		r.rep.fail("I2", "702 ответил про %s, спрашивали про %s", got.MediaID, r.mediaID)
	} else {
		r.rep.pass("I2", "media_id совпал")
	}
	if got.Size != r.srcSize {
		r.rep.fail("I3", "702 сообщил размер %d при независимо посчитанном %d", got.Size, r.srcSize)
	} else {
		r.rep.pass("I3", "702 сообщил размер %d — совпал с независимо посчитанным", got.Size)
	}
	if got.SHA256 != r.srcHash {
		r.rep.fail("I4", "702 сообщил sha256 %s при независимо посчитанном %s", got.SHA256, r.srcHash)
	} else {
		r.rep.pass("I4", "702 сообщил sha256 %s — совпал с независимо посчитанным", got.SHA256)
	}
}

// ── 4. Прикрепление и лента ─────────────────────────────────────────

// lieSHA — заведомо неверный sha256, который прогон СПЕЦИАЛЬНО присылает в
// createPost вместе с media_id.
//
// Смысл в том, что props обязаны собраться из хранилища, а не из запроса
// (ADR-048 §3). Прислать в запросе правильные величины значило бы поставить
// проверку, которая пройдёт и при полном доверии клиенту: у неё не было бы
// различающего случая.
const lieSHA = "00000000000000000000000000000000000000000000000000000000000000ff"

func (r *Runner) attach(ctx context.Context) {
	fmt.Println("\n── 100 createPost + 101 getFeed ───────────────")
	if r.mediaID == "" {
		r.rep.skip("A1", "прикрепление", "media_id не получен")
		return
	}

	payload := []byte(fmt.Sprintf(
		`{"channel_id":%q,"message":"mediaslice %s","photon_media":[{"media_id":%q,"sha256":%q,"size":1,"name":"lie.txt","mime":"text/plain"}]}`,
		r.channelID, time.Now().Format(time.RFC3339), r.mediaID, lieSHA))

	var post postEnvelope
	if err := r.actionJSON(ctx, r.a, photonpb.Action_ACTION_CREATE_POST, payload, &post); err != nil {
		r.rep.fail("A1", "createPost с photon_media: %v", err)
		return
	}
	if post.ID == "" {
		r.rep.fail("A1", "createPost вернул пост без id")
		return
	}
	r.postID = post.ID
	r.created = append(r.created, post.ID)
	r.rep.pass("A1", "пост %s создан с photon_media", post.ID)

	// Лента — авторитетный источник. media_id для скачивания берётся отсюда.
	var feed json.RawMessage
	feedPayload := []byte(fmt.Sprintf(`{"channel_id":%q,"limit":20}`, r.channelID))
	if err := r.actionJSON(ctx, r.a, photonpb.Action_ACTION_GET_FEED, feedPayload, &feed); err != nil {
		r.rep.fail("A2", "getFeed: %v", err)
		return
	}
	raw, err := findPostInFeed(feed, post.ID)
	if err != nil {
		r.rep.fail("A2", "пост в ленте: %v", err)
		return
	}
	r.rep.pass("A2", "пост %s найден в ленте", post.ID)

	refs, err := mediaFromPost(raw)
	if err != nil {
		r.rep.fail("A3", "photon_media из ленты: %v", err)
		return
	}
	if len(refs) != 1 {
		r.rep.fail("A3", "в photon_media %d записей, ожидалась одна", len(refs))
		return
	}
	got := refs[0]
	r.feedMediaID = got.MediaID
	r.rep.pass("A3", "photon_media извлечён из ленты: media_id=%s", got.MediaID)

	// Главное свойство этой секции: в props лежат АВТОРИТЕТНЫЕ величины, а не
	// присланная клиентом ложь.
	switch {
	case got.MediaID != r.mediaID:
		r.rep.fail("A4", "в props чужой media_id %s", got.MediaID)
	case got.SHA256 == lieSHA:
		// Ветка ДИАГНОСТИЧЕСКАЯ: следующая поймала бы тот же случай, но
		// сказала бы «не совпало», а не «сохранено присланное клиентом».
		// Её удаление проверку не ослабляет (мутация эквивалентная) —
		// оставлена ради точности сообщения, а не ради обнаружения.
		r.rep.fail("A4", "в props сохранён sha256 ИЗ ЗАПРОСА КЛИЕНТА — props не авторитетны")
	case got.SHA256 != r.srcHash:
		r.rep.fail("A4", "в props sha256 %s при независимо посчитанном %s", got.SHA256, r.srcHash)
	case got.Size != r.srcSize:
		r.rep.fail("A4", "в props размер %d при независимо посчитанном %d (в запросе было 1)", got.Size, r.srcSize)
	default:
		r.rep.pass("A4", "props собраны из хранилища: sha256 и size авторитетны, ложь из запроса (sha=%s…, size=1, name=lie.txt) отброшена", lieSHA[:8])
	}
	if got.Name == "lie.txt" {
		r.rep.fail("A5", "в props сохранено имя ИЗ ЗАПРОСА КЛИЕНТА")
	} else {
		r.rep.pass("A5", "имя в props авторитетно: %q", got.Name)
	}

	// Повторное прикрепление того же объекта обязано быть отвергнуто, и
	// пост при этом создан быть не должен.
	second := []byte(fmt.Sprintf(
		`{"channel_id":%q,"message":"mediaslice second attach","photon_media":[{"media_id":%q}]}`,
		r.channelID, r.mediaID))
	resp, err := r.action(ctx, r.a, photonpb.Action_ACTION_CREATE_POST, second)
	switch {
	case err != nil:
		r.rep.fail("A6", "повторное прикрепление вернуло ошибку вызова вместо конверта: %v", err)
	case resp.OK:
		var p postEnvelope
		_ = json.Unmarshal(resp.Data, &p)
		if p.ID != "" {
			r.created = append(r.created, p.ID)
		}
		r.rep.fail("A6", "повторное прикрепление ПРИНЯТО: объект прикреплён ко второму посту %s (§11: ATTACHED — объект назван в photon_media ЖИВОГО поста, второй пост это нарушает)", p.ID)
	default:
		r.rep.pass("A6", "повторное прикрепление отвергнуто: %q", resp.Error)
	}
}

// ── 5. Скачивание (701) ─────────────────────────────────────────────

func (r *Runner) download(ctx context.Context) {
	fmt.Println("\n── 701 download ───────────────────────────────")
	id := r.feedMediaID
	source := "из ленты"
	if id == "" {
		id, source = r.mediaID, "из ответа загрузки (лента недоступна)"
	}
	if id == "" {
		r.rep.skip("D1", "701 download", "media_id не получен")
		return
	}

	dst := filepath.Join(r.opts.WorkDir, "downloaded.bin")
	f, err := os.Create(dst)
	if err != nil {
		r.rep.fail("D1", "создать файл приёмника: %v", err)
		return
	}

	cl := media.New(r.a)
	cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
	defer cancel()

	sampler := startHeapSampler(50 * time.Millisecond)
	start := time.Now()
	info, err := cl.Download(cctx, id, f)
	took := time.Since(start)
	r.noteHeap(sampler.finish())
	closeErr := f.Close()

	if err != nil {
		r.rep.fail("D1", "701 download (%s): %v", source, err)
		return
	}
	if closeErr != nil {
		r.rep.fail("D1", "закрыть приёмник: %v", closeErr)
		return
	}
	r.rep.pass("D1", "701 download по media_id %s за %s", source, took.Round(time.Millisecond))
	r.rep.measure("скачивание %d байт за %s (%.1f МиБ/с)", info.Size, took.Round(time.Millisecond),
		float64(info.Size)/(1<<20)/took.Seconds())

	// НЕЗАВИСИМАЯ сверка: файл на диске перечитывается и хешируется заново.
	gotHash, gotSize, err := hashFile(dst)
	if err != nil {
		r.rep.fail("D2", "независимый sha256 скачанного: %v", err)
		return
	}
	if gotSize != r.srcSize {
		r.rep.fail("D2", "скачано %d байт при исходных %d", gotSize, r.srcSize)
	} else {
		r.rep.pass("D2", "размер скачанного файла совпал с исходником: %d байт", gotSize)
	}
	if gotHash != r.srcHash {
		r.rep.fail("D3", "независимый sha256 скачанного %s ≠ %s исходника", gotHash, r.srcHash)
	} else {
		r.rep.pass("D3", "независимый sha256 скачанного файла совпал с исходником: %s", gotHash)
	}
	if info.SHA256 != r.srcHash || info.Size != r.srcSize {
		r.rep.fail("D4", "заголовок скачивания сообщил size=%d sha256=%s при независимых %d/%s",
			info.Size, info.SHA256, r.srcSize, r.srcHash)
	} else {
		r.rep.pass("D4", "заголовок скачивания сообщил те же size и sha256")
	}
}

// ── 6. Отказы ───────────────────────────────────────────────────────

func (r *Runner) negative(ctx context.Context) {
	fmt.Println("\n── отказы ─────────────────────────────────────")

	// Причина отказа, которую требует контракт для «не твоё» и «нет такого»:
	// объект сводится к каналу, а отказ в канале — forbidden. Одинаковая
	// причина для чужого и несуществующего объекта — не небрежность, а
	// решение: различие подсказывало бы существование объекта.
	const wantForbidden = "forbidden"

	// N1. Чужой media_id. Объект лежит в самопереписке A, читать её B не
	// может — значит и объект тоже (objMedia сводит право на объект к праву
	// на его канал).
	switch {
	case r.mediaID == "":
		r.rep.skip("N1", "чужой media_id отвергнут", "media_id не получен")
		r.rep.skip("N2", "чужой media_id не скачивается", "media_id не получен")
	case r.b == nil:
		r.rep.skip("N1", "чужой media_id отвергнут", "второй субъект не задан (-session-b)")
		r.rep.skip("N2", "чужой media_id не скачивается", "второй субъект не задан (-session-b)")
	default:
		// Свидетель для B: без него отказ B неотличим от неподтверждённой
		// сессии B, и обе проверки прошли бы на сервере, отвечающем
		// unauthorized на всё.
		if err := r.witness(ctx, r.b); err != nil {
			r.rep.skip("N1", "чужой media_id отвергнут",
				fmt.Sprintf("субъект B не отвечает на положительный вызов: %v", err))
			r.rep.skip("N2", "чужой media_id не скачивается",
				fmt.Sprintf("субъект B не отвечает на положительный вызов: %v", err))
			break
		}

		_, err := r.mediaInfo(ctx, r.b, r.mediaID)
		r.expectDenied("N1", "702 для субъекта B по чужому объекту", wantForbidden, err)

		cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
		_, err = media.New(r.b).Download(cctx, r.mediaID, io.Discard)
		cancel()
		r.expectDenied("N2", "701 для субъекта B по чужому объекту", wantForbidden, err)
	}

	// N3. Несуществующий объект. Идентификатор синтаксически допустимый —
	// иначе отказ доказывал бы только проверку формы.
	const ghost = "mediaslice-ghost-0123456789abcdef"
	if err := r.witness(ctx, r.a); err != nil {
		// Свидетель A не отозвался: ни одна проверка отказа ниже ничего не
		// измерит. Объявляются невыполненными ВСЕ, включая N5, — молча
		// пропустить их значило бы потерять их из отчёта.
		why := fmt.Sprintf("субъект A не отвечает на положительный вызов: %v", err)
		r.rep.skip("N3", "702 для несуществующего объекта отвергнут", why)
		r.rep.skip("N4", "701 для несуществующего объекта отвергнут", why)
		r.rep.skip("N5", "неприкреплённый объект доступен и прикрепляется один раз", why)
	} else {
		_, err := r.mediaInfo(ctx, r.a, ghost)
		r.expectDenied("N3", "702 для несуществующего объекта", wantForbidden, err)

		cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
		_, err = media.New(r.a).Download(cctx, ghost, io.Discard)
		cancel()
		r.expectDenied("N4", "701 для несуществующего объекта", wantForbidden, err)

		// N5. Неприкреплённый объект: загружен и НЕ прикреплён.
		//
		// Состояние STORED клиенту напрямую не видно — в ответах нет ни
		// state, ни post_id. Поэтому оно измеряется поведением:
		// неприкреплённый объект читается, а прикрепить его можно РОВНО
		// ОДИН раз. Успех прикрепления в конце и есть доказательство, что
		// до него объект был STORED.
		r.unattached(ctx)
	}

	// N6, N7. Недоступный объект (DELETED). Стимул появился вместе с уборкой
	// C: удаление поста переводит его media в DELETED (ADR-055). До неё
	// проверка объявлялась НЕ ВЫПОЛНЕННОЙ — состояние не присваивалось нигде.
	r.deletedMedia(ctx)
}

// deletedMedia — N6 и N7: media удалённого поста отвергается как unavailable.
//
// Объект и пост здесь СВОИ, а не основные. Основной media_id нужен живым
// проверкам A, D, N1 и N2, и удаление его поста разрушило бы их — причём
// молча, потому что порядок секций прогона не является частью контракта.
//
// Измеряется РОВНО различение unavailable и forbidden (media-transfer.md §5):
// первое означает «объект известен субъекту, байтов больше нет», второе —
// «нет права ЛИБО объекта не существует». Сервер, снёсший метаданные вместе
// с байтами, ответил бы forbidden, и проверка «пришла какая-то ошибка»
// приняла бы это за успех.
func (r *Runner) deletedMedia(ctx context.Context) {
	if r.channelID == "" {
		r.rep.skip("N6", "media удалённого поста отвергается как unavailable", "канал не получен")
		r.rep.skip("N7", "701 по media удалённого поста отвергается как unavailable", "канал не получен")
		return
	}

	const wantGone = "unavailable"
	skipBoth := func(format string, a ...any) {
		why := fmt.Sprintf(format, a...)
		r.rep.skip("N6", "media удалённого поста отвергается как unavailable", why)
		r.rep.skip("N7", "701 по media удалённого поста отвергается как unavailable", why)
	}

	body := bytes.Repeat([]byte("to-be-deleted-"), 300) // 4200 байт
	sum := sha256.Sum256(body)
	hexSum := hex.EncodeToString(sum[:])

	cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
	ref, err := media.New(r.a).Upload(cctx, media.UploadRequest{
		ChannelID: r.channelID, Name: "doomed.bin", Mime: "application/octet-stream",
		SHA256: hexSum, Size: int64(len(body)),
	}, bytes.NewReader(body))
	cancel()
	if err != nil {
		skipBoth("загрузка объекта под удаление не прошла: %v", err)
		return
	}

	payload := []byte(fmt.Sprintf(
		`{"channel_id":%q,"message":"mediaslice doomed","photon_media":[{"media_id":%q}]}`,
		r.channelID, ref.MediaID))
	var post postEnvelope
	if err := r.actionJSON(ctx, r.a, photonpb.Action_ACTION_CREATE_POST, payload, &post); err != nil {
		skipBoth("пост с объектом под удаление не создан: %v", err)
		return
	}
	if post.ID == "" {
		skipBoth("createPost вернул пост без id")
		return
	}

	// Свидетель ЖИВОСТИ: до удаления объект обязан отвечать положительно.
	// Без него unavailable ниже ничего не измеряет — сервер, отвергающий
	// вообще всё, прошёл бы обе проверки.
	if _, err := r.mediaInfo(ctx, r.a, ref.MediaID); err != nil {
		r.created = append(r.created, post.ID)
		skipBoth("объект недоступен ЕЩЁ ДО удаления поста: %v", err)
		return
	}

	del := []byte(fmt.Sprintf(`{"post_id":%q}`, post.ID))
	resp, err := r.action(ctx, r.a, photonpb.Action_ACTION_DELETE_POST, del)
	switch {
	case err != nil:
		r.created = append(r.created, post.ID)
		skipBoth("удалить пост не удалось: %v", err)
		return
	case !resp.OK:
		r.created = append(r.created, post.ID)
		skipBoth("сервер отказал в удалении поста: %q", resp.Error)
		return
	}
	// В список на уборку пост НЕ добавляется: он уже удалён этой проверкой.

	_, err = r.mediaInfo(ctx, r.a, ref.MediaID)
	r.expectDenied("N6", "702 по media удалённого поста", wantGone, err)

	cctx, cancel = context.WithTimeout(ctx, r.opts.CallTimeout)
	_, err = media.New(r.a).Download(cctx, ref.MediaID, io.Discard)
	cancel()
	r.expectDenied("N7", "701 по media удалённого поста", wantGone, err)

	r.cascadeMedia(ctx)
	r.knownOrphan(ctx)
}

// cascadeMedia — N8: media ответа, удалённого КАСКАДОМ вместе с корнем.
//
// Заведена находкой живого прогона уборки C: MessageHasBeenDeleted приходит
// только по корню, ответам событий нет вовсе, и до ADR-056 их media
// оставалась читаемой. Догнать её нечем — пред-хука на удаление в API нет, а
// перечислить удалённые посты невозможно. Гасит её не уборка, а проверка
// живости поста на чтении.
//
// Отличие от N6/N7 существенно: там пост удаляют НАПРЯМУЮ и событие приходит,
// здесь пост умирает МОЛЧА. Сервер с работающей уборкой C и без живости
// проходит N6 и N7 и падает здесь.
func (r *Runner) cascadeMedia(ctx context.Context) {
	const wantGone = "unavailable"
	skip := func(format string, a ...any) {
		r.rep.skip("N8", "media каскадно удалённого ответа отвергается как unavailable",
			fmt.Sprintf(format, a...))
	}
	if r.channelID == "" {
		skip("канал не получен")
		return
	}

	rootMedia := r.uploadSmall(ctx, "cascade-root", 2600)
	replyMedia := r.uploadSmall(ctx, "cascade-reply", 2700)
	if rootMedia == "" || replyMedia == "" {
		skip("загрузка объектов треда не прошла")
		return
	}

	root := r.createPostWithMedia(ctx, "", "mediaslice cascade root", rootMedia)
	if root == "" {
		skip("корневой пост не создан")
		return
	}
	reply := r.createPostWithMedia(ctx, root, "mediaslice cascade reply", replyMedia)
	if reply == "" {
		r.created = append(r.created, root)
		skip("ответ не создан")
		return
	}

	// Свидетель: до удаления media ОТВЕТА обязана отдаваться. Без него
	// проверка прошла бы и на сервере, отвергающем всё подряд.
	if _, err := r.mediaInfo(ctx, r.a, replyMedia); err != nil {
		r.created = append(r.created, root)
		skip("media ответа недоступна ЕЩЁ ДО удаления: %v", err)
		return
	}

	// Удаляется ТОЛЬКО корень. Ответ уносит каскад сервера, и события по нему
	// не будет — в этом весь смысл проверки.
	resp, err := r.action(ctx, r.a, photonpb.Action_ACTION_DELETE_POST,
		[]byte(fmt.Sprintf(`{"post_id":%q}`, root)))
	switch {
	case err != nil:
		r.created = append(r.created, root)
		skip("удалить корень не удалось: %v", err)
		return
	case !resp.OK:
		r.created = append(r.created, root)
		skip("сервер отказал в удалении корня: %q", resp.Error)
		return
	}

	_, err = r.mediaInfo(ctx, r.a, replyMedia)
	r.expectDenied("N8", "702 по media каскадно удалённого ответа", wantGone, err)
}

// knownOrphan — N9: объект, осиротевший ДО появления проверки живости.
//
// Идентификатор приходит флагом, а не зашит: прогон обязан оставаться
// переносимым между стендами. Без флага проверка объявляется невыполненной —
// молча пропустить её значило бы потерять её из отчёта.
//
// Смысл отдельно от N8: N8 создаёт сироту сама и проверяет механизм, N9 берёт
// УЖЕ ЛЕЖАЩУЮ на сервере и доказывает, что закрыт класс, а не путь. Ни уборка
// C, ни что-либо иное этих объектов не касалось — их посты умерли до того,
// как появился код.
func (r *Runner) knownOrphan(ctx context.Context) {
	const wantGone = "unavailable"
	const what = "давно осиротевший объект отвергается как unavailable"
	if r.opts.OrphanMedia == "" {
		r.rep.skip("N9", what,
			"не задан -orphan-media: проверка требует объекта, осиротевшего ДО появления проверки живости")
		return
	}
	if err := r.witness(ctx, r.a); err != nil {
		r.rep.skip("N9", what, fmt.Sprintf("субъект A не отвечает на положительный вызов: %v", err))
		return
	}
	_, err := r.mediaInfo(ctx, r.a, r.opts.OrphanMedia)
	r.expectDenied("N9", "702 по давно осиротевшему объекту "+r.opts.OrphanMedia, wantGone, err)
}

// uploadSmall — вспомогательная загрузка. Пустая строка при неудаче:
// вызывающий решает, чем это считать.
func (r *Runner) uploadSmall(ctx context.Context, name string, n int) string {
	body := bytes.Repeat([]byte(name+"-"), n/len(name+"-")+1)[:n]
	sum := sha256.Sum256(body)
	cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
	defer cancel()
	ref, err := media.New(r.a).Upload(cctx, media.UploadRequest{
		ChannelID: r.channelID, Name: name + ".bin", Mime: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)),
	}, bytes.NewReader(body))
	if err != nil {
		return ""
	}
	return ref.MediaID
}

// createPostWithMedia — пост с одним вложением. Пустая строка при неудаче.
func (r *Runner) createPostWithMedia(ctx context.Context, rootID, message, mediaID string) string {
	payload := []byte(fmt.Sprintf(
		`{"channel_id":%q,"message":%q,"root_id":%q,"photon_media":[{"media_id":%q}]}`,
		r.channelID, message, rootID, mediaID))
	var post postEnvelope
	if err := r.actionJSON(ctx, r.a, photonpb.Action_ACTION_CREATE_POST, payload, &post); err != nil {
		return ""
	}
	return post.ID
}

func (r *Runner) unattached(ctx context.Context) {
	body := bytes.Repeat([]byte("unattached-"), 400) // 4400 байт
	sum := sha256.Sum256(body)
	hexSum := hex.EncodeToString(sum[:])

	cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
	ref, err := media.New(r.a).Upload(cctx, media.UploadRequest{
		ChannelID: r.channelID, Name: "unattached.bin", Mime: "application/octet-stream",
		SHA256: hexSum, Size: int64(len(body)),
	}, bytes.NewReader(body))
	cancel()
	if err != nil {
		r.rep.fail("N5", "загрузка неприкреплённого объекта: %v", err)
		return
	}

	// Читается без прикрепления.
	var sink bytes.Buffer
	cctx, cancel = context.WithTimeout(ctx, r.opts.CallTimeout)
	_, err = media.New(r.a).Download(cctx, ref.MediaID, &sink)
	cancel()
	if err != nil {
		r.rep.fail("N5", "неприкреплённый объект не скачивается: %v", err)
		return
	}
	if !bytes.Equal(sink.Bytes(), body) {
		r.rep.fail("N5", "неприкреплённый объект скачан искажённым")
		return
	}

	// И прикрепляется — значит до этого он не был прикреплён.
	payload := []byte(fmt.Sprintf(
		`{"channel_id":%q,"message":"mediaslice unattached","photon_media":[{"media_id":%q}]}`,
		r.channelID, ref.MediaID))
	var post postEnvelope
	if err := r.actionJSON(ctx, r.a, photonpb.Action_ACTION_CREATE_POST, payload, &post); err != nil {
		r.rep.fail("N5", "неприкреплённый объект не удалось прикрепить: %v", err)
		return
	}
	if post.ID != "" {
		r.created = append(r.created, post.ID)
	}
	r.rep.pass("N5", "объект %s без прикрепления доступен и скачивается, затем принят к прикреплению (STORED)", ref.MediaID)
}

// ── 7. Дедлайн стрима ───────────────────────────────────────────────

// deadline — НАСТОЯЩИЙ зависший сценарий.
//
// Клиент объявляет body_length и отправляет заведомо меньше, после чего
// замолкает, не закрывая соединения. С точки зрения сервера это в точности
// брошенная передача: байты не идут, FIN не приходит. Без дедлайна такой
// стрим держал бы горутину, открытый файл и слот из ста до конца соединения,
// а KeepAlivePeriod делает этот срок неограниченным (ADR-049 §5).
//
// Что именно измеряется: вызов вернулся РАНЬШЕ внешней границы ожидания.
// Если бы граница сработала первой, измеренным оказалось бы наше собственное
// терпение, а не поведение сервера, — поэтому этот случай считается отказом,
// а не «проверка не состоялась».
func (r *Runner) deadline(ctx context.Context) {
	fmt.Println("\n── дедлайн зависшего стрима ───────────────────")
	if r.channelID == "" {
		r.rep.skip("S1", "зависший стрим освобождается сервером", "подготовка не выполнена")
		return
	}

	const declared = 1 << 20 // объявляем мегабайт
	const sent = 4 << 10     // отправляем 4 КиБ и замолкаем
	body := bytes.Repeat([]byte{0x5A}, declared)
	sum := sha256.Sum256(body)

	cctx, cancel := context.WithTimeout(ctx, r.opts.StallWait)
	defer cancel()

	start := time.Now()
	_, err := media.New(r.a).Upload(cctx, media.UploadRequest{
		ChannelID: r.channelID, Name: "stalled.bin", Mime: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: declared,
	}, bytes.NewReader(body[:sent]))
	elapsed := time.Since(start)

	// Порог снизу. Освобождение брошенного стрима — это ОЖИДАНИЕ, а затем
	// отказ; мгновенный отказ означает, что стрим не был принят и зависание
	// не проверялось. Половина серверного дедлайна выбрана как порог, ниже
	// которого «ждал» уже нельзя утверждать: сам дедлайн скользящий, и точное
	// совпадение с ним не гарантировано.
	//
	// Прежде этой проверки не было, и на Bridge с умершим RPC-соединением
	// S1 давал PASS «освобождён за 0 s» с причиной unauthorized — измерялось
	// собственное отсутствие авторизации, а не поведение сервера.
	floor := r.opts.StallDeadline / 2
	reason, denied := denialReason(err)

	switch {
	case err == nil:
		r.rep.fail("S1", "сервер ПРИНЯЛ загрузку, получив %d байт из объявленного %d", sent, declared)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		r.rep.fail("S1", "стрим не был освобождён за %s — сработала наша граница ожидания, а не дедлайн сервера", elapsed.Round(time.Second))
	case denied && reasonAuthz(reason):
		r.rep.fail("S1", "стрим отвергнут по авторизации (%q) за %s — зависание не проверялось: "+
			"сервер не принял передачу, а значит и не освобождал её",
			reason, elapsed.Round(time.Millisecond))
	case elapsed < floor:
		r.rep.fail("S1", "ответ пришёл через %s — раньше половины серверного дедлайна (%s из %s); "+
			"измерен отказ, а не освобождение брошенного стрима: %v",
			elapsed.Round(time.Millisecond), floor, r.opts.StallDeadline, err)
	case denied && !strings.HasPrefix(reason, "invalid"):
		r.rep.fail("S1", "стрим освобождён за %s, но причина %q — контракт для недосланного тела "+
			"требует invalid (media-transfer.md §5)", elapsed.Round(time.Second), reason)
	default:
		r.rep.pass("S1", "зависший стрим освобождён сервером за %s (дедлайн %s): %v",
			elapsed.Round(time.Second), r.opts.StallDeadline, err)
		r.rep.measure("дедлайн зависшего стрима сработал через %s (серверный дедлайн — %s, граница ожидания — %s)",
			elapsed.Round(time.Second), r.opts.StallDeadline, r.opts.StallWait)
	}
}

// ── 8. Кредит MAX_STREAMS ───────────────────────────────────────────

// credit — регрессия ADR-050 на живом сервере.
//
// Одно соединение, больше ста последовательных стримов. Предел
// MaxIncomingStreams — сто на соединение, и кредит возвращается только
// завершённым стримом. Если возврат ломается, сто первый вызов не отвергается
// с ошибкой, а ЖДЁТ кредита — то есть выглядит как зависание, а не как отказ.
// Поэтому каждый вызов ограничен своим контекстом: превышение предела и есть
// признак деградации.
//
// Медийные стримы обязаны быть в этой последовательности: именно у них путь
// завершения стрима отдельный, и именно он в ADR-050 оказался дефектным.
func (r *Runner) credit(ctx context.Context) {
	fmt.Println("\n── кредит MAX_STREAMS ─────────────────────────")
	if r.channelID == "" {
		r.rep.skip("C1", "кредит стримов не деградирует", "подготовка не выполнена")
		return
	}
	if r.opts.Streams <= 100 {
		r.rep.skip("C1", "кредит стримов не деградирует",
			fmt.Sprintf("запрошено %d стримов; предел — 100 на соединение, проверка требует большего", r.opts.Streams))
		return
	}

	// Маленький объект специально для этой секции: гонять по кругу основной
	// файл значило бы мерить пропускную способность, а не кредит.
	probe := bytes.Repeat([]byte("credit-probe;"), 320) // 4160 байт
	sum := sha256.Sum256(probe)
	cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
	ref, err := media.New(r.a).Upload(cctx, media.UploadRequest{
		ChannelID: r.channelID, Name: "credit-probe.bin", Mime: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(probe)),
	}, bytes.NewReader(probe))
	cancel()
	if err != nil {
		r.rep.fail("C1", "подготовка пробного объекта: %v", err)
		return
	}

	perCall := r.opts.CallTimeout
	if perCall > 20*time.Second {
		perCall = 20 * time.Second
	}
	cl := media.New(r.a)
	durations := make([]time.Duration, 0, r.opts.Streams)

	for i := 0; i < r.opts.Streams; i++ {
		cctx, cancel := context.WithTimeout(ctx, perCall)
		start := time.Now()
		var callErr error
		if i%3 == 0 {
			// Медийный стрим: тело идёт мимо JSON, завершение стрима — тот
			// самый путь, что чинил ADR-050.
			_, callErr = cl.Download(cctx, ref.MediaID, io.Discard)
		} else {
			_, callErr = r.a.Do(cctx, int32(photonpb.Action_ACTION_GET_ME), nil)
		}
		took := time.Since(start)
		cancel()
		durations = append(durations, took)

		if callErr != nil {
			if errors.Is(callErr, context.DeadlineExceeded) {
				r.rep.fail("C1", "стрим №%d не получил кредита за %s — кредит деградировал после %d стримов",
					i+1, perCall, i)
			} else {
				r.rep.fail("C1", "стрим №%d: %v", i+1, callErr)
			}
			return
		}
	}
	r.rep.pass("C1", "%d последовательных стримов на одном соединении выполнены, из них %d медийных",
		r.opts.Streams, (r.opts.Streams+2)/3)

	first, last := medianOf(durations[:10]), medianOf(durations[len(durations)-10:])
	r.rep.measure("медиана первых десяти вызовов %s, последних десяти %s",
		first.Round(time.Millisecond), last.Round(time.Millisecond))
}

func medianOf(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	c := append([]time.Duration(nil), d...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[len(c)/2]
}

// ── 9. Отдельно фиксируемое ─────────────────────────────────────────

// evidence — два утверждения, ради которых заводился весь срез, и точный
// способ, которым каждое доказано.
func (r *Runner) evidence() {
	fmt.Println("\n── байты мимо JSON и отсутствие буферизации ────")

	// E1. Байты не проходят через JSON.
	//
	// Доказано СОДЕРЖИМЫМ: файл несёт все 256 значений байта, заведомо
	// невалидный UTF-8, NUL и последовательность, неотличимую от конверта
	// ответа. Любая JSON-обёртка либо изменила бы эти байты (экранирование,
	// замена на U+FFFD), либо сорвала бы разбор границы. Побайтовое совпадение
	// round-trip'а — единственное, что при таком содержимом возможно только
	// при прозрачном тракте.
	switch {
	case r.srcHash == "":
		r.rep.skip("E1", "тело не проходит через JSON", "подготовка не выполнена")
	case !r.checkPassed("D3"):
		r.rep.fail("E1", "round-trip побайтово не совпал — утверждение о прозрачности тракта не подтверждено")
	default:
		r.rep.pass("E1", "тело прошло побайтово при содержимом, непереносимом через JSON (256 значений байта, NUL, невалидный UTF-8, псевдоконверт {\"ok\":true…})")
	}

	// E2. Клиент не собирает файл в память.
	limit := r.srcSize / 4
	switch {
	case r.srcSize < 4<<20:
		r.rep.skip("E2", "клиент не буферизует файл целиком",
			fmt.Sprintf("файл %d байт слишком мал: прирост пика неотличим от шума аллокатора", r.srcSize))
	case r.heapPeak > uint64(limit):
		r.rep.fail("E2", "пик кучи вырос на %d байт при файле %d — похоже на буферизацию (порог %d)",
			r.heapPeak, r.srcSize, limit)
	default:
		r.rep.pass("E2", "пик кучи вырос на %d байт при передаче %d — файл целиком в память не собирался (порог %d)",
			r.heapPeak, r.srcSize, limit)
	}

	// E3. Серверная половина того же утверждения.
	r.rep.skip("E3", "сервер не буферизует файл целиком",
		"пик памяти плагина этим прогоном не измеряется; серверная сторона доказана структурно — TestMediaPathNeverReadsWholeBodyIntoMemory запрещает io.ReadAll в медийном пути")
}

func (r *Runner) checkPassed(id string) bool {
	r.rep.mu.Lock()
	defer r.rep.mu.Unlock()
	for _, c := range r.rep.checks {
		if c.id == id {
			return c.st == statusPass
		}
	}
	return false
}

func (r *Runner) noteHeap(delta uint64) {
	if delta > r.heapPeak {
		r.heapPeak = delta
	}
}

// ── Уборка ──────────────────────────────────────────────────────────

// cleanup удаляет созданные посты. Медийные объекты остаются: операции
// удаления в модели нет (ADR-048), и придумывать её ради уборки прогона
// нельзя.
func (r *Runner) cleanup(ctx context.Context) {
	if r.opts.Keep || len(r.created) == 0 {
		if len(r.created) > 0 {
			fmt.Printf("\n  🧹 посты оставлены (-keep): %v\n", r.created)
		}
		return
	}
	fmt.Println("\n── уборка ─────────────────────────────────────")
	for _, id := range r.created {
		payload := []byte(fmt.Sprintf(`{"post_id":%q}`, id))
		if _, err := r.action(ctx, r.a, photonpb.Action_ACTION_DELETE_POST, payload); err != nil {
			fmt.Printf("  ⚠️  пост %s удалить не удалось: %v\n", id, err)
			continue
		}
		fmt.Printf("  🧹 пост %s удалён\n", id)
	}
	if r.mediaID != "" {
		// Уборка C (ADR-055) снимает доступность объектов вместе с их постами:
		// удалённые выше посты уводят свою media в DELETED. Остаются
		// НАДГРОБИЯ — метаданные без байтов — и объекты, поста не имевшие:
		// брошенная загрузка N5 остаётся в STORED, потому что это уборка A, и
		// она в срез не входила.
		fmt.Println("  ℹ️  media удалённых постов переведена в DELETED (уборка C); надгробия и брошенные загрузки остаются — уборки A и B вне среза")
	}
}

// ── Вспомогательное ─────────────────────────────────────────────────

type mediaInfoData struct {
	Media feedMedia `json:"media"`
}

// mediaInfo — 702. Обычное действие, идущее общим JSON-путём: тела у него нет.
func (r *Runner) mediaInfo(ctx context.Context, who Actor, mediaID string) (feedMedia, error) {
	var out mediaInfoData
	payload := []byte(fmt.Sprintf(`{"media_id":%q}`, mediaID))
	if err := r.actionJSON(ctx, who, photonpb.Action_ACTION_MEDIA_GET_INFO, payload, &out); err != nil {
		return feedMedia{}, err
	}
	return out.Media, nil
}

// serverDenial — ОТКАЗ СЕРВЕРА, то есть принятое им решение, а не сбой
// вызова. Разница существенна: сбой означает, что измерить ничего не удалось,
// а отказ — что сервер повёл себя так-то. Прежде и то и другое было
// безымянным error, и проверки не могли их различить.
type serverDenial struct {
	action photonpb.Action
	reason string
}

func (d *serverDenial) Error() string {
	return fmt.Sprintf("сервер отверг действие %d: %s", d.action, d.reason)
}

// denialReason достаёт из ошибки ПРИЧИНУ отказа.
//
// Словарь причин задан контрактом: forbidden, unavailable, invalid
// (media-transfer.md §5, Bridge — mediaErrorText). Вне этого словаря стоит
// unauthorized: его пишет не медийный путь, а ревалидация сессии на каждом
// действии, и означает он «субъект не подтверждён», а не «в этом отказано».
//
// Различать их обязательно. Прогон Stage 6 против Bridge, у которого умерло
// RPC-соединение с хостом, получал unauthorized НА ВСЁ — и проверки отказов
// зеленели, потому что принимали любую ошибку. Зелёный результат на мёртвом
// сервере хуже красного: он утверждает то, чего не измерял.
func denialReason(err error) (string, bool) {
	var d *serverDenial
	if errors.As(err, &d) {
		return d.reason, true
	}
	if errors.Is(err, media.ErrRejected) {
		prefix := media.ErrRejected.Error() + ": "
		text := err.Error()
		if i := strings.Index(text, prefix); i >= 0 {
			return text[i+len(prefix):], true
		}
		return "", true
	}
	return "", false
}

// reasonAuthz — отказ по авторизации субъекта, а не по существу запроса.
func reasonAuthz(reason string) bool {
	return reason == "unauthorized" || strings.HasPrefix(reason, "unauthorized:")
}

// expectDenied — общая форма проверки отказа: отказ обязан быть решением с
// ОЖИДАЕМОЙ причиной.
//
// Прежде эти проверки принимали любую ошибку, то есть проходили бы и при
// выключенном сервере. Теперь несовпадение причины — отказ проверки, а не
// «ну всё равно же отвергнуто».
func (r *Runner) expectDenied(id, what, want string, err error) {
	switch {
	case err == nil:
		r.rep.fail(id, "%s: ПРИНЯТО, ожидался отказ %q", what, want)
	default:
		got, ok := denialReason(err)
		switch {
		case !ok:
			r.rep.fail(id, "%s: вызов не дошёл до решения сервера (%v) — отказ не измерен", what, err)
		case got != want:
			r.rep.fail(id, "%s: отказано с причиной %q, контракт требует %q", what, got, want)
		default:
			r.rep.pass(id, "%s: отказано с причиной %q", what, got)
		}
	}
}

// witness — доказательство, что сервер ОТВЕЧАЕТ и субъект подтверждён.
//
// Свидетель нужен ровно потому, что отказ неотличим от неспособности сервера
// принять решение: и то и другое приходит как ok:false. Если положительный
// вызов тем же субъектом не проходит, отказы рядом не значат ничего, и
// объявлять их успехом нельзя.
func (r *Runner) witness(ctx context.Context, who Actor) error {
	var me struct {
		ID string `json:"id"`
	}
	if err := r.actionJSON(ctx, who, photonpb.Action_ACTION_GET_ME, []byte(`{}`), &me); err != nil {
		return err
	}
	if me.ID == "" {
		return errors.New("getMe вернул пустой id")
	}
	return nil
}

func (r *Runner) action(ctx context.Context, who Actor, action photonpb.Action, payload []byte) (session.Response, error) {
	cctx, cancel := context.WithTimeout(ctx, r.opts.CallTimeout)
	defer cancel()
	return who.Do(cctx, int32(action), payload)
}

// actionJSON выполняет действие и разбирает data. Отказ сервера (ok:false)
// здесь ошибка, а не конверт: это вспомогательный вызов, а проверяемые
// свойства сформулированы отдельно.
func (r *Runner) actionJSON(ctx context.Context, who Actor, action photonpb.Action, payload []byte, out any) error {
	resp, err := r.action(ctx, who, action, payload)
	if err != nil {
		return err
	}
	if !resp.OK {
		return &serverDenial{action: action, reason: resp.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resp.Data, out)
}
