// Package dispatcher — маршрутизация действий C ABI в сессию Bridge
// (Phase 13.5).
//
// Спецификация: photon-core/docs/action-routing.md (написана до кода).
// Семантика результата: ADR-038 — PHOTON_OK означает состоявшийся вызов, а
// вердикт операции живёт в конверте ответа.
//
// Что этот пакет делает: разбирает config_json, держит сессию, подключается по
// требованию, отправляет action и собирает конверт ответа для границы ABI.
// Что он НЕ делает: не знает о C (это bindings), не разбирает data ответа
// (доменные сущности принадлежат ENGRAM/Bridge), не переподключается по
// собственному расписанию (§3.4).
package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/localfeed"
	"github.com/wiloris/photon-core/services/outbox"
	"github.com/wiloris/photon-core/services/session"
)

// Dispatcher — маршрутизатор действий одного инстанса Photon.
//
// Одному инстансу соответствует один адресат (D-4, §8): адресат — это сам
// PhotonHandle, а многобэкендность выражается двумя вызовами Photon_Create с
// разным config_json. Отсюда требование, которое проверяется тестом, а не
// поддерживается соглашением: ВСЁ состояние маршрутизации висит на этой
// структуре. Ни одной пакетной переменной, ни одного общего транспорта, ни
// одного sync.Once уровня пакета.
type Dispatcher struct {
	cfg  Config
	tr   ports.Transport
	sess *session.Session

	// ownTransport — транспорт создан здесь, значит закрывается здесь.
	// Подставленный вызывающим (тесты, будущий autodial) не закрывается: он
	// может быть общим для нескольких потребителей, и закрыть чужое — способ
	// сломать соседа.
	ownTransport bool

	// Локальный backend (ADR-064). st — хранилище инстанса, feed — лента
	// поверх него; nil у обоих означает «backend не настроен», законное
	// состояние. ownStorage — закрывать ли его при Close, по тому же
	// правилу, что ownTransport выше.
	st         ports.Storage
	feed       *localfeed.Store
	box        *outbox.Store // исходящие (ADR-093), тот же файл, что feed
	ownStorage bool

	// ctx живёт столько же, сколько инстанс. Отмена — единственный способ
	// снять с сети вызовы в полёте, не дожидаясь их таймаутов (§9).
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	closed  bool
	attempt *connectAttempt

	// pumpStarted — насос событий запущен. Отдельного поля сессии подписки
	// здесь НЕТ намеренно: ею владеет исключительно насос, он же закрывает её
	// при каждом обрыве и при выходе. Close добирается до неё через отмену
	// ctx, а inflight.Wait гарантирует, что к моменту закрытия транспорта
	// насос уже вышел и сессию закрыл — то есть свойство «обе сессии закрыты
	// раньше транспорта» держится, и второе поле было бы заведомым no-op.
	pumpStarted bool

	// Цель подписки (ADR-123 §2.3, events_target.go). target — под mu;
	// targetGen растёт с каждой сменой; subCancel отменяет поколение текущей
	// подписки; targetWake будит насос, ждущий цели. sink/onStop — адресат,
	// запомненный StartEvents до появления цели. subTr — транспорт подписки
	// инстанса без секции bridge, создаётся лениво при первой цели.
	target     subTarget
	targetGen  uint64
	subCancel  context.CancelFunc
	targetWake chan struct{}
	sink       EventSink
	onStop     func()
	subTr      ports.Transport
	ownSubTr   bool

	// inflight считает и вызовы, и попытки подключения. Add происходит под
	// mu, после проверки closed, — иначе Add мог бы случиться уже после
	// Wait в Close.
	inflight sync.WaitGroup
}

// connectAttempt — одна попытка подключения, разделяемая всеми ожидающими.
//
// Результат хранится в самой попытке, а не в поле Dispatcher: общее поле
// могла бы перезаписать СЛЕДУЮЩАЯ попытка между close(done) и чтением
// результата проснувшимся ожидающим. Гонка узкая и воспроизводится редко —
// то есть ровно того класса, который потом ищут часами.
type connectAttempt struct {
	done chan struct{}
	err  error // пишется до close(done), читается только после <-done
}

// New создаёт диспетчер. Сети не касается — ни одного пакета до возврата.
//
// Это то же правило, что у session.New и у Photon_Create (photon_core.h): в
// конструкторе нет ни таймаута, ни способа отмены, поэтому сетевой вызов в нём
// означал бы создание объекта, блокирующееся на неопределённое время.
//
// Инстанс без Endpoint создаётся успешно и без транспорта: см. ErrNoEndpoint.
func New(cfg Config) (*Dispatcher, error) {
	cfg = cfg.withDefaults()

	d := &Dispatcher{cfg: cfg, targetWake: make(chan struct{}, 1)}
	if cfg.Endpoint != "" {
		d.target = subTarget{endpoint: cfg.Endpoint, token: cfg.SessionToken}
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())

	// Хранилище открывается ДО сессии и независимо от неё: инстанс может
	// иметь локальный backend без Bridge и Bridge без backend (ADR-064).
	st, own, err := openStorage(cfg)
	if err != nil {
		return nil, err
	}
	if st != nil {
		feed, box, ferr := prepareFeed(d.ctx, st)
		if ferr != nil {
			if own {
				_ = st.Close(context.Background())
			}
			return nil, ferr
		}
		d.st, d.feed, d.box, d.ownStorage = st, feed, box, own
	}

	if cfg.Endpoint == "" {
		return d, nil
	}

	tr := cfg.Transport
	ownTr := false
	if tr == nil {
		tr = newQUICTransport(cfg)
		ownTr = true
	}

	s, err := session.New(session.Config{
		Transport:    tr,
		Endpoint:     cfg.Endpoint,
		SessionToken: cfg.SessionToken,
		// Только action: push — отдельный режим и отдельное соединение
		// (bridge-session.md §2.4), и он за рамками этой фазы.
		Mode: session.ModeAction,
	})
	if err != nil {
		// Ничего удерживаемого после неудачного New не остаётся — то же
		// требование атомарности, что ADR-028 предъявляет к Photon_Create.
		if own {
			tr.Close()
		}
		d.cancel()
		return nil, fmt.Errorf("dispatcher: %w", err)
	}

	d.tr, d.sess, d.ownTransport = tr, s, ownTr
	return d, nil
}

// SendAction выполняет действие и возвращает конверт ответа как JSON.
//
// Порядок проверок — часть контракта, а не деталь реализации (§5): всё, что
// можно отвергнуть без сети, отвергается без сети. Неизвестный action-код,
// невалидный payload и отсутствующий адресат не зависят ни от состояния связи,
// ни от её наличия — иначе диагностика уезжает в сторону, а на инстансе без
// Bridge неизвестный код вообще перестал бы отличаться от отсутствия связи.
//
// Возвращаемая ошибка означает, что вызов НЕ состоялся. Ответ сервера с
// ok:false — состоявшийся вызов: err == nil, а отказ лежит в конверте
// (ADR-038).
func (d *Dispatcher) SendAction(action int32, payload []byte) ([]byte, error) {
	// 1. Код действия. Ровно та же проверка, что делает session.Do первой
	//    строкой; здесь она нужна раньше, чтобы не зависеть от наличия
	//    сессии — её может не быть вовсе.
	resource, err := session.ResourceFor(action)
	if err != nil {
		return nil, err
	}

	// 2. Payload. Это проверка контракта ABI (payload_json объявлен JSON'ом,
	//    ADR-026 п.7), а не формата провода, поэтому она здесь и остаётся,
	//    даже когда кодек проверяет своё. nil и пустая строка одинаково
	//    невалидны: правило одно, без тонкостей про nil против "".
	if !json.Valid(payload) {
		return nil, fmt.Errorf("dispatcher: %w", session.ErrInvalidPayload)
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, ErrClosed
	}

	// 3. Локальное действие исполняется ЗДЕСЬ и до сессии не доходит
	//    никогда (ADR-064 п. 2.4). Ветка стоит после проверки закрытия и
	//    до проверки адресата: локальному действию адресат не нужен, и
	//    инстанс без Bridge обязан его исполнять.
	if resource == session.ResourceLocalFeed {
		d.inflight.Add(1)
		d.mu.Unlock()
		defer d.inflight.Done()

		lctx, lcancel := context.WithTimeout(d.ctx, d.cfg.CallTimeout)
		defer lcancel()
		return d.localAction(lctx, action, payload)
	}

	// 4. Адресат.
	if d.sess == nil {
		d.mu.Unlock()
		return nil, ErrNoEndpoint
	}
	d.inflight.Add(1)
	d.mu.Unlock()
	defer d.inflight.Done()

	ctx, cancel := context.WithTimeout(d.ctx, d.cfg.CallTimeout)
	defer cancel()

	if err := d.ensureReady(ctx); err != nil {
		return nil, err
	}

	resp, err := d.sess.Do(ctx, action, payload)
	if err != nil {
		return nil, err
	}
	return envelope(resp)
}

// ensureReady возвращает управление, когда сессия подключена.
//
// Синглфлайт (§3.3): одновременно идёт не более одной попытки, ожидающие
// получают результат ТОЙ ЖЕ попытки, а не начинают свою. Иначе N параллельных
// SendAction на свежем инстансе открыли бы N соединений, а часть вызовов
// получила бы session.ErrConnectInProgress не по делу.
//
// Ключевое место — атомарность: проверка состояния и постановка попытки
// происходят под ОДНОЙ блокировкой. Разделить их (проверить, отпустить,
// запустить) — самая правдоподобная ошибка в этом коде: тест на число
// одновременных вызовов её не заметит, а второе соединение появится.
//
// Подключение не делается внутри session.Do и без удержания d.mu через
// сетевой вызов — bridge-session.md §6 п. 5.
func (d *Dispatcher) ensureReady(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	if d.sess.State() == session.StateReady {
		d.mu.Unlock()
		return nil
	}
	a := d.attempt
	if a == nil {
		a = &connectAttempt{done: make(chan struct{})}
		d.attempt = a
		d.inflight.Add(1)
		go d.connectOnce(a)
	}
	d.mu.Unlock()

	select {
	case <-a.done:
		return a.err
	case <-ctx.Done():
		// Вызывающий не ждёт дольше своего дедлайна; попытка продолжается для
		// остальных ожидающих и завершится сама.
		return ctx.Err()
	}
}

// connectOnce — единственное место, где вызывается session.Connect.
//
// Подключение идёт под ctx инстанса, а не под ctx вызывающего: иначе отмена
// одного вызова (его дедлайн истёк) убивала бы попытку, результата которой
// ждут остальные.
func (d *Dispatcher) connectOnce(a *connectAttempt) {
	defer d.inflight.Done()

	cctx, cancel := context.WithTimeout(d.ctx, d.cfg.ConnectTimeout)
	defer cancel()

	err := d.sess.Connect(cctx)

	d.mu.Lock()
	d.attempt = nil
	d.mu.Unlock()

	a.err = err
	close(a.done)
}

// Cancel снимает с сети всё, что идёт прямо сейчас, НЕ дожидаясь завершения и
// ничего не закрывая.
//
// Существует ради одного случая: реентерабельный Photon_Destroy, вызванный из
// колбэка, обязан вернуть управление немедленно и не может позвать Close —
// тот ждёт d.inflight, в котором состоит сам насос, вызвавший колбэк
// (event-delivery.md §10.2). Cancel будит насос, дальше он выходит сам, а
// закрытие выполняется уже после его выхода.
//
// Для всех остальных случаев есть Close: Cancel без него не закрывает ни
// сессии, ни транспорт.
func (d *Dispatcher) Cancel() { d.cancel() }

// Close останавливает диспетчер. Идемпотентен.
//
// Порядок обязателен и обоснован (§9):
//
//  1. отменить ctx — иначе ожидание превратится в зависание на время
//     call_timeout; прецедент прямой — вечное зависание host.Stop на
//     неотвечающем плагине (Phase 9), найденное тестом отказа, а не ревью;
//  2. дождаться вызовов в полёте — после отмены они возвращаются сразу;
//  3. закрыть сессию — только conn.Close освобождает горутину чтения внутри
//     session.Do: ports.Stream.Close — half-close только пишущей стороны,
//     CancelRead порт намеренно не экспортирует (bridge-session.md §6 п. 6);
//  4. закрыть транспорт, если он наш.
func (d *Dispatcher) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()

	d.cancel()
	d.inflight.Wait()

	var err error
	if d.sess != nil {
		err = d.sess.Close()
	}
	if d.ownTransport && d.tr != nil {
		if cerr := d.tr.Close(); err == nil {
			err = cerr
		}
	}
	// Транспорт подписки — после выхода насоса (inflight.Wait выше).
	if d.ownSubTr && d.subTr != nil {
		if cerr := d.subTr.Close(); err == nil {
			err = cerr
		}
	}
	// Хранилище закрывается ПОСЛЕ сессии и транспорта (ADR-064 п. 1.4):
	// пока идут вызовы, оно может понадобиться, и порядок здесь тот же, что
	// у остальных ресурсов инстанса — сначала то, что говорит по сети.
	if d.ownStorage && d.st != nil {
		if cerr := d.st.Close(context.Background()); err == nil {
			err = cerr
		}
	}
	return err
}

// envelope собирает конверт ответа для границы ABI.
//
// Форма взята из канонической схемы (photon.proto, PhotonResponse: ok / error
// / data), а не изобретена здесь: «response-схема того же action_code», о
// которой говорит photon_core.h, — это конверт, а не голое тело.
//
// data вставляется СЫРЫМИ байтами: свойство bridge-session.md §3.1 (ни одной
// лишней сериализации, никакой потери точности чисел на float64) держится
// именно здесь. Пересобираются только три поля конверта.
//
// Поле code не отправляется. Схема объявляет int32 code = 3, но на проводе его
// нет ни разу (bridge-session.md §2.5); выдать константный ноль значило бы
// придумать данные, которых не существует.
func envelope(r session.Response) ([]byte, error) {
	// Проверка валидности data — не «лишний проход» в смысле §3.1: там речь о
	// разборе и повторной сборке, здесь — сканирование без аллокаций. Она
	// защищает контракт границы: response_json обязан быть валидным JSON, и
	// склейка непроверенного куска — способ отдать клиенту битую строку,
	// причину которой он будет искать у себя.
	if len(r.Data) > 0 && !json.Valid(r.Data) {
		return nil, fmt.Errorf("dispatcher: %w: response data is not valid JSON", session.ErrMalformedFrame)
	}

	var b bytes.Buffer
	b.WriteString(`{"ok":`)
	b.WriteString(strconv.FormatBool(r.OK))
	if r.Error != "" {
		e, err := json.Marshal(r.Error)
		if err != nil {
			return nil, fmt.Errorf("dispatcher: encode error field: %w", err)
		}
		b.WriteString(`,"error":`)
		b.Write(e)
	}
	if len(r.Data) > 0 {
		b.WriteString(`,"data":`)
		b.Write(r.Data)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}
