package dispatcher

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/wiloris/photon-core/services/diag"
	"github.com/wiloris/photon-core/services/session"
)

// EventSink получает готовый event_json. Вызывается НАСОСОМ СИНХРОННО и
// последовательно: следующее событие не начнёт доставляться, пока не вернулось
// предыдущее (event-delivery.md §9.1).
//
// Отсюда два свойства, которые иначе пришлось бы обеспечивать отдельно:
// порядок сохраняется, и колбэки границы ABI никогда не параллельны — для
// стороны JNI это существенно, там AttachCurrentThread выполняется без
// конкуренции за один env.
//
// Sink не должен блокироваться надолго: очереди между ним и сетью нет
// намеренно (§9.2), поэтому медленный потребитель замедляет чтение из сети.
type EventSink func(eventJSON []byte)

// StartEvents запускает подписное соединение и насос событий.
//
// Идемпотентен: повторный вызов насос не создаёт. Возвращает ErrNoEndpoint на
// инстансе без цели подписки — там подписываться не к кому, ровно как и
// вызывать действия; адресат доставки при этом ЗАПОМИНАЕТСЯ, и насос
// запустит первая цель, заданная действием 814 (ADR-123 §2.3). ErrClosed —
// после Close.
//
// onStop вызывается ТОЙ ЖЕ горутиной после выхода насоса и ПОСЛЕ снятия себя с
// d.inflight. Порядок обязателен: он даёт вызывающему право выполнить в onStop
// уничтожение, которое ждёт d.inflight, — иначе горутина ждала бы сама себя
// (event-delivery.md §10.2).
func (d *Dispatcher) StartEvents(sink EventSink, onStop func()) error {
	if sink == nil {
		return errors.New("dispatcher: event sink is required")
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	if d.pumpStarted {
		d.mu.Unlock()
		return nil
	}
	if !d.target.set() {
		d.sink, d.onStop = sink, onStop
		d.mu.Unlock()
		return ErrNoEndpoint
	}
	d.startPumpLocked(sink, onStop)
	d.mu.Unlock()
	return nil
}

// startPumpLocked запускает насос. Вызывается под d.mu, после проверки
// d.closed и d.pumpStarted.
func (d *Dispatcher) startPumpLocked(sink EventSink, onStop func()) {
	d.pumpStarted = true
	// Add строго под d.mu и после проверки d.closed — общая дисциплина
	// диспетчера, без исключений для долгоживущей горутины: иначе Add мог бы
	// случиться уже после Wait в Close.
	d.inflight.Add(1)

	go func() {
		// Поток доставки обязан быть стабильным: потребитель за границей ABI
		// определяет реентерабельный Photon_Destroy сравнением
		// идентификаторов OS-потока, а между двумя вызовами cgo горутину
		// может перенести на другой поток. Без привязки захваченный
		// идентификатор не гарантированно принадлежит потоку, который
		// исполнит колбэк (event-delivery.md §10.3).
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		d.pump(sink)

		if onStop != nil {
			onStop()
		}
	}()
}

// pump — единственная горутина доставки на инстанс.
//
// Снимает себя с d.inflight при выходе. Всё, что должно произойти ПОСЛЕ этого,
// живёт в onStop у StartEvents, а не здесь.
func (d *Dispatcher) pump(sink EventSink) {
	defer d.inflight.Done()

	backoff := d.cfg.EventBackoffMin
	failures := 0 // неудач подряд — номер попытки в событии sub_backoff
	var lastGen uint64
	for {
		// Цель и её поколение. Смена цели (814) отменяет gctx: текущая
		// подписка и пауза прерываются, следующий виток идёт к новой цели.
		t, gen, gctx, ok := d.nextTarget()
		if !ok {
			return
		}
		if gen != lastGen {
			// Новая цель не наследует паузу неудач прежней.
			backoff, lastGen, failures = d.cfg.EventBackoffMin, gen, 0
		}

		sub, err := d.newSubscription(t)
		if err != nil {
			// Ошибка построения сессии — дефект цели, а не обрыв: повтор её
			// не исправит. Ждём следующей цели.
			<-gctx.Done()
			continue
		}

		cctx, cancel := context.WithTimeout(gctx, d.cfg.ConnectTimeout)
		t0 := d.diag.ConnStart(diag.ConnSubscribe)
		err = sub.Connect(cctx)
		d.diag.ConnResult(diag.ConnSubscribe, t0, Classify(err).String(), err == nil)
		cancel()
		if err != nil {
			_ = sub.Close()
			failures++
			d.diag.SubBackoff(failures, backoff)
			if !d.backoffWait(gctx, &backoff) {
				return
			}
			continue
		}

		// Успех сбрасывает паузу: следующий обрыв не должен наследовать
		// длину предыдущей серии неудач.
		backoff, failures = d.cfg.EventBackoffMin, 0
		sink(eventConnected())
		d.diag.EventSent(diag.EventConnected)

		lost := d.receive(gctx, sub, sink)
		if lost {
			sink(eventNetworkChanged())
			d.diag.EventSent(diag.EventNetworkChanged)
		}
		// Оборванная сессия НЕ переиспользуется: markLost не закрывает и не
		// обнуляет conn, а повторный Connect перезаписал бы его, оставив
		// прежнее соединение висеть навсегда (event-delivery.md §6.1).
		// Восстановление — это Close и НОВАЯ сессия следующим витком.
		_ = sub.Close()

		if gctx.Err() != nil {
			// Смена или снятие цели: без паузы к следующему витку.
			continue
		}
		failures++
		d.diag.SubBackoff(failures, backoff)
		if !d.backoffWait(gctx, &backoff) {
			return
		}
	}
}

// receive принимает события до обрыва, отмены или закрытия сессии.
// Возвращает true, если причиной выхода был обрыв связи.
func (d *Dispatcher) receive(ctx context.Context, sub *session.Session, sink EventSink) bool {
	for {
		p, err := sub.AcceptPush(ctx)
		if err == nil {
			ev, encErr := eventMessage(p)
			if encErr != nil {
				// Битое событие отвергает СОБЫТИЕ, а не подписку: один
				// испорченный фрейм не повод рвать связь (event-delivery.md
				// §13). Тот же принцип, что у неизвестного признака
				// направления потока в adapters/pion.
				continue
			}
			sink(ev)
			d.diag.EventSent(diag.EventMessage)
			continue
		}

		switch {
		case ctx.Err() != nil:
			// Отмена вызывающим (Close или смена цели) обрывом не считается —
			// так же, как этого не делает сам AcceptPush.
			return false
		case errors.Is(err, session.ErrConnectionLost):
			return true
		case errors.Is(err, session.ErrSessionClosed):
			// Сессию закрыли снаружи: восстанавливать нечего.
			return false
		case errors.Is(err, session.ErrMalformedFrame):
			continue
		default:
			// Прочее приравнивается к обрыву: подписка без приёма бесполезна,
			// а тишина на ней есть норма и сама себя не обнаружит.
			return true
		}
	}
}

// newSubscription создаёт ОТДЕЛЬНУЮ сессию в режиме subscribe.
//
// Транспорт тот же: ports.Transport.Connect можно звать повторно (в адаптере
// QUIC каждый вызов делает свой DialAddr), а Transport.Close установленных
// соединений не закрывает. Второй экземпляр транспорта означал бы второй набор
// общих ресурсов без единой причины (event-delivery.md §4).
func (d *Dispatcher) newSubscription(t subTarget) (*session.Session, error) {
	return session.New(session.Config{
		Transport:    diag.WrapTransport(d.subTransport(), d.diag, diag.ConnSubscribe),
		Diag:         d.diag,
		DiagConn:     diag.ConnSubscribe,
		Endpoint:     t.endpoint,
		SessionToken: t.token,
		// mode фиксируется на СОЕДИНЕНИЕ, а не на стрим (bridge-session.md
		// §2.4): получить push по action-соединению нельзя.
		Mode: session.ModeSubscribe,
	})
}

// backoffWait выдерживает паузу и удваивает её до потолка.
// Возвращает false, если инстанс уничтожается; true — пауза истекла или
// прервана сменой цели (ctx поколения).
//
// Пауза ОБЯЗАНА быть отменяемой: неотменяемая заставила бы Photon_Destroy
// ждать её длину — ровно то зависание, от которого защищает порядок Close
// (отмена строго до ожидания).
func (d *Dispatcher) backoffWait(ctx context.Context, backoff *time.Duration) bool {
	t := time.NewTimer(*backoff)
	defer t.Stop()

	if next := *backoff * 2; next > d.cfg.EventBackoffMax {
		*backoff = d.cfg.EventBackoffMax
	} else {
		*backoff = next
	}

	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return d.ctx.Err() == nil
	}
}
