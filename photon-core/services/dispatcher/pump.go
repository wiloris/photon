package dispatcher

import (
	"context"
	"errors"
	"runtime"
	"time"

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
// инстансе без адресата — там подписываться не к кому, ровно как и вызывать
// действия; и ErrClosed после Close.
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
	if d.sess == nil {
		d.mu.Unlock()
		return ErrNoEndpoint
	}
	if d.pumpStarted {
		d.mu.Unlock()
		return nil
	}
	d.pumpStarted = true
	// Add строго под d.mu и после проверки d.closed — общая дисциплина
	// диспетчера, без исключений для долгоживущей горутины: иначе Add мог бы
	// случиться уже после Wait в Close.
	d.inflight.Add(1)
	d.mu.Unlock()

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
	return nil
}

// pump — единственная горутина доставки на инстанс.
//
// Снимает себя с d.inflight при выходе. Всё, что должно произойти ПОСЛЕ этого,
// живёт в onStop у StartEvents, а не здесь.
func (d *Dispatcher) pump(sink EventSink) {
	defer d.inflight.Done()

	backoff := d.cfg.EventBackoffMin
	for {
		if d.ctx.Err() != nil {
			return
		}

		sub, err := d.newSubscription()
		if err != nil {
			// Ошибка построения сессии — дефект конфигурации, а не обрыв:
			// повтор её не исправит.
			return
		}

		cctx, cancel := context.WithTimeout(d.ctx, d.cfg.ConnectTimeout)
		err = sub.Connect(cctx)
		cancel()
		if err != nil {
			_ = sub.Close()
			if !d.backoffWait(&backoff) {
				return
			}
			continue
		}

		// Успех сбрасывает паузу: следующий обрыв не должен наследовать
		// длину предыдущей серии неудач.
		backoff = d.cfg.EventBackoffMin
		sink(eventConnected())

		lost := d.receive(sub, sink)
		if lost {
			sink(eventNetworkChanged())
		}
		// Оборванная сессия НЕ переиспользуется: markLost не закрывает и не
		// обнуляет conn, а повторный Connect перезаписал бы его, оставив
		// прежнее соединение висеть навсегда (event-delivery.md §6.1).
		// Восстановление — это Close и НОВАЯ сессия следующим витком.
		_ = sub.Close()

		if !d.backoffWait(&backoff) {
			return
		}
	}
}

// receive принимает события до обрыва, отмены или закрытия сессии.
// Возвращает true, если причиной выхода был обрыв связи.
func (d *Dispatcher) receive(sub *session.Session, sink EventSink) bool {
	for {
		p, err := sub.AcceptPush(d.ctx)
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
			continue
		}

		switch {
		case d.ctx.Err() != nil:
			// Отмена вызывающим обрывом не считается — так же, как этого не
			// делает сам AcceptPush.
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
func (d *Dispatcher) newSubscription() (*session.Session, error) {
	return session.New(session.Config{
		Transport:    d.tr,
		Endpoint:     d.cfg.Endpoint,
		SessionToken: d.cfg.SessionToken,
		// mode фиксируется на СОЕДИНЕНИЕ, а не на стрим (bridge-session.md
		// §2.4): получить push по action-соединению нельзя.
		Mode: session.ModeSubscribe,
	})
}

// backoffWait выдерживает паузу и удваивает её до потолка.
// Возвращает false, если инстанс уничтожается.
//
// Пауза ОБЯЗАНА быть отменяемой: неотменяемая заставила бы Photon_Destroy
// ждать её длину — ровно то зависание, от которого защищает порядок Close
// (отмена строго до ожидания).
func (d *Dispatcher) backoffWait(backoff *time.Duration) bool {
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
	case <-d.ctx.Done():
		return false
	}
}
