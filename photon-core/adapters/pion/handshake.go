package pion

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/wiloris/photon-core/ports"
)

// Домен подписи P2P-handshake. ADR-037 требует собственный домен для любого
// нового формата, подписываемого ключом устройства: без него подпись из
// device-auth или plugin-sign предъявлялась бы как P2P-handshake.
const handshakeDomain = "photon-p2p-auth:"

// Метка RFC 5705 для channel binding (ADR-041 §4, p2p-session.md §5.1).
const (
	bindingLabel  = "photon-p2p-channel-binding"
	bindingLength = 32
	nonceLen      = 32
)

func newNonce() ([]byte, error) {
	n := make([]byte, nonceLen)
	if _, err := rand.Read(n); err != nil {
		return nil, fmt.Errorf("pion: nonce: %w", err)
	}
	return n, nil
}

// transcript — то, что подписывается обеими сторонами (ADR-041 §3,
// p2p-session.md §5.2).
//
// Границы полей кодируются ЯВНО, длиной перед каждым полем. Это прямое
// требование ADR-037: там сказано, что отсутствие длины в device-auth есть
// свойство формы с одним полем, а не принцип, и при появлении структуры из
// нескольких полей границы обязаны кодироваться явно — иначе две разные пары
// полей дают одну байтовую строку.
//
// Порядок полей — по РОЛЯМ согласования, а не по тому, кто вычисляет: обе
// стороны обязаны построить одну и ту же строку.
func transcript(initiatorPub, responderPub ports.PublicKey, initiatorNonce, responderNonce, binding []byte) []byte {
	out := make([]byte, 0, len(handshakeDomain)+
		len(initiatorPub)+len(responderPub)+len(initiatorNonce)+len(responderNonce)+len(binding)+5*4)
	out = append(out, handshakeDomain...)
	for _, f := range [][]byte{initiatorPub, responderPub, initiatorNonce, responderNonce, binding} {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(f)))
		out = append(out, n[:]...)
		out = append(out, f...)
	}
	return out
}

// handshakeMsg — то, что уходит по выделенному потоку.
type handshakeMsg struct {
	PublicKey []byte `json:"pubkey"`
	Signature []byte `json:"sig"`
}

// handshakeConn — то, по чему идёт обмен подписями.
//
// Дедлайны в интерфейсе не для удобства: без них шаг handshake оставался бы
// ЕДИНСТВЕННЫМ шагом согласования, не подчиняющимся контексту. ICE и DTLS
// принимают ctx сами, а чтение из SCTP-потока блокируется до прихода данных
// и об отмене не знает — молчащая сторона подвешивала бы согласование
// навсегда, минуя обязательный потолок ADR-044 §3.
//
// Оба реального и тестового транспорта его удовлетворяют: *sctp.Stream и
// соединения net.Pipe.
type handshakeConn interface {
	io.ReadWriter
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// bindDeadline подчиняет чтение и запись контексту.
//
// Двумя способами сразу, и это не дублирование: дедлайн из ctx покрывает
// истечение потолка, наблюдатель — отмену вызывающим, у которой своего
// дедлайна нет. Возвращённая функция снимает наблюдателя; вызывать
// обязательно, иначе горутина живёт до отмены контекста.
func bindDeadline(ctx context.Context, c handshakeConn) func() {
	if d, ok := ctx.Deadline(); ok {
		_ = c.SetReadDeadline(d)
		_ = c.SetWriteDeadline(d)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			// Дедлайн в прошлом снимает блокировку немедленно — тот же
			// приём, что у net.Conn при отмене.
			past := time.Now().Add(-time.Second)
			_ = c.SetReadDeadline(past)
			_ = c.SetWriteDeadline(past)
		case <-stop:
		}
	}()
	return func() {
		close(stop)
		<-done
		// Дедлайн снимается и после успеха. *sctp.Stream держит на каждый
		// дедлайн горутину-таймер до его срабатывания: без сброса каждая
		// сессия удерживала её, а с ней и поток handshake, до истечения
		// потолка согласования — уже после Close(). Найдено
		// TestSessionCyclesDoNotLeakGoroutines.
		_ = c.SetReadDeadline(time.Time{})
		_ = c.SetWriteDeadline(time.Time{})
	}
}

// ctxCause отвечает, чем на самом деле вызван отказ ввода-вывода.
//
// Простого ctx.Err() недостаточно: дедлайн, выставленный на соединении, и
// таймер самого контекста срабатывают в один и тот же момент, но независимо —
// чтение успевает вернуть «i/o timeout» раньше, чем ctx.Err() перестанет быть
// nil. Тогда истечение потолка выглядело бы отказом разбора, то есть отказом
// согласования, — а p2p-session.md §9 требует различать эти два исхода.
func ctxCause(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d, ok := ctx.Deadline(); ok && !time.Now().Before(d) {
		return context.DeadlineExceeded
	}
	return nil
}

// runHandshake обменивается подписями и подтверждает ключ собеседника.
//
// Возвращает ПОДТВЕРЖДЁННЫЙ публичный ключ. До успеха этой функции Session
// не имеет права сообщать Peer() ничего: ADR-041 §1 запрещает возвращать
// «ожидаемый» ключ — это и есть подмена, которую ADR предотвращает.
func runHandshake(
	ctx context.Context,
	rw handshakeConn,
	id ports.Identity,
	self ports.IdentityPair,
	expected ports.PublicKey,
	initiatorPub, responderPub ports.PublicKey,
	initiatorNonce, responderNonce, binding []byte,
) (ports.PublicKey, error) {
	unbind := bindDeadline(ctx, rw)
	defer unbind()

	msg := transcript(initiatorPub, responderPub, initiatorNonce, responderNonce, binding)

	sig, err := id.Sign(ctx, self.Private, msg)
	if err != nil {
		return nil, fmt.Errorf("pion: handshake sign: %w", err)
	}
	out, err := json.Marshal(handshakeMsg{PublicKey: self.Public, Signature: sig})
	if err != nil {
		return nil, fmt.Errorf("pion: handshake encode: %w", err)
	}

	// Пишем и читаем параллельно: обе стороны шлют первыми, и
	// последовательный порядок «сначала записать, потом читать» одинаков у
	// обеих — тупика нет, но при большом сообщении запись может блокироваться
	// до чтения на той стороне. Держим сообщение маленьким и пишем целиком.
	errCh := make(chan error, 1)
	go func() {
		_, werr := rw.Write(append(out, '\n'))
		errCh <- werr
	}()

	var in handshakeMsg
	dec := json.NewDecoder(rw)
	if derr := dec.Decode(&in); derr != nil {
		// Отмена вызывающего возвращается как ошибка контекста, а не как
		// «сломался разбор»: подменять её отказом согласования запрещено
		// (p2p-session.md §9).
		if cerr := ctxCause(ctx); cerr != nil {
			return nil, cerr
		}
		return nil, fmt.Errorf("pion: handshake decode: %w", derr)
	}
	if werr := <-errCh; werr != nil {
		if cerr := ctxCause(ctx); cerr != nil {
			return nil, cerr
		}
		return nil, fmt.Errorf("pion: handshake write: %w", werr)
	}

	ok, err := id.Verify(ctx, in.PublicKey, msg, ports.Signature(in.Signature))
	if err != nil {
		return nil, fmt.Errorf("pion: handshake verify: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("pion: handshake signature rejected")
	}

	// Сверка с ожиданием вызывающего. Аргумент peer в CreateOffer/AcceptOffer
	// — намерение, а не факт (p2p-session.md §3): подтвердился другой ключ —
	// терминальный отказ, а не тихая замена ожидания фактом.
	if len(expected) != 0 && string(in.PublicKey) != string(expected) {
		return nil, fmt.Errorf("pion: peer key mismatch: authenticated a different key than requested")
	}
	return ports.PublicKey(in.PublicKey), nil
}
