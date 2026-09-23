package pion

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// Регрессия на дефект, найденный двухпроцессным acceptance ADR-075.
//
// ports.Stream обещает io.Reader, то есть БАЙТОВЫЙ поток. SCTP же
// message-oriented, и pion/sctp при буфере короче сообщения не копирует
// НИЧЕГО, оставляет сообщение в очереди и возвращает n = размер всего
// сообщения вместе с io.ErrShortBuffer. Стандартный io.ReadFull такую ошибку
// гасит (io.ReadAtLeast: `if n >= min { err = nil }`) и отдаёт нетронутый
// нулевой буфер БЕЗ признака отказа.
//
// Ни один тест этого не показывал: все читали буфером ровно в размер
// сообщения, а при точном совпадении ветка короткого буфера не исполняется.
func TestReadIsAByteStreamNotMessageOriented(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i*7 + 1) // не нули: нулевая нагрузка прошла бы и при подмене
	}

	done := make(chan error, 1)
	go func() {
		st, err := aSess.OpenStream(ctx)
		if err != nil {
			done <- err
			return
		}
		_, err = st.Write(payload)
		done <- err
	}()

	st, err := bSess.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	// Читаем ЗАВЕДОМО короткими кусками — именно так работает любой разбор
	// заголовка фиксированной длины (io.ReadFull, bufio, encoding/binary).
	var got bytes.Buffer
	chunk := make([]byte, 7)
	for got.Len() < len(payload) {
		n, err := st.Read(chunk)
		if err != nil {
			t.Fatalf("Read после %d байт: %v", got.Len(), err)
		}
		if n > len(chunk) {
			t.Fatalf("Read вернул n=%d при буфере %d — нарушение io.Reader", n, len(chunk))
		}
		got.Write(chunk[:n])
	}
	if err := <-done; err != nil {
		t.Fatalf("отправка: %v", err)
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("принято %d байт, отправлено %d; побайтово не совпало", got.Len(), len(payload))
	}
}

// То же свойство через io.ReadFull — форма, в которой дефект и проявился:
// ошибки нет, а буфер нулевой.
func TestReadFullOnShortHeaderDoesNotSilentlyZero(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	msg := append([]byte{0xDE, 0xAD, 0xBE, 0xEF}, bytes.Repeat([]byte{0x5A}, 1000)...)
	done := make(chan error, 1)
	go func() {
		st, err := aSess.OpenStream(ctx)
		if err != nil {
			done <- err
			return
		}
		_, err = st.Write(msg)
		done <- err
	}()

	st, err := bSess.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	var hdr [4]byte
	if _, err := io.ReadFull(st, hdr[:]); err != nil {
		t.Fatalf("io.ReadFull заголовка: %v", err)
	}
	if !bytes.Equal(hdr[:], msg[:4]) {
		t.Fatalf("заголовок прочитан как % x, отправлено % x", hdr[:], msg[:4])
	}
	rest := make([]byte, len(msg)-4)
	if _, err := io.ReadFull(st, rest); err != nil {
		t.Fatalf("io.ReadFull остатка: %v", err)
	}
	if !bytes.Equal(rest, msg[4:]) {
		t.Fatal("остаток сообщения не совпал — хвост потерян")
	}
	if err := <-done; err != nil {
		t.Fatalf("отправка: %v", err)
	}
}

// Регрессия на вторую половину того же дефекта: io.Writer предела длины не
// имеет, а SCTP ограничивает одно сообщение (pion: 65536 по умолчанию, ошибка
// ErrOutboundPacketTooLarge). Найдено тем же acceptance: кадр в 1 MiB не
// уходил вовсе.
func TestWriteAcceptsPayloadLargerThanSCTPMessageLimit(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	const size = 1 << 20 // ровно MaxMessageSize контракта ADR-075
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*13 + 3)
	}

	done := make(chan error, 1)
	go func() {
		st, err := aSess.OpenStream(ctx)
		if err != nil {
			done <- err
			return
		}
		n, err := st.Write(payload)
		if err == nil && n != len(payload) {
			err = io.ErrShortWrite
		}
		done <- err
	}()

	st, err := bSess.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	got := make([]byte, size)
	if _, err := io.ReadFull(st, got); err != nil {
		t.Fatalf("io.ReadFull %d байт: %v", size, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("запись %d байт: %v", size, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("нагрузка в 1 MiB не совпала побайтово")
	}
}
