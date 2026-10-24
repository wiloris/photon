package p2pmsg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/wiloris/photon/photon-core/ports"
)

// Контракт: разговор строится поверх ports.Stream и ничего больше не требует.
var _ = func() ports.Stream { return (*loop)(nil) }

// loop — поток, в который пишут и из него же читают. Позволяет проверить и
// круговой оборот (Send → Recv), и приём ПОДЛОЖЕННЫХ на провод байт, которых
// корректный Send не произвёл бы никогда.
type loop struct {
	bytes.Buffer
	closes int
	// writeErr, если задана, роняет запись — путь, которого нет у исправного
	// потока, но который обязан быть терминальным.
	writeErr error
}

func (l *loop) Write(p []byte) (int, error) {
	if l.writeErr != nil {
		return 0, l.writeErr
	}
	return l.Buffer.Write(p)
}
func (l *loop) Close() error { l.closes++; return nil }

// pattern — детерминированная нагрузка. Не нули и не текст: нулевая нагрузка
// прошла бы тест, даже если байты потеряны и заменены нулями.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251 + 1)
	}
	return b
}

func TestZeroLengthFrameIsValidEmptyMessage(t *testing.T) {
	l := &loop{}
	c := New(l)

	if err := c.Send(nil); err != nil {
		t.Fatalf("Send пустого: %v", err)
	}
	// На проводе — ровно заголовок и ни байта больше.
	if got := l.Len(); got != 4 {
		t.Fatalf("на проводе %d байт, ожидалось 4 (только заголовок)", got)
	}

	msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if len(msg) != 0 {
		t.Fatalf("длина %d, ожидалась 0", len(msg))
	}
	// Пустое сообщение обязано отличаться от конца потока БЕЗ обращения к
	// ошибке: иначе вызывающий не отличит «пришло пустое» от «всё кончилось».
	if msg == nil {
		t.Fatal("пустое сообщение пришло как nil — неотличимо от отказа")
	}
}

func TestExactMaxMessageSizePasses(t *testing.T) {
	l := &loop{}
	c := New(l)
	want := pattern(MaxMessageSize)

	if err := c.Send(want); err != nil {
		t.Fatalf("Send ровно MaxMessageSize: %v", err)
	}
	got, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("нагрузка не совпала побайтово (получено %d байт из %d)", len(got), len(want))
	}
}

// Локальный отказ по размеру НЕ терминален, и провод не тронут — это не
// симметрично нарушению на проводе (p2p-messaging.md §4.1 против §4.2).
func TestOverMaxRejectedLocallyLeavesStreamUsable(t *testing.T) {
	l := &loop{}
	c := New(l)

	err := c.Send(make([]byte, MaxMessageSize+1))
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("ошибка %v, ожидалась ErrMessageTooLarge", err)
	}
	if l.Len() != 0 {
		t.Fatalf("на провод ушло %d байт — отказ обязан наступать ДО записи", l.Len())
	}

	// Разговор исправен: следующее годное сообщение обязано пройти.
	if err := c.Send([]byte("still fine")); err != nil {
		t.Fatalf("после локального отказа разговор стал непригоден: %v", err)
	}
	got, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv после локального отказа: %v", err)
	}
	if string(got) != "still fine" {
		t.Fatalf("получено %q", got)
	}
}

// Заголовок с длиной сверх предела — терминально: где следующий кадр,
// уже неизвестно.
func TestDeclaredLengthOverMaxIsTerminal(t *testing.T) {
	l := &loop{}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxMessageSize+1)
	l.Buffer.Write(hdr[:])
	l.Buffer.Write(pattern(16)) // следом идут байты: соблазн «пропустить и читать дальше»

	c := New(l)
	_, err := c.Recv()
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("ошибка %v, ожидалась ErrMessageTooLarge", err)
	}
	assertTerminal(t, c, err)
}

func TestTruncatedHeaderIsTerminal(t *testing.T) {
	for _, n := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("%d_байта_заголовка", n), func(t *testing.T) {
			l := &loop{}
			l.Buffer.Write(make([]byte, n))

			c := New(l)
			_, err := c.Recv()
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("ошибка %v, ожидалась io.ErrUnexpectedEOF", err)
			}
			// Оборванный заголовок концом потока НЕ является.
			if errors.Is(err, io.EOF) {
				t.Fatal("оборванный заголовок выдан за чистый конец потока")
			}
			assertTerminal(t, c, err)
		})
	}
}

// Обрыв ПОСЛЕ заголовка — не чистое завершение, сколько бы байт ни дошло.
// Случай «дошло ноль байт нагрузки» отдельный: io.ReadFull отдаёт там io.EOF,
// и без нормализации обрыв посреди кадра выглядел бы корректным концом.
func TestTruncatedPayloadIsUnexpectedEOFNotEOF(t *testing.T) {
	for _, delivered := range []int{0, 1, 63} {
		t.Run(fmt.Sprintf("дошло_%d_из_64", delivered), func(t *testing.T) {
			l := &loop{}
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], 64)
			l.Buffer.Write(hdr[:])
			l.Buffer.Write(pattern(delivered))

			c := New(l)
			_, err := c.Recv()
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("ошибка %v, ожидалась io.ErrUnexpectedEOF", err)
			}
			if errors.Is(err, io.EOF) {
				t.Fatal("обрыв посреди кадра выдан за чистый конец потока")
			}
			assertTerminal(t, c, err)
		})
	}
}

// Чистый конец приёма — io.EOF, и он НЕ запрещает отправку: это half-close,
// а не отказ разговора. Смешать их значило бы запретить ответ на последнее
// сообщение собеседника.
func TestCleanEndIsEOFAndSendStillWorks(t *testing.T) {
	l := &loop{}
	c := New(l)
	if err := c.Send([]byte("last")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := c.Recv(); err != nil {
		t.Fatalf("Recv сообщения: %v", err)
	}

	_, err := c.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ошибка %v, ожидался io.EOF ровно на границе кадра", err)
	}
	// Повтор даёт тот же io.EOF, а не новое чтение потока.
	if _, again := c.Recv(); !errors.Is(again, io.EOF) {
		t.Fatalf("повторный Recv дал %v, ожидался io.EOF", again)
	}
	// И отправка по-прежнему возможна.
	if err := c.Send([]byte("reply after peer finished")); err != nil {
		t.Fatalf("Send после io.EOF запрещён, а не должен: %v", err)
	}
}

// Терминальный отказ необратим и касается ОБОИХ направлений, а первая
// причина не подменяется более поздней.
func TestTerminalIsIrreversibleAndKeepsFirstCause(t *testing.T) {
	l := &loop{}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxMessageSize+1)
	l.Buffer.Write(hdr[:])

	c := New(l)
	_, first := c.Recv()
	if !errors.Is(first, ErrMessageTooLarge) {
		t.Fatalf("первая ошибка %v", first)
	}

	// Дальше на проводе появляется совершенно годный кадр — он не должен
	// «вылечить» разговор.
	l.Buffer.Reset()
	var ok [4]byte
	binary.BigEndian.PutUint32(ok[:], 4)
	l.Buffer.Write(ok[:])
	l.Buffer.Write([]byte("good"))

	if _, err := c.Recv(); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("после терминального отказа Recv дал %v — разговор восстановился", err)
	}
	if err := c.Send([]byte("x")); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("после терминального отказа Send дал %v — отказ не коснулся отправки", err)
	}
}

func TestWriteFailureIsTerminal(t *testing.T) {
	boom := errors.New("провод оборван")
	l := &loop{writeErr: boom}
	c := New(l)

	if err := c.Send([]byte("x")); !errors.Is(err, boom) {
		t.Fatalf("ошибка %v, ожидалась %v", err, boom)
	}
	l.writeErr = nil // провод «починился» — разговор чиниться не должен
	if err := c.Send([]byte("x")); !errors.Is(err, boom) {
		t.Fatalf("после отказа записи Send дал %v — разговор восстановился", err)
	}
	if _, err := c.Recv(); !errors.Is(err, boom) {
		t.Fatalf("после отказа записи Recv дал %v", err)
	}
}

// Нагрузка непрозрачна: нули внутри и невалидный UTF-8 проходят побайтово.
// Проверяется именно то, что Photon её НЕ интерпретирует.
func TestOpaquePayloadSurvivesByteForByte(t *testing.T) {
	cases := map[string][]byte{
		"нули_внутри":       {'a', 0x00, 0x00, 'b', 0x00},
		"невалидный_utf8":   {0xff, 0xfe, 0xc0, 0x80, 0xed, 0xa0, 0x80},
		"только_нули":       make([]byte, 32),
		"все_256_значений":  allBytes(),
		"похоже_на_заголов": {0x00, 0x10, 0x00, 0x00, 0xff},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			l := &loop{}
			c := New(l)
			if err := c.Send(want); err != nil {
				t.Fatalf("Send: %v", err)
			}
			got, err := c.Recv()
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("получено % x, отправлено % x", got, want)
			}
		})
	}
}

func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestRepeatedMessagesPreserveOrderAndContent(t *testing.T) {
	const n = 200
	l := &loop{}
	c := New(l)

	sent := make([][]byte, n)
	for i := 0; i < n; i++ {
		sent[i] = pattern(i * 3) // в том числе нулевой длины на i == 0
		if err := c.Send(sent[i]); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		got, err := c.Recv()
		if err != nil {
			t.Fatalf("Recv #%d: %v", i, err)
		}
		if !bytes.Equal(got, sent[i]) {
			t.Fatalf("сообщение #%d не совпало: %d байт против %d", i, len(got), len(sent[i]))
		}
	}
}

// Срез, отданный Recv, не переиспользуется следующим вызовом: иначе
// вызывающий, придержавший предыдущее сообщение, тихо получил бы испорченные
// данные.
func TestRecvReturnsFreshBuffer(t *testing.T) {
	l := &loop{}
	c := New(l)
	first, second := pattern(64), bytes.Repeat([]byte{0xAA}, 64)
	if err := c.Send(first); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(second); err != nil {
		t.Fatal(err)
	}

	got1, err := c.Recv()
	if err != nil {
		t.Fatal(err)
	}
	got2, err := c.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got1, first) {
		t.Fatal("первое сообщение испорчено вторым Recv")
	}
	if !bytes.Equal(got2, second) {
		t.Fatal("второе сообщение не совпало")
	}
	if &got1[0] == &got2[0] {
		t.Fatal("Recv вернул один и тот же буфер дважды")
	}
}

func TestCloseIsIdempotentAndDoesNotTouchStreamTwice(t *testing.T) {
	l := &loop{}
	c := New(l)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("повторный Close: %v", err)
	}
	if l.closes != 1 {
		t.Fatalf("поток закрыт %d раз, ожидался 1", l.closes)
	}
}

// assertTerminal — общая проверка необратимости: оба направления отдают ту же
// ошибку, не обращаясь к потоку заново.
func assertTerminal(t *testing.T, c *Conversation, want error) {
	t.Helper()
	if _, err := c.Recv(); !errors.Is(err, want) {
		t.Fatalf("повторный Recv дал %v, ожидалась та же ошибка %v", err, want)
	}
	if err := c.Send([]byte("x")); !errors.Is(err, want) {
		t.Fatalf("Send после терминального отказа дал %v, ожидалась %v", err, want)
	}
}
