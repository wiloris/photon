package tcp

import (
	"testing"
	"time"
)

// Закрытие соединения, пока СОБСТВЕННЫЙ однонаправленный поток ещё открыт.
//
// openStream закрывал канал readable такого потока, а connClosed будит
// читателей отправкой в readable — отправка в закрытый канал, паника
// процесса. TestUniStream_ServerPush падал на этом изредка (поток обычно
// успевал уйти из таблицы до закрытия соединения); здесь — детерминированно:
// поток открыт и не закрыт.
func TestCloseConnWithOpenOwnUniStream(t *testing.T) {
	t.Run("локальное закрытие", func(t *testing.T) {
		p := newPair(t)
		s, err := p.client.OpenUniStream(ctx5s(t))
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		s.Write([]byte("x"))
		if err := p.client.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	t.Run("закрытие удалённой стороной", func(t *testing.T) {
		p := newPair(t)
		if _, err := p.client.OpenUniStream(ctx5s(t)); err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		p.server.Close()
		// Соединение клиента узнаёт о закрытии в своём цикле чтения —
		// туда и приходила паника. Ждём, пока он это обработает.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := p.client.OpenStream(ctx5s(t)); err != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("клиент не заметил закрытия соединения сервером")
	})
}
