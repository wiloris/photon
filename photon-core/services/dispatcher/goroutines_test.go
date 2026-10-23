package dispatcher

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// После Close горутин столько же, сколько до создания инстанса (ADR-130
// §5.4): диагностика включена, есть хранилище, подписка, action- и
// медиасоединение, чтение 819/820 и вызов, оставшийся без ответа сервера
// (его горутина чтения живёт до закрытия соединения), — ничто из этого не
// оставляет горутин.
func TestNoGoroutinesLeftAfterClose(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	br := newFakeBridge()
	d, err := New(Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: br,
		ConnectTimeout: 2 * timeUnit, CallTimeout: 3 * timeUnit,
		StoragePath: filepath.Join(t.TempDir(), "g.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	if err := d.StartEvents(c.sink, nil); err != nil {
		t.Fatal(err)
	}
	c.waitFor(t, 1)
	for i := 0; i < 5; i++ {
		d.SendAction(200, []byte(`{}`))
		d.SendAction(800, []byte(`{"channel_id":"c","limit":5}`))
		d.SendAction(819, []byte(`{}`))
		d.SendAction(820, []byte(`{"after_seq":0,"limit":10}`))
	}
	if _, err := d.mediaSession(context.Background()); err != nil {
		t.Fatal(err)
	}
	if during := runtime.NumGoroutine(); during <= before {
		t.Fatalf("горутин во время работы %d ≤ %d — проверка ничего не наблюдает", during, before)
	}
	// Последним — вызов без ответа сервера: горутина чтения ответа живёт до
	// закрытия соединения (ограничение порта, session.Do) — Close обязан её
	// снять. Поле фиктивного сервера после этого не восстанавливается: его
	// читает зависший поток, и запись была бы гонкой самого теста.
	br.action = nil
	if _, err := d.SendAction(200, []byte(`{}`)); err == nil {
		t.Fatal("вызов без ответа сервера прошёл")
	}
	d.Close()

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("горутин после Close %d > %d до создания:\n%s", after, before, buf[:n])
	}
}
