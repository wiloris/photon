package ipc

import (
	"context"
	"net"
	"testing"
	"time"
)

/*
 * STOP не опережает ответ на запрос, который уже исполняется.
 *
 * Свойство общее для любого REQUEST, а не частный случай какой-то операции.
 * Проверяется ПОРЯДКОМ сообщений на проводе, а не паузами: порядок либо
 * соблюдён, либо нет, и от скорости машины это не зависит.
 *
 * Зачем свойство нужно. Ответ пишется ПОСЛЕ возврата Handle, а STOP — из
 * другой горутины; обе записи идут через writeMu, и без ожидания порядок
 * определялся тем, кто первым возьмёт мьютекс. Выиграв гонку, STOP приходил
 * плагину раньше ответа на его собственный запрос: SDK закрывал сессию, и
 * ДОСТАВЛЕННЫЙ вызов возвращал ошибку доставки. Измерено на Android — два
 * одинаковых запуска Wallet дали коды выхода 0 и 4.
 */
func TestStop_WaitsForInflightResponse(t *testing.T) {
	const (
		pluginID = "com.engram.stoporder"
		caller   = "caller-stoporder"
	)

	entered := make(chan struct{})
	release := make(chan struct{})

	handler := HandlerFunc(func(_ context.Context, _ CallerInfo, op string, _ []byte) ([]byte, error) {
		close(entered)
		<-release // держим запрос в полёте, пока тест не разрешит
		return []byte("ok:" + op), nil
	})

	plugin, sess, _ := setup(t, pluginID, caller, []string{"storage.read"}, handler)
	go sess.Run(context.Background())
	plugin.completeHandshake(t, pluginID, 1, []string{"ssh.session"})

	if err := Encode(plugin.conn, &Message{Type: MsgRequest, RequestID: 1, Op: "storage.get"}); err != nil {
		t.Fatalf("плагин: отправить REQUEST: %v", err)
	}
	<-entered // запрос принят и исполняется

	/*
	 * Stop запускается в своей горутине и сообщает, что вот-вот будет
	 * вызван. Сигнал нужен не для синхронизации ожидания, а чтобы
	 * проверка ниже не прошла просто потому, что горутина ещё не начала
	 * работу, — иначе при снятом ожидании тест мог бы пройти по случайности.
	 */
	stopCalling := make(chan struct{})
	stopDone := make(chan error, 1)
	go func() {
		close(stopCalling)
		stopDone <- sess.Stop(context.Background())
	}()
	<-stopCalling

	/*
	 * 1. На проводе пока не должно быть НИЧЕГО.
	 *
	 * Дедлайн здесь не задаёт правильность: при исправном коде Stop ждёт
	 * закрытия канала, который закроется только после того, как тест
	 * отпустит handler, — то есть не напишет ничего, сколько ни жди. При
	 * снятом ожидании STOP уходит немедленно. Дедлайн лишь ограничивает
	 * время самого теста.
	 */
	if err := plugin.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if m, err := Decode(plugin.conn); err == nil {
		t.Fatalf("на проводе %s, хотя ответ на запрос ещё не отправлен", m.Type)
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("ожидался таймаут чтения, получено: %v", err)
	}
	if err := plugin.conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("снять дедлайн: %v", err)
	}

	// 2. Отпускаем handler — теперь ответ может быть записан.
	close(release)

	// 3. ПЕРВЫМ обязан прийти ответ на запрос.
	first, err := Decode(plugin.conn)
	if err != nil {
		t.Fatalf("плагин: читать первое сообщение: %v", err)
	}
	if first.Type != MsgResponse {
		t.Fatalf("первым пришёл %s, ожидался RESPONSE — STOP опередил ответ", first.Type)
	}
	if first.RequestID != 1 {
		t.Fatalf("ответ на запрос %d, ожидался 1", first.RequestID)
	}

	// 4. И только вторым — STOP.
	second, err := Decode(plugin.conn)
	if err != nil {
		t.Fatalf("плагин: читать второе сообщение: %v", err)
	}
	if second.Type != MsgStop {
		t.Fatalf("вторым пришёл %s, ожидался STOP", second.Type)
	}

	if err := <-stopDone; err != nil {
		t.Fatalf("Stop вернул ошибку: %v", err)
	}
}

/*
 * Ожидание ограничено context: зависший плагин не превращает штатную
 * остановку в вечную.
 *
 * Fallback-семантика сохранена — по истечении ctx STOP уходит всё равно, как
 * и до появления ожидания.
 */
func TestStop_ExpiredContextStillSendsStop(t *testing.T) {
	const (
		pluginID = "com.engram.stophang"
		caller   = "caller-stophang"
	)

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	handler := HandlerFunc(func(_ context.Context, _ CallerInfo, _ string, _ []byte) ([]byte, error) {
		close(entered)
		<-release
		return nil, nil
	})

	plugin, sess, _ := setup(t, pluginID, caller, []string{"storage.read"}, handler)
	go sess.Run(context.Background())
	plugin.completeHandshake(t, pluginID, 1, []string{"ssh.session"})

	if err := Encode(plugin.conn, &Message{Type: MsgRequest, RequestID: 1, Op: "storage.get"}); err != nil {
		t.Fatalf("плагин: отправить REQUEST: %v", err)
	}
	<-entered

	// Контекст уже истёк — ждать нечего, STOP обязан уйти.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stopDone := make(chan error, 1)
	go func() { stopDone <- sess.Stop(ctx) }()

	m, err := Decode(plugin.conn)
	if err != nil {
		t.Fatalf("плагин: читать сообщение: %v", err)
	}
	if m.Type != MsgStop {
		t.Fatalf("пришёл %s, ожидался STOP: при истёкшем ctx ожидание не должно его удерживать", m.Type)
	}
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop вернул ошибку: %v", err)
	}
}
