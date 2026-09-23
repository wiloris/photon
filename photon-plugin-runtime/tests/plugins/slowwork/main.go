// Command slowwork — плагин с АСИНХРОННОЙ работой, доказывающий completion.
//
// Он существует ради одного свойства: работа идёт заметное время ПОСЛЕ
// handshake, и хост не вправе останавливать плагин, пока она не закончена.
// Раньше такого плагина написать было нельзя — единственным барьером служила
// строка журнала "count:<N>", то есть формат examples/counter.
//
// Чего здесь намеренно НЕТ: ни "count:", ни "signature:". Если тест проходит,
// значит синхронизация держится на work.complete (ADR-072), а не на разборе
// журнала.
//
// Задержки — ВНУТРИ плагина. Это его собственное поведение, а не таймер хоста:
// хост никакими паузами не пользуется и пользоваться не должен.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	sdk "github.com/wiloris/photon-plugin-sdk"
)

const (
	pluginID  = "dev.photon.slowwork"
	namespace = "work"

	// writes и delay подобраны так, чтобы работа заведомо не успевала
	// завершиться до STOP, если бы хост отправил его сразу после старта.
	writes = 5
	delay  = 20 * time.Millisecond

	// afterEventKey пишется ПОСЛЕ первого доклада, по событию: его наличие
	// доказывает, что доклад плагина не остановил.
	afterEventKey = "after-event"
)

func main() {
	ctx := context.Background()

	p, err := sdk.Connect(ctx, sdk.Config{
		PluginID:        pluginID,
		ProtocolVersion: 1,
	})
	if err != nil {
		fail(2, "connect", err)
	}

	// Обработчик только сигналит: блокирующий вызов SDK изнутри него
	// зависает (api.go, OnEvent).
	events := make(chan struct{}, 8)
	p.OnEvent(func(_ context.Context, _ sdk.Event) error {
		select {
		case events <- struct{}{}:
		default:
		}
		return nil
	})

	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	// Работа идёт в отдельной горутине — как у любого плагина, который не
	// успевает всё сделать между Connect и первым сообщением хоста.
	workDone := make(chan error, 1)
	go func() { workDone <- work(ctx, p) }()

	select {
	case err := <-workDone:
		if err != nil {
			fail(3, "work", err)
		}
	case err := <-runDone:
		// Сессия оборвалась раньше, чем закончилась работа. Для этого
		// плагина это отказ: именно его и обязан исключать work.complete.
		fail(4, "сессия закрыта до конца работы", err)
	}

	/*
	 * Работа закончена — но плагин ПРОДОЛЖАЕТ ЖИТЬ и ждёт STOP от хоста.
	 * Доклад не останавливает ни сессию, ни процесс (ADR-072 §3), и
	 * доказывается это тем, что после доклада плагин ещё способен принять
	 * событие и отработать по нему.
	 */
	for {
		select {
		case <-events:
			if err := handleEvent(ctx, p); err != nil {
				fail(6, "event", err)
			}

		case err := <-runDone:
			if err != nil {
				fail(5, "run", err)
			}
			return
		}
	}
}

// handleEvent — работа по событию и повторный доклад.
//
// Повторяемость доклада здесь не декларируется, а используется: единица
// работы у этого плагина — обработка одного события.
func handleEvent(ctx context.Context, p sdk.Plugin) error {
	if err := p.Storage().Put(ctx, namespace, afterEventKey, []byte("1")); err != nil {
		return fmt.Errorf("put %s: %w", afterEventKey, err)
	}
	return p.WorkComplete(ctx, sdk.WorkSuccess, nil)
}

func work(ctx context.Context, p sdk.Plugin) error {
	for i := 0; i < writes; i++ {
		time.Sleep(delay)

		key := "item-" + strconv.Itoa(i)
		if err := p.Storage().Put(ctx, namespace, key, []byte(strconv.Itoa(i))); err != nil {
			return fmt.Errorf("put %s: %w", key, err)
		}
	}

	/*
	 * Доклад — ПОСЛЕ последней записи и только тогда.
	 *
	 * Права на него не требуется, и это проверяется самим тестом: плагину
	 * выдан лишь storage.write, а identity.*, crypto.* и storage.read — нет.
	 */
	return p.WorkComplete(ctx, sdk.WorkSuccess, nil)
}

func fail(code int, what string, err error) {
	fmt.Fprintf(os.Stderr, "slowwork: %s: %v\n", what, err)
	os.Exit(code)
}
