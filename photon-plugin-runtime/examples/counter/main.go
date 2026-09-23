// Command counter — ПЕРВЫЙ НАСТОЯЩИЙ ПЛАГИН ENGRAM, а не тестовая фикстура.
//
// Отличие от tests/plugins/* принципиальное и не в размере: фикстуры
// существуют, чтобы ломать runtime и доказывать его свойства, и живут под
// tests/. Этот плагин делает осмысленную работу и написан так, как будет
// написан любой следующий: считает собственные запуски, хранит счётчик в
// своём namespace, подписывает результат ключом устройства и докладывает
// наружу.
//
// Он использует ТОЛЬКО публичный Plugin SDK. Ни runtime, ни photon-core не
// импортируются и импортироваться не могут: go.mod SDK не содержит ни одного
// require (ADR-031), а маппинг «операция → право» живёт в runtime (ADR-032),
// поэтому объявить себе права изнутри плагин не в состоянии.
//
// Права, которые плагину нужны: storage.read, storage.write, identity.sign.
// Выдаёт их хост; без identity.sign плагин продолжает считать, но подписи не
// будет — и это видно в его собственном докладе, а не в падении.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"

	sdk "github.com/wiloris/photon-plugin-sdk"
)

const (
	pluginID   = "dev.engram.counter"
	namespace  = "state"
	counterKey = "count"

	// Событие, по которому плагин перечитывает счётчик и докладывает снова.
	// Имя с префиксом вендора: пространство типов событий общее.
	eventReport = "engram.counter.report"
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

	/*
	 * Обработчик события НИЧЕГО не вызывает у SDK — только сигналит в канал.
	 *
	 * Это не стилистика, а обязательное правило контракта: обработчик
	 * выполняется синхронно в том же потоке, что читает RESPONSE, поэтому
	 * блокирующий вызов изнутри него ждёт ответа, которому неоткуда прийти
	 * (api.go, OnEvent). SDK ловит частный случай и возвращает
	 * ErrReentrantCall, но только при том же ctx — подмена контекста обходит
	 * защиту и возвращает зависание.
	 */
	reports := make(chan struct{}, 8)
	p.OnEvent(func(_ context.Context, ev sdk.Event) error {
		if ev.Type == eventReport {
			select {
			case reports <- struct{}{}:
			default: // доклад уже запрошен — второй в очередь не нужен
			}
		}
		return nil
	})

	// Run обязан работать ДО первого вызова: он и доставляет ответы.
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	n, err := increment(ctx, p)
	if err != nil {
		fail(3, "increment", err)
	}
	if err := report(ctx, p, n); err != nil {
		fail(4, "report", err)
	}

	for {
		select {
		case <-reports:
			cur, err := read(ctx, p)
			if err != nil {
				// Доклад по запросу не удался — это не повод падать:
				// счётчик уже сохранён, и потеря одного доклада не
				// повреждает состояние.
				_ = p.Log(ctx, sdk.LogError, "report failed: "+err.Error())
				continue
			}
			_ = report(ctx, p, cur)

		case err := <-runDone:
			// Возврат Run без запроса остановки host классифицирует как
			// ExitCrash, а не ExitCleanStop — ровно та разница, которую
			// проверяет E2E.
			if err != nil {
				fail(5, "run", err)
			}
			return
		}
	}
}

// increment читает счётчик, увеличивает и сохраняет.
//
// Отсутствие ключа — не ошибка, а первый запуск: ErrNotFound означает «ещё
// не считали», и отличать его от настоящего отказа обязательно, иначе
// первый запуск выглядел бы сбоем.
func increment(ctx context.Context, p sdk.Plugin) (int, error) {
	n, err := read(ctx, p)
	if err != nil {
		return 0, err
	}
	n++
	if err := p.Storage().Put(ctx, namespace, counterKey, []byte(strconv.Itoa(n))); err != nil {
		return 0, fmt.Errorf("put: %w", err)
	}
	return n, nil
}

func read(ctx context.Context, p sdk.Plugin) (int, error) {
	raw, err := p.Storage().Get(ctx, namespace, counterKey)
	switch {
	case errors.Is(err, sdk.ErrNotFound):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("get: %w", err)
	}
	n, convErr := strconv.Atoi(string(raw))
	if convErr != nil {
		// Хранилище содержит не число. Обнулять молча нельзя: это скрыло бы
		// порчу данных, а счётчик после этого начал бы считать заново, и
		// причину никто бы не нашёл.
		return 0, fmt.Errorf("stored value %q is not a number", raw)
	}
	return n, nil
}

/*
 * report — ЕДИНСТВЕННЫЙ способ отдать состояние наружу, доступный плагину.
 *
 * Ответа хосту плагин отправить не может: RESPONSE идёт только на REQUEST
 * самого плагина, а SendEvent односторонний (ipc/protocol.go). Поэтому
 * состояние уходит строками журнала — тем же приёмом, которым пользуется
 * tests/plugins/sdkscenario, и его читает Log-колбэк, который хост передаёт
 * в coreops.
 *
 * Формат «имя:значение» выбран потому, что хост обязан отличать доклад от
 * произвольного сообщения, а разбирать свободный текст — плохая идея.
 */
func report(ctx context.Context, p sdk.Plugin, n int) error {
	value := strconv.Itoa(n)

	sig, err := p.Identity().Sign(ctx, []byte(value))
	if err != nil {
		// Подпись — отдельное право. Его отсутствие не мешает считать, и
		// плагин продолжает работу, сказав об этом вслух.
		if logErr := p.Log(ctx, sdk.LogWarn, "signature unavailable: "+err.Error()); logErr != nil {
			return fmt.Errorf("log warning: %w", logErr)
		}
	} else if logErr := p.Log(ctx, sdk.LogInfo, "signature:"+base64.StdEncoding.EncodeToString(sig)); logErr != nil {
		return fmt.Errorf("log signature: %w", logErr)
	}

	/*
	 * count — ДИАГНОСТИКА, и только она.
	 *
	 * Раньше эта строка была ещё и барьером: хост ждал её, чтобы понять,
	 * что доклад закончен, и порядок записей приходилось соблюдать —
	 * первая редакция писала count первым, и хост останавливал плагин
	 * ровно между count и подписью. С ADR-072 барьером служит явный
	 * доклад work.complete ниже, и от порядка строк журнала больше ничего
	 * не зависит.
	 */
	if err := p.Log(ctx, sdk.LogInfo, "count:"+value); err != nil {
		return fmt.Errorf("log count: %w", err)
	}

	/*
	 * Доклад о завершении единицы работы (ADR-072).
	 *
	 * Это НЕ остановка: плагин сообщает факт и продолжает жить, ожидая
	 * STOP от хоста. Для доклада по событию вызов повторяется — единица
	 * работы у counter ровно одна на доклад.
	 */
	// WorkSuccess и пустая нагрузка — прежнее поведение, записанное явно
	// (ADR-074): счётчик свой результат по-прежнему отдаёт строкой журнала
	// "count:", которая остаётся ДИАГНОСТИКОЙ композиции, а не контрактом.
	// Переводить counter на структурную нагрузку — не задача этого среза.
	if err := p.WorkComplete(ctx, sdk.WorkSuccess, nil); err != nil {
		return fmt.Errorf("work complete: %w", err)
	}
	return nil
}

func fail(code int, what string, err error) {
	fmt.Fprintf(os.Stderr, "counter: %s: %v\n", what, err)
	os.Exit(code)
}
