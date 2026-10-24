// Command reporter — GENERIC плагин для проверки контракта доклада (ADR-074).
//
// Он ничего не знает ни про кошелёк, ни про счётчик, ни про какую-либо
// прикладную схему: что доложить, целиком задаётся переменной окружения.
// Именно это и требуется — контракт платформы обязан проверяться плагином,
// который не является ни одним из её потребителей (ADR-074 §13).
//
// REPORTER_SPEC — доклады через ';', каждый в виде <статус>/<нагрузка>:
//
//	success/empty      доклад об успехе без нагрузки
//	failure/vector     отказ с байтовым вектором (см. payloadVector)
//	success/size:1024  успех с нагрузкой заданного размера
//	bogus/empty        заведомо неизвестный статус — проверка отказа SDK
//	crash              выход с ненулевым кодом БЕЗ доклада
//
// REPORTER_EVENT_SPEC — то же самое, но докладывается на КАЖДОЕ событие от
// хоста. Повторный доклад проверяется именно так, а не двумя подряд после
// старта: хост ждёт доклад, затем шлёт событие, затем ждёт снова — порядок
// задан протоколом, а не расстановкой пауз (ADR-072 §6).
//
// О каждом вызове плагин пишет в журнал "wc:<i>:ok" либо "wc:<i>:err:<текст>".
// Это и есть наблюдаемая форма §4.1: доклад об ОТКАЗЕ работы обязан вернуть
// nil, потому что ошибка вызова означает недоставку, а не неудачу работы.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	sdk "github.com/wiloris/photon/photon-plugin-sdk"
)

const pluginID = "dev.photon.reporter"

// payloadVector — байты, которые нельзя подменить текстом.
//
// Здесь намеренно: нулевой байт (обрывает строку в C), 0xFF (не бывает в
// UTF-8), одиночный continuation-байт 0x80 и overlong-последовательность
// 0xC0 0xAF. Если нагрузка где-то по пути пройдёт через string, нормализацию
// UTF-8 или обрезку по нулю — сравнение байтов это покажет.
var payloadVector = []byte{0x00, 0xFF, 0xFE, 0x01, 0x80, 0x81, 0xC0, 0xAF, 0x00, 'z'}

func main() {
	ctx := context.Background()

	spec := strings.TrimSpace(os.Getenv("REPORTER_SPEC"))
	if spec == "crash" {
		// Падение ДО подключения не проверяет ничего интересного: сессии
		// ещё нет. Подключаемся, затем умираем, не доложив.
		p, err := sdk.Connect(ctx, sdk.Config{PluginID: pluginID, ProtocolVersion: 1})
		if err != nil {
			fail(2, "connect", err)
		}
		go func() { _ = p.Run(ctx) }()
		_ = p.Log(ctx, sdk.LogInfo, "reporter:about-to-crash")
		os.Exit(7)
	}

	p, err := sdk.Connect(ctx, sdk.Config{PluginID: pluginID, ProtocolVersion: 1})
	if err != nil {
		fail(2, "connect", err)
	}

	// Обработчик только сигналит: блокирующий вызов SDK изнутри него
	// возвращает ErrReentrantCall (api.go, OnEvent).
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

	if err := report(ctx, p, spec); err != nil {
		fail(3, "report", err)
	}

	// Плагин ПРОДОЛЖАЕТ ЖИТЬ после докладов и ждёт STOP от хоста: доклад не
	// терминален, и отказ не терминален тем более (ADR-074 §5). Что он
	// по-прежнему работоспособен, видно по обработке события.
	eventSpec := strings.TrimSpace(os.Getenv("REPORTER_EVENT_SPEC"))
	for {
		select {
		case <-events:
			if err := report(ctx, p, eventSpec); err != nil {
				fail(6, "event report", err)
			}

		case err := <-runDone:
			if err != nil {
				fail(5, "run", err)
			}
			return
		}
	}
}

// reported — сквозной номер доклада, чтобы строки журнала не повторялись
// между стартовыми докладами и докладами по событию.
var reported int

func report(ctx context.Context, p sdk.Plugin, spec string) error {
	if spec == "" {
		return nil
	}

	for _, item := range strings.Split(spec, ";") {
		i := reported
		reported++
		status, payload, err := parseItem(strings.TrimSpace(item))
		if err != nil {
			return err
		}

		/*
		 * Ошибка НЕ ПРЕРЫВАЕТ прогон и не превращается в отказ плагина:
		 * она записывается в журнал как результат вызова. Иначе проверка
		 * «отказ работы возвращает nil» не отличалась бы от проверки
		 * «вызов не упал» — падение плагина дало бы тот же исход.
		 */
		line := "wc:" + strconv.Itoa(i) + ":ok"
		if err := p.WorkComplete(ctx, status, payload); err != nil {
			line = "wc:" + strconv.Itoa(i) + ":err:" + err.Error()
		}

		/*
		 * Отказ ЗАПИСИ В ЖУРНАЛ не является отказом плагина, и ошибка
		 * здесь намеренно игнорируется.
		 *
		 * Причина измерена: хост отправляет STOP сразу за последним
		 * ожидаемым докладом, и строка, написанная после него, может уже
		 * не дойти — сессия закрывается. Считать это отказом значило бы
		 * ронять плагин кодом 3 в совершенно штатном сценарии, что и
		 * происходило в первой редакции фикстуры.
		 *
		 * Строки, за которыми следует ОЖИДАЕМЫЙ докладом барьер, доходят
		 * всегда — на них тесты и опираются.
		 */
		_ = p.Log(ctx, sdk.LogInfo, line)
	}
	return nil
}

func parseItem(item string) (sdk.WorkStatus, []byte, error) {
	status, payloadSpec, ok := strings.Cut(item, "/")
	if !ok {
		return "", nil, fmt.Errorf("негодный элемент спецификации %q", item)
	}

	payload, err := buildPayload(payloadSpec)
	if err != nil {
		return "", nil, err
	}
	// Статус НЕ проверяется здесь намеренно: "bogus" обязан дойти до SDK,
	// иначе проверять было бы нечего.
	return sdk.WorkStatus(status), payload, nil
}

func buildPayload(spec string) ([]byte, error) {
	switch {
	case spec == "empty":
		return nil, nil

	case spec == "vector":
		return payloadVector, nil

	case strings.HasPrefix(spec, "size:"):
		n, err := strconv.Atoi(strings.TrimPrefix(spec, "size:"))
		if err != nil || n < 0 {
			return nil, fmt.Errorf("негодный размер нагрузки %q", spec)
		}
		// Детерминированный узор, а не нули и не случайные байты: нули
		// прошли бы даже при потере содержимого, а случайные не
		// воспроизводились бы между процессом и тестом. 251 — простое
		// число, поэтому узор не совпадает с границами блоков.
		out := make([]byte, n)
		for i := range out {
			out[i] = byte(i % 251)
		}
		return out, nil
	}
	return nil, fmt.Errorf("неизвестная нагрузка %q", spec)
}

func fail(code int, what string, err error) {
	fmt.Fprintf(os.Stderr, "reporter: %s: %v\n", what, err)
	os.Exit(code)
}
