// Command greeter — минимальное расширение ENGRAM: второй пример после
// counter и первый, который устанавливается только из каталога ENSTOR.
//
// Он существует ради одного утверждения ADR-089: механизм магазина не знает
// ни одного конкретного расширения. Greeter не упоминается ни в ENGRAM, ни в
// его <queries>; обнаружение — по маркеру пакета, установка — из каталога,
// запуск — общим PluginLauncher.
//
// Работа: доложить о завершении с маленькой нагрузкой (ADR-074) — своей
// версией и приветствием. Прав не требует.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	sdk "github.com/wiloris/photon-plugin-sdk"
)

const (
	pluginID = "dev.photon.greeter"

	// version — та же, что identity.version в манифесте пакета; из
	// манифеста же выводятся versionName/versionCode APK (ADR-089 §2.2).
	version = "0.1.0"
)

func main() {
	ctx := context.Background()
	p, err := sdk.Connect(ctx, sdk.Config{PluginID: pluginID, ProtocolVersion: 1})
	if err != nil {
		fail(2, "connect", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	payload, _ := json.Marshal(map[string]any{
		"version": 1,
		"kind":    "greeter.hello",
		"result":  map[string]string{"greeting": "Привет из ENSTOR", "extension_version": version},
	})
	if err := p.WorkComplete(ctx, sdk.WorkSuccess, payload); err != nil {
		fail(4, "work complete", err)
	}
	if err := <-runDone; err != nil {
		fail(5, "run", err)
	}
}

func fail(code int, what string, err error) {
	fmt.Fprintf(os.Stderr, "greeter: %s: %v\n", what, err)
	os.Exit(code)
}
