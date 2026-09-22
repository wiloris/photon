package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

/*
 * Точка входа. Здесь только разбор флагов и печать — вся работа в
 * launcher.go, и она же вызывается тестом напрямую. Логика, доступная
 * только через процесс, проверялась бы разбором его вывода, а это худший из
 * способов.
 *
 * Путь к базе и к плагину — АРГУМЕНТЫ. В коде их нет и не будет: хост,
 * знающий, где лежат чужие файлы, перестаёт быть переносимым в тот момент,
 * когда его переносят.
 */

func main() {
	var (
		db       = flag.String("db", "", "файл хранилища bbolt (обязателен)")
		plugin   = flag.String("plugin", "", "путь к исполняемому файлу плагина (обязателен)")
		pluginID = flag.String("id", "dev.engram.counter", "идентификатор плагина")
		grants   = flag.String("grants", "storage.read,storage.write,identity.sign",
			"права через запятую; выдаёт их хост, экрана согласия в этом срезе нет")
		events  = flag.String("events", "", "события через запятую, отправляемые после старта")
		timeout = flag.Duration("timeout", 15*time.Second, "предел ожидания доклада плагина")
		keyDir  = flag.String("keys", "", "каталог ключевого материала; по умолчанию — каталог хранилища")
		install = flag.Bool("install", false,
			"зарегистрировать плагин, если он ещё не установлен (ADR-068 п. 6.2: установка — событие явное)")
		initIdentity = flag.Bool("init", false,
			"создать идентичность устройства, если её ещё не было (ADR-069). "+
				"С существующим device.key — ошибка: это операция создания, а не ensure")
	)
	flag.Parse()

	if *db == "" || *plugin == "" {
		fmt.Fprintln(os.Stderr, "нужны -db и -plugin")
		flag.Usage()
		os.Exit(2)
	}

	abs, err := filepath.Abs(*plugin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "путь плагина: %v\n", err)
		os.Exit(2)
	}

	res, err := Run(context.Background(), Options{
		DBPath: *db,
		// Каталог плагина становится PackageRoot: host склеивает его с
		// Entrypoint и делает рабочим каталогом процесса.
		PackageRoot:  filepath.Dir(abs),
		Entrypoint:   filepath.Base(abs),
		PluginID:     *pluginID,
		Grants:       splitList(*grants),
		Events:       splitList(*events),
		Timeout:      *timeout,
		KeyDir:       *keyDir,
		InitIdentity: *initIdentity,
		Install:      *install,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "запуск плагина: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("плагин:    %s\n", *pluginID)
	fmt.Printf("счётчик:   %v\n", res.Counts)
	fmt.Printf("подпись:   %s\n", verdict(res.Signature != nil, res.SignatureVerified))
	fmt.Printf("завершение: %v\n", res.Exit)
	fmt.Println("журнал:")
	for _, l := range res.Lines {
		fmt.Printf("  %s\n", l)
	}
}

func verdict(present, verified bool) string {
	switch {
	case !present:
		return "нет (право identity.sign не выдано?)"
	case verified:
		return "проверена хостом"
	default:
		return "НЕ СОШЛАСЬ"
	}
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
