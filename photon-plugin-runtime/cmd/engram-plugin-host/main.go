package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wiloris/photon-plugin-runtime/manifest"
)

/*
 * Точка входа. Здесь разбор аргументов и печать — вся работа в launcher.go, и
 * она же вызывается тестом напрямую. Логика, доступная только через процесс,
 * проверялась бы разбором его вывода, а это худший из способов.
 *
 * ДВА РЕЖИМА ЗАПУСКА (ADR-071), взаимоисключающие:
 *
 *	--manifest <path> --package-root <path>   манифест приходит ИЗВНЕ, из
 *	                                          пакета плагина; на Android
 *	                                          корень — текущий nativeLibraryDir;
 *	-plugin <path>                            legacy: манифест собирается в
 *	                                          коде, корень и entrypoint
 *	                                          выводятся из пути к бинарю.
 *
 * Одновременно заданные — отказ, а не приоритет одного над другим: два
 * источника одного значения расходятся молча, и выбрать между ними молча
 * значит выбрать неправильно ровно тогда, когда это важно.
 *
 * Путь к базе и к плагину — АРГУМЕНТЫ. В коде их нет и не будет: хост,
 * знающий, где лежат чужие файлы, перестаёт быть переносимым в тот момент,
 * когда его переносят.
 */

// legacyDefaultPluginID — идентификатор, который legacy-режим подставляет,
// когда -id не задан.
//
// Умолчание уехало ИЗ ФЛАГА сюда, и это не косметика: пока оно стояло у
// флага, «задан ли -id» было неразличимо, а без этого различия проверка
// ожидаемого plugin_id (ADR-071) не работала бы вовсе.
const legacyDefaultPluginID = "dev.engram.counter"

func main() {
	o, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	res, err := Run(context.Background(), o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "запуск плагина: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("плагин:    %s\n", o.PluginID)
	fmt.Printf("счётчик:   %v\n", res.Counts)
	fmt.Printf("подпись:   %s\n", verdict(res.Signature != nil, res.SignatureVerified))
	fmt.Printf("завершение: %v\n", res.Exit)
	fmt.Println("журнал:")
	for _, l := range res.Lines {
		fmt.Printf("  %s\n", l)
	}
}

/*
 * parseArgs — разбор аргументов в Options.
 *
 * Вынесен из main отдельной функцией, потому что взаимное исключение режимов,
 * обязательность -package-root и сверка ожидаемого plugin_id — это правила, а
 * не оформление. Внутри main() они проверялись бы запуском процесса и
 * разбором его stderr.
 */
func parseArgs(args []string) (Options, error) {
	fs := flag.NewFlagSet("engram-plugin-host", flag.ContinueOnError)

	var (
		db           = fs.String("db", "", "файл хранилища bbolt (обязателен)")
		plugin       = fs.String("plugin", "", "legacy-режим: путь к исполняемому файлу плагина")
		manifestPath = fs.String("manifest", "", "manifest-режим: файл манифеста из пакета плагина")
		packageRoot  = fs.String("package-root", "", "manifest-режим: корень пакета; на Android — nativeLibraryDir")
		pluginID     = fs.String("id", "",
			"в manifest-режиме — ОЖИДАЕМЫЙ plugin_id (обязан совпасть с манифестом); "+
				"в legacy-режиме — идентификатор плагина")
		grants = fs.String("grants", "storage.read,storage.write,identity.sign",
			"права через запятую; выдаёт их хост, экрана согласия в этом срезе нет")
		events  = fs.String("events", "", "события через запятую, отправляемые после старта")
		timeout = fs.Duration("timeout", 15*time.Second, "предел ожидания доклада плагина")
		keyDir  = fs.String("keys", "", "каталог ключевого материала; по умолчанию — каталог хранилища")
		install = fs.Bool("install", false,
			"зарегистрировать плагин, если он ещё не установлен (ADR-068 п. 6.2: установка — событие явное)")
		initIdentity = fs.Bool("init", false,
			"создать идентичность устройства, если её ещё не было (ADR-069). "+
				"С существующим device.key — ошибка: это операция создания, а не ensure")
	)

	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}

	// Заданность -id — ТОЛЬКО через Visit: он обходит фактически заданные
	// флаги, а VisitAll — все. Сравнение с "" заданности не показывает:
	// `-id ""` и отсутствие флага дают одно значение, и правило «совпасть
	// обязан» стало бы тождественно истинным (ADR-071, Source of truth).
	idSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "id" {
			idSet = true
		}
	})

	if *db == "" {
		return Options{}, errors.New("нужен -db")
	}

	o := Options{
		DBPath:       *db,
		Grants:       splitList(*grants),
		Events:       splitList(*events),
		Timeout:      *timeout,
		KeyDir:       *keyDir,
		InitIdentity: *initIdentity,
		Install:      *install,
	}

	switch {
	case *manifestPath != "" && *plugin != "":
		return Options{}, errors.New("-manifest и -plugin — взаимоисключающие режимы (ADR-071)")

	case *manifestPath != "":
		if *packageRoot == "" {
			// Выводить корень в этом режиме не из чего: манифест несёт путь
			// относительный, абсолютного у хоста нет.
			return Options{}, errors.New("в manifest-режиме обязателен -package-root")
		}
		m, err := parseManifestFile(*manifestPath)
		if err != nil {
			return Options{}, err
		}
		if idSet && *pluginID != m.Identity.PluginID {
			return Options{}, fmt.Errorf(
				"-id %q не совпадает с plugin_id манифеста %q: запуск отвергнут до побочных эффектов",
				*pluginID, m.Identity.PluginID)
		}
		o.Manifest = m
		o.PackageRoot = *packageRoot
		// Источник истины — манифест, и только он. -id сюда не попадает
		// никогда, даже совпав.
		o.PluginID = m.Identity.PluginID
		o.Entrypoint = m.Runtime.Entrypoint

	case *plugin != "":
		if *packageRoot != "" {
			// Тот же запрет двух источников: в legacy корень выводится из
			// пути к бинарю.
			return Options{}, errors.New("-package-root относится к manifest-режиму; в legacy корень выводится из -plugin")
		}
		abs, err := filepath.Abs(*plugin)
		if err != nil {
			return Options{}, fmt.Errorf("путь плагина: %w", err)
		}
		// Каталог плагина становится PackageRoot: host склеивает его с
		// Entrypoint и делает рабочим каталогом процесса.
		o.PackageRoot = filepath.Dir(abs)
		o.Entrypoint = filepath.Base(abs)
		o.PluginID = *pluginID
		if !idSet {
			o.PluginID = legacyDefaultPluginID
		}

	default:
		return Options{}, errors.New("нужен -plugin (legacy) или -manifest вместе с -package-root")
	}

	return o, nil
}

// parseManifestFile читает манифест существующим разборщиком.
//
// Своего разбора не появляется: manifest.Parse уже держит предел размера,
// forward compatibility и отказ на неизвестном required_* поле.
func parseManifestFile(path string) (*manifest.Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("манифест: %w", err)
	}
	defer f.Close()

	m, err := manifest.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("манифест %s: %w", filepath.Base(path), err)
	}
	return m, nil
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
