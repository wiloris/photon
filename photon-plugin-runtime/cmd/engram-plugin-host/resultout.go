package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

/*
 * Файл результата для вызывающего приложения (ADR-085).
 *
 * Это ЕДИНСТВЕННЫЙ публичный канал результата хоста. stdout и stderr им не
 * являются и не станут: диагностика печатается как прежде, и payload в неё
 * не попадает никогда (§2.5). Android пишет вывод хоста в logcat построчно —
 * результат через stdout означал бы нагрузку плагина в журнале устройства.
 *
 * Хост payload НЕ РАЗБИРАЕТ: переносит байты докладов base64-строкой и не
 * знает ни одного имени поля внутри (ADR-074 §3, §6).
 */

const (
	resultFormat  = "photon.host-result"
	resultVersion = 1

	// maxResultReports — потолок докладов в файле (ADR-085 §2.3). Сверх него
	// файл сообщает отказ, а не список, обрезанный молча.
	maxResultReports = 16

	errTooManyReports = "too_many_reports"
)

// hostResult — конверт файла, версия 1 (ADR-085 §2.2).
type hostResult struct {
	Format   string         `json:"format"`
	Version  int            `json:"version"`
	PluginID string         `json:"plugin_id"`
	Host     resultHost     `json:"host"`
	Exit     resultExit     `json:"exit"`
	Reports  []resultReport `json:"reports"`
}

type resultHost struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

// resultExit — статус процесса плагина. Kind и Code осмысленны только при
// Known: нулевое значение ExitKind есть clean_stop, и без признака файл
// утверждал бы штатную остановку там, где статуса не было (ADR-074 §9).
type resultExit struct {
	Known bool   `json:"known"`
	Kind  string `json:"kind"`
	Code  int    `json:"code"`
}

type resultReport struct {
	Status  string `json:"status"`
	Payload string `json:"payload"`
}

// buildResult собирает конверт из того, что Run вернул.
//
// Ничего не синтезируется: нет доклада — пустой список, а не failure;
// прикладной failure не трогает exit. Доклады идут ВСЕ и в порядке
// поступления — выбор «последнего» принадлежит потребителю (ADR-074 §10).
func buildResult(pluginID string, res Result, runErr error) hostResult {
	out := hostResult{
		Format:   resultFormat,
		Version:  resultVersion,
		PluginID: pluginID,
		Host:     resultHost{OK: runErr == nil},
		// Пустой массив, а не null: «докладов не было» — значение, а не
		// отсутствие поля.
		Reports: []resultReport{},
	}
	if runErr != nil {
		out.Host.Error = runErr.Error()
	}
	if res.ExitKnown {
		out.Exit = resultExit{Known: true, Kind: res.Exit.Kind.String(), Code: res.Exit.Code}
	}

	if len(res.Reports) > maxResultReports {
		out.Host = resultHost{OK: false, Error: errTooManyReports}
		return out
	}
	for _, r := range res.Reports {
		out.Reports = append(out.Reports, resultReport{
			Status:  string(r.Status),
			Payload: base64.StdEncoding.EncodeToString(r.Payload),
		})
	}
	return out
}

// clearResult убирает результат прошлого запуска (ADR-085 §2.4 п. 1).
//
// Отсутствие файла — норма. Любая другая ошибка — отказ: оставить чужой
// результат на месте значило бы дать потребителю прочесть прошлый запуск
// как нынешний.
func clearResult(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("убрать прежний файл результата: %w", err)
	}
	return nil
}

// resultBeforeRename — точка наблюдения для теста атомарности: вызывается,
// когда временный файл записан и синхронизирован, а целевого имени ещё нет.
var resultBeforeRename = func(tmp string) {}

// writeResult записывает конверт атомарно (ADR-085 §2.4 п. 2, п. 4).
//
// Временный файл — в ТОМ ЖЕ каталоге: rename между файловыми системами не
// атомарен. Потребитель видит либо отсутствие файла, либо файл целиком.
func writeResult(path string, r hostResult) (err error) {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("кодировать результат: %w", err)
	}

	dir := filepath.Dir(path)
	// CreateTemp создаёт файл с правами 0600 — ровно тем, что требует §2.4
	// п. 4; Chmod ниже закрепляет это явно, а не полагается на умолчание.
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("создать временный файл результата: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if err = tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("права файла результата: %w", err)
	}
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("записать результат: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync результата: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("закрыть файл результата: %w", err)
	}

	resultBeforeRename(tmpName)

	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("переименовать файл результата: %w", err)
	}

	// Синхронизация каталога закрепляет само переименование. Ошибка здесь
	// результат уже не отменяет — файл на месте и целиком, — поэтому не
	// превращается в отказ.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}
