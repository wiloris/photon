package localfeed

import (
	"encoding/json"
	"fmt"
)

// Кодек записи. JSON по тем же соображениям, что ADR-034: отлаживаемость и
// отсутствие второго формата на границе. Тело вкладывается СЫРЫМ — Core его
// не разбирает и не пересобирает.
type storedRecord struct {
	ID       string          `json:"id"`
	OrderKey int64           `json:"order_key"`
	Version  int64           `json:"version"`
	Body     json.RawMessage `json:"body"`
}

func encode(r Record) ([]byte, error) {
	body := r.Body
	if len(body) == 0 {
		body = []byte("null")
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("localfeed: record %q body is not valid JSON", r.ID)
	}
	return json.Marshal(storedRecord{ID: r.ID, OrderKey: r.OrderKey, Version: r.Version, Body: body})
}

// storedVersion — ТОТ ЖЕ формат, прочитанный до одного поля.
//
// Формат хранения не менялся: это тот же документ, у которого разбирается
// только `version`. Лишние поля encoding/json пропускает, не выделяя под них
// памяти, — в том числе `body`, который при полном decode копируется ДВАЖДЫ
// (сначала в json.RawMessage, потом в Record.Body).
type storedVersion struct {
	Version int64 `json:"version"`
}

// decodeVersion — версия хранимой записи без разбора тела.
//
// Заведён под единственного потребителя: согласование версий в Ingest читает
// существующую запись целиком, а использует из неё РОВНО `Version`. Измерено
// (`ingest_get_cost_test.go`, baseline 2026-09-11): полный decode стоил
// 13.2 аллокации и ≈587 байт на запись блока — 49 % всех аллокаций заливки.
//
// Синтаксическую строгость это НЕ ослабляет: encoding/json разбирает документ
// целиком и на испорченном JSON ошибётся так же, как decode, — пропуск
// касается только выделения памяти под неиспользуемые поля. Отсутствующий
// `version` даёт 0 — ровно как у decode, где сработал бы нулевой элемент
// структуры.
func decodeVersion(v []byte) (int64, error) {
	var sv storedVersion
	if err := json.Unmarshal(v, &sv); err != nil {
		return 0, fmt.Errorf("localfeed: decode version: %w", err)
	}
	return sv.Version, nil
}

func decode(v []byte) (Record, error) {
	var sr storedRecord
	if err := json.Unmarshal(v, &sr); err != nil {
		return Record{}, fmt.Errorf("localfeed: decode: %w", err)
	}
	return Record{ID: sr.ID, OrderKey: sr.OrderKey, Version: sr.Version,
		Body: append([]byte(nil), sr.Body...)}, nil
}
