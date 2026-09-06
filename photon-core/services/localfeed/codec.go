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

func decode(v []byte) (Record, error) {
	var sr storedRecord
	if err := json.Unmarshal(v, &sr); err != nil {
		return Record{}, fmt.Errorf("localfeed: decode: %w", err)
	}
	return Record{ID: sr.ID, OrderKey: sr.OrderKey, Version: sr.Version,
		Body: append([]byte(nil), sr.Body...)}, nil
}
