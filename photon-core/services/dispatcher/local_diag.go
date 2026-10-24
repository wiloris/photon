package dispatcher

import (
	"fmt"

	photonpb "github.com/wiloris/photon/photon-core/protobuf"
	"github.com/wiloris/photon/photon-core/services/diag"
	"github.com/wiloris/photon/photon-core/services/session"
)

// Встроенная диагностика наружу (ADR-130 §2.7): 819 — снимок, 820 — журнал
// по курсору. Контракт — docs/local-actions.md, раздел 819/820.
//
// Обе исполняются здесь, без хранилища и без Bridge, и в журнал/счётчики
// вызовов не попадают: чтение диагностики само себя не наблюдает (§2.8) —
// SendAction обходит для них учёт (isDiagAction).
const (
	actionLocalDiagSnapshot = int32(photonpb.Action_ACTION_LOCAL_DIAG_SNAPSHOT)
	actionLocalDiagEvents   = int32(photonpb.Action_ACTION_LOCAL_DIAG_EVENTS)
)

func isDiagAction(action int32) bool {
	return action == actionLocalDiagSnapshot || action == actionLocalDiagEvents
}

// diagAction — 819/820.
func (d *Dispatcher) diagAction(action int32, payload []byte) ([]byte, error) {
	if action == actionLocalDiagSnapshot {
		var req struct{}
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		// Выключенная диагностика — снимок с config.enabled = false, а не
		// отказ: клиент различает «выключено» по снимку.
		return okEnvelope(d.diag.Snapshot())
	}

	var req struct {
		AfterSeq uint64 `json:"after_seq"`
		Limit    int    `json:"limit"`
	}
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.Limit < 1 || req.Limit > diag.MaxReadLimit {
		return nil, fmt.Errorf("%w: %w: limit %d is outside 1…%d", session.ErrInvalidPayload, diag.ErrInvalidLimit, req.Limit, diag.MaxReadLimit)
	}
	if d.diag == nil {
		// Журнала нет — пустая страница, курсор не проверяется.
		return okEnvelope(diagEventsResponse{Events: []map[string]any{}, NextSeq: req.AfterSeq})
	}
	p, err := d.diag.Read(req.AfterSeq, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", session.ErrInvalidPayload, err)
	}
	resp := diagEventsResponse{InstanceID: p.InstanceID, InstanceStartWallMS: p.InstanceStartWallMS,
		Events: make([]map[string]any, len(p.Events)), NextSeq: p.NextSeq, Dropped: p.Dropped}
	for i := range p.Events {
		resp.Events[i] = p.Events[i].Wire()
	}
	return okEnvelope(resp)
}

// diagEventsResponse — форма ответа 820 (ADR-130 §2.4; §2.3 с поправкой о
// времени: instance_start_wall_ms — календарное время старта инстанса).
type diagEventsResponse struct {
	InstanceID          string           `json:"instance_id"`
	InstanceStartWallMS int64            `json:"instance_start_wall_ms"`
	Events              []map[string]any `json:"events"`
	NextSeq             uint64           `json:"next_seq"`
	Dropped             uint64           `json:"dropped"`
}
