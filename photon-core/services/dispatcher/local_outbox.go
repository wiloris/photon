package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wiloris/photon/photon-core/services/localfeed"
	"github.com/wiloris/photon/photon-core/services/outbox"
	"github.com/wiloris/photon/photon-core/services/session"
)

/*
 * Действия 804–812: каталог и курсор ленты (ADR-095 п. 2.9), исходящие
 * операции (ADR-093). Контракт payload и ответов — docs/local-actions.md;
 * здесь он исполняется и ничего к нему не добавляет.
 *
 * Отказ локального действия — отказ вызова, а не {ok:false} (ADR-065 п. 8.2):
 * вердикта у локального хранилища нет. Два штатных исхода аренды — пустая
 * очередь и занятая голова — НЕ отказы: они выходят результатом 807.
 */

// ── 804, 805 ────────────────────────────────────────────────────────────

func feedCatalog(ctx context.Context, feed *localfeed.Store) ([]byte, error) {
	cat, err := feed.Catalog(ctx)
	if err != nil {
		return nil, wrapFeedWriteError(err)
	}
	type entry struct {
		ChannelID string       `json:"channel_id"`
		Newest    wirePosition `json:"newest"`
	}
	out := struct {
		Channels []entry `json:"channels"`
	}{Channels: make([]entry, 0, len(cat))}
	for _, c := range cat {
		out.Channels = append(out.Channels, entry{
			ChannelID: c.ChannelID,
			Newest:    wirePosition{OrderKey: c.Newest.OrderKey, ID: c.Newest.ID},
		})
	}
	return okEnvelope(out)
}

type feedCursorRequest struct {
	ChannelID string `json:"channel_id"`
}

func feedCursor(ctx context.Context, feed *localfeed.Store, payload []byte) ([]byte, error) {
	var req feedCursorRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	cur, found, err := feed.Cursor(ctx, req.ChannelID)
	if err != nil {
		return nil, wrapFeedWriteError(err)
	}
	out := struct {
		Found  bool    `json:"found"`
		Cursor *string `json:"cursor,omitempty"`
	}{Found: found}
	if found {
		s := string(cur)
		out.Cursor = &s
	}
	return okEnvelope(out)
}

// ── 806–812 ─────────────────────────────────────────────────────────────

type outboxEnqueueRequest struct {
	Queue string          `json:"queue"`
	Body  json.RawMessage `json:"body"`
}

type outboxLeaseRequest struct {
	Queue   string `json:"queue"`
	LeaseMS int64  `json:"lease_ms"`
}

type outboxOpRequest struct {
	OpID string `json:"op_id"`
}

type outboxFailRequest struct {
	OpID      string `json:"op_id"`
	Kind      string `json:"kind"`
	Reason    string `json:"reason"`
	RetryAtMS *int64 `json:"retry_at_ms"`
}

type outboxListRequest struct {
	Queue string `json:"queue"`
}

// opResult — ответ переходов 808–810: операция и изменилась ли она.
type opResult struct {
	Op      outbox.Op `json:"op"`
	Changed bool      `json:"changed"`
}

// leaseResult — ответ 807. Op отсутствует при reason:"empty".
type leaseResult struct {
	Leased bool       `json:"leased"`
	Reason string     `json:"reason,omitempty"`
	Op     *outbox.Op `json:"op,omitempty"`
}

func outboxAction(ctx context.Context, box *outbox.Store, action int32, payload []byte) ([]byte, error) {
	switch action {
	case actionOutboxEnqueue:
		var req outboxEnqueueRequest
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		op, err := box.Enqueue(ctx, req.Queue, req.Body)
		if err != nil {
			return nil, wrapOutboxError(err)
		}
		return okEnvelope(struct {
			Op outbox.Op `json:"op"`
		}{op})

	case actionOutboxLease:
		var req outboxLeaseRequest
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		op, err := box.Lease(ctx, req.Queue, time.Duration(req.LeaseMS)*time.Millisecond)
		switch {
		case err == nil:
			return okEnvelope(leaseResult{Leased: true, Op: &op})
		case errors.Is(err, outbox.ErrQueueEmpty):
			return okEnvelope(leaseResult{Leased: false, Reason: "empty"})
		case errors.Is(err, outbox.ErrHeadLeased):
			return okEnvelope(leaseResult{Leased: false, Reason: "head_leased", Op: &op})
		}
		return nil, wrapOutboxError(err)

	case actionOutboxComplete:
		var req outboxOpRequest
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		return opTransition(box.Complete(ctx, req.OpID))

	case actionOutboxFail:
		var req outboxFailRequest
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		switch req.Kind {
		case "transient":
			var retryAt time.Time
			if req.RetryAtMS != nil {
				retryAt = time.UnixMilli(*req.RetryAtMS)
			}
			return opTransition(box.FailTransient(ctx, req.OpID, req.Reason, retryAt))
		case "permanent":
			if req.RetryAtMS != nil {
				return nil, fmt.Errorf("%w: retry_at_ms is allowed only for kind \"transient\"", session.ErrInvalidPayload)
			}
			return opTransition(box.FailPermanent(ctx, req.OpID, req.Reason))
		}
		// Умолчания нет: опечатка в kind не получает поведение одного из
		// известных (тот же принцип, что parseDirection, ADR-065 п. 3.7).
		return nil, fmt.Errorf("%w: unknown kind %q", session.ErrInvalidPayload, req.Kind)

	case actionOutboxRetry:
		var req outboxOpRequest
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		return opTransition(box.Retry(ctx, req.OpID))

	case actionOutboxCancel:
		var req outboxOpRequest
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		if err := box.Cancel(ctx, req.OpID); err != nil {
			return nil, wrapOutboxError(err)
		}
		return okEnvelope(struct {
			Cancelled bool `json:"cancelled"`
		}{true})

	case actionOutboxList:
		var req outboxListRequest
		if err := decodeRequest(payload, &req); err != nil {
			return nil, err
		}
		ops, err := box.List(ctx, req.Queue)
		if err != nil {
			return nil, wrapOutboxError(err)
		}
		if ops == nil {
			ops = []outbox.Op{}
		}
		return okEnvelope(struct {
			Ops []outbox.Op `json:"ops"`
		}{ops})
	}
	return nil, fmt.Errorf("%w: %d", session.ErrUnknownAction, action)
}

func opTransition(op outbox.Op, changed bool, err error) ([]byte, error) {
	if err != nil {
		return nil, wrapOutboxError(err)
	}
	return okEnvelope(opResult{Op: op, Changed: changed})
}

// outboxArgErrors — ошибки очереди, являющиеся ошибкой АРГУМЕНТА. Критерий
// тот же, что у feedWriteArgErrors: есть ли что исправлять вызывающему.
// Полнота проверяется машинно (TestOutbox_EverySentinelIsClassified).
var outboxArgErrors = []error{
	outbox.ErrInvalidQueue,
	outbox.ErrInvalidBody,
	outbox.ErrInvalidLease,
	outbox.ErrUnknownOp,
	outbox.ErrStateConflict,
}

// outboxResultNotErrors — исходы, которые наружу выходят РЕЗУЛЬТАТОМ 807, а
// не ошибкой вызова. Список существует, чтобы их появление в этой роли было
// решением, а не умолчанием.
var outboxResultNotErrors = []error{
	outbox.ErrQueueEmpty,
	outbox.ErrHeadLeased,
}

// wrapOutboxError — тот же двойной %w, что wrapFeedWriteError: класс из
// classTable плюс исходный sentinel для errors.Is.
func wrapOutboxError(err error) error {
	for _, sentinel := range outboxArgErrors {
		if errors.Is(err, sentinel) {
			return fmt.Errorf("%w: %w", session.ErrInvalidPayload, err)
		}
	}
	return fmt.Errorf("%w: %w", ErrStorageUnavailable, err)
}

func okEnvelope(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode local result: %w", err)
	}
	return envelope(session.Response{OK: true, Data: data})
}
