package main

import (
	"context"
	"errors"
)

// Сверка групповых разговоров — conversation-storage.md §8, implementation
// contract §3.6, §4.5; ADR-101 §5.10, §5.11.
//
// Реализовано как вызываемые методы convService. КТО и КОГДА их запускает и
// каков порог «брошенного» резерва — открыто (Q2, ADR-101 §5.11): ни
// фонового задания, ни хука, ни настройки здесь нет, порог — параметр.
// Безопасность от момента отмены не зависит (ADR-101 §5.11): отменить
// можно в любой момент — живой владелец не сможет сообщить успех,
// барьер 931 стоит за открытым резервом до его закрытия.

// convReconcileResult — что сделал проход по одному разговору.
type convReconcileResult struct {
	EventsWritten int // события pending → written (E2–E4)
	Published     int // брошенные резервы сообщений → published
	Aborted       int // брошенные резервы сообщений → aborted (надгробие)
}

// reconcileConversation — один разговор: незаписанные события под X, затем
// брошенные резервы сообщений с reserved_at < abandonedBefore (мс часов
// Bridge).
func (s *convService) reconcileConversation(ctx context.Context, convID string, abandonedBefore int64) (convReconcileResult, error) {
	var res convReconcileResult
	n, err := s.reconcileEvents(ctx, convID)
	if err != nil {
		return res, err
	}
	res.EventsWritten = n
	res.Published, res.Aborted, err = s.closeAbandonedMessages(ctx, convID, abandonedBefore, "")
	return res, err
}

// reconcileEvents — незаписанные события разговора дописываются под X с
// сохранёнными позициями (E2–E4, спецификация §8, строки «событие pending» и
// «событие записано, written не отмечено»). Писать события без X запрещено
// (contract §5.1): X берётся с бюджетом, не дождались — ошибка.
func (s *convService) reconcileEvents(ctx context.Context, convID string) (int, error) {
	release, err := s.holdLock(ctx, convLockKey(convID))
	if err != nil {
		return 0, err
	}
	defer release()
	pending, err := s.pendingEvents(ctx, convID)
	if err != nil || pending == 0 {
		return 0, err
	}
	if err := s.flushEvents(ctx, convID, nil); err != nil {
		return 0, err
	}
	left, err := s.pendingEvents(ctx, convID)
	return pending - left, err
}

func (s *convService) pendingEvents(ctx context.Context, convID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND state = 'pending'`, convID).Scan(&n)
	return n, err
}

// closeAbandonedMessages — contract §3.6, ADR-101 §5.10: ОДИН условный
// оператор над резервами сообщений в состоянии reserved и reserved_at <
// abandonedBefore. Пост с CreateAt = pos и ключом найден → published (с
// post_id и last_activity_at); иначе → aborted (надгробие). Условие
// state = 'reserved' проверяется при записи: резерв, который владелец
// успел опубликовать, не трогается. itemKey != "" ограничивает проход одним
// ключом (точечная сверка и тесты).
func (s *convService) closeAbandonedMessages(ctx context.Context, convID string, abandonedBefore int64, itemKey string) (published, aborted int, err error) {
	rows, err := s.db.QueryContext(ctx, `
WITH open AS (
  SELECT pp.pos, pp.item_key, c.carrier_channel_id AS ch
  FROM engram_position pp JOIN engram_conversation c ON c.conversation_id = pp.conversation_id
  WHERE pp.conversation_id = $1 AND pp.item_kind = 'message' AND pp.state = 'reserved'
    AND pp.reserved_at < $2 AND ($3 = '' OR pp.item_key = $3)),
found AS (
  SELECT o.pos, (SELECT p.id FROM posts p WHERE p.channelid = o.ch AND p.deleteat = 0 AND p.createat = o.pos
                 AND p.props->>'engram_client_msg_id' = o.item_key ORDER BY p.id LIMIT 1) AS id
  FROM open o),
closed AS (
  UPDATE engram_position pp SET
    state = CASE WHEN f.id IS NULL THEN 'aborted' ELSE 'published' END, post_id = f.id
  FROM found f
  WHERE pp.conversation_id = $1 AND pp.pos = f.pos AND pp.state = 'reserved'
  RETURNING pp.pos, pp.state),
activity AS (
  UPDATE engram_conversation SET last_activity_at = GREATEST(last_activity_at, (SELECT max(pos) FROM closed WHERE state = 'published'))
  WHERE conversation_id = $1 AND EXISTS (SELECT 1 FROM closed WHERE state = 'published'))
SELECT state, count(*) FROM closed GROUP BY state`, convID, abandonedBefore, itemKey)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return 0, 0, err
		}
		if st == "published" {
			published = n
		} else {
			aborted = n
		}
	}
	return published, aborted, rows.Err()
}

// reconcileOpen — проход по разговорам с незаписанными событиями или
// брошенными резервами (индексы engram_event_pending и
// engram_position_open_age), не больше limit разговоров. Возвращает число
// разговоров, по которым проход выполнен. Вызывающий — оператор или
// будущий планировщик (Q2).
func (s *convService) reconcileOpen(ctx context.Context, abandonedBefore int64, limit int) (int, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT conversation_id FROM (
  SELECT conversation_id FROM engram_event WHERE state = 'pending'
  UNION
  SELECT conversation_id FROM engram_position WHERE state = 'reserved' AND item_kind = 'message' AND reserved_at < $1
) t ORDER BY conversation_id LIMIT $2`, abandonedBefore, limit)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var errs []error
	done := 0
	for _, id := range ids {
		if _, err := s.reconcileConversation(ctx, id, abandonedBefore); err != nil {
			errs = append(errs, err)
			continue
		}
		done++
	}
	return done, errors.Join(errs...)
}
