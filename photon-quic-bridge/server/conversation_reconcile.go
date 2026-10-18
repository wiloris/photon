package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
)

// Сверка групповых разговоров — conversation-storage.md §8, implementation
// contract §3.6, §4.5; ADR-101 §5.10, §5.11.
//
// Методы convService ниже — сама сверка; запускает их convReconciler
// (conversation_reconciler.go) — фоновый проход с настраиваемыми периодом и
// порогом «брошенного» резерва (contract §4.5, §7: Q2 — эксплуатационный
// параметр). Безопасность от момента отмены не зависит (ADR-101 §5.11):
// отменить можно в любой момент — живой владелец не сможет сообщить успех,
// барьер 931 стоит за открытым резервом до его закрытия, а пост, записанный
// после отмены, скрыт надгробием (contract §4.2).

// convInflightMaxMs — сколько публикация, идущая в этом процессе, защищает
// свою позицию от сверки. Дольше вызов не длится в норме (convCallTimeout);
// предел нужен, чтобы зависший RPC не держал барьер ленты вечно — после него
// резерв закрывается по общему правилу, а поздний пост скроет надгробие.
const convInflightMaxMs = int64(30 * 60 * 1000)

// convInflight — позиции 930, публикация которых идёт в этом процессе
// (от CreatePost до записи итога). Сверка их не закрывает: «поста нет» в
// этот момент — не установленное отсутствие, а незавершённый вызов.
// Другой процесс (узел кластера, процесс прежней активации) этого не видит —
// для него действует порог давности.
type convInflight struct {
	mu sync.Mutex
	m  map[string]int64 // conversation_id + "/" + pos → начало, мс
}

func inflightKey(convID string, pos int64) string {
	return convID + "/" + strconv.FormatInt(pos, 10)
}

// begin отмечает публикацию; возвращает снятие отметки.
func (f *convInflight) begin(convID string, pos int64, now int64) func() {
	if f == nil {
		return func() {}
	}
	k := inflightKey(convID, pos)
	f.mu.Lock()
	if f.m == nil {
		f.m = map[string]int64{}
	}
	f.m[k] = now
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		delete(f.m, k)
		f.mu.Unlock()
	}
}

// positions — позиции разговора, публикация которых началась не раньше
// notBefore: их сверка не трогает.
func (f *convInflight) positions(convID string, notBefore int64) []int64 {
	out := []int64{}
	if f == nil {
		return out
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := convID + "/"
	for k, at := range f.m {
		if at < notBefore || len(k) <= len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		if pos, err := strconv.ParseInt(k[len(prefix):], 10, 64); err == nil {
			out = append(out, pos)
		}
	}
	return out
}

// convReconcileResult — что сделал проход по одному разговору.
type convReconcileResult struct {
	EventsWritten int // события pending → written (E2–E4)
	Published     int // брошенные резервы сообщений → published
	Aborted       int // брошенные резервы сообщений → aborted (надгробие)
}

// reconcileConversation — один разговор: незаписанные события под X, затем
// брошенные резервы сообщений с reserved_at < abandonedBefore (мс часов
// Bridge). Шаги независимы: закрытию резервов X не нужен, и занятый X
// (событие не дописано в этот раз) не оставляет брошенный резерв держать
// барьер ленты до следующего прохода.
func (s *convService) reconcileConversation(ctx context.Context, convID string, abandonedBefore int64) (convReconcileResult, error) {
	var res convReconcileResult
	n, errEvents := s.reconcileEvents(ctx, convID)
	res.EventsWritten = n
	var errMsgs error
	res.Published, res.Aborted, errMsgs = s.closeAbandonedMessages(ctx, convID, abandonedBefore, "")
	return res, errors.Join(errEvents, errMsgs)
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
	// Одна транзакция: видимость сообщения (published) и его строка в индексе
	// поиска (ADR-110) появляются вместе.
	busy, err := json.Marshal(s.inflight.positions(convID, s.now()-convInflightMaxMs))
	if err != nil {
		return 0, 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
WITH open AS (
  SELECT pp.pos, pp.item_key, c.carrier_channel_id AS ch
  FROM engram_position pp JOIN engram_conversation c ON c.conversation_id = pp.conversation_id
  WHERE pp.conversation_id = $1 AND pp.item_kind = 'message' AND pp.state = 'reserved'
    AND pp.reserved_at < $2 AND ($3 = '' OR pp.item_key = $3)
    AND NOT ($4::jsonb @> to_jsonb(pp.pos))),
found AS (
  SELECT o.pos, (SELECT p.id FROM posts p WHERE p.channelid = o.ch AND p.deleteat = 0 AND p.createat = o.pos
                 AND p.props->>'engram_client_msg_id' = o.item_key ORDER BY p.id LIMIT 1) AS id
  FROM open o),
closed AS (
  UPDATE engram_position pp SET
    state = CASE WHEN f.id IS NULL THEN 'aborted' ELSE 'published' END, post_id = f.id
  FROM found f
  WHERE pp.conversation_id = $1 AND pp.pos = f.pos AND pp.state = 'reserved'
  RETURNING pp.pos, pp.state, pp.post_id),
activity AS (
  UPDATE engram_conversation SET last_activity_at = GREATEST(last_activity_at, (SELECT max(pos) FROM closed WHERE state = 'published'))
  WHERE conversation_id = $1 AND EXISTS (SELECT 1 FROM closed WHERE state = 'published'))
SELECT pos, state, post_id FROM closed`, convID, abandonedBefore, itemKey, string(busy))
	if err != nil {
		return 0, 0, err
	}
	type pub struct {
		pos int64
		id  string
	}
	var pubs []pub
	for rows.Next() {
		var pos int64
		var st string
		var postID sql.NullString
		if err := rows.Scan(&pos, &st, &postID); err != nil {
			rows.Close()
			return 0, 0, err
		}
		if st == "published" {
			published++
			pubs = append(pubs, pub{pos, postID.String})
		} else {
			aborted++
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	authors := map[string]bool{}
	for _, pb := range pubs {
		var msg, author string
		if err := tx.QueryRowContext(ctx, `SELECT message, userid FROM posts WHERE id = $1`, pb.id).Scan(&msg, &author); err != nil {
			return 0, 0, err
		}
		if err := upsertItemText(ctx, tx, convID, pb.id, pb.pos, msg); err != nil {
			return 0, 0, err
		}
		authors[author] = true
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	// Сообщение, опубликованное сверкой, — такое же новое для участников, как
	// опубликованное 930 (ADR-123 §2.2). Автор не найден — события нет.
	for mmID := range authors {
		if pid, _, found, err := ownPhotonID(s.registry, s.users, mmID); err == nil && found {
			s.pushMessage(convID, pid)
		}
	}
	return published, aborted, nil
}

// convReconcilePass — итог прохода по разговорам.
type convReconcilePass struct {
	Selected int    // разговоров в выборке
	Done     int    // из них сверено без ошибки
	Last     string // последний conversation_id выборки ("" — выборка пуста)
	convReconcileResult
}

// reconcileOpen — проход по разговорам с незаписанными событиями или
// брошенными резервами (индексы engram_event_pending и
// engram_position_open_age), не больше limit разговоров. Возвращает число
// разговоров, по которым проход выполнен.
func (s *convService) reconcileOpen(ctx context.Context, abandonedBefore int64, limit int) (int, error) {
	p, err := s.reconcileOpenAfter(ctx, abandonedBefore, limit, "")
	return p.Done, err
}

// reconcileOpenAfter — то же, начиная с разговоров, чей id больше after:
// фоновый проход идёт по кругу, и разговор, сверка которого раз за разом
// не удаётся, не загораживает остальные. Ошибка одного разговора не
// прерывает проход — они собираются в итоговую ошибку.
func (s *convService) reconcileOpenAfter(ctx context.Context, abandonedBefore int64, limit int, after string) (convReconcilePass, error) {
	var pass convReconcilePass
	rows, err := s.db.QueryContext(ctx, `
SELECT conversation_id FROM (
  SELECT conversation_id FROM engram_event WHERE state = 'pending'
  UNION
  SELECT conversation_id FROM engram_position WHERE state = 'reserved' AND item_kind = 'message' AND reserved_at < $1
) t WHERE conversation_id > $3 ORDER BY conversation_id LIMIT $2`, abandonedBefore, limit, after)
	if err != nil {
		return pass, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return pass, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return pass, err
	}
	pass.Selected = len(ids)
	if len(ids) > 0 {
		pass.Last = ids[len(ids)-1]
	}
	var errs []error
	for _, id := range ids {
		res, err := s.reconcileConversation(ctx, id, abandonedBefore)
		pass.EventsWritten += res.EventsWritten
		pass.Published += res.Published
		pass.Aborted += res.Aborted
		if err != nil {
			errs = append(errs, err)
			continue
		}
		pass.Done++
	}
	return pass, errors.Join(errs...)
}
