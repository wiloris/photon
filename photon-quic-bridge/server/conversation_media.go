package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Вложения разговоров ENGRAM, срез M1 «Файл в разговоре» — ADR-124 (ПРЕДЛОЖЕНО),
// профиль M1 §7; implementation contract §8.
//
// Разделение хранения: байты, длина и SHA-256 — MediaStore (файлы, объект с
// conversation_id и пустым channel_id); права и привязка — engram_media в
// PostgreSQL. Общей транзакции у них нет, и она не изображается: байты пишутся
// раньше строки, строка — раньше ответа; файл без строки недостижим (уборка M2).
//
// Привязка к сообщению — пара (conversation_id, bound_pos). Её живость НЕ
// хранится, а выводится из позиции в engram_position (contract §8.2):
// reserved/published — привязан; deleted_at — сообщение удалено; aborted —
// объект снова свободен. Поэтому 933 и сверка резервов вложения не трогают.

// convMediaKindFile — единственный вид M1 (ADR-124 §7 M1-1).
const convMediaKindFile = "file"

// convMediaMime — mime вложения M1 в любом ответе: содержимое не разбирается,
// встроенного показа нет (contract §8.1).
const convMediaMime = "application/octet-stream"

// convMediaMaxAttachments — предел вложений в сообщении (ADR-124 §2.1).
const convMediaMaxAttachments = 10

// convMediaUnavailable — байтов объекта нет: строка жива, файл утрачен. Код
// из реестра 700–702 (mediaErrorText), новый не вводится (ADR-124 §7 M1-4).
const convMediaUnavailable = "unavailable"

// propPhotonMedia — проекция вложений в props поста-носителя (ADR-048 §3).
// Пишет только Bridge при публикации 930; права от неё не зависят.
const propPhotonMedia = "photon_media"

var uploadKeyRe = regexp.MustCompile(`^[0-9A-Za-z._:-]{1,100}$`)

// ConvAttachment — вложение в ответе 930 и в элементе ленты (contract §8.7–§8.8).
type ConvAttachment struct {
	MediaID string `json:"media_id"`
	Kind    string `json:"kind"`
	Mime    string `json:"mime"`
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// convMediaProjection — элемент props.photon_media.
type convMediaProjection struct {
	MediaID string `json:"media_id"`
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

func (p convMediaProjection) attachment() ConvAttachment {
	return ConvAttachment{MediaID: p.MediaID, Kind: p.Kind, Mime: convMediaMime, Name: p.Name, Size: p.Size, SHA256: p.SHA256}
}

// convMediaRow — строка engram_media.
type convMediaRow struct {
	mediaID, convID, uploader, uploadKey, kind, name, sha256, state string
	size                                                            int64
	boundPos                                                        sql.NullInt64
}

func (m *convMediaRow) attachment() ConvAttachment {
	return ConvAttachment{MediaID: m.mediaID, Kind: m.kind, Mime: convMediaMime, Name: m.name, Size: m.size, SHA256: m.sha256}
}

const convMediaColumns = `media_id, conversation_id, uploader_pid, upload_key, kind, name, sha256, state, size, bound_pos`

func scanConvMedia(row interface{ Scan(...any) error }) (*convMediaRow, error) {
	var m convMediaRow
	err := row.Scan(&m.mediaID, &m.convID, &m.uploader, &m.uploadKey, &m.kind, &m.name, &m.sha256, &m.state, &m.size, &m.boundPos)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func loadConvMedia(ctx context.Context, q convQuerier, mediaID string) (*convMediaRow, error) {
	return scanConvMedia(q.QueryRowContext(ctx, `SELECT `+convMediaColumns+` FROM engram_media WHERE media_id = $1`, mediaID))
}

func loadConvMediaByKey(ctx context.Context, q convQuerier, uploaderPID, key string) (*convMediaRow, error) {
	return scanConvMedia(q.QueryRowContext(ctx, `SELECT `+convMediaColumns+` FROM engram_media
WHERE uploader_pid = $1 AND upload_key = $2 AND state <> 'deleted'`, uploaderPID, key))
}

// validMediaName — имя файла вложения: 1–255 символов UTF-8 без разделителей
// пути и управляющих символов (contract §8.3).
func validMediaName(name string) bool {
	if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 255 {
		return false
	}
	for _, r := range name {
		if r == '/' || r == '\\' || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// ConvMediaUploadPayload — второй кадр 703 (contract §8.3). body_length — в
// payload, как у 700.
type ConvMediaUploadPayload struct {
	ConversationID string `json:"conversation_id"`
	UploadKey      string `json:"upload_key"`
	Kind           string `json:"kind"`
	Name           string `json:"name"`
	SHA256         string `json:"sha256"`
	BodyLength     int64  `json:"body_length"`
}

func (p ConvMediaUploadPayload) valid() bool {
	return p.ConversationID != "" && uploadKeyRe.MatchString(p.UploadKey) && p.Kind == convMediaKindFile &&
		validMediaName(p.Name) && sha256Hex.MatchString(p.SHA256) && p.BodyLength > 0 && p.BodyLength <= MaxMediaBytes
}

// sameUpload — повтор загрузки с тем же ключом описывает тот же объект
// (ADR-124 §7 M1-6).
func (m *convMediaRow) sameUpload(p ConvMediaUploadPayload) bool {
	return m.convID == p.ConversationID && m.name == p.Name && m.size == p.BodyLength && m.sha256 == p.SHA256 && m.kind == p.Kind
}

// convMediaUploadResult — ответ 703.
type convMediaUploadResult struct {
	MediaID string `json:"media_id"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
}

// uploadMedia — 703 (contract §8.3). body — ровно p.BodyLength байт после
// кадров; при любом исходе тело либо принято, либо дочитано и отброшено,
// либо отвергнуто до чтения (тогда стрим закрывается обработчиком).
func (s *convService) uploadMedia(ctx context.Context, svc *MediaService, callerID string, raw json.RawMessage, body io.Reader) Response {
	var p ConvMediaUploadPayload
	if !decodeStrict(raw, &p) || !p.valid() {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("703 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}
	// Допуск — тот же, что у 930: загрузка в разговор, куда писать нельзя,
	// не нужна никому (contract §8.3).
	if _, code, err := sendAdmission(ctx, s.db, p.ConversationID, pid); err != nil {
		return s.convFailFor("703 admission", err)
	} else if code != "" {
		return convFail(code)
	}

	// Повтор с тем же ключом: тот же объект — тело дочитывается и
	// отбрасывается, ответ прежний; другой — конфликт ключа.
	if m, err := loadConvMediaByKey(ctx, s.db, pid, p.UploadKey); err != nil {
		return s.convFailFor("703 key lookup", err)
	} else if m != nil {
		return s.repeatUpload(m, p, body)
	}

	obj, err := svc.Upload(UploadRequest{
		ConversationID: p.ConversationID, UploaderID: callerID, Name: p.Name, Mime: convMediaMime,
		SHA256: p.SHA256, BodyLength: p.BodyLength,
	}, body)
	if err != nil {
		if errors.Is(err, ErrMediaSizeMismatch) || errors.Is(err, ErrMediaIntegrity) || errors.Is(err, ErrMediaInvalidRequest) {
			return convFail(convErrInvalidRequest)
		}
		return s.convFailFor("703 store", err)
	}
	// Строка — только после записанных и проверенных байтов; ответ — только
	// после её фиксации. Сбой здесь оставляет файл без строки: он недостижим
	// для прав и уходит уборке M2 (contract §8.10 п. 2).
	//
	// Свой контекст: приём тела не ограничен бюджетом вызова (100 МБ по
	// медленной сети идут дольше), и истёкший бюджет не должен отнимать у
	// уже принятых байтов их строку.
	ctx, cancel := context.WithTimeout(context.Background(), convCallTimeout)
	defer cancel()
	_, err = s.db.ExecContext(ctx, `INSERT INTO engram_media
  (media_id, conversation_id, uploader_pid, upload_key, kind, name, size, sha256, state, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'stored', $9)`,
		obj.MediaID, p.ConversationID, pid, p.UploadKey, p.Kind, p.Name, obj.Size, obj.SHA256, s.now())
	if err != nil {
		// Одновременная загрузка с тем же ключом: победитель уже записан.
		// Проигравший отвечает по правилам повтора; его файл — без строки.
		m, lerr := loadConvMediaByKey(ctx, s.db, pid, p.UploadKey)
		if lerr == nil && m != nil {
			if m.sameUpload(p) {
				return Response{OK: true, Data: convMediaUploadResult{MediaID: m.mediaID, SHA256: m.sha256, Size: m.size}}
			}
			return convFail(convErrIdempotency)
		}
		return s.convFailFor("703 insert", err)
	}
	return Response{OK: true, Data: convMediaUploadResult{MediaID: obj.MediaID, SHA256: obj.SHA256, Size: obj.Size}}
}

// repeatUpload — ключ уже занят строкой m.
func (s *convService) repeatUpload(m *convMediaRow, p ConvMediaUploadPayload, body io.Reader) Response {
	if !m.sameUpload(p) {
		return convFail(convErrIdempotency)
	}
	if body != nil {
		if _, err := io.CopyN(io.Discard, body, p.BodyLength); err != nil {
			// Тело недодано: ответ всё равно определён ключом, но клиент,
			// оборвавший передачу, его уже не прочтёт — повтор даст тот же.
			s.logger("[Photon][Media] 703 repeat body drain failed", "error", err.Error())
		}
	}
	return Response{OK: true, Data: convMediaUploadResult{MediaID: m.mediaID, SHA256: m.sha256, Size: m.size}}
}

// mediaAccess — доступ вызывающего к объекту как к вложению сообщения
// (contract §8.6). nil без ошибки — отказ forbidden: отсутствие объекта и
// отсутствие права неразличимы (ADR-039).
func (s *convService) mediaAccess(ctx context.Context, mediaID, pid string) (*convMediaRow, error) {
	m, err := loadConvMedia(ctx, s.db, mediaID)
	if err != nil || m == nil {
		return nil, err
	}
	ok, err := mediaVisibleTo(ctx, s.db, m, pid)
	if err != nil || !ok {
		return nil, err
	}
	return m, nil
}

// mediaVisibleTo — п. 1–5 contract §8.6.
func mediaVisibleTo(ctx context.Context, q convQuerier, m *convMediaRow, pid string) (bool, error) {
	if m.state != "stored" || !m.boundPos.Valid {
		return false, nil
	}
	var posState string
	var deleted bool
	var postID sql.NullString
	err := q.QueryRowContext(ctx, `SELECT state, deleted_at IS NOT NULL, post_id FROM engram_position
WHERE conversation_id = $1 AND pos = $2`, m.convID, m.boundPos.Int64).Scan(&posState, &deleted, &postID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if posState != "published" || deleted || !postID.Valid {
		return false, nil
	}
	code, err := callerAccess(ctx, q, m.convID, pid)
	if err != nil || code != "" {
		return false, err
	}
	var policy string
	if err := q.QueryRowContext(ctx, `SELECT history_policy FROM engram_conversation WHERE conversation_id = $1`, m.convID).Scan(&policy); err != nil {
		return false, err
	}
	periods, _, ok, err := convVisibility(ctx, q, m.convID, policy, pid)
	if err != nil || !ok {
		return false, err
	}
	if len(periods) > 0 && !inPeriods(periods, m.boundPos.Int64) {
		return false, nil
	}
	// Пост-носитель удалён в обход Bridge — доступ закрыт сразу, не дожидаясь
	// сверки Н-6 (ADR-124 §7 M1-8).
	var deleteAt int64
	err = q.QueryRowContext(ctx, `SELECT deleteat FROM posts WHERE id = $1`, postID.String).Scan(&deleteAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return deleteAt == 0, nil
}

// mediaUnbound — объект не привязан к живому сообщению: привязки нет или её
// позиция отменена. Только такой объект виден загрузившему через 705 и
// только такой можно привязать (contract §8.5, §8.7).
func mediaUnbound(ctx context.Context, q convQuerier, m *convMediaRow) (bool, error) {
	if !m.boundPos.Valid {
		return true, nil
	}
	var state string
	err := q.QueryRowContext(ctx, `SELECT state FROM engram_position WHERE conversation_id = $1 AND pos = $2`,
		m.convID, m.boundPos.Int64).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return state == "aborted", nil
}

// convMediaInfoResult — ответ 705.
type convMediaInfoResult struct {
	ConvAttachment
	Bound bool `json:"bound"`
}

// mediaIDPayload — 704/705.
type convMediaIDPayload struct {
	MediaID string `json:"media_id"`
}

// mediaInfo — 705 (contract §8.5).
func (s *convService) mediaInfo(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convMediaIDPayload
	if !decodeStrict(raw, &p) || p.MediaID == "" {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("705 caller", err)
	}
	if !found {
		return convFail(convErrForbidden)
	}
	m, err := loadConvMedia(ctx, s.db, p.MediaID)
	if err != nil {
		return s.convFailFor("705 media", err)
	}
	if m == nil || m.state != "stored" {
		return convFail(convErrForbidden)
	}
	ok, err := mediaVisibleTo(ctx, s.db, m, pid)
	if err != nil {
		return s.convFailFor("705 access", err)
	}
	if ok {
		return Response{OK: true, Data: convMediaInfoResult{ConvAttachment: m.attachment(), Bound: true}}
	}
	if m.uploader == pid {
		free, err := mediaUnbound(ctx, s.db, m)
		if err != nil {
			return s.convFailFor("705 binding", err)
		}
		if free {
			return Response{OK: true, Data: convMediaInfoResult{ConvAttachment: m.attachment(), Bound: false}}
		}
	}
	return convFail(convErrForbidden)
}

// mediaForDownload — проверка 704 до кадра ответа (contract §8.4, §8.6).
// code — код отказа, "" — можно отдавать.
func (s *convService) mediaForDownload(ctx context.Context, callerID string, raw json.RawMessage) (*convMediaRow, string) {
	var p convMediaIDPayload
	if !decodeStrict(raw, &p) || p.MediaID == "" {
		return nil, convErrInvalidRequest
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		s.logger("[Photon][Media] 704 caller failed", "error", err.Error())
		return nil, convErrTemporarilyUnavail
	}
	if !found {
		return nil, convErrForbidden
	}
	m, err := s.mediaAccess(ctx, p.MediaID, pid)
	if err != nil {
		s.logger("[Photon][Media] 704 access failed", "error", err.Error())
		return nil, convErrTemporarilyUnavail
	}
	if m == nil {
		return nil, convErrForbidden
	}
	return m, ""
}

// ── 930: вложения сообщения ──────────────────────────────────────────────────

// convAttachmentWire — элемент attachments запроса 930.
type convAttachmentWire struct {
	MediaID string `json:"media_id"`
}

// attachmentIDs — проверенный список media_id запроса 930 (contract §8.7):
// nil — вложений нет; ok=false — форма неверна.
func attachmentIDs(list *[]convAttachmentWire) ([]string, bool) {
	if list == nil {
		return nil, true
	}
	if len(*list) == 0 || len(*list) > convMediaMaxAttachments {
		return nil, false
	}
	seen := make(map[string]bool, len(*list))
	ids := make([]string, 0, len(*list))
	for _, a := range *list {
		if a.MediaID == "" || seen[a.MediaID] {
			return nil, false
		}
		seen[a.MediaID] = true
		ids = append(ids, a.MediaID)
	}
	return ids, true
}

// bindAttachments — привязка вложений к позиции pos в транзакции резерва
// (contract §8.7). Условный UPDATE берёт блокировку строки объекта: две
// привязки одного объекта сериализуются, вторая видит живую чужую и не
// проходит. false — хотя бы один объект не привязан, транзакцию откатить.
func bindAttachments(ctx context.Context, tx *sql.Tx, convID, sender string, pos int64, ids []string) (bool, error) {
	for i, id := range ids {
		res, err := tx.ExecContext(ctx, `UPDATE engram_media m SET bound_pos = $4, bound_ord = $5
WHERE m.media_id = $1 AND m.conversation_id = $2 AND m.uploader_pid = $3
  AND m.state = 'stored' AND m.kind = 'file'
  AND (m.bound_pos IS NULL OR EXISTS (SELECT 1 FROM engram_position p
       WHERE p.conversation_id = m.conversation_id AND p.pos = m.bound_pos AND p.state = 'aborted'))`,
			id, convID, sender, pos, i)
		if err != nil {
			return false, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return false, err
		}
		if n != 1 {
			return false, nil
		}
	}
	return true, nil
}

// loadAttachments — вложения позиции в порядке bound_ord: источник проекции
// props и ответа 930.
func loadAttachments(ctx context.Context, q convQuerier, convID string, pos int64) ([]ConvAttachment, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+convMediaColumns+` FROM engram_media
WHERE conversation_id = $1 AND bound_pos = $2 ORDER BY bound_ord`, convID, pos)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConvAttachment
	for rows.Next() {
		m, err := scanConvMedia(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m.attachment())
	}
	return out, rows.Err()
}

func projectionOf(atts []ConvAttachment) []convMediaProjection {
	out := make([]convMediaProjection, len(atts))
	for i, a := range atts {
		out[i] = convMediaProjection{MediaID: a.MediaID, Kind: a.Kind, Name: a.Name, Size: a.Size, SHA256: a.SHA256}
	}
	return out
}

// ── чтение: признак клиента и заглушка ───────────────────────────────────────

// convFeatureAttachments — признак клиента, понимающего вложения (contract §8.8).
const convFeatureAttachments = "attachments"

func hasFeature(features []string, f string) bool {
	for _, x := range features {
		if x == f {
			return true
		}
	}
	return false
}

// parseMediaProjection — props.photon_media строки ленты. Испорченная проекция
// не роняет ленту: элемент выдаётся без вложений (права от неё не зависят).
func parseMediaProjection(raw string) []ConvAttachment {
	if raw == "" || raw == "null" {
		return nil
	}
	var list []convMediaProjection
	if json.Unmarshal([]byte(raw), &list) != nil {
		return nil
	}
	out := make([]ConvAttachment, 0, len(list))
	for _, p := range list {
		if p.MediaID != "" {
			out = append(out, p.attachment())
		}
	}
	return out
}

// attachmentPlaceholder — текст для клиента без признака (contract §8.8).
func attachmentPlaceholder(text string, atts []ConvAttachment) string {
	var stub string
	if len(atts) == 1 {
		stub = fmt.Sprintf("[Вложение: %s]", atts[0].Name)
	} else {
		stub = fmt.Sprintf("[Вложения: %d]", len(atts))
	}
	if strings.TrimSpace(text) == "" {
		return stub
	}
	return text + "\n" + stub
}
