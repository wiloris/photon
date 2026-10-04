package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// Групповые разговоры ENGRAM, действия 900–931 (ADR-099 с поправкой ADR-103,
// ADR-100, ADR-101, ADR-102). Шаги — photon-quic-bridge/docs/conversation-storage.md
// §6, мост к коду — conversation-implementation-contract.md.

// Коды ошибок ADR-099 §12.
const (
	convErrInvalidRequest     = "invalid_request"
	convErrConversationNF     = "conversation_not_found"
	convErrNoAccess           = "no_access"
	convErrForbidden          = "forbidden"
	convErrInvalidCursor      = "invalid_cursor"
	convErrInvalidLimit       = "invalid_limit"
	convErrIdempotency        = "idempotency_conflict"
	convErrTemporarilyUnavail = errCodeTemporarilyUnavailable
)

// Идентификаторы ошибок Mattermost 9.11, по которым различаются
// определённые исходы вызовов (conversation-storage.md §6.0.4, §6.1).
const (
	mmErrTeamNameExists = "store.sql_team.save_team.existing.app_error"
	mmErrChannelExists  = "store.sql_channel.save_channel.exists.app_error"
	mmErrChannelLimit   = "store.sql_channel.save_channel.limit.app_error"
)

// Имена в Mattermost (conversation-storage.md §6.0.2, §6.0.7; S3).
const (
	carrierTeamPrefix    = "engram-carriers-"
	carrierChannelPrefix = "c-"
	convBotUsername      = "engram-bridge"
	convEventPostType    = "custom_engram_event"
	propEngramEvent      = "engram_event"
	propEngramEventKey   = "engram_event_key"
)

// convMM — вызовы Mattermost, которыми пользуются 900–902 и распределитель.
// plugin.API удовлетворяет интерфейсу; тесты подставляют дублёра.
type convMM interface {
	CreateTeam(team *model.Team) (*model.Team, *model.AppError)
	GetTeamByName(name string) (*model.Team, *model.AppError)
	CreateTeamMember(teamID, userID string) (*model.TeamMember, *model.AppError)
	CreateChannel(channel *model.Channel) (*model.Channel, *model.AppError)
	GetChannelByName(teamID, name string, includeDeleted bool) (*model.Channel, *model.AppError)
	CreatePost(post *model.Post) (*model.Post, *model.AppError)
	GetConfig() *model.Config
}

// convService — исполнитель действий над разговорами поверх таблиц Bridge.
type convService struct {
	db       *sql.DB
	mm       convMM
	registry photonAccountFinder
	users    profileUserAPI
	botID    string // бот engram-bridge (§6.0.7); "" — бота нет
	now      func() int64
	// lockWait — бюджет ожидания advisory-блокировок (§5); pollEvery — шаг опроса.
	lockWait  time.Duration
	pollEvery time.Duration
	logger    func(msg string, keyValuePairs ...any)
}

var (
	errConvLockTimeout   = errors.New("conversation: блокировка не получена в бюджет ожидания")
	errConvUnavailable   = errors.New("conversation: временно недоступно")
	errConvNoCapacity    = errors.New("conversation: MaxChannelsPerTeam не оставляет места для носителя")
	errConvNoBot         = errors.New("conversation: нет бота engram-bridge")
	errConvRetryNextName = errors.New("conversation: имя служебной команды занято")
)

func advisoryKey(name string) int64 {
	h := sha256.Sum256([]byte(name))
	return int64(binary.BigEndian.Uint64(h[:8]))
}

// holdLock — «держащая» транзакция с транзакционной advisory-блокировкой
// (conversation-storage.md §5): в ней не выполняется ничего, кроме взятия
// блокировки; release завершает транзакцию и снимает блокировку. Попытка с
// опросом в пределах бюджета, исход «не дождались» различим.
func (s *convService) holdLock(ctx context.Context, key int64) (release func(), err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(s.lockWait)
	for {
		var got bool
		if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&got); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if got {
			return func() { _ = tx.Rollback() }, nil
		}
		if time.Now().After(deadline) {
			_ = tx.Rollback()
			return nil, errConvLockTimeout
		}
		select {
		case <-ctx.Done():
			_ = tx.Rollback()
			return nil, ctx.Err()
		case <-time.After(s.pollEvery):
		}
	}
}

func convLockKey(conversationID string) int64 { return advisoryKey("photon:conv:" + conversationID) }

var carrierTeamsLockKey = advisoryKey("photon:carrier-teams")

// convPermissions — права ролей v1 (ADR-099 §7.1), в порядке таблицы.
var convPermissions = map[string][]string{
	"owner":  {"conversation.update", "conversation.archive", "members.add", "members.remove", "roles.manage", "owner.transfer", "messages.send", "messages.read"},
	"admin":  {"conversation.update", "members.add", "members.remove", "messages.send", "messages.read"},
	"member": {"messages.send", "messages.read"},
}

// Формы ответа ADR-099 §5.
type ConvMe struct {
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
	JoinedAt    int64    `json:"joined_at"`
}

type ConvSnapshot struct {
	ConversationID      string `json:"conversation_id"`
	Kind                string `json:"kind"`
	Title               string `json:"title"`
	Description         string `json:"description"`
	State               string `json:"state"`
	HistoryPolicy       string `json:"history_policy"`
	ConversationVersion int64  `json:"conversation_version"`
	CreatedAt           int64  `json:"created_at"`
	CreatedBy           string `json:"created_by"`
	MemberCount         int    `json:"member_count"`
	Me                  ConvMe `json:"me"`
}

type ConvMember struct {
	PhotonID string `json:"photon_id"`
	Role     string `json:"role"`
	JoinedAt int64  `json:"joined_at"`
}

type ConvSnapshotResponse struct {
	Conversation ConvSnapshot `json:"conversation"`
	Members      []ConvMember `json:"members"`
}

type convQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// snapshotFor — снимок и полный состав для вызывающего (ADR-099 §5).
// Вызывающий обязан быть в периоде active — это проверено до вызова.
func snapshotFor(ctx context.Context, q convQuerier, conversationID, callerPID string) (ConvSnapshotResponse, error) {
	var r ConvSnapshotResponse
	c := &r.Conversation
	if err := q.QueryRowContext(ctx, `SELECT conversation_id, kind, title, description, state, history_policy,
version, created_at, created_by, member_count FROM engram_conversation WHERE conversation_id = $1`, conversationID).
		Scan(&c.ConversationID, &c.Kind, &c.Title, &c.Description, &c.State, &c.HistoryPolicy,
			&c.ConversationVersion, &c.CreatedAt, &c.CreatedBy, &c.MemberCount); err != nil {
		return r, err
	}
	rows, err := q.QueryContext(ctx, `SELECT photon_id, role, COALESCE(open_at, 0) FROM engram_member_period
WHERE conversation_id = $1 AND state = 'active' ORDER BY photon_id`, conversationID)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	r.Members = []ConvMember{}
	for rows.Next() {
		var m ConvMember
		if err := rows.Scan(&m.PhotonID, &m.Role, &m.JoinedAt); err != nil {
			return r, err
		}
		r.Members = append(r.Members, m)
		if m.PhotonID == callerPID {
			c.Me = ConvMe{Role: m.Role, Permissions: convPermissions[m.Role], JoinedAt: m.JoinedAt}
		}
	}
	return r, rows.Err()
}

// callerAccess — доступ вызывающего к разговору (ADR-099 §11): последний
// период. Разговор в creating не виден никому (S5). Возвращает код отказа
// или "" при периоде active.
func callerAccess(ctx context.Context, q convQuerier, conversationID, callerPID string) (string, error) {
	var state string
	err := q.QueryRowContext(ctx, `SELECT p.state FROM engram_member_period p
JOIN engram_conversation c ON c.conversation_id = p.conversation_id
WHERE p.conversation_id = $1 AND p.photon_id = $2 AND c.state <> 'creating'
ORDER BY p.period_no DESC LIMIT 1`, conversationID, callerPID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return convErrConversationNF, nil
	}
	if err != nil {
		return "", err
	}
	if state != "active" {
		return convErrNoAccess, nil
	}
	return "", nil
}

func convFail(code string) Response { return Response{OK: false, Error: code} }

// convFailFor — ответ на внутреннюю ошибку. Сбой хранилища, блокировки или
// Mattermost — temporarily_unavailable (ADR-099 §12: повторять можно); 900
// и так продолжается повтором с тем же request_id (§6.1).
func (s *convService) convFailFor(op string, err error) Response {
	s.logger("[Photon][Conv] "+op+" failed", "error", err.Error())
	return convFail(convErrTemporarilyUnavail)
}
