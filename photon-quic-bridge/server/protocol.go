package main

import (
	photonpb "github.com/wiloris/photon-core/protobuf"
)

// ─────────────────────────────────────────────
//  Wire protocol (every QUIC stream follows this):
//
//  Frame 1 — auth
//    { "session_token": "xxx" }
//
//  Frame 2 — header
//    { "action": 101, "resource": "PostResource" }
//
//  Frame 3 — payload
//    { ... resource-specific JSON ... }
//
//  Response
//    { "ok": true,  "data": { ... } }
//    { "ok": false, "error": "reason" }
//
//  Push stream (server → client, UniStream):
//    { "event": "posted", "user_id": "...", "data": { ... } }
// ─────────────────────────────────────────────

// Action-коды. Не объявлены заново — алиасы на канонический источник
// в photon-core/protobuf (photon.proto, enum Action). Явный int(...)
// нужен, потому что photonpb.Action — отдельный тип (int32), а не
// голый int: handler'ы этого репозитория сравнивают action int, и
// без конверсии switch на разнотипных константах не скомпилируется.
//
// Раньше это были package-level const с теми же значениями,
// объявленными здесь напрямую — то, от чего явно предостерегает
// ARCHITECTURE.md, раздел 7.6 (Photon Protocol — общий контракт,
// не деталь Bridge): Core и Bridge — равноправные потребители одного
// протокола, ни один не должен объявлять его самостоятельно.
const (
	// ── PostResource (100-199) ────────────────────────────────────────────────
	ActionCreatePost        = int(photonpb.Action_ACTION_CREATE_POST)
	ActionGetFeed           = int(photonpb.Action_ACTION_GET_FEED)
	ActionDeletePost        = int(photonpb.Action_ACTION_DELETE_POST)
	ActionUpdatePost        = int(photonpb.Action_ACTION_UPDATE_POST)
	ActionGetThread         = int(photonpb.Action_ACTION_GET_THREAD)
	ActionReplyToPost       = int(photonpb.Action_ACTION_REPLY_TO_POST)
	ActionPinPost           = int(photonpb.Action_ACTION_PIN_POST)
	ActionUnpinPost         = int(photonpb.Action_ACTION_UNPIN_POST)
	ActionGetPinnedPosts    = int(photonpb.Action_ACTION_GET_PINNED_POSTS)
	ActionSearchPosts       = int(photonpb.Action_ACTION_SEARCH_POSTS)
	ActionAddReaction       = int(photonpb.Action_ACTION_ADD_REACTION)
	ActionRemoveReaction    = int(photonpb.Action_ACTION_REMOVE_REACTION)
	ActionGetReactions      = int(photonpb.Action_ACTION_GET_REACTIONS)
	ActionToggleLike        = int(photonpb.Action_ACTION_TOGGLE_LIKE)
	ActionGetPostSocialInfo = int(photonpb.Action_ACTION_GET_POST_SOCIAL_INFO)

	// ── UserResource (200-299) ────────────────────────────────────────────────
	ActionGetMe          = int(photonpb.Action_ACTION_GET_ME)
	ActionGetUser        = int(photonpb.Action_ACTION_GET_USER)
	ActionGetUsers       = int(photonpb.Action_ACTION_GET_USERS)
	ActionGetUserStatus  = int(photonpb.Action_ACTION_GET_USER_STATUS)
	ActionGetUserProfile = int(photonpb.Action_ACTION_GET_USER_PROFILE)
	ActionSearchUsers    = int(photonpb.Action_ACTION_SEARCH_USERS)
	ActionGetUsersByIds  = int(photonpb.Action_ACTION_GET_USERS_BY_IDS)
	ActionPatchMe        = int(photonpb.Action_ACTION_PATCH_ME)
	ActionSetStatus      = int(photonpb.Action_ACTION_SET_STATUS)
	ActionUploadAvatar   = int(photonpb.Action_ACTION_UPLOAD_AVATAR)
	ActionGetUserAvatar  = int(photonpb.Action_ACTION_GET_USER_AVATAR)

	// Профиль и поиск по PhotonID (ADR-081).
	ActionGetMyPhotonProfile = int(photonpb.Action_ACTION_GET_MY_PHOTON_PROFILE)
	ActionFindByPhotonID     = int(photonpb.Action_ACTION_FIND_BY_PHOTON_ID)
	// Своё отображаемое имя (ADR-091).
	ActionSetMyDisplayName = int(photonpb.Action_ACTION_SET_MY_DISPLAY_NAME)

	// Личные сообщения по PhotonID (ADR-082).
	ActionSendDirectMessage  = int(photonpb.Action_ACTION_SEND_DIRECT_MESSAGE)
	ActionListDirectMessages = int(photonpb.Action_ACTION_LIST_DIRECT_MESSAGES)
	// Личные сообщения v2 (ADR-092 ред. 3): идемпотентная отправка и
	// синхронизация с непрозрачным курсором.
	ActionSendDirectMessageV2  = int(photonpb.Action_ACTION_SEND_DIRECT_MESSAGE_V2)
	ActionListDirectMessagesV2 = int(photonpb.Action_ACTION_LIST_DIRECT_MESSAGES_V2)
	// ADR-105: DM «назад» (M1 — только код и заглушка).
	ActionListDirectMessagesBefore = int(photonpb.Action_ACTION_LIST_DIRECT_MESSAGES_BEFORE)

	// Входящие: список разговоров 1:1 (ADR-084).
	ActionListDirectConversations = int(photonpb.Action_ACTION_LIST_DIRECT_CONVERSATIONS)

	// ── ChannelResource (300-399) ─────────────────────────────────────────────
	ActionListChannels             = int(photonpb.Action_ACTION_LIST_CHANNELS)
	ActionGetChannelMembers        = int(photonpb.Action_ACTION_GET_CHANNEL_MEMBERS)
	ActionGetMyChannels            = int(photonpb.Action_ACTION_GET_MY_CHANNELS)
	ActionGetChannel               = int(photonpb.Action_ACTION_GET_CHANNEL)
	ActionGetChannelByName         = int(photonpb.Action_ACTION_GET_CHANNEL_BY_NAME)
	ActionCreateChannel            = int(photonpb.Action_ACTION_CREATE_CHANNEL)
	ActionCreateDirectChannel      = int(photonpb.Action_ACTION_CREATE_DIRECT_CHANNEL)
	ActionGetDirectChannels        = int(photonpb.Action_ACTION_GET_DIRECT_CHANNELS)
	ActionGetOrCreateDirectChannel = int(photonpb.Action_ACTION_GET_OR_CREATE_DIRECT_CHANNEL)
	ActionAddMember                = int(photonpb.Action_ACTION_ADD_MEMBER)
	ActionRemoveMember             = int(photonpb.Action_ACTION_REMOVE_MEMBER)
	ActionUpdateChannel            = int(photonpb.Action_ACTION_UPDATE_CHANNEL)
	ActionGetUnreadForChannel      = int(photonpb.Action_ACTION_GET_UNREAD_FOR_CHANNEL)
	ActionMarkAsRead               = int(photonpb.Action_ACTION_MARK_AS_READ)
	ActionGetChannelMember         = int(photonpb.Action_ACTION_GET_CHANNEL_MEMBER)

	// ── EngramResource (400-499) ──────────────────────────────────────────────
	ActionEngramStore      = int(photonpb.Action_ACTION_ENGRAM_STORE)
	ActionEngramQuery      = int(photonpb.Action_ACTION_ENGRAM_QUERY)
	ActionEngramDelete     = int(photonpb.Action_ACTION_ENGRAM_DELETE)
	ActionEngramList       = int(photonpb.Action_ACTION_ENGRAM_LIST)
	ActionEngramBatchStore = int(photonpb.Action_ACTION_ENGRAM_BATCH_STORE)
	ActionEngramBatchQuery = int(photonpb.Action_ACTION_ENGRAM_BATCH_QUERY)

	// ── FileResource (500-599) ────────────────────────────────────────────────
	ActionUploadFile   = int(photonpb.Action_ACTION_UPLOAD_FILE)
	ActionDownloadFile = int(photonpb.Action_ACTION_DOWNLOAD_FILE)
	ActionGetFileInfo  = int(photonpb.Action_ACTION_GET_FILE_INFO)
	ActionGetThumbnail = int(photonpb.Action_ACTION_GET_THUMBNAIL)
	ActionGetPreview   = int(photonpb.Action_ACTION_GET_PREVIEW)
	ActionGetImageURL  = int(photonpb.Action_ACTION_GET_IMAGE_URL)

	// ── WallResource (600-699) ────────────────────────────────────────────────
	ActionGetOrCreateWall = int(photonpb.Action_ACTION_GET_OR_CREATE_WALL)
	ActionGetWallPosts    = int(photonpb.Action_ACTION_GET_WALL_POSTS)
	ActionPublishWallPost = int(photonpb.Action_ACTION_PUBLISH_WALL_POST)

	// ── MediaResource (700-702) ──
	// Стримы 700 и 701 несут двоичный хвост (media-transfer.md §1) и потому
	// не проходят через обычный JSON-путь Dispatch — см. handler_media.go.
	ActionMediaUpload   = int(photonpb.Action_ACTION_MEDIA_UPLOAD)
	ActionMediaDownload = int(photonpb.Action_ACTION_MEDIA_DOWNLOAD)
	ActionMediaGetInfo  = int(photonpb.Action_ACTION_MEDIA_GET_INFO)

	// ── LocalFeed (800-899) ──
	// ЛОКАЛЬНОЕ действие клиента: исполняется хранилищем самого инстанса и
	// на провод не выходит (ADR-064). Здесь оно объявлено не потому, что
	// сервер его исполняет, а потому, что Dispatch принимает любое число с
	// провода и обязан иметь для него ЯВНОЕ правило отказа.
	ActionLocalFeedPage = int(photonpb.Action_ACTION_LOCAL_FEED_PAGE)

	// Запись в локальную ленту (ADR-065). Причина объявления та же, что у
	// 800, и она не про сервер: записи эти действия делают в хранилище
	// КЛИЕНТА, до Bridge не доходят вовсе, и правило отказа нужно ровно
	// потому, что число с провода может прийти любое.
	ActionLocalFeedPut       = int(photonpb.Action_ACTION_LOCAL_FEED_PUT)
	ActionLocalFeedIngest    = int(photonpb.Action_ACTION_LOCAL_FEED_INGEST)
	ActionLocalFeedGapAnchor = int(photonpb.Action_ACTION_LOCAL_FEED_GAP_ANCHOR)

	// Каталог и курсор локальной ленты (ADR-095), исходящие операции
	// (ADR-093). Тоже локальные: объявлены ради явного правила отказа.
	ActionLocalFeedCatalog = int(photonpb.Action_ACTION_LOCAL_FEED_CATALOG)
	ActionLocalFeedCursor  = int(photonpb.Action_ACTION_LOCAL_FEED_CURSOR)
	ActionOutboxEnqueue    = int(photonpb.Action_ACTION_OUTBOX_ENQUEUE)
	ActionOutboxLease      = int(photonpb.Action_ACTION_OUTBOX_LEASE)
	ActionOutboxComplete   = int(photonpb.Action_ACTION_OUTBOX_COMPLETE)
	ActionOutboxFail       = int(photonpb.Action_ACTION_OUTBOX_FAIL)
	ActionOutboxRetry      = int(photonpb.Action_ACTION_OUTBOX_RETRY)
	ActionOutboxCancel     = int(photonpb.Action_ACTION_OUTBOX_CANCEL)
	ActionOutboxList       = int(photonpb.Action_ACTION_OUTBOX_LIST)

	// ConversationResource (900-999), ADR-099 §2.2: групповые разговоры
	// ENGRAM. Обработчик — ConversationHandler.
	ActionConversationCreate        = int(photonpb.Action_ACTION_CONVERSATION_CREATE)
	ActionConversationGet           = int(photonpb.Action_ACTION_CONVERSATION_GET)
	ActionConversationList          = int(photonpb.Action_ACTION_CONVERSATION_LIST)
	ActionConversationUpdate        = int(photonpb.Action_ACTION_CONVERSATION_UPDATE)
	ActionConversationArchive       = int(photonpb.Action_ACTION_CONVERSATION_ARCHIVE)
	ActionConversationMembersAdd    = int(photonpb.Action_ACTION_CONVERSATION_MEMBERS_ADD)
	ActionConversationMemberRemove  = int(photonpb.Action_ACTION_CONVERSATION_MEMBER_REMOVE)
	ActionConversationLeave         = int(photonpb.Action_ACTION_CONVERSATION_LEAVE)
	ActionConversationRoleSet       = int(photonpb.Action_ACTION_CONVERSATION_ROLE_SET)
	ActionConversationOwnerTransfer = int(photonpb.Action_ACTION_CONVERSATION_OWNER_TRANSFER)
	ActionConversationSend          = int(photonpb.Action_ACTION_CONVERSATION_SEND)
	ActionConversationSync          = int(photonpb.Action_ACTION_CONVERSATION_SYNC)

	// Messaging v2 (ADR-104–107), срез M1: коды и заглушки без поведения.
	ActionConversationOpenDirect      = int(photonpb.Action_ACTION_CONVERSATION_OPEN_DIRECT)
	ActionConversationUnarchive       = int(photonpb.Action_ACTION_CONVERSATION_UNARCHIVE)
	ActionConversationMemberStateSet  = int(photonpb.Action_ACTION_CONVERSATION_MEMBER_STATE_SET)
	ActionConversationInviteCreate    = int(photonpb.Action_ACTION_CONVERSATION_INVITE_CREATE)
	ActionConversationInviteRevoke    = int(photonpb.Action_ACTION_CONVERSATION_INVITE_REVOKE)
	ActionConversationInviteList      = int(photonpb.Action_ACTION_CONVERSATION_INVITE_LIST)
	ActionConversationJoinByInvite    = int(photonpb.Action_ACTION_CONVERSATION_JOIN_BY_INVITE)
	ActionConversationDirectorySearch = int(photonpb.Action_ACTION_CONVERSATION_DIRECTORY_SEARCH)
	ActionConversationJoinPublic      = int(photonpb.Action_ACTION_CONVERSATION_JOIN_PUBLIC)
	ActionConversationMarkRead        = int(photonpb.Action_ACTION_CONVERSATION_MARK_READ)
	ActionConversationMessageEdit     = int(photonpb.Action_ACTION_CONVERSATION_MESSAGE_EDIT)
	ActionConversationMessageDelete   = int(photonpb.Action_ACTION_CONVERSATION_MESSAGE_DELETE)
	ActionConversationHistoryBefore   = int(photonpb.Action_ACTION_CONVERSATION_HISTORY_BEFORE)
)

// ── Push event names (server → client) ───────────────────────────────────────

const (
	PushEventPosted          = "posted"
	PushEventPostEdited      = "post_edited"
	PushEventPostDeleted     = "post_deleted"
	PushEventReactionAdded   = "reaction_added"
	PushEventReactionRemoved = "reaction_removed"
	PushEventTyping          = "typing"
	PushEventUserAdded       = "user_added"
	PushEventUserRemoved     = "user_removed"
	PushEventStatusChange    = "status_change"
	PushEventChannelUpdated  = "channel_updated"
	PushEventNewUnread       = "new_unread_channel"
)

// ── Wire frames ───────────────────────────────────────────────────────────────

type AuthFrame struct {
	SessionToken string `json:"session_token"`
}

type HeaderFrame struct {
	Action   int    `json:"action"`
	Resource string `json:"resource"`
}

type Response struct {
	OK    bool        `json:"ok"`
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

// ── PostResource payloads ─────────────────────────────────────────────────────

type CreatePostPayload struct {
	ChannelID string   `json:"channel_id"`
	Message   string   `json:"message"`
	RootID    string   `json:"root_id,omitempty"`
	FileIds   []string `json:"file_ids,omitempty"`

	// PhotonMedia — вложения ENGRAM, ТИПИЗИРОВАННО.
	//
	// Поля кроме MediaID из запроса НЕ используются: props строятся из
	// метаданных MediaStore, который и есть авторитет (ADR-048 §7). Иначе
	// клиент мог бы объявить чужой sha256 или размер, и сообщение несло бы
	// ложь о вложении.
	//
	// Сырых props в этой структуре нет и не будет: приём произвольного props
	// дал бы клиенту запись в чужие ключи, включая те, что пишет сам
	// Mattermost (ADR-048 §3).
	PhotonMedia []MediaRef `json:"photon_media,omitempty"`
}

type GetFeedPayload struct {
	ChannelID string `json:"channel_id"`
	Page      int    `json:"page"`
	PerPage   int    `json:"per_page"`

	// Phase 12: курсорная пагинация. Форма не заморожена до завершения
	// первого вертикального среза (ADR-046 §6.1).
	//
	// Курсор — id записи, ОТ которой листаем; сама она в выдачу не входит.
	// before и after взаимно исключающие; оба пустые — самые свежие.
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type DeletePostPayload struct {
	PostID string `json:"post_id"`
}

type UpdatePostPayload struct {
	PostID  string `json:"post_id"`
	Message string `json:"message"`
}

type AddReactionPayload struct {
	PostID    string `json:"post_id"`
	EmojiName string `json:"emoji_name"`
}

type RemoveReactionPayload struct {
	PostID    string `json:"post_id"`
	EmojiName string `json:"emoji_name"`
}

// ── UserResource payloads ─────────────────────────────────────────────────────

type GetUserStatusPayload struct {
	UserID string `json:"user_id"`
}

type GetUserProfilePayload struct {
	UserID string `json:"user_id"`
}

type SearchUsersPayload struct {
	Term   string `json:"term"`
	InTeam string `json:"in_team,omitempty"`
	Limit  int    `json:"limit"`
}

// ── ChannelResource payloads ──────────────────────────────────────────────────

type ListChannelsPayload struct {
	TeamID  string `json:"team_id"`
	Page    int    `json:"page"`
	PerPage int    `json:"per_page"`
}

type GetChannelMembersPayload struct {
	ChannelID string `json:"channel_id"`
	Page      int    `json:"page"`
	PerPage   int    `json:"per_page"`
}

// ── EngramResource payloads ───────────────────────────────────────────────────

type EngramStorePayload struct {
	Key     string                 `json:"key"`
	Value   map[string]interface{} `json:"value"`
	TTLSecs int                    `json:"ttl_secs,omitempty"`

	// PreserveTTL — если true, существующий TTL при обновлении сохраняется.
	// TTLSecs игнорируется когда PreserveTTL=true и запись уже существует.
	// При создании новой записи PreserveTTL не имеет эффекта.
	PreserveTTL bool `json:"preserve_ttl,omitempty"`
}

type EngramQueryPayload struct {
	Key string `json:"key"`
}

type EngramDeletePayload struct {
	Key string `json:"key"`
}

// ── FileResource payloads ─────────────────────────────────────────────────────

type UploadFilePayload struct {
	ChannelID string `json:"channel_id"`
	FileName  string `json:"file_name"`
	FileBytes []byte `json:"file_bytes"`
}

type FileIDPayload struct {
	FileID string `json:"file_id"`
}

// ── MediaResource payloads ────────────────────────────────────────────────────

// MediaIDPayload — запрос по идентификатору объекта (701, 702).
type MediaIDPayload struct {
	MediaID string `json:"media_id"`
}

// MediaRef — запись о вложении в props сообщения.
//
// Состав заморожен ADR-048 §3. Клиент присылает её ТИПИЗИРОВАННО в
// CreatePostPayload; сырые props не принимаются, props строит Bridge.
type MediaRef struct {
	MediaID string `json:"media_id"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
	Mime    string `json:"mime"`
	Name    string `json:"name"`
	W       int32  `json:"w,omitempty"`
	H       int32  `json:"h,omitempty"`
}

// MediaInfoResponse — ответ 702: метаданные без тела.
type MediaInfoResponse struct {
	Media MediaRef `json:"media"`
}
