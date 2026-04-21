package main

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

const (
	// ── PostResource (100-199) ────────────────────────────────────────────────
	ActionCreatePost        = 100
	ActionGetFeed           = 101
	ActionDeletePost        = 102
	ActionUpdatePost        = 103
	ActionGetThread         = 104
	ActionReplyToPost       = 105
	ActionPinPost           = 106
	ActionUnpinPost         = 107
	ActionGetPinnedPosts    = 108
	ActionSearchPosts       = 109
	ActionAddReaction       = 110
	ActionRemoveReaction    = 111
	ActionGetReactions      = 112
	ActionToggleLike        = 113
	ActionGetPostSocialInfo = 114

	// ── UserResource (200-299) ────────────────────────────────────────────────
	ActionGetMe          = 200
	ActionGetUser        = 201
	ActionGetUsers       = 202
	ActionGetUserStatus  = 203
	ActionGetUserProfile = 204
	ActionSearchUsers    = 205
	ActionGetUsersByIds  = 206
	ActionPatchMe        = 207
	ActionSetStatus      = 208
	ActionUploadAvatar   = 209
	ActionGetUserAvatar  = 210

	// ── ChannelResource (300-399) ─────────────────────────────────────────────
	ActionListChannels             = 300
	ActionGetChannelMembers        = 301
	ActionGetMyChannels            = 302
	ActionGetChannel               = 303
	ActionGetChannelByName         = 304
	ActionCreateChannel            = 305
	ActionCreateDirectChannel      = 306
	ActionGetDirectChannels        = 307
	ActionGetOrCreateDirectChannel = 308
	ActionAddMember                = 309
	ActionRemoveMember             = 310
	ActionUpdateChannel            = 311
	ActionGetUnreadForChannel      = 312
	ActionMarkAsRead               = 313
	ActionGetChannelMember         = 314

	// ── EngramResource (400-499) ──────────────────────────────────────────────
	ActionEngramStore      = 400
	ActionEngramQuery      = 401
	ActionEngramDelete     = 402
	ActionEngramList       = 403
	ActionEngramBatchStore = 404
	ActionEngramBatchQuery = 405

	// ── FileResource (500-599) ────────────────────────────────────────────────
	ActionUploadFile   = 500
	ActionDownloadFile = 501
	ActionGetFileInfo  = 502
	ActionGetThumbnail = 503
	ActionGetPreview   = 504
	ActionGetImageURL  = 505

	// ── WallResource (600-699) ────────────────────────────────────────────────
	ActionGetOrCreateWall = 600
	ActionGetWallPosts    = 601
	ActionPublishWallPost = 602
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
}

type GetFeedPayload struct {
	ChannelID string `json:"channel_id"`
	Page      int    `json:"page"`
	PerPage   int    `json:"per_page"`
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