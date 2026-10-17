package main

import (
	"encoding/json"
	"errors"

	"github.com/mattermost/mattermost/server/public/model"
)

// Слой авторизации Bridge — ADR-039.
//
// Разделение, которое здесь нельзя нарушать:
//
//	resolve(object) → policy(predicate) → handler
//
// resolve знает про поля payload и Plugin API и НЕ принимает решений.
// policy состоит из комбинаторов закрытого словаря §3 и НЕ ходит за объектами.
// handler решений не принимает вовсе — ADR-039 §6.
//
// Собственных прав здесь нет ни одного: каждое решение — вызов
// HasPermissionTo/ToTeam/ToChannel с константой model.Permission*.

// errForbidden — единственный отказ авторизации. Один и тот же для «нет прав»
// и «нет объекта»: различие текстов рисует карту чужих объектов (§7.3).
var errForbidden = errors.New("forbidden")

// authzAPI — та часть Plugin API, которой пользуется слой. Узкий интерфейс, а
// не plugin.API целиком: он же и граница для тестового дублёра.
type authzAPI interface {
	HasPermissionTo(userID string, p *model.Permission) bool
	HasPermissionToTeam(userID, teamID string, p *model.Permission) bool
	HasPermissionToChannel(userID, channelID string, p *model.Permission) bool
	GetChannel(channelID string) (*model.Channel, *model.AppError)
	GetChannelByName(teamID, name string, includeDeleted bool) (*model.Channel, *model.AppError)
	GetPost(postID string) (*model.Post, *model.AppError)
	GetFileInfo(fileID string) (*model.FileInfo, *model.AppError)
	GetConfig() *model.Config
	// GetUser — способ входа субъекта (ADR-091 §2.4: 207 закрыт для
	// пользователей ENGRAM-входа).
	GetUser(userID string) (*model.User, *model.AppError)
}

type authz struct {
	api authzAPI
	// accounts — реестр для резолва objDirectPeer (ADR-082). nil там, где
	// личные сообщения не разрешаются (push-роутер): тогда objDirectPeer
	// отказывает, а не паникует.
	accounts photonAccountFinder
	// media — авторитетный индекс для резолва objMedia (ADR-048 §7).
	// Может быть nil там, где медийные объекты не разрешаются (push-роутер):
	// тогда objMedia отказывает, а не паникует — отказ по умолчанию, §7.2.
	media MediaStore
}

func newAuthz(api authzAPI, media MediaStore) *authz {
	return &authz{api: api, media: media}
}

// ── объекты ──────────────────────────────────────────────────────────────────

type objectKind int

const (
	objNone          objectKind = iota
	objChannel                  // channel_id
	objChannelByName            // team_id + name
	objPost                     // post_id
	objRoot                     // root_id
	objFile                     // file_id
	objUser                     // user_id | target_user_id
	objTeam                     // team_id
	objKV                       // key — пространство имён по построению
	objWall                     // user_id → канал стены
	objMember                   // channel_id + user_id
	objMedia                    // media_id → канал через MediaStore
	objDirectPeer               // to_photon_id | with_photon_id → личный канал с собеседником (ADR-082 §3.7)
)

// objectRef — разрешённый объект. Заполняется resolve, читается policy.
type objectRef struct {
	channel *model.Channel
	post    *model.Post
	target  string // цель-пользователь
	teamID  string
	private bool // is_private из payload (только для createChannel)
	isSelf  bool // цель совпадает с субъектом
}

// payloadFields — все поля payload, которые может назвать правило. Собраны в
// одну структуру намеренно: resolve не должен знать, какому action какой
// handler соответствует.
type payloadFields struct {
	ChannelID    string `json:"channel_id"`
	PostID       string `json:"post_id"`
	RootID       string `json:"root_id"`
	FileID       string `json:"file_id"`
	MediaID      string `json:"media_id"`
	UserID       string `json:"user_id"`
	TargetUserID string `json:"target_user_id"`
	TeamID       string `json:"team_id"`
	Name         string `json:"name"`
	Key          string `json:"key"`
	IsPrivate    bool   `json:"is_private"`
	ToPhotonID   string `json:"to_photon_id"`
	WithPhotonID string `json:"with_photon_id"`
}

func (z *authz) channelByID(id string) (*model.Channel, error) {
	if id == "" {
		return nil, errForbidden
	}
	ch, appErr := z.api.GetChannel(id)
	if appErr != nil || ch == nil {
		return nil, errForbidden
	}
	return ch, nil
}

// resolve — payload → объект. Любая неудача разрешения даёт отказ, а не
// «проверять нечего» (§7.2).
func (z *authz) resolve(kind objectKind, subject string, raw json.RawMessage) (objectRef, error) {
	var o objectRef
	var p payloadFields
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return o, errForbidden
		}
	}
	o.teamID = p.TeamID
	o.private = p.IsPrivate

	target := p.UserID
	if target == "" {
		target = p.TargetUserID
	}

	switch kind {
	case objNone, objKV:
		o.isSelf = true // цели нет — self по построению (§3)
		return o, nil

	case objTeam:
		if p.TeamID == "" {
			return o, errForbidden
		}
		return o, nil

	case objUser:
		if target == "" {
			target = subject
		}
		o.target = target
		o.isSelf = target == subject
		return o, nil

	case objChannel:
		ch, err := z.channelByID(p.ChannelID)
		if err != nil {
			return o, err
		}
		o.channel = ch
		return o, nil

	case objChannelByName:
		if p.TeamID == "" || p.Name == "" {
			return o, errForbidden
		}
		ch, appErr := z.api.GetChannelByName(p.TeamID, p.Name, false)
		if appErr != nil || ch == nil {
			return o, errForbidden
		}
		o.channel = ch
		return o, nil

	case objPost, objRoot:
		id := p.PostID
		if kind == objRoot {
			id = p.RootID
		}
		if id == "" {
			return o, errForbidden
		}
		post, appErr := z.api.GetPost(id)
		if appErr != nil || post == nil {
			return o, errForbidden
		}
		ch, err := z.channelByID(post.ChannelId)
		if err != nil {
			return o, err
		}
		o.post, o.channel = post, ch
		return o, nil

	case objFile:
		if p.FileID == "" {
			return o, errForbidden
		}
		info, appErr := z.api.GetFileInfo(p.FileID)
		if appErr != nil || info == nil {
			return o, errForbidden
		}
		// ChannelId денормализован и у неприкреплённого файла пуст.
		// Пустое значение обязано давать отказ — ADR-039 §5.
		ch, err := z.channelByID(info.ChannelId)
		if err != nil {
			return o, err
		}
		o.channel = ch
		return o, nil

	case objMedia:
		// Форма повторяет objFile: идентификатор → метаданные → канал →
		// право чтения канала. Отличие одно: авторитет здесь MediaStore, а
		// не Mattermost, потому что байты и связь с каналом принадлежат ему
		// (ADR-048 §7).
		//
		// Отсутствие объекта и отсутствие хранилища дают ОДИН И ТОТ ЖЕ отказ,
		// что и отсутствие права: различать их значило бы позволить пробовать
		// существование чужих media_id (ADR-039, media-transfer.md §5).
		if p.MediaID == "" || z.media == nil {
			return o, errForbidden
		}
		mo, err := z.media.Get(p.MediaID)
		if err != nil || mo == nil {
			return o, errForbidden
		}
		ch, cerr := z.channelByID(mo.ChannelID)
		if cerr != nil {
			return o, cerr
		}
		o.channel = ch
		return o, nil

	case objMember:
		ch, err := z.channelByID(p.ChannelID)
		if err != nil {
			return o, err
		}
		if target == "" {
			target = subject
		}
		o.channel, o.target, o.isSelf = ch, target, target == subject
		return o, nil

	case objWall:
		if p.TeamID == "" {
			return o, errForbidden
		}
		if target == "" {
			target = subject
		}
		o.target, o.isSelf = target, target == subject
		if o.isSelf {
			// Своя стена: создание разрешено, поэтому канала может ещё не
			// быть, и отсутствие — не отказ (§9).
			return o, nil
		}
		ch, appErr := z.api.GetChannelByName(p.TeamID, wallChannelName(target), false)
		if appErr != nil || ch == nil {
			return o, errForbidden
		}
		o.channel = ch
		return o, nil

	case objDirectPeer:
		// Личный канал вызывающего с собеседником, найденный БЕЗ создания
		// (ADR-082 §3.7). Негодный или несуществующий PhotonID — объекта нет,
		// и это не отказ: защищать нечего, а различимый ответ
		// (invalid_photon_id, photon_id_not_found) даёт обработчик по
		// ADR-082 §3.8. Сбой реестра или Mattermost — отказ (§7.2).
		if z.accounts == nil {
			return o, errForbidden
		}
		peerID := p.ToPhotonID
		if peerID == "" {
			peerID = p.WithPhotonID
		}
		if !isCanonicalPhotonID(peerID) {
			return o, nil
		}
		rec, found, err := z.accounts.FindByPhotonID(peerID)
		if err != nil {
			return o, errForbidden
		}
		if !found {
			return o, nil
		}
		o.target, o.isSelf = rec.MMUserID, rec.MMUserID == subject
		ch, appErr := z.api.GetChannelByName("", model.GetDMNameFromIds(subject, rec.MMUserID), false)
		if appErr != nil {
			if appErr.StatusCode == 404 {
				return o, nil // канала ещё нет
			}
			return o, errForbidden
		}
		if ch == nil || ch.Type != model.ChannelTypeDirect {
			return o, errForbidden
		}
		o.channel = ch
		return o, nil
	}
	return o, errForbidden
}

// ── предикаты (закрытый словарь §3) ──────────────────────────────────────────

type policy func(z *authz, subject string, o objectRef) bool

func authenticated() policy { return func(*authz, string, objectRef) bool { return true } }

// notServedHere — действие, которого сервер не исполняет: отказ всегда и
// всем (ADR-064 п. 2.5).
//
// Локальные действия клиента (диапазон 800-899) живут в том же каноническом
// enum, что и сетевые (ADR-025), и Dispatch обязан ответить на любое число,
// пришедшее с провода. Явное правило лучше умолчания ровно потому, что
// умолчание нигде не записано как решение, а инвариант полноты
// (TestAuthzCoversEveryAction) не допускает исключений «для своих».
func notServedHere() policy { return func(*authz, string, objectRef) bool { return false } }

func isSelf() policy {
	return func(_ *authz, _ string, o objectRef) bool { return o.isSelf }
}

// isSelfNotEngram — isSelf, но не для пользователя ENGRAM-входа
// (AuthService = engram, ADR-080). ADR-091 §2.4: контракт 207 — правка
// произвольного объекта пользователя (username, Props), и для пользователя
// ENGRAM он закрыт целиком, а не отфильтрован. Не удалось узнать способ входа
// — отказ.
func isSelfNotEngram() policy {
	return func(z *authz, s string, o objectRef) bool {
		if !o.isSelf {
			return false
		}
		u, appErr := z.api.GetUser(s)
		if appErr != nil || u == nil {
			return false
		}
		return u.AuthService != engramAuthService
	}
}

// ownNamespace — свойство конструкции ключа engram:<userID>:<key>, а не
// проверка. Объявлено явно, чтобы у действия было правило (§7.1).
func ownNamespace() policy { return func(*authz, string, objectRef) bool { return true } }

func permSys(p *model.Permission) policy {
	return func(z *authz, s string, _ objectRef) bool { return z.api.HasPermissionTo(s, p) }
}

func permTeam(p *model.Permission) policy {
	return func(z *authz, s string, o objectRef) bool {
		team := o.teamID
		if team == "" && o.channel != nil {
			team = o.channel.TeamId
		}
		if team == "" {
			return z.api.HasPermissionTo(s, p)
		}
		return z.api.HasPermissionToTeam(s, team, p)
	}
}

func permChan(p *model.Permission) policy {
	return func(z *authz, s string, o objectRef) bool {
		if o.channel == nil {
			return false
		}
		return z.api.HasPermissionToChannel(s, o.channel.Id, p)
	}
}

// readC — единый предикат доступа к содержимому канала, ADR-039 §4.
// Порядок веток взят из channels/app/authorization.go:409-419 и подтверждён
// ТОЛЬКО по исходникам: живой прогон порядок не различал.
func readC() policy {
	return func(z *authz, s string, o objectRef) bool {
		if o.channel == nil {
			return false
		}
		if z.api.HasPermissionToChannel(s, o.channel.Id, model.PermissionReadChannelContent) {
			return true
		}
		if o.channel.Type != model.ChannelTypeOpen || o.channel.TeamId == "" {
			return false
		}
		if cfg := z.api.GetConfig(); cfg != nil &&
			cfg.ComplianceSettings.Enable != nil && *cfg.ComplianceSettings.Enable {
			return false
		}
		return z.api.HasPermissionToTeam(s, o.channel.TeamId, model.PermissionReadPublicChannel)
	}
}

// ownerSel — селектор между двумя константами Mattermost, а не самостоятельное
// решение (ADR-039 §3). Без него администратор с delete_others_posts получал
// отказ — измерено baseline'ом.
func ownerSel(own, others *model.Permission) policy {
	return func(z *authz, s string, o objectRef) bool {
		if o.post != nil && o.post.UserId == s {
			return permChan(own)(z, s, o)
		}
		return permChan(others)(z, s, o)
	}
}

// byChannelType — выбор константы по типу канала. Своей модели
// «приватный/публичный» не заводится: выбирается, что спросить у Mattermost.
func byChannelType(open, private policy) policy {
	return func(z *authz, s string, o objectRef) bool {
		if o.channel == nil {
			return false
		}
		if o.channel.Type == model.ChannelTypeOpen {
			return open(z, s, o)
		}
		return private(z, s, o)
	}
}

func ifSelf(then, otherwise policy) policy {
	return func(z *authz, s string, o objectRef) bool {
		if o.isSelf {
			return then(z, s, o)
		}
		return otherwise(z, s, o)
	}
}

func ifPrivate(private, public policy) policy {
	return func(z *authz, s string, o objectRef) bool {
		if o.private {
			return private(z, s, o)
		}
		return public(z, s, o)
	}
}

// ifChannel — объект, который может ещё не существовать (личный канал до
// первого сообщения, ADR-082 §3.7): есть канал — решает then, нет — otherwise.
func ifChannel(then, otherwise policy) policy {
	return func(z *authz, s string, o objectRef) bool {
		if o.channel != nil {
			return then(z, s, o)
		}
		return otherwise(z, s, o)
	}
}

func allOf(ps ...policy) policy {
	return func(z *authz, s string, o objectRef) bool {
		for _, p := range ps {
			if !p(z, s, o) {
				return false
			}
		}
		return true
	}
}

func anyOf(ps ...policy) policy {
	return func(z *authz, s string, o objectRef) bool {
		for _, p := range ps {
			if p(z, s, o) {
				return true
			}
		}
		return false
	}
}

// ── таблица правил ───────────────────────────────────────────────────────────

type rule struct {
	object objectKind
	policy policy
}

// rules — строка на каждый action из photonpb.Action_name. Полнота проверяется
// машинно (TestAuthzCoversEveryAction): новый action без правила роняет тесты.
var rules = map[int]rule{
	// ── PostResource ──
	ActionCreatePost:        {objChannel, permChan(model.PermissionCreatePost)},
	ActionGetFeed:           {objChannel, readC()},
	ActionDeletePost:        {objPost, allOf(readC(), ownerSel(model.PermissionDeletePost, model.PermissionDeleteOthersPosts))},
	ActionUpdatePost:        {objPost, allOf(readC(), ownerSel(model.PermissionEditPost, model.PermissionEditOthersPosts))},
	ActionGetThread:         {objPost, readC()},
	ActionReplyToPost:       {objRoot, allOf(readC(), permChan(model.PermissionCreatePost))},
	ActionPinPost:           {objPost, readC()},
	ActionUnpinPost:         {objPost, readC()},
	ActionGetPinnedPosts:    {objChannel, readC()},
	ActionSearchPosts:       {objTeam, permTeam(model.PermissionViewTeam)},
	ActionAddReaction:       {objPost, allOf(readC(), permChan(model.PermissionAddReaction))},
	ActionRemoveReaction:    {objPost, allOf(readC(), permChan(model.PermissionRemoveReaction))},
	ActionGetReactions:      {objPost, readC()},
	ActionToggleLike:        {objPost, allOf(readC(), permChan(model.PermissionAddReaction), permChan(model.PermissionRemoveReaction))},
	ActionGetPostSocialInfo: {objPost, readC()},

	// ── UserResource ──
	ActionGetMe:          {objNone, authenticated()},
	ActionGetUser:        {objUser, permSys(model.PermissionViewMembers)},
	ActionGetUsers:       {objNone, permSys(model.PermissionViewMembers)},
	ActionGetUserStatus:  {objUser, authenticated()},
	ActionGetUserProfile: {objUser, permSys(model.PermissionViewMembers)},
	ActionSearchUsers:    {objNone, permSys(model.PermissionViewMembers)},
	ActionGetUsersByIds:  {objNone, permSys(model.PermissionViewMembers)},
	ActionPatchMe:        {objNone, isSelfNotEngram()}, // ADR-091 §2.4
	ActionSetStatus:      {objNone, isSelf()},
	ActionUploadAvatar:   {objNone, isSelf()},
	ActionGetUserAvatar:  {objUser, permSys(model.PermissionViewMembers)},
	// ADR-081 §2.1: свой профиль — как getMe; поиск — как searchUsers.
	ActionGetMyPhotonProfile: {objNone, authenticated()},
	ActionFindByPhotonID:     {objNone, permSys(model.PermissionViewMembers)},
	// ADR-091: субъект — только вызывающий, объекта в запросе нет.
	ActionSetMyDisplayName: {objNone, authenticated()},
	// ADR-082 §3.7: отправка — право создать личный канал и, если он уже
	// есть, писать в него; чтение — readC, а канала нет — читать нечего.
	ActionSendDirectMessage: {objDirectPeer, allOf(
		permSys(model.PermissionCreateDirectChannel),
		ifChannel(permChan(model.PermissionCreatePost), authenticated()))},
	ActionListDirectMessages: {objDirectPeer, ifChannel(readC(), authenticated())},
	// ADR-092 ред. 3: v2 — те же объект и права, что у 213/214; меняется
	// только контракт ответа, не круг допущенных.
	ActionSendDirectMessageV2: {objDirectPeer, allOf(
		permSys(model.PermissionCreateDirectChannel),
		ifChannel(permChan(model.PermissionCreatePost), authenticated()))},
	ActionListDirectMessagesV2: {objDirectPeer, ifChannel(readC(), authenticated())},
	// ADR-105: 219 — доступ как у 218 (контракт Messaging v2 §3).
	ActionListDirectMessagesBefore: {objDirectPeer, ifChannel(readC(), authenticated())},
	// ADR-111: объекта Mattermost нет, субъект — только вызывающий; доступ
	// и существование целевого PhotonID решает обработчик.
	ActionUserBlock:     {objNone, authenticated()},
	ActionUserUnblock:   {objNone, authenticated()},
	ActionUserBlockList: {objNone, authenticated()},
	// ADR-084 §2.1: объекта во входе нет; список — только каналы участия.
	ActionListDirectConversations: {objNone, authenticated()},

	// ── ChannelResource ──
	ActionListChannels:      {objTeam, permTeam(model.PermissionViewTeam)},
	ActionGetChannelMembers: {objChannel, permChan(model.PermissionReadChannel)},
	ActionGetMyChannels:     {objTeam, permTeam(model.PermissionViewTeam)},
	ActionGetChannel:        {objChannel, readC()},
	ActionGetChannelByName:  {objChannelByName, readC()},
	ActionCreateChannel: {objTeam, ifPrivate(
		permTeam(model.PermissionCreatePrivateChannel),
		permTeam(model.PermissionCreatePublicChannel))},
	ActionCreateDirectChannel:      {objUser, permSys(model.PermissionCreateDirectChannel)},
	ActionGetDirectChannels:        {objTeam, permTeam(model.PermissionViewTeam)},
	ActionGetOrCreateDirectChannel: {objUser, permSys(model.PermissionCreateDirectChannel)},
	ActionAddMember: {objMember, ifSelf(
		byChannelType(
			permTeam(model.PermissionJoinPublicChannels),
			permChan(model.PermissionManagePrivateChannelMembers)),
		byChannelType(
			permChan(model.PermissionManagePublicChannelMembers),
			permChan(model.PermissionManagePrivateChannelMembers)))},
	ActionRemoveMember: {objMember, ifSelf(
		readC(),
		byChannelType(
			permChan(model.PermissionManagePublicChannelMembers),
			permChan(model.PermissionManagePrivateChannelMembers)))},
	ActionUpdateChannel: {objChannel, byChannelType(
		permChan(model.PermissionManagePublicChannelProperties),
		permChan(model.PermissionManagePrivateChannelProperties))},
	ActionGetUnreadForChannel: {objChannel, readC()},
	ActionMarkAsRead:          {objChannel, readC()},
	ActionGetChannelMember:    {objChannel, permChan(model.PermissionReadChannel)},

	// ── EngramResource — данные Photon, источника в Mattermost нет ──
	ActionEngramStore:      {objKV, allOf(authenticated(), ownNamespace())},
	ActionEngramQuery:      {objKV, allOf(authenticated(), ownNamespace())},
	ActionEngramDelete:     {objKV, allOf(authenticated(), ownNamespace())},
	ActionEngramList:       {objKV, allOf(authenticated(), ownNamespace())},
	ActionEngramBatchStore: {objKV, allOf(authenticated(), ownNamespace())},
	ActionEngramBatchQuery: {objKV, allOf(authenticated(), ownNamespace())},

	// ── MediaResource ──
	// Загрузка авторизуется ПО КАНАЛУ, а не по объекту: в момент загрузки
	// объекта ещё нет. Та же форма, что у ActionUploadFile
	// (media-transfer.md §12). Право берётся то же самое — вложение остаётся
	// вложением независимо от того, где лежат байты.
	ActionMediaUpload:   {objChannel, permChan(model.PermissionUploadFile)},
	ActionMediaDownload: {objMedia, readC()},
	ActionMediaGetInfo:  {objMedia, readC()},

	// ── LocalFeed: сервер не исполняет, отказ всегда ──
	ActionLocalFeedPage:      {objNone, notServedHere()},
	ActionLocalFeedPut:       {objNone, notServedHere()},
	ActionLocalFeedIngest:    {objNone, notServedHere()},
	ActionLocalFeedGapAnchor: {objNone, notServedHere()},
	ActionLocalFeedCatalog:   {objNone, notServedHere()},
	ActionLocalFeedCursor:    {objNone, notServedHere()},

	// ── Outbox (ADR-093): локальная очередь клиента, отказ всегда ──
	ActionOutboxEnqueue:  {objNone, notServedHere()},
	ActionOutboxLease:    {objNone, notServedHere()},
	ActionOutboxComplete: {objNone, notServedHere()},
	ActionOutboxFail:     {objNone, notServedHere()},
	ActionOutboxRetry:    {objNone, notServedHere()},
	ActionOutboxCancel:   {objNone, notServedHere()},
	ActionOutboxList:     {objNone, notServedHere()},

	// ── 813 (ADR-112): очистка локальной ленты, отказ всегда ──
	ActionLocalFeedClear: {objNone, notServedHere()},

	// ── 814 (ADR-123 §2.3): цель подписки Core, отказ всегда ──
	ActionLocalEventsTarget: {objNone, notServedHere()},

	// ── FileResource ──
	ActionUploadFile:   {objChannel, permChan(model.PermissionUploadFile)},
	ActionDownloadFile: {objFile, readC()},
	ActionGetFileInfo:  {objFile, readC()},
	ActionGetThumbnail: {objFile, readC()},
	ActionGetPreview:   {objFile, readC()},
	ActionGetImageURL:  {objFile, readC()},

	// ── WallResource ──
	ActionGetOrCreateWall: {objWall, ifSelf(authenticated(), readC())},
	ActionGetWallPosts:    {objChannel, readC()},
	ActionPublishWallPost: {objChannel, allOf(readC(), permChan(model.PermissionCreatePost))},

	// ── ConversationResource (ADR-099 §2.4) ──
	// Правило таблицы — только «аутентифицирован». Доступ к разговору и право
	// на операцию решает обработчик по данным ENGRAM (ADR-099 §11); права и
	// членство Mattermost для кодов 9xx не проверяются и не используются.
	ActionConversationCreate:        {objNone, authenticated()},
	ActionConversationGet:           {objNone, authenticated()},
	ActionConversationList:          {objNone, authenticated()},
	ActionConversationUpdate:        {objNone, authenticated()},
	ActionConversationArchive:       {objNone, authenticated()},
	ActionConversationMembersAdd:    {objNone, authenticated()},
	ActionConversationMemberRemove:  {objNone, authenticated()},
	ActionConversationLeave:         {objNone, authenticated()},
	ActionConversationRoleSet:       {objNone, authenticated()},
	ActionConversationOwnerTransfer: {objNone, authenticated()},
	ActionConversationSend:          {objNone, authenticated()},
	ActionConversationSync:          {objNone, authenticated()},

	// Messaging v2 (ADR-104–107): то же правило (ADR-099 §2.4), доступ решает обработчик.
	ActionConversationOpenDirect:      {objNone, authenticated()},
	ActionConversationUnarchive:       {objNone, authenticated()},
	ActionConversationMemberStateSet:  {objNone, authenticated()},
	ActionConversationInviteCreate:    {objNone, authenticated()},
	ActionConversationInviteRevoke:    {objNone, authenticated()},
	ActionConversationInviteList:      {objNone, authenticated()},
	ActionConversationJoinByInvite:    {objNone, authenticated()},
	ActionConversationDirectorySearch: {objNone, authenticated()},
	ActionConversationJoinPublic:      {objNone, authenticated()},
	ActionConversationMarkRead:        {objNone, authenticated()},
	ActionConversationMessageEdit:     {objNone, authenticated()},
	ActionConversationMessageDelete:   {objNone, authenticated()},
	ActionConversationReactionSet:     {objNone, authenticated()},
	ActionConversationHistoryBefore:   {objNone, authenticated()},
	ActionConversationSearch:          {objNone, authenticated()},
	ActionConversationMemberBan:       {objNone, authenticated()},
}

// authorize — единственная точка принятия решения. Вызывается Dispatcher'ом до
// маршрутизации в handler, одинаково для QUIC и HTTP (ADR-039 §6).
func (z *authz) authorize(userID string, action int, payload json.RawMessage) error {
	r, ok := rules[action]
	if !ok {
		return errForbidden // §7.1: по умолчанию — отказ
	}
	o, err := z.resolve(r.object, userID, payload)
	if err != nil {
		return err
	}
	if !r.policy(z, userID, o) {
		return errForbidden
	}
	return nil
}
