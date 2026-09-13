package main

import (
	"encoding/json"
	"fmt"

	"github.com/mattermost/mattermost/server/public/model"
)

// PostHandler зеркалит PostResource из Kotlin SDK.
// Actions: 100=CreatePost, 101=GetFeed, 102=DeletePost, 103=UpdatePost,
//
//	104=GetThread, 105=ReplyToPost, 106=PinPost, 107=UnpinPost,
//	108=GetPinnedPosts, 109=SearchPosts, 110=AddReaction,
//	111=RemoveReaction, 112=GetReactions, 113=ToggleLike,
//	114=GetPostSocialInfo
type PostHandler struct {
	plugin *Plugin
	// media — тот же экземпляр сервиса, что у медийного стрима. Один на всех:
	// два разошлись бы по состоянию хранилища.
	media *MediaService
}

func (h *PostHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionCreatePost:
		return h.createPost(userID, raw)
	case ActionGetFeed:
		return h.getFeed(userID, raw)
	case ActionDeletePost:
		return h.deletePost(userID, raw)
	case ActionUpdatePost:
		return h.updatePost(userID, raw)
	case ActionGetThread:
		return h.getThread(userID, raw)
	case ActionReplyToPost:
		return h.replyToPost(userID, raw)
	case ActionPinPost:
		return h.pinPost(userID, raw)
	case ActionUnpinPost:
		return h.unpinPost(userID, raw)
	case ActionGetPinnedPosts:
		return h.getPinnedPosts(userID, raw)
	case ActionSearchPosts:
		return h.searchPosts(userID, raw)
	case ActionAddReaction:
		return h.addReaction(userID, raw)
	case ActionRemoveReaction:
		return h.removeReaction(userID, raw)
	case ActionGetReactions:
		return h.getReactions(userID, raw)
	case ActionToggleLike:
		return h.toggleLike(userID, raw)
	case ActionGetPostSocialInfo:
		return h.getPostSocialInfo(userID, raw)
	default:
		return Response{OK: false, Error: fmt.Sprintf("PostResource: unknown action %d", action)}
	}
}

// createPost — Action 100.
func (h *PostHandler) createPost(userID string, raw json.RawMessage) Response {
	var p CreatePostPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (createPost)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" || p.Message == "" {
		return Response{OK: false, Error: "channel_id and message are required"}
	}
	// Вложения ENGRAM проверяются ДО создания поста: сообщение не должно
	// появиться со ссылкой на media, которой нет или которая чужая.
	refs, err := h.resolveMedia(userID, p.ChannelID, p.PhotonMedia)
	if err != nil {
		h.plugin.API.LogWarn("[Photon][PostHandler] photon_media rejected",
			"user_id", userID, "channel_id", p.ChannelID, "error", err.Error())
		return Response{OK: false, Error: mediaErrorText(err)}
	}

	post := &model.Post{
		UserId:    userID,
		ChannelId: p.ChannelID,
		Message:   p.Message,
		RootId:    p.RootID,
		FileIds:   p.FileIds,
	}
	if len(refs) > 0 {
		// props строятся ЗДЕСЬ, из метаданных хранилища. Пространство имён
		// photon_* — единственное, куда пишет Photon; чужие ключи не
		// трогаются (ADR-048 §3).
		value, err := mediaPropsValue(refs)
		if err != nil {
			h.plugin.API.LogError("[Photon][PostHandler] photon_media props encode",
				"user_id", userID, "channel_id", p.ChannelID, "error", err.Error())
			return Response{OK: false, Error: "internal: photon_media props"}
		}
		post.AddProp("photon_media", value)
	}
	created, appErr := h.plugin.API.CreatePost(post)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] CreatePost API error",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	// (nil, nil) — не «пост создан пустым», а НЕУДАВШИЙСЯ ВЫЗОВ.
	// Сгенерированный клиент plugin API при отказе транспорта только пишет в
	// лог и возвращает нули (client_rpc_generated.go: `if err := g.client.Call(
	// ...); err != nil { log.Printf(...) }`), поэтому отсутствие *model.AppError
	// не означает успеха. Без этой проверки дальше идёт created.Id — разыменование
	// nil, паника в горутине стрима и ответ, которого клиент не дождётся.
	if created == nil {
		h.plugin.API.LogError("[Photon][PostHandler] CreatePost returned no post and no error",
			"user_id", userID, "channel_id", p.ChannelID)
		return Response{OK: false, Error: "internal: create post failed"}
	}
	// Прикрепление ПОСЛЕ создания: до него post_id не существует.
	for _, r := range refs {
		if _, err := h.media.Attach(r.MediaID, created.Id); err != nil {
			// Пост уже создан и его props ссылаются на существующие объекты;
			// неудача перевода в ATTACHED не делает сообщение неверным, но
			// обязана быть видна: объект останется STORED и будет найден как
			// брошенный.
			h.plugin.API.LogError("[Photon][PostHandler] Attach failed after post creation",
				"user_id", userID, "post_id", created.Id, "media_id", r.MediaID,
				"error", err.Error())
		}
	}

	h.plugin.API.LogInfo("[Photon][PostHandler] Post created",
		"user_id", userID, "post_id", created.Id, "channel_id", p.ChannelID,
		"media", len(refs))
	return Response{OK: true, Data: created}
}

// resolveMedia проверяет заявленные вложения и строит записи props из
// АВТОРИТЕТНЫХ метаданных.
//
// Из запроса берётся только media_id. Всё остальное — sha256, размер, тип,
// имя — читается из MediaStore: клиент не может объявить о вложении то, чего
// в нём нет (ADR-048 §3, §4).
//
// Правила допуска — ADR-048 §4: объект обязан существовать, лежать в ТОМ ЖЕ
// канале, что и пост, быть загружен ЭТИМ пользователем и иметь байты. Любая
// неудача — отказ, а не пропуск вложения: молча создать пост без вложения
// значило бы потерять то, что пользователь отправил.
func (h *PostHandler) resolveMedia(userID, channelID string, want []MediaRef) ([]MediaRef, error) {
	if len(want) == 0 {
		return nil, nil
	}
	svc := h.media
	out := make([]MediaRef, 0, len(want))
	seen := map[string]bool{}
	for _, w := range want {
		if w.MediaID == "" || seen[w.MediaID] {
			return nil, fmt.Errorf("%w: media_id is empty or duplicated", ErrMediaInvalidRequest)
		}
		seen[w.MediaID] = true

		obj, err := svc.Info(w.MediaID)
		if err != nil {
			return nil, err
		}
		if obj.ChannelID != channelID {
			return nil, ErrMediaNotFound
		}
		if obj.UploaderID != userID {
			return nil, ErrMediaNotFound
		}
		if obj.PostID != "" {
			return nil, ErrMediaAlreadyAttached
		}
		out = append(out, MediaRef{
			MediaID: obj.MediaID, SHA256: obj.SHA256, Size: obj.Size,
			Mime: obj.Mime, Name: obj.Name, W: obj.W, H: obj.H,
		})
	}
	return out, nil
}

// mediaPropsValue переводит записи в JSON-КАНОНИЧЕСКИЙ вид: map[string]any,
// []any, string, float64, bool, nil — и ничего больше.
//
// Причина не косметическая. Props (model.StringInterface = map[string]any)
// уезжают в хост через plugin API, а тот сериализуется gob'ом поверх net/rpc.
// gob кодирует значение, лежащее в интерфейсе, ПО ИМЕНИ типа, и имя обязано
// быть известно ОБЕИМ сторонам; хост регистрирует ровно шесть типов
// (plugin/client_rpc.go: []any, map[string]any, []*model.SlackAttachment,
// *model.AppError, *pq.Error, …). Типа []main.MediaRef в хосте нет ни в одном
// пакете, поэтому gob.Register на нашей стороне ничего не решает: он лишь
// перенёс бы отказ с кодирования у нас на декодирование у хоста.
//
// Второй довод сильнее первого и не зависит от gob: props хранятся в БД как
// JSON, и любой их читатель — webapp, наш же getFeed — получает именно эти
// формы. Записывая структуру, мы записывали бы то, чего при чтении не бывает.
func mediaPropsValue(refs []MediaRef) (any, error) {
	raw, err := json.Marshal(refs)
	if err != nil {
		return nil, fmt.Errorf("photon_media marshal: %w", err)
	}
	var canonical []any
	if err := json.Unmarshal(raw, &canonical); err != nil {
		return nil, fmt.Errorf("photon_media canonicalize: %w", err)
	}
	return canonical, nil
}

// mediaIDsFromPost — читатель того, что записал mediaPropsValue.
//
// Стоит рядом с писателем намеренно: разойдясь, они не сломают ни сборку, ни
// один существующий тест — хук просто перестанет находить вложения, и уборка
// C станет молча пустой. Обратный разбор проверяется тестом от writer'а к
// reader'у, а не двумя независимыми представлениями о форме.
//
// Разбор идёт через JSON, а не утверждением типа: значение приходит либо из
// БД ([]any из map[string]any), либо, в том же процессе, тем самым []any,
// который построил mediaPropsValue. Обе формы кодируются одинаково, и
// json.Marshal избавляет от перебора вариантов.
//
// Из записи берётся ТОЛЬКО media_id. Всё остальное в props — проекция, а
// авторитет принадлежит MediaStore (ADR-048 §7); читать оттуда размер или
// хеш значило бы верить проекции.
func mediaIDsFromPost(post *model.Post) []string {
	if post == nil {
		return nil
	}
	value, ok := post.Props["photon_media"]
	if !ok || value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var refs []struct {
		MediaID string `json:"media_id"`
	}
	if err := json.Unmarshal(raw, &refs); err != nil {
		// Props пришли не той формы. Молчать здесь правильно: пост уже удалён,
		// и уборка не должна становиться источником отказов.
		return nil
	}
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		if r.MediaID != "" {
			out = append(out, r.MediaID)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// getFeed — Action 101.
func (h *PostHandler) getFeed(userID string, raw json.RawMessage) Response {
	var p GetFeedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (getFeed)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	// Phase 12: limit — курсорное имя того же предела; per_page оставлен ради
	// совместимости с уже написанным клиентом и матрицей Phase 14.
	perPage := p.Limit
	if perPage <= 0 {
		perPage = p.PerPage
	}
	if perPage <= 0 {
		perPage = 30
	}

	// Курсорная выборка. Смещение внутри курсорного окна не используется
	// (page = 0): курсор и offset одновременно дали бы две несогласованные
	// системы координат, а вопрос «что такое вторая страница ПОСЛЕ курсора»
	// не имеет устойчивого ответа при вставке.
	var (
		postList *model.PostList
		appErr   *model.AppError
	)
	switch {
	case p.Before != "" && p.After != "":
		return Response{OK: false, Error: "before and after are mutually exclusive"}
	case p.Before != "":
		postList, appErr = h.plugin.API.GetPostsBefore(p.ChannelID, p.Before, 0, perPage)
	case p.After != "":
		postList, appErr = h.plugin.API.GetPostsAfter(p.ChannelID, p.After, 0, perPage)
	default:
		postList, appErr = h.plugin.API.GetPostsForChannel(p.ChannelID, p.Page, perPage)
	}
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPosts API error",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Feed fetched",
		"user_id", userID, "channel_id", p.ChannelID,
		"cursor", cursorLabel(p), "count", len(postList.Posts))
	return Response{OK: true, Data: postList}
}

// deletePost — Action 102.
func (h *PostHandler) deletePost(userID string, raw json.RawMessage) Response {
	var p DeletePostPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (deletePost)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" {
		return Response{OK: false, Error: "post_id is required"}
	}
	// Владельца больше не проверяем здесь: решение принимает ownerSel в
	// authz.go — селектор между delete_post и delete_others_posts (ADR-039 §3).
	// Прежняя проверка была второй точкой принятия решения и отказывала
	// администратору, у которого право на чужие посты есть; измерено на живом
	// сервере после развёртывания 0.4.0.
	if _, appErr := h.plugin.API.GetPost(p.PostID); appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPost API error",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	if appErr := h.plugin.API.DeletePost(p.PostID); appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] DeletePost API error",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Post deleted",
		"user_id", userID, "post_id", p.PostID)
	return Response{OK: true, Data: map[string]string{"deleted_post_id": p.PostID}}
}

// updatePost — Action 103.
func (h *PostHandler) updatePost(userID string, raw json.RawMessage) Response {
	var p UpdatePostPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (updatePost)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" || p.Message == "" {
		return Response{OK: false, Error: "post_id and message are required"}
	}
	existing, appErr := h.plugin.API.GetPost(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPost API error (updatePost)",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	// Владельца не проверяем: ownerSel выбрал edit_post либо edit_others_posts
	// ещё до входа в handler (ADR-039 §3, §6).
	existing.Message = p.Message
	updated, appErr := h.plugin.API.UpdatePost(existing)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] UpdatePost API error",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Post updated",
		"user_id", userID, "post_id", updated.Id)
	return Response{OK: true, Data: updated}
}

// getThread — Action 104.
func (h *PostHandler) getThread(userID string, raw json.RawMessage) Response {
	var p struct {
		PostID string `json:"post_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (getThread)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" {
		return Response{OK: false, Error: "post_id is required"}
	}
	thread, appErr := h.plugin.API.GetPostThread(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPostThread API error",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Thread fetched",
		"user_id", userID, "post_id", p.PostID, "count", len(thread.Posts))
	return Response{OK: true, Data: thread}
}

// replyToPost — Action 105.
func (h *PostHandler) replyToPost(userID string, raw json.RawMessage) Response {
	var p struct {
		RootID  string   `json:"root_id"`
		Message string   `json:"message"`
		FileIds []string `json:"file_ids,omitempty"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (replyToPost)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.RootID == "" || p.Message == "" {
		return Response{OK: false, Error: "root_id and message are required"}
	}
	root, appErr := h.plugin.API.GetPost(p.RootID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPost (root) API error",
			"user_id", userID, "root_id", p.RootID, "error", appErr.Error())
		return Response{OK: false, Error: "root post not found: " + appErr.Error()}
	}
	post := &model.Post{
		UserId:    userID,
		ChannelId: root.ChannelId,
		RootId:    p.RootID,
		Message:   p.Message,
		FileIds:   p.FileIds,
	}
	created, appErr := h.plugin.API.CreatePost(post)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] CreatePost (reply) API error",
			"user_id", userID, "root_id", p.RootID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Reply created",
		"user_id", userID, "post_id", created.Id, "root_id", p.RootID)
	return Response{OK: true, Data: created}
}

// pinPost — Action 106.
// Plugin API v11 не имеет PinPost — обновляем поле IsPinned через UpdatePost.
func (h *PostHandler) pinPost(userID string, raw json.RawMessage) Response {
	var p struct {
		PostID string `json:"post_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (pinPost)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" {
		return Response{OK: false, Error: "post_id is required"}
	}
	post, appErr := h.plugin.API.GetPost(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPost API error (pinPost)",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	post.IsPinned = true
	updated, appErr := h.plugin.API.UpdatePost(post)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] UpdatePost API error (pinPost)",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Post pinned",
		"user_id", userID, "post_id", p.PostID)
	return Response{OK: true, Data: updated}
}

// unpinPost — Action 107.
func (h *PostHandler) unpinPost(userID string, raw json.RawMessage) Response {
	var p struct {
		PostID string `json:"post_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (unpinPost)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" {
		return Response{OK: false, Error: "post_id is required"}
	}
	post, appErr := h.plugin.API.GetPost(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPost API error (unpinPost)",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	post.IsPinned = false
	updated, appErr := h.plugin.API.UpdatePost(post)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] UpdatePost API error (unpinPost)",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Post unpinned",
		"user_id", userID, "post_id", p.PostID)
	return Response{OK: true, Data: updated}
}

// getPinnedPosts — Action 108.
// Plugin API v11 не имеет GetPinnedPosts — ищем через SearchPostsInTeam с is_pinned.
// Если team_id неизвестен на клиенте, передаём channel_id и ищем в канале вручную.
func (h *PostHandler) getPinnedPosts(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (getPinnedPosts)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	// Получаем посты канала и фильтруем по IsPinned.
	// Ограничение: проверяем первые 200 постов.
	postList, appErr := h.plugin.API.GetPostsForChannel(p.ChannelID, 0, 200)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPostsForChannel API error (getPinnedPosts)",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	pinned := make([]*model.Post, 0)
	for _, post := range postList.Posts {
		if post.IsPinned {
			pinned = append(pinned, post)
		}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Pinned posts fetched",
		"user_id", userID, "channel_id", p.ChannelID, "count", len(pinned))
	return Response{OK: true, Data: map[string]interface{}{
		"posts": pinned,
		"count": len(pinned),
	}}
}

// searchPosts — Action 109.
func (h *PostHandler) searchPosts(userID string, raw json.RawMessage) Response {
	var p struct {
		TeamID     string `json:"team_id"`
		Query      string `json:"query"`
		IsOrSearch bool   `json:"is_or_search"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (searchPosts)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TeamID == "" || p.Query == "" {
		return Response{OK: false, Error: "team_id and query are required"}
	}
	// Scope layer, ADR-039 §12: вариант вызова С ПОЛЬЗОВАТЕЛЕМ. Прежний
	// SearchPostsInTeam ищет по всей команде без скоупа — измерено, что он
	// возвращал обычному пользователю посты из чужой личной переписки и с
	// чужой стены. Правило доступа к самому действию этого не чинит.
	results, appErr := h.plugin.API.SearchPostsInTeamForUser(p.TeamID, userID, model.SearchParameter{
		Terms:      &p.Query,
		IsOrSearch: &p.IsOrSearch,
	})
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] SearchPostsInTeam API error",
			"user_id", userID, "query", p.Query, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	posts := []*model.Post{}
	if results != nil && results.PostList != nil {
		posts = results.PostList.ToSlice()
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Posts searched",
		"user_id", userID, "query", p.Query, "results", len(posts))
	return Response{OK: true, Data: posts}
}

// addReaction — Action 110.
func (h *PostHandler) addReaction(userID string, raw json.RawMessage) Response {
	var p AddReactionPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (addReaction)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" || p.EmojiName == "" {
		return Response{OK: false, Error: "post_id and emoji_name are required"}
	}
	reaction, appErr := h.plugin.API.AddReaction(&model.Reaction{
		UserId:    userID,
		PostId:    p.PostID,
		EmojiName: p.EmojiName,
	})
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] AddReaction API error",
			"user_id", userID, "post_id", p.PostID, "emoji", p.EmojiName, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Reaction added",
		"user_id", userID, "post_id", p.PostID, "emoji", p.EmojiName)
	return Response{OK: true, Data: reaction}
}

// removeReaction — Action 111.
func (h *PostHandler) removeReaction(userID string, raw json.RawMessage) Response {
	var p RemoveReactionPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (removeReaction)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" || p.EmojiName == "" {
		return Response{OK: false, Error: "post_id and emoji_name are required"}
	}
	if appErr := h.plugin.API.RemoveReaction(&model.Reaction{
		UserId:    userID,
		PostId:    p.PostID,
		EmojiName: p.EmojiName,
	}); appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] RemoveReaction API error",
			"user_id", userID, "post_id", p.PostID, "emoji", p.EmojiName, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Reaction removed",
		"user_id", userID, "post_id", p.PostID, "emoji", p.EmojiName)
	return Response{OK: true, Data: map[string]string{"removed_emoji": p.EmojiName}}
}

// getReactions — Action 112.
func (h *PostHandler) getReactions(userID string, raw json.RawMessage) Response {
	var p struct {
		PostID string `json:"post_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (getReactions)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" {
		return Response{OK: false, Error: "post_id is required"}
	}
	reactions, appErr := h.plugin.API.GetReactions(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetReactions API error",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Reactions fetched",
		"user_id", userID, "post_id", p.PostID, "count", len(reactions))
	return Response{OK: true, Data: reactions}
}

// toggleLike — Action 113.
func (h *PostHandler) toggleLike(userID string, raw json.RawMessage) Response {
	var p struct {
		PostID string `json:"post_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (toggleLike)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" {
		return Response{OK: false, Error: "post_id is required"}
	}
	reactions, appErr := h.plugin.API.GetReactions(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetReactions API error (toggleLike)",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	const likeEmoji = "+1"
	alreadyLiked := false
	for _, r := range reactions {
		if r.UserId == userID && r.EmojiName == likeEmoji {
			alreadyLiked = true
			break
		}
	}
	reaction := &model.Reaction{
		UserId:    userID,
		PostId:    p.PostID,
		EmojiName: likeEmoji,
	}
	if alreadyLiked {
		if appErr := h.plugin.API.RemoveReaction(reaction); appErr != nil {
			h.plugin.API.LogError("[Photon][PostHandler] RemoveReaction (toggleLike) API error",
				"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
			return Response{OK: false, Error: appErr.Error()}
		}
		h.plugin.API.LogInfo("[Photon][PostHandler] Like removed",
			"user_id", userID, "post_id", p.PostID)
		return Response{OK: true, Data: map[string]bool{"liked": false}}
	}
	if _, appErr := h.plugin.API.AddReaction(reaction); appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] AddReaction (toggleLike) API error",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Like added",
		"user_id", userID, "post_id", p.PostID)
	return Response{OK: true, Data: map[string]bool{"liked": true}}
}

// getPostSocialInfo — Action 114.
func (h *PostHandler) getPostSocialInfo(userID string, raw json.RawMessage) Response {
	var p struct {
		PostID string `json:"post_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][PostHandler] JSON unmarshal error (getPostSocialInfo)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PostID == "" {
		return Response{OK: false, Error: "post_id is required"}
	}
	reactions, appErr := h.plugin.API.GetReactions(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetReactions API error (getPostSocialInfo)",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	counts := map[string]int{}
	myReactions := []string{}
	seenUsers := map[string]struct{}{}
	topUserIDs := []string{}
	for _, r := range reactions {
		counts[r.EmojiName]++
		if r.UserId == userID {
			myReactions = append(myReactions, r.EmojiName)
		}
		if _, seen := seenUsers[r.UserId]; !seen && len(topUserIDs) < 5 {
			seenUsers[r.UserId] = struct{}{}
			topUserIDs = append(topUserIDs, r.UserId)
		}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] SocialInfo fetched",
		"user_id", userID, "post_id", p.PostID, "total_reactions", len(reactions))
	return Response{OK: true, Data: map[string]interface{}{
		"reaction_counts": counts,
		"my_reactions":    myReactions,
		"top_user_ids":    topUserIDs,
		"total":           len(reactions),
	}}
}

// cursorLabel — что именно листали, одной строкой для лога.
func cursorLabel(p GetFeedPayload) string {
	switch {
	case p.Before != "":
		return "before:" + p.Before
	case p.After != "":
		return "after:" + p.After
	default:
		return "head"
	}
}
