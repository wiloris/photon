package main

import (
	"encoding/json"
	"fmt"

	"github.com/mattermost/mattermost/server/public/model"
)

// PostHandler зеркалит PostResource из Kotlin SDK.
// Actions: 100=CreatePost, 101=GetFeed, 102=DeletePost, 103=UpdatePost,
//          104=GetThread, 105=ReplyToPost, 106=PinPost, 107=UnpinPost,
//          108=GetPinnedPosts, 109=SearchPosts, 110=AddReaction,
//          111=RemoveReaction, 112=GetReactions, 113=ToggleLike,
//          114=GetPostSocialInfo
type PostHandler struct {
	plugin *Plugin
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
	post := &model.Post{
		UserId:    userID,
		ChannelId: p.ChannelID,
		Message:   p.Message,
		RootId:    p.RootID,
		FileIds:   p.FileIds,
	}
	created, appErr := h.plugin.API.CreatePost(post)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] CreatePost API error",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Post created",
		"user_id", userID, "post_id", created.Id, "channel_id", p.ChannelID)
	return Response{OK: true, Data: created}
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
	if p.PerPage <= 0 {
		p.PerPage = 30
	}
	postList, appErr := h.plugin.API.GetPostsForChannel(p.ChannelID, p.Page, p.PerPage)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPostsForChannel API error",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][PostHandler] Feed fetched",
		"user_id", userID, "channel_id", p.ChannelID,
		"page", p.Page, "count", len(postList.Posts))
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
	existing, appErr := h.plugin.API.GetPost(p.PostID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] GetPost API error",
			"user_id", userID, "post_id", p.PostID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	if existing.UserId != userID {
		h.plugin.API.LogWarn("[Photon][PostHandler] Delete forbidden — not owner",
			"user_id", userID, "post_id", p.PostID, "owner_id", existing.UserId)
		return Response{OK: false, Error: "forbidden: not the post owner"}
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
	if existing.UserId != userID {
		h.plugin.API.LogWarn("[Photon][PostHandler] Update forbidden — not owner",
			"user_id", userID, "post_id", p.PostID, "owner_id", existing.UserId)
		return Response{OK: false, Error: "forbidden: not the post owner"}
	}
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
	posts, appErr := h.plugin.API.SearchPostsInTeam(p.TeamID, []*model.SearchParams{
		{Terms: p.Query, IsHashtag: false, OrTerms: p.IsOrSearch},
	})
	if appErr != nil {
		h.plugin.API.LogError("[Photon][PostHandler] SearchPostsInTeam API error",
			"user_id", userID, "query", p.Query, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
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