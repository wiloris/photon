package main

import (
	"encoding/json"
	"fmt"
)

// MediaHandler — действия MediaResource (700-702), приходящие по обычному
// JSON-пути.
//
// Существенное различие, которое видно уже здесь: 702 (метаданные) — обычное
// действие и обслуживается полностью. 700 и 701 несут двоичный хвост после двух
// JSON-кадров (media-transfer.md §1) и потому НЕ МОГУТ быть выполнены на этом
// пути — ни сейчас, ни потом. Их обслуживает отдельная ветка стрима, которая
// разбирает кадр сама и получает поток; сюда они попадают только если клиент
// прислал их как обычное действие, например через HTTP-вход.
//
// Отказ для них — не заглушка и не «пока не реализовано», а постоянный и
// правильный ответ: у этого входа нет потока, а без потока действие не имеет
// смысла.
type MediaHandler struct {
	plugin *Plugin
	store  MediaStore
}

func (h *MediaHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionMediaGetInfo:
		return h.getInfo(userID, raw)
	case ActionMediaUpload, ActionMediaDownload:
		return Response{OK: false, Error: "invalid: media transfer requires a media stream"}
	default:
		return Response{OK: false, Error: fmt.Sprintf("MediaResource: unknown action %d", action)}
	}
}

// getInfo — Action 702. Метаданные объекта без тела.
//
// Авторизация уже выполнена Dispatcher.Authorize через objMedia: субъект,
// дошедший сюда, имеет право читать канал этого объекта. Поэтому здесь
// различаются только два исхода — объект доступен или его байтов больше нет.
func (h *MediaHandler) getInfo(userID string, raw json.RawMessage) Response {
	var p MediaIDPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][MediaHandler] JSON unmarshal error (getInfo)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid: " + err.Error()}
	}
	if p.MediaID == "" {
		return Response{OK: false, Error: "invalid: media_id is required"}
	}

	mo, err := h.store.Get(p.MediaID)
	if err != nil || mo == nil {
		// Сюда можно попасть только гонкой: authz уже разрешил объект. Ответ
		// тот же, что при отсутствии права, — по той же причине, по которой
		// он один и тот же в authz (ADR-039, media-transfer.md §5).
		h.plugin.API.LogWarn("[Photon][MediaHandler] Media object vanished after authorization",
			"user_id", userID, "media_id", p.MediaID)
		return Response{OK: false, Error: errForbidden.Error()}
	}
	if !mo.HasBytes() {
		// unavailable, а не forbidden: субъект уже имеет право видеть этот
		// объект, и утечки существования здесь нет — он узнаёт только, что
		// байтов больше нет (media-transfer.md §5).
		return Response{OK: false, Error: "unavailable"}
	}

	h.plugin.API.LogInfo("[Photon][MediaHandler] Media info fetched",
		"user_id", userID, "media_id", mo.MediaID)
	return Response{OK: true, Data: MediaInfoResponse{Media: MediaRef{
		MediaID: mo.MediaID,
		SHA256:  mo.SHA256,
		Size:    mo.Size,
		Mime:    mo.Mime,
		Name:    mo.Name,
		W:       mo.W,
		H:       mo.H,
	}}}
}
