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
	// media — ИСПОЛНИТЕЛЬ, а не хранилище. Прежде handler ходил в MediaStore
	// напрямую и повторял у себя правило «байты доступны?». Пока условие
	// сводилось к хранимому состоянию, две копии давали один ответ; с
	// появлением живости поста (ADR-056) они разошлись бы молча: 702 отдавал
	// бы метаданные объекта, чей пост мёртв, а 701 по тому же объекту —
	// отказывал. Найдено тестом до живого прогона.
	media *MediaService
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

	mo, err := h.media.Info(p.MediaID)
	if err != nil || mo == nil {
		// Различение задаёт mediaErrorText, и оно ровно то же, что на пути
		// стрима: unavailable — объект известен субъекту, байтов больше нет
		// (истёк, убран уборкой C или его пост мёртв); forbidden — объекта
		// нет ЛИБО нет права, намеренно неразличимо (ADR-039,
		// media-transfer.md §5).
		//
		// Отсутствие объекта здесь — гонка: authz его уже разрешил.
		reason := "forbidden"
		if err != nil {
			reason = mediaErrorText(err)
		}
		if reason == errForbidden.Error() {
			h.plugin.API.LogWarn("[Photon][MediaHandler] Media object vanished after authorization",
				"user_id", userID, "media_id", p.MediaID)
		}
		return Response{OK: false, Error: reason}
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
