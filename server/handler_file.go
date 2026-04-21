package main

import (
	"encoding/json"
	"fmt"
)

// FileHandler зеркалит FileResource из Kotlin SDK.
// Actions: 500=UploadFile, 501=DownloadFile, 502=GetFileInfo,
//          503=GetThumbnail, 504=GetPreview, 505=GetImageURL
//
// Важно: Plugin API v11 не предоставляет GetFileThumbnail/GetFilePreview —
// для 503 и 504 возвращаем URL, клиент загружает сам с Bearer-токеном.
type FileHandler struct {
	plugin *Plugin
}

func (h *FileHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionUploadFile:
		return h.uploadFile(userID, raw)
	case ActionDownloadFile:
		return h.downloadFile(userID, raw)
	case ActionGetFileInfo:
		return h.getFileInfo(userID, raw)
	case ActionGetThumbnail:
		return h.getThumbnail(userID, raw)
	case ActionGetPreview:
		return h.getPreview(userID, raw)
	case ActionGetImageURL:
		return h.getImageURL(userID, raw)
	default:
		return Response{OK: false, Error: fmt.Sprintf("FileResource: unknown action %d", action)}
	}
}

// uploadFile — Action 500.
func (h *FileHandler) uploadFile(userID string, raw json.RawMessage) Response {
	var p UploadFilePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][FileHandler] JSON unmarshal error (uploadFile)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" || p.FileName == "" || len(p.FileBytes) == 0 {
		return Response{OK: false, Error: "channel_id, file_name and file_bytes are required"}
	}
	info, appErr := h.plugin.API.UploadFile(p.FileBytes, p.ChannelID, p.FileName)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][FileHandler] UploadFile API error",
			"user_id", userID, "file_name", p.FileName, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][FileHandler] File uploaded",
		"user_id", userID, "file_id", info.Id, "file_name", p.FileName)
	return Response{OK: true, Data: info}
}

// downloadFile — Action 501. Возвращает байты файла через GetFile.
func (h *FileHandler) downloadFile(userID string, raw json.RawMessage) Response {
	var p FileIDPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][FileHandler] JSON unmarshal error (downloadFile)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.FileID == "" {
		return Response{OK: false, Error: "file_id is required"}
	}
	data, appErr := h.plugin.API.GetFile(p.FileID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][FileHandler] GetFile API error",
			"user_id", userID, "file_id", p.FileID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][FileHandler] File downloaded",
		"user_id", userID, "file_id", p.FileID, "bytes", len(data))
	return Response{OK: true, Data: map[string]interface{}{
		"file_id": p.FileID,
		"bytes":   data,
	}}
}

// getFileInfo — Action 502. Метаданные без байт.
func (h *FileHandler) getFileInfo(userID string, raw json.RawMessage) Response {
	var p FileIDPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][FileHandler] JSON unmarshal error (getFileInfo)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.FileID == "" {
		return Response{OK: false, Error: "file_id is required"}
	}
	info, appErr := h.plugin.API.GetFileInfo(p.FileID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][FileHandler] GetFileInfo API error",
			"user_id", userID, "file_id", p.FileID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][FileHandler] File info fetched",
		"user_id", userID, "file_id", p.FileID)
	return Response{OK: true, Data: info}
}

// getThumbnail — Action 503.
// GetFileThumbnail отсутствует в Plugin API — возвращаем URL,
// клиент делает HTTP GET с заголовком Authorization: Bearer <token>.
func (h *FileHandler) getThumbnail(userID string, raw json.RawMessage) Response {
	var p FileIDPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][FileHandler] JSON unmarshal error (getThumbnail)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.FileID == "" {
		return Response{OK: false, Error: "file_id is required"}
	}
	url := h.buildFileURL(p.FileID, "thumbnail")
	h.plugin.API.LogInfo("[Photon][FileHandler] Thumbnail URL built",
		"user_id", userID, "file_id", p.FileID)
	return Response{OK: true, Data: map[string]interface{}{
		"file_id": p.FileID,
		"url":     url,
	}}
}

// getPreview — Action 504.
// GetFilePreview отсутствует в Plugin API — возвращаем URL превью.
func (h *FileHandler) getPreview(userID string, raw json.RawMessage) Response {
	var p FileIDPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][FileHandler] JSON unmarshal error (getPreview)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.FileID == "" {
		return Response{OK: false, Error: "file_id is required"}
	}
	url := h.buildFileURL(p.FileID, "preview")
	h.plugin.API.LogInfo("[Photon][FileHandler] Preview URL built",
		"user_id", userID, "file_id", p.FileID)
	return Response{OK: true, Data: map[string]interface{}{
		"file_id": p.FileID,
		"url":     url,
	}}
}

// getImageURL — Action 505. file_type: "FULL" | "PREVIEW" | "THUMB"
func (h *FileHandler) getImageURL(userID string, raw json.RawMessage) Response {
	var p struct {
		FileID   string `json:"file_id"`
		FileType string `json:"file_type"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][FileHandler] JSON unmarshal error (getImageURL)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.FileID == "" {
		return Response{OK: false, Error: "file_id is required"}
	}
	var url string
	switch p.FileType {
	case "THUMB":
		url = h.buildFileURL(p.FileID, "thumbnail")
	case "PREVIEW":
		url = h.buildFileURL(p.FileID, "preview")
	default: // "FULL" или пусто
		url = h.buildFileURL(p.FileID, "")
	}
	h.plugin.API.LogInfo("[Photon][FileHandler] Image URL built",
		"user_id", userID, "file_id", p.FileID, "type", p.FileType)
	return Response{OK: true, Data: map[string]interface{}{
		"file_id":   p.FileID,
		"file_type": p.FileType,
		"url":       url,
	}}
}

// buildFileURL — строит URL к файлу на основе SiteURL из конфига.
// suffix: "" = полный файл, "thumbnail", "preview"
func (h *FileHandler) buildFileURL(fileID, suffix string) string {
	cfg := h.plugin.API.GetConfig()
	baseURL := ""
	if cfg.ServiceSettings.SiteURL != nil {
		baseURL = *cfg.ServiceSettings.SiteURL
	}
	if suffix == "" {
		return fmt.Sprintf("%s/api/v4/files/%s", baseURL, fileID)
	}
	return fmt.Sprintf("%s/api/v4/files/%s/%s", baseURL, fileID, suffix)
}