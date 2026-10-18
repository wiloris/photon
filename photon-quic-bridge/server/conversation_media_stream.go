package main

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/quic-go/quic-go"
)

// Потоковые 703/704 (ADR-124 M1, contract §8.3–§8.4). Кадр — ADR-049; права —
// convService по таблицам Bridge, авторизация входа уже выполнена
// Dispatcher.Authorize (правило «аутентифицирован»).

// convMediaResetCode — код сброса стрима 704 после кадра ответа (Н-1 (а)):
// клиент видит обрыв, а не тихое ожидание; ресурсы Bridge освобождаются без
// закрытия соединения. Значение — диагностика, протоколом не толкуется.
const convMediaResetCode quic.StreamErrorCode = 0x704

func (ql *QuicListener) convService() *convService {
	if ql.dispatcher == nil || ql.dispatcher.conversation == nil {
		return nil
	}
	return ql.dispatcher.conversation.service
}

// convMediaUpload — 703. Тело читается потоково со скользящим дедлайном, как
// у 700; ответ — только после фиксации строки (contract §8.3).
func (ql *QuicListener) convMediaUpload(stream *quic.Stream, dec *json.Decoder, userID string, payload json.RawMessage) {
	svc := ql.convService()
	if svc == nil {
		writeResponse(stream, Response{OK: false, Error: errCodeTemporarilyUnavailable})
		return
	}
	body := deadlineReader{stream: stream, body: bodyAfterFrames(dec, stream), d: mediaStreamDeadline}
	ctx, cancel := context.WithTimeout(context.Background(), convCallTimeout)
	defer cancel()
	resp := svc.uploadMedia(ctx, ql.dispatcher.mediaSvc(), userID, payload, body)
	if !resp.OK {
		ql.plugin.API.LogWarn("[Photon][Media] 703 rejected", "user_id", userID, "error", resp.Error)
	}
	writeResponse(stream, resp)
}

// convMediaDownload — 704. Проверка доступа на каждом запросе; кадр ответа
// перед телом; отказ после кадра — сброс стрима (contract §8.4).
func (ql *QuicListener) convMediaDownload(stream *quic.Stream, userID string, payload json.RawMessage) {
	svc := ql.convService()
	if svc == nil {
		writeResponse(stream, Response{OK: false, Error: errCodeTemporarilyUnavailable})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), convCallTimeout)
	m, code := svc.mediaForDownload(ctx, userID, payload)
	cancel()
	if code != "" {
		writeResponse(stream, Response{OK: false, Error: code})
		return
	}
	media := ql.dispatcher.mediaSvc()
	obj, err := media.Info(m.mediaID)
	// Строка есть, а файла нет или он не тот: сообщение остаётся, байтов нет
	// (contract §8.10 п. 9). Расхождение файла и строки — тоже «нет байтов»:
	// отдавать содержимое, не совпадающее с привязанным, нельзя.
	if err != nil || obj == nil || obj.ConversationID != m.convID || obj.Size != m.size || obj.SHA256 != m.sha256 {
		if err != nil && !errors.Is(err, ErrMediaUnavailable) && !errors.Is(err, ErrMediaNotFound) {
			ql.plugin.API.LogError("[Photon][Media] 704 info failed", "media_id", m.mediaID, "error", err.Error())
		} else {
			ql.plugin.API.LogWarn("[Photon][Media] 704 bytes unavailable", "media_id", m.mediaID)
		}
		writeResponse(stream, Response{OK: false, Error: convMediaUnavailable})
		return
	}
	a := m.attachment()
	if err := json.NewEncoder(stream).Encode(mediaResponse{OK: true, Data: a, BodyLength: m.size}); err != nil {
		stream.CancelWrite(convMediaResetCode)
		return
	}
	if _, err := media.Download(m.mediaID, deadlineWriter{stream: stream, d: mediaStreamDeadline}); err != nil {
		ql.plugin.API.LogError("[Photon][Media] 704 failed after header", "media_id", m.mediaID, "error", err.Error())
		stream.CancelWrite(convMediaResetCode)
		return
	}
	ql.plugin.API.LogInfo("[Photon][Media] 704 downloaded", "user_id", userID, "media_id", m.mediaID, "size", m.size)
}
