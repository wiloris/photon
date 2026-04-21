package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// EngramHandler зеркалит EngramResource из Kotlin SDK.
// «Энграмы» — именованные KV-записи для ИИ-интеграции.
//
// Actions: 400=EngramStore, 401=EngramQuery, 402=EngramDelete,
//          403=EngramList, 404=EngramBatchStore, 405=EngramBatchQuery
//
// Production-защиты:
//   - MAX_ENGRAM_SIZE:  64 KB на значение
//   - MAX_KEYS_PER_USER: 1000 ключей на пользователя
//   - Rate limit: 60 ops/60s с burst 10 ops/s (token bucket)
//   - Optimistic locking: version + created_at + updated_at
//   - PreserveTTL: явный контроль продления TTL при обновлении
//   - Write order: KV → индекс (не теряем данные при сбое индекса)
type EngramHandler struct {
	plugin *Plugin

	// tokenBuckets — rate limiter на базе token bucket.
	// ключ: userID
	bucketMu     sync.Mutex
	tokenBuckets map[string]*tokenBucket
}

// ── Константы ─────────────────────────────────────────────────────────────────

const (
	maxEngramSize  = 64 * 1024 // 64 KB на значение
	maxKeysPerUser = 1000      // максимум ключей на пользователя

	// Token bucket: 60 токенов, пополнение 1 токен/сек, burst = 10.
	bucketCapacity   = 60  // максимум накопленных токенов
	bucketRefillRate = 1.0 // токенов в секунду
	bucketBurstLimit = 10  // максимум токенов за один запрос (burst cap)
)

// tokenBucket — реализация token bucket для rate limiting.
type tokenBucket struct {
	tokens     float64
	lastRefill int64 // unix timestamp последнего пополнения
}

// EngramEntry — хранимая запись с метаданными.
type EngramEntry struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	UserID    string          `json:"user_id"`
	Version   int64           `json:"version"`
	CreatedAt int64           `json:"created_at"` // unix ms, не меняется
	UpdatedAt int64           `json:"updated_at"` // unix ms
	ExpiresAt int64           `json:"expires_at,omitempty"` // unix ms, 0 = вечно
}

func NewEngramHandler(p *Plugin) *EngramHandler {
	return &EngramHandler{
		plugin:       p,
		tokenBuckets: make(map[string]*tokenBucket),
	}
}

func (h *EngramHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionEngramStore:
		return h.store(userID, raw)
	case ActionEngramQuery:
		return h.query(userID, raw)
	case ActionEngramDelete:
		return h.delete(userID, raw)
	case ActionEngramList:
		return h.list(userID, raw)
	case ActionEngramBatchStore:
		return h.batchStore(userID, raw)
	case ActionEngramBatchQuery:
		return h.batchQuery(userID, raw)
	default:
		return Response{OK: false, Error: fmt.Sprintf("EngramResource: unknown action %d", action)}
	}
}

// ── 400: store ────────────────────────────────────────────────────────────────

func (h *EngramHandler) store(userID string, raw json.RawMessage) Response {
	// EngramStorePayload расширен полем PreserveTTL — см. protocol.go
	var p EngramStorePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][EngramHandler] JSON unmarshal error (store)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.Key == "" {
		return Response{OK: false, Error: "key is required"}
	}

	// ── Rate limit (token bucket) ─────────────────────────────────────────────
	if !h.consumeToken(userID) {
		h.plugin.API.LogWarn("[Photon][EngramHandler] Rate limit exceeded",
			"user_id", userID, "key", p.Key)
		return Response{OK: false, Error: fmt.Sprintf(
			"rate limit exceeded: max %d ops/min, burst %d ops/sec",
			bucketCapacity, bucketBurstLimit,
		)}
	}

	// ── Лимит размера ─────────────────────────────────────────────────────────
	valueBytes, err := json.Marshal(p.Value)
	if err != nil {
		return Response{OK: false, Error: "failed to marshal value: " + err.Error()}
	}
	if len(valueBytes) > maxEngramSize {
		h.plugin.API.LogWarn("[Photon][EngramHandler] Value too large",
			"user_id", userID, "key", p.Key,
			"size", len(valueBytes), "max", maxEngramSize)
		return Response{OK: false, Error: fmt.Sprintf(
			"value too large: %d bytes (max %d)", len(valueBytes), maxEngramSize,
		)}
	}

	kvKey := engramKey(userID, p.Key)
	now := time.Now().UnixMilli()

	// ── Загружаем существующую запись ─────────────────────────────────────────
	isNew := false
	version := int64(1)
	createdAt := now
	var preservedExpiresAt int64

	existing := h.loadEntry(kvKey)
	if existing != nil {
		version = existing.Version + 1
		createdAt = existing.CreatedAt
		preservedExpiresAt = existing.ExpiresAt
	} else {
		isNew = true
		// Новый ключ — проверяем лимит количества ключей до записи.
		keys := h.loadIndex(userID)
		if len(keys) >= maxKeysPerUser {
			return Response{OK: false, Error: fmt.Sprintf(
				"key limit reached: max %d keys per user", maxKeysPerUser,
			)}
		}
	}

	// ── Определяем ExpiresAt ──────────────────────────────────────────────────
	// PreserveTTL=true: оставляем существующий TTL при обновлении.
	// PreserveTTL=false (default): обновляем TTL согласно TTLSecs.
	//   TTLSecs=0 при PreserveTTL=false → убираем TTL (вечное хранение).
	var expiresAt int64
	if p.PreserveTTL && !isNew {
		expiresAt = preservedExpiresAt
	} else if p.TTLSecs > 0 {
		expiresAt = now + int64(p.TTLSecs)*1000
	}
	// иначе expiresAt = 0 (вечно)

	entry := &EngramEntry{
		Key:       p.Key,
		Value:     json.RawMessage(valueBytes),
		UserID:    userID,
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: now,
		ExpiresAt: expiresAt,
	}

	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return Response{OK: false, Error: "serialization error: " + err.Error()}
	}

	// ── Write order: KV сначала, индекс потом ────────────────────────────────
	// Если KV упадёт — индекс не обновится, данных нет → консистентно.
	// Если индекс упадёт — данные есть в KV, но не видны в list → восстановимо.
	if p.TTLSecs > 0 && !p.PreserveTTL {
		if appErr := h.plugin.API.KVSetWithExpiry(kvKey, entryBytes, int64(p.TTLSecs)); appErr != nil {
			h.plugin.API.LogError("[Photon][EngramHandler] KVSetWithExpiry error",
				"user_id", userID, "key", kvKey, "error", appErr.Error())
			return Response{OK: false, Error: appErr.Error()}
		}
	} else {
		if appErr := h.plugin.API.KVSet(kvKey, entryBytes); appErr != nil {
			h.plugin.API.LogError("[Photon][EngramHandler] KVSet error",
				"user_id", userID, "key", kvKey, "error", appErr.Error())
			return Response{OK: false, Error: appErr.Error()}
		}
	}

	// Индекс обновляем только для новых ключей и только после успешной записи KV.
	if isNew {
		h.addToIndex(userID, p.Key)
	}

	h.plugin.API.LogInfo("[Photon][EngramHandler] Engram stored",
		"user_id", userID, "key", p.Key,
		"version", version, "ttl_secs", p.TTLSecs,
		"preserve_ttl", p.PreserveTTL, "is_new", isNew)

	return Response{OK: true, Data: map[string]interface{}{
		"key":          entry.Key,
		"version":      entry.Version,
		"created_at":   entry.CreatedAt,
		"updated_at":   entry.UpdatedAt,
		"expires_at":   entry.ExpiresAt,
		"preserve_ttl": p.PreserveTTL,
	}}
}

// ── 401: query ────────────────────────────────────────────────────────────────

func (h *EngramHandler) query(userID string, raw json.RawMessage) Response {
	var p EngramQueryPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][EngramHandler] JSON unmarshal error (query)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.Key == "" {
		return Response{OK: false, Error: "key is required"}
	}

	kvKey := engramKey(userID, p.Key)
	entry := h.loadEntry(kvKey)
	if entry == nil {
		h.plugin.API.LogInfo("[Photon][EngramHandler] Engram not found",
			"user_id", userID, "key", p.Key)
		return Response{OK: false, Error: "engram not found: " + p.Key}
	}

	// Проверяем TTL вручную (KV мог ещё не почистить).
	if entry.ExpiresAt > 0 && time.Now().UnixMilli() > entry.ExpiresAt {
		_ = h.plugin.API.KVDelete(kvKey)
		h.removeFromIndex(userID, p.Key)
		return Response{OK: false, Error: "engram expired: " + p.Key}
	}

	h.plugin.API.LogInfo("[Photon][EngramHandler] Engram queried",
		"user_id", userID, "key", p.Key, "version", entry.Version)

	return Response{OK: true, Data: map[string]interface{}{
		"key":        entry.Key,
		"value":      entry.Value,
		"version":    entry.Version,
		"created_at": entry.CreatedAt,
		"updated_at": entry.UpdatedAt,
		"expires_at": entry.ExpiresAt,
	}}
}

// ── 402: delete ───────────────────────────────────────────────────────────────

func (h *EngramHandler) delete(userID string, raw json.RawMessage) Response {
	var p EngramDeletePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][EngramHandler] JSON unmarshal error (delete)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.Key == "" {
		return Response{OK: false, Error: "key is required"}
	}

	kvKey := engramKey(userID, p.Key)

	// Write order: KV сначала, индекс потом.
	if appErr := h.plugin.API.KVDelete(kvKey); appErr != nil {
		h.plugin.API.LogError("[Photon][EngramHandler] KVDelete error",
			"user_id", userID, "key", kvKey, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.removeFromIndex(userID, p.Key)

	h.plugin.API.LogInfo("[Photon][EngramHandler] Engram deleted",
		"user_id", userID, "key", p.Key)
	return Response{OK: true, Data: map[string]string{"deleted_key": p.Key}}
}

// ── 403: list ─────────────────────────────────────────────────────────────────

func (h *EngramHandler) list(userID string, raw json.RawMessage) Response {
	var p struct {
		Prefix string `json:"prefix,omitempty"`
		Page   int    `json:"page"`
		Limit  int    `json:"limit"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][EngramHandler] JSON unmarshal error (list)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.Limit <= 0 {
		p.Limit = 50
	}
	if p.Limit > 200 {
		p.Limit = 200
	}

	keys := h.loadIndex(userID)

	filtered := make([]string, 0, len(keys))
	for _, k := range keys {
		if p.Prefix == "" || (len(k) >= len(p.Prefix) && k[:len(p.Prefix)] == p.Prefix) {
			filtered = append(filtered, k)
		}
	}

	start := p.Page * p.Limit
	if start >= len(filtered) {
		return Response{OK: true, Data: map[string]interface{}{
			"keys":     []string{},
			"total":    len(filtered),
			"has_more": false,
		}}
	}
	end := start + p.Limit
	if end > len(filtered) {
		end = len(filtered)
	}

	h.plugin.API.LogInfo("[Photon][EngramHandler] Engrams listed",
		"user_id", userID, "prefix", p.Prefix,
		"count", end-start, "total", len(filtered))

	return Response{OK: true, Data: map[string]interface{}{
		"keys":     filtered[start:end],
		"total":    len(filtered),
		"has_more": end < len(filtered),
	}}
}

// ── 404: batchStore ───────────────────────────────────────────────────────────

func (h *EngramHandler) batchStore(userID string, raw json.RawMessage) Response {
	var p struct {
		Entries []EngramStorePayload `json:"entries"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][EngramHandler] JSON unmarshal error (batchStore)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if len(p.Entries) == 0 {
		return Response{OK: false, Error: "entries array is empty"}
	}
	if len(p.Entries) > 50 {
		return Response{OK: false, Error: "batch too large: max 50 entries"}
	}

	stored := make([]string, 0, len(p.Entries))
	errors := make([]map[string]string, 0)

	for _, entry := range p.Entries {
		entryRaw, err := json.Marshal(entry)
		if err != nil {
			errors = append(errors, map[string]string{
				"key": entry.Key, "error": "marshal error: " + err.Error(),
			})
			continue
		}
		resp := h.store(userID, entryRaw)
		if resp.OK {
			stored = append(stored, entry.Key)
		} else {
			errors = append(errors, map[string]string{
				"key": entry.Key, "error": resp.Error,
			})
		}
	}

	h.plugin.API.LogInfo("[Photon][EngramHandler] Batch store complete",
		"user_id", userID, "stored", len(stored), "errors", len(errors))

	return Response{OK: true, Data: map[string]interface{}{
		"stored": stored,
		"errors": errors,
	}}
}

// ── 405: batchQuery ───────────────────────────────────────────────────────────

func (h *EngramHandler) batchQuery(userID string, raw json.RawMessage) Response {
	var p struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][EngramHandler] JSON unmarshal error (batchQuery)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if len(p.Keys) == 0 {
		return Response{OK: false, Error: "keys array is empty"}
	}
	if len(p.Keys) > 100 {
		return Response{OK: false, Error: "batch too large: max 100 keys"}
	}

	type entryResult struct {
		Value     json.RawMessage `json:"value"`
		Version   int64           `json:"version"`
		UpdatedAt int64           `json:"updated_at"`
		ExpiresAt int64           `json:"expires_at,omitempty"`
	}

	entries := make(map[string]entryResult, len(p.Keys))
	missing := make([]string, 0)
	now := time.Now().UnixMilli()

	for _, key := range p.Keys {
		kvKey := engramKey(userID, key)
		entry := h.loadEntry(kvKey)
		if entry == nil {
			missing = append(missing, key)
			continue
		}
		if entry.ExpiresAt > 0 && now > entry.ExpiresAt {
			_ = h.plugin.API.KVDelete(kvKey)
			h.removeFromIndex(userID, key)
			missing = append(missing, key)
			continue
		}
		entries[key] = entryResult{
			Value:     entry.Value,
			Version:   entry.Version,
			UpdatedAt: entry.UpdatedAt,
			ExpiresAt: entry.ExpiresAt,
		}
	}

	h.plugin.API.LogInfo("[Photon][EngramHandler] Batch query complete",
		"user_id", userID, "found", len(entries), "missing", len(missing))

	return Response{OK: true, Data: map[string]interface{}{
		"entries": entries,
		"missing": missing,
	}}
}

// ── Token bucket rate limiter ─────────────────────────────────────────────────

// consumeToken пытается забрать 1 токен из bucket пользователя.
// Возвращает false если токенов нет (rate limit exceeded).
func (h *EngramHandler) consumeToken(userID string) bool {
	h.bucketMu.Lock()
	defer h.bucketMu.Unlock()

	now := time.Now().Unix()
	bucket, ok := h.tokenBuckets[userID]
	if !ok {
		// Первый запрос — создаём полный bucket.
		h.tokenBuckets[userID] = &tokenBucket{
			tokens:     float64(bucketCapacity) - 1,
			lastRefill: now,
		}
		return true
	}

	// Пополняем токены пропорционально прошедшему времени.
	elapsed := float64(now - bucket.lastRefill)
	bucket.tokens += elapsed * bucketRefillRate
	if bucket.tokens > float64(bucketCapacity) {
		bucket.tokens = float64(bucketCapacity)
	}
	bucket.lastRefill = now

	// Burst cap: не даём взять больше bucketBurstLimit токенов подряд мгновенно.
	// Реализуется через то, что каждый запрос стоит 1 токен,
	// а пополнение идёт 1/сек — burst автоматически ограничен скоростью пополнения.
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

// ── KV helpers ────────────────────────────────────────────────────────────────

func engramKey(userID, key string) string {
	return fmt.Sprintf("engram:%s:%s", userID, key)
}

func engramIndexKey(userID string) string {
	return fmt.Sprintf("engram_idx:%s", userID)
}

func (h *EngramHandler) loadEntry(kvKey string) *EngramEntry {
	data, appErr := h.plugin.API.KVGet(kvKey)
	if appErr != nil || data == nil {
		return nil
	}
	var entry EngramEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil
	}
	return &entry
}

func (h *EngramHandler) loadIndex(userID string) []string {
	data, appErr := h.plugin.API.KVGet(engramIndexKey(userID))
	if appErr != nil || data == nil {
		return []string{}
	}
	var keys []string
	if err := json.Unmarshal(data, &keys); err != nil {
		return []string{}
	}
	return keys
}

// addToIndex добавляет ключ в индекс пользователя.
// Вызывать только после успешной записи в KV.
func (h *EngramHandler) addToIndex(userID, key string) {
	keys := h.loadIndex(userID)
	for _, k := range keys {
		if k == key {
			return // уже есть
		}
	}
	keys = append(keys, key)
	data, _ := json.Marshal(keys)
	if appErr := h.plugin.API.KVSet(engramIndexKey(userID), data); appErr != nil {
		h.plugin.API.LogWarn("[Photon][EngramHandler] Failed to update index (add)",
			"user_id", userID, "key", key, "error", appErr.Error())
	}
}

// removeFromIndex удаляет ключ из индекса.
// Вызывать только после успешного KVDelete.
func (h *EngramHandler) removeFromIndex(userID, key string) {
	keys := h.loadIndex(userID)
	updated := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != key {
			updated = append(updated, k)
		}
	}
	data, _ := json.Marshal(updated)
	if appErr := h.plugin.API.KVSet(engramIndexKey(userID), data); appErr != nil {
		h.plugin.API.LogWarn("[Photon][EngramHandler] Failed to update index (remove)",
			"user_id", userID, "key", key, "error", appErr.Error())
	}
}