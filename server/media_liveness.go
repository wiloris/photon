package main

import (
	"net/http"

	"github.com/mattermost/mattermost/server/public/model"
)

// Живость поста как условие отдачи media (ADR-056, уборка C-2).
//
// Зачем это понадобилось. Уборка C (ADR-055) убирает media по СОБЫТИЮ
// удаления, и live acceptance показал, что событий приходит не на все посты:
// при каскадном удалении треда MessageHasBeenDeleted приходит только для
// корня, а для ответов — не приходит вовсе. Их media остаётся AVAILABLE и
// по-прежнему скачивается. Догнать её после факта нечем: пред-хука на
// удаление в API НЕТ (в public@v0.1.21 из *WillBe* только MessageWillBePosted,
// MessageWillBeUpdated и MessagesWillBeConsumed), а перечислить удалённые
// посты нечем — параметра includeDeleted у постов не существует ни в одном
// методе. Измерено: getThread по каскадно удалённому корню отвечает отказом.
//
// Отсюда решение: не догонять событие, а СПРАШИВАТЬ при чтении. Объект,
// чей пост мёртв, не отдаётся — независимо от того, каким путём пост умер:
// каскадом, из webapp, по REST, retention'ом или до появления этой проверки.
//
// Это НЕ уборка и не подметание: ни байты, ни состояние не меняются, диск не
// освобождается (ADR-048 §12 B и D остаются открытыми). Меняется только
// ответ на чтение.

// PostLiveness — исход опроса живости поста. Три значения, а не два, и это
// главное в контракте: «не удалось спросить» обязано быть отличимо от
// «доказано мёртв», иначе сбой инфраструктуры погасил бы живые вложения.
type PostLiveness int

const (
	// PostUnknown — сигнала нет. Значение по умолчанию НАМЕРЕННО: забытый
	// оракул и неотвеченный вызов дают самый безопасный исход — отдать.
	PostUnknown PostLiveness = iota
	// PostAlive — пост существует, подтверждено.
	PostAlive
	// PostGone — пост ДОКАЗАНО отсутствует: 404 от прикладного слоя.
	PostGone
)

// postGetter — ровно та часть Plugin API, которая нужна оракулу. Интерфейс, а
// не *Plugin: то же, что делает authzAPI, и по той же причине — сервис не
// должен зависеть от всего API ради одного метода.
type postGetter interface {
	GetPost(postID string) (*model.Post, *model.AppError)
}

// logger — часть API, нужная для записи аномалий. Отдельно от postGetter,
// чтобы тестовый двойник не тащил заглушки логирования.
type logger interface {
	LogDebug(msg string, keyValuePairs ...any)
	LogWarn(msg string, keyValuePairs ...any)
}

// postLivenessOracle — опрос живости через Plugin API.
//
// РАЗБОР ИСХОДОВ — существо решения, а не деталь. Prior art: на клиенте
// ADR-054 уже принял ровно этот принцип («нет однозначного сигнала —
// сохраняем»), здесь он применён на сервере.
//
//	пост жив            → (post, nil)            → PostAlive
//	пост удалён         → (nil, 404)             → PostGone
//	RPC плагина мёртв   → (nil, nil)             → PostUnknown
//	инфраструктура      → (nil, 5xx)             → PostUnknown
//
// (nil, nil) — не выдумка и не перестраховка: сгенерированный клиент plugin
// API при отказе транспорта только пишет в лог и возвращает нули (тот же
// случай, ради которого стоит проверка created == nil в createPost). Считать
// его удалением значило бы гасить вложения всякий раз, когда умирает RPC.
//
// Мёртвым считается ТОЛЬКО 404. Любой другой код — включая 5xx — оставляет
// исход неизвестным, и объект отдаётся.
func postLivenessOracle(api postGetter, log logger) func(string) PostLiveness {
	return func(postID string) PostLiveness {
		if postID == "" {
			return PostUnknown
		}
		post, appErr := api.GetPost(postID)
		switch {
		case appErr == nil && post != nil:
			return PostAlive
		case appErr == nil:
			// Ни поста, ни ошибки — отказ транспорта plugin API.
			if log != nil {
				log.LogWarn("[Photon][Media] Liveness probe returned neither post nor error",
					"post_id", postID)
			}
			return PostUnknown
		case appErr.StatusCode == http.StatusNotFound:
			if log != nil {
				log.LogDebug("[Photon][Media] Post is gone",
					"post_id", postID, "error_id", appErr.Id)
			}
			return PostGone
		default:
			// 5xx и всё прочее: сигнала об отсутствии нет.
			if log != nil {
				log.LogWarn("[Photon][Media] Liveness probe failed",
					"post_id", postID, "status", appErr.StatusCode, "error_id", appErr.Id)
			}
			return PostUnknown
		}
	}
}
