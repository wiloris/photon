// Package ports — см. storage.go для общих правил пакета.
package ports

import (
	"context"
	"time"
)

// Media — порт обработки медиаданных (инспекция и превью).
// Первая реализация будет определена при выборе конкретной библиотеки
// обработки (номер фазы не закреплён в ROADMAP.md — см. заметку в
// Phase 1).
//
// Не порт хранения: байты медиафайла хранятся через Storage, как и
// любые другие данные (см. storage.go) — Media только обрабатывает
// уже полученные байты и не знает, откуда они и куда денутся после
// вызова (тот же принцип, что в Crypto/Identity: один порт — одна
// область ответственности).
//
// Не содержит URL/адресации — это деталь Bridge (Mattermost отдаёт
// файлы по HTTP), не концепт уровня Core. См. ADR-011/013.
//
// "Thumbnail"/"preview"/"full" из существующего FileResource
// (photon-quic-bridge) — не три разные операции, а один
// параметризованный запрос уменьшенной копии; "full" — исходные
// байты, которые уже лежат в Storage и не требуют Media вообще.
type Media interface {
	// Inspect определяет формат и параметры data (тип, размеры для
	// изображений, длительность для видео/аудио), не производя новых
	// байт. ErrUnsupportedFormat, если data не распознан адаптером.
	Inspect(ctx context.Context, data []byte) (MediaInfo, error)

	// Thumbnail строит уменьшенную копию data, вписанную в
	// maxWidth×maxHeight с сохранением пропорций. ErrUnsupportedFormat,
	// если data не распознан адаптером.
	Thumbnail(ctx context.Context, data []byte, maxWidth, maxHeight int) (thumbnail []byte, err error)
}

// MediaInfo — метаданные, извлечённые из медиаданных. Width/Height
// — 0, если неприменимо (например, аудио); Duration — 0, если
// неприменимо (например, изображение).
type MediaInfo struct {
	MimeType string
	Width    int
	Height   int
	Duration time.Duration
}
