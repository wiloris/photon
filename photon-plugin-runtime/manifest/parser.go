package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Parser отвечает ровно на один вопрос:
//
//	«Можно ли превратить эти байты в структуру Manifest?»
//
// Он НЕ отвечает на вопрос «допустим ли и безопасен ли этот манифест» —
// это работа валидатора. Разделение не стилистическое: манифест с
// entrypoint="../../evil" синтаксически безупречен и обязан успешно
// разбираться, чтобы затем быть отвергнутым валидатором с внятной
// причиной. Смешение двух ответственностей дало бы ошибки вида
// "parse error" там, где на самом деле нарушение безопасности.

// maxManifestSize — предел размера входа.
//
// Проверяется ДО разбора: манифест приходит из недоверенного пакета, и
// без предела файл на несколько гигабайт вызвал бы попытку прочитать
// его целиком в память. Тот же принцип, что в adapters/tcp, где длина
// кадра валидируется до аллокации буфера.
const maxManifestSize = 1 << 20 // 1 MiB

var (
	// ErrManifestTooLarge — вход превышает maxManifestSize.
	ErrManifestTooLarge = errors.New("manifest: input exceeds maximum size")

	// ErrMalformedJSON — вход не является корректным JSON.
	ErrMalformedJSON = errors.New("manifest: malformed JSON")

	// ErrUnknownRequiredField — присутствует неизвестное поле с
	// префиксом "required_". См. ниже, почему это ошибка парсера.
	ErrUnknownRequiredField = errors.New("manifest: unsupported required field")
)

// Parse разбирает манифест из r.
//
// Возвращает *Manifest при синтаксическом успехе. Проверки
// допустимости (пути, версии, имена прав) выполняет Validate.
func Parse(r io.Reader) (*Manifest, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxManifestSize+1))
	if err != nil {
		return nil, fmt.Errorf("manifest: read: %w", err)
	}
	if len(data) > maxManifestSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrManifestTooLarge, len(data))
	}
	return ParseBytes(data)
}

// ParseBytes разбирает манифест из среза байт.
func ParseBytes(data []byte) (*Manifest, error) {
	if len(data) > maxManifestSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrManifestTooLarge, len(data))
	}

	// Сначала — в generic map, чтобы обнаружить неизвестные поля
	// required_*. Строгий json.Decoder с DisallowUnknownFields здесь не
	// подходит: он отверг бы ЛЮБОЕ неизвестное поле, лишив формат
	// forward compatibility, которая специально заложена в
	// docs/manifest.md.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedJSON, err)
	}

	// Неизвестное поле required_* — отказ; неизвестное обычное —
	// игнорируется.
	//
	// Асимметрия намеренная (docs/manifest.md): автор манифеста,
	// пометивший поле как required_, считает его обязательным для
	// безопасной работы. Старый Runtime, не знающий этого поля, не
	// может выполнить требование — и тихо запустить плагин без него
	// означало бы запустить его без режима безопасности, который автор
	// счёл обязательным. Неизвестное необязательное поле — вопрос
	// удобства; неизвестное обязательное — вопрос безопасности.
	//
	// Проверка выполняется в парсере, а не в валидаторе, потому что
	// относится к самой возможности интерпретировать документ: если
	// здесь есть требование, которого мы не понимаем, дальнейший разбор
	// бессмыслен.
	for key := range raw {
		if !strings.HasPrefix(key, "required_") {
			continue
		}
		if !isKnownRequiredField(key) {
			return nil, fmt.Errorf("%w: %q", ErrUnknownRequiredField, key)
		}
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedJSON, err)
	}
	return &m, nil
}

// knownRequiredFields — поля с префиксом required_, которые этот
// Runtime понимает. Сейчас пуст: механизм заложен заранее, чтобы
// будущее добавление обязательного поля корректно отвергалось старыми
// версиями Runtime, а не игнорировалось ими.
var knownRequiredFields = map[string]bool{}

func isKnownRequiredField(key string) bool {
	return knownRequiredFields[key]
}
