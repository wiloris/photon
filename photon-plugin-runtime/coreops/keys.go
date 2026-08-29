package coreops

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Ключ в ports.Storage собирается из трёх частей: идентификатора плагина,
// его логического namespace и собственно ключа. Первые две подставляет
// runtime, третью присылает плагин (docs/core-operations.md §5).
//
// ГРАНИЦЫ КОДИРУЮТСЯ ЯВНО, а не выводятся из содержимого — тот же принцип,
// что в ADR-037 для подписей, и по той же причине.
//
// Наивная склейка "plugin." + id + "." + ns + "|" + key неоднозначна:
//
//	плагин "com.a"   + namespace "b.c" -> plugin.com.a.b.c|key
//	плагин "com.a.b" + namespace "c"   -> plugin.com.a.b.c|key
//
// Один и тот же ключ у двух РАЗНЫХ плагинов. Это не теоретическая
// возможность: оба идентификатора проходят валидацию манифеста, и такая
// пара появляется естественно у одного вендора.
//
// Полагаться на то, что разделитель не встретится внутри полей, тоже
// нельзя: namespace приходит от плагина и произвольным быть может, а
// алфавит plugin_id задаётся регуляркой в manifest/validator.go, которую
// однажды расширят, не подумав об этом файле.
//
// Поэтому длина перед каждым полем переменной длины. Последнему полю
// (ключу плагина) граница не нужна — оно замыкает строку.
const keyPrefixLiteral = "plugin:"

// storageKey собирает полный ключ.
func storageKey(pluginID, namespace, key string) []byte {
	var b bytes.Buffer
	b.WriteString(keyPrefixLiteral)
	writeLenPrefixed(&b, pluginID)
	writeLenPrefixed(&b, namespace)
	b.WriteString(key)
	return b.Bytes()
}

// storagePrefix собирает префикс, общий для всех ключей плагина в его
// namespace. Используется и для листинга, и для снятия префикса с
// результата.
func storagePrefix(pluginID, namespace string) []byte {
	var b bytes.Buffer
	b.WriteString(keyPrefixLiteral)
	writeLenPrefixed(&b, pluginID)
	writeLenPrefixed(&b, namespace)
	return b.Bytes()
}

func writeLenPrefixed(b *bytes.Buffer, s string) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s)))
	b.Write(n[:])
	b.WriteString(s)
}

// stripPrefix снимает префикс с полного ключа, полученного из хранилища.
//
// Несовпадение префикса — НЕ повод вернуть ключ как есть. Оно означает, что
// ключ пришёл не из пространства этого плагина, то есть либо сломан обход
// хранилища, либо перепутан префикс. Отдавать такой ключ плагину нельзя ни
// в каком виде: в первом случае это чужие данные, во втором — сведения о
// внутреннем устройстве. Ошибка уровня INTERNAL_ERROR
// (docs/core-operations.md §5).
func stripPrefix(full, prefix []byte) (string, error) {
	if !bytes.HasPrefix(full, prefix) {
		return "", fmt.Errorf("coreops: key outside caller namespace")
	}
	return string(full[len(prefix):]), nil
}
