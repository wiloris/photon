// Package session — слой сессии Bridge-границы (Phase 12.5).
//
// Спецификация: photon-core/docs/bridge-session.md (написана до кода).
// Формат: ADR-034 — JSON на проводе, protobuf как каноническая схема.
//
// Что этот пакет делает: устанавливает соединение через ports.Transport,
// проводит handshake, отправляет action'ы и принимает push-события.
// Что он НЕ делает: не исполняет action'ы (Dispatcher, Phase 13) и не знает,
// что внутри payload'а.
package session

import "io"

// ProtocolVersion — версия протокола Bridge-границы, которую объявляет и
// поддерживает этот клиент.
//
// Одно число, а не пара major/minor: PHOTON_ABI_MAJOR/MINOR (ADR-026 п.4)
// версионируют ДРУГУЮ границу (Core ↔ binding) с другим темпом изменений.
// Смешивать две версии в одном поле — способ однажды сломать обе.
const ProtocolVersion = 1

// Mode — режим соединения. Фиксируется на соединение, не на стрим:
// клиенту, которому нужны и action'ы, и push, требуется два соединения
// (bridge-session.md §2.4).
type Mode string

const (
	ModeAction    Mode = "action"
	ModeSubscribe Mode = "subscribe"
)

// Handshake — то, что клиент сообщает о себе при подключении.
type Handshake struct {
	SessionToken    string
	Mode            Mode
	ProtocolVersion uint32
}

// HandshakeAck — ответ сервера на handshake.
type HandshakeAck struct {
	OK    bool
	Error string

	// ProtocolVersion — версия, объявленная сервером. Ноль означает "сервер
	// поля не прислал", и это НЕ ошибка: развёрнутый сегодня Bridge о
	// версиях не знает, и трактовать его молчание как несовместимость
	// значило бы сломать работающую систему (bridge-session.md §5).
	ProtocolVersion uint32
}

// Request — запрос к серверу.
type Request struct {
	Action   int32
	Resource string

	// Payload — тело запроса. Непрозрачно для сессии; для JSON-кодека это
	// валидный JSON. nil означает "payload'а нет" и кодируется как {}.
	Payload []byte
}

// Response — ответ сервера.
type Response struct {
	OK    bool
	Error string

	// Data — тело ответа, непрозрачное. Тип []byte, а не interface{}, —
	// осознанный выбор (bridge-session.md §3.1): граница C ABI отдаёт
	// клиенту JSON (ADR-026 п.7), поэтому JSON-кодек возвращает исходные
	// байты без разбора — ни одной лишней сериализации и никакой потери
	// точности чисел на float64. Может быть объектом, массивом, null или
	// отсутствовать (nil) — предполагать объект нельзя: на этом уже падал
	// тест-клиент Bridge на ответах-списках (getUsers, searchUsers,
	// getReactions).
	Data []byte
}

// Push — событие, пришедшее от сервера по UniStream.
type Push struct {
	// Event — имя события строкой ("posted", "typing", ...), а не число:
	// на проводе именно строка (константы PushEvent* в
	// photon-quic-bridge/server/protocol.go), несмотря на enum EventType в
	// схеме. Это одно из расхождений «схема ≠ провод» (ADR-034).
	Event string

	// UserID пустой = широковещательное событие.
	UserID string

	Data []byte
}

// Codec — граница между моделью протокола и его сериализацией (ADR-034 п.4).
//
// Интерфейс объявлен ПОСООБЩЕНЧНО, а не пофреймово, и это главное в его
// форме. Число фреймов — деталь формата: JSON-кодек пишет запрос двумя
// фреймами (header + payload), protobuf-кодек написал бы одним PhotonRequest.
// Пофреймовый интерфейс (WriteHeader/WritePayload) протащил бы это различие
// наружу, и смена формата снова стала бы правкой по всем вызовам — то есть
// ровно тем, чего ADR-034 п.4 требует избежать.
//
// Вызывающий (Dispatcher, Phase 13) не знает и не может узнать, какой формат
// за интерфейсом.
type Codec interface {
	WriteHandshake(w io.Writer, h Handshake) error
	ReadHandshakeAck(r io.Reader) (HandshakeAck, error)

	WriteRequest(w io.Writer, req Request) error
	ReadResponse(r io.Reader) (Response, error)

	ReadPush(r io.Reader) (Push, error)
}
