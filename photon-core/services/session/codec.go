// Package session — слой сессии Bridge-границы (Phase 12.5).
//
// Спецификация: photon-core/docs/bridge-session.md (написана до кода).
// Формат: ADR-034 — JSON на проводе, protobuf как каноническая схема.
//
// Что этот пакет делает: устанавливает соединение через ports.Transport,
// проводит handshake, отправляет action'ы и принимает push-события.
// Что он НЕ делает: не исполняет action'ы (Dispatcher, Phase 13.5) и не знает,
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

// StatusReady / StatusSubscribed — подтверждение режима, которое сервер
// возвращает в ответе на handshake. Значения взяты не из схемы, а из кода
// развёрнутого сервера: photon-quic-bridge/server/quic_listener.go:279 и
// :287 отвечают Data: map[string]string{"status": "subscribed"|"ready"}.
const (
	StatusReady      = "ready"
	StatusSubscribed = "subscribed"
)

// statusForMode — подтверждение, которого клиент ждёт для запрошенного
// режима. Пустая строка означает "режим неизвестен этому слою" и не может
// произойти: New отвергает любой Mode кроме двух (session.go).
func statusForMode(m Mode) string {
	switch m {
	case ModeAction:
		return StatusReady
	case ModeSubscribe:
		return StatusSubscribed
	}
	return ""
}

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

	// Status — режим, который сервер ПОДТВЕРДИЛ ("ready" | "subscribed").
	// Единственное место, где сервер сообщает, что он на самом деле принял,
	// а не что у него попросили — и потому единственная защита от
	// молчаливого отката subscribe→action (bridge-session.md §2.1).
	//
	// Пустая строка = сервер не сообщил; это НЕ ошибка, по той же причине,
	// что и нулевой ProtocolVersion.
	//
	// Поле не раскрывает формат (§3.2): "ready"/"subscribed" — значения
	// протокола, а не JSON. Где именно они лежат на проводе — у JSON-кодека
	// внутри data, — знает только кодек.
	Status string
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

	// BodyLength — длина двоичного хвоста, следующего за этим кадром на том
	// же стриме. Ноль означает «хвоста нет» и верен для всех действий, кроме
	// медийного скачивания.
	//
	// Поле КОНВЕРТА, а не домена, и это существенно. Длина тела — свойство
	// кадра, а не содержимого: она говорит транспорту, сколько байт читать.
	// Положить её внутрь Data значило бы заставить транспорт разбирать
	// доменный payload, ровно то, что запрещает §3.1 ниже: Data проходит
	// сырыми байтами, без разбора и пересборки.
	//
	// media-transfer.md §4 показывает body_length в составе ответа на
	// скачивание. Это уточнение, а не отступление: поле остаётся обязательным
	// (§3), меняется лишь его место — конверт вместо data. Записано здесь
	// потому, что решение принято при реализации, а спецификация писалась до
	// неё.
	BodyLength int64

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

	// UserID пустой НЕ означает широковещательное событие — так здесь было
	// записано, и это неверно. Поле объявлено сервером с omitempty
	// (server/push.go:14), а заполняет его ровно один хук из восьми —
	// status_change (server/hooks.go:98). Для остальных семи, включая
	// posted, поле с провода исчезает, а идентификатор автора лежит внутри
	// Data ("data":{"user_id":...}).
	//
	// Найдено прогоном против живого Bridge: событие пришло полностью
	// корректным, с user_id=<nil> на верхнем уровне. Обещание обратного
	// стоит в двух местах (server/protocol.go:23-24 и шапка
	// photon.proto:42) и для posted неверно в обоих.
	// См. bridge-session.md §2.3.
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
// Вызывающий (Dispatcher, Phase 13.5) не знает и не может узнать, какой формат
// за интерфейсом.
type Codec interface {
	WriteHandshake(w io.Writer, h Handshake) error
	ReadHandshakeAck(r io.Reader) (HandshakeAck, error)

	WriteRequest(w io.Writer, req Request) error
	ReadResponse(r io.Reader) (Response, error)

	// WriteRequestStream — запрос, за которым идёт двоичный хвост: всё, что
	// отдаст body.
	//
	// Длины здесь НЕТ намеренно. Она объявляется в payload'е (body_length,
	// media-transfer.md §3), то есть принадлежит домену, а не транспорту, и
	// ограничить тело обязан вызывающий — например, io.LimitReader. Согласие
	// объявленного с отправленным проверяет ПРИЁМНИК: он читает ровно
	// объявленное и отвечает ошибкой на расхождение. Добавить длину сюда
	// значило бы расширить сигнатуру DoStream, закреплённую ADR-049 §4.
	//
	// Тело копируется ПОТОКОВО и целиком в память не собирается: сигнатура
	// принимает io.Reader, а не []byte (media-transfer.md §10).
	WriteRequestStream(w io.Writer, req Request, body io.Reader) error

	// ReadResponseStream — ответ, за которым может идти двоичный хвост.
	//
	// Если sink не nil и ответ успешен, копирует в него РОВНО
	// Response.BodyLength байт. Граница тела вычисляется здесь и только здесь
	// (media-transfer.md §2): остаток буфера декодера с отброшенными ведущими
	// пробелами, затем сам поток. Оба слагаемых обязательны, и нарушение
	// любого из них тихое.
	ReadResponseStream(r io.Reader, sink io.Writer) (Response, error)

	ReadPush(r io.Reader) (Push, error)
}
