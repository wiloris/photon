package session

import "errors"

// Sentinel-ошибки пакета. Классификация — только через errors.Is, никогда
// строковым поиском по тексту: ADR-033 заведён ровно после того, как
// строковая классификация дважды дала реальные дефекты (ipc.isUnknownCaller,
// host.classify), причём во втором случае текст, по которому шёл матчинг,
// частично приходил по проводу от недоверенной стороны.
var (
	// ErrSessionClosed — сессия закрыта. Возвращается из Connect и Do после
	// Close, включая случай "Close произошёл во время Connect". Сессия после
	// закрытия не воскресает: см. bridge-session.md §6, п. 4 — прецедент
	// (воскрешение плагина после Stop во время backoff) стоил Phase 9
	// реального бага.
	ErrSessionClosed = errors.New("session: closed")

	// ErrNotReady — Do вызван на неподключённой сессии. Do намеренно не
	// подключается неявно (bridge-session.md §6, п. 5).
	ErrNotReady = errors.New("session: not connected")

	// ErrConnectInProgress — Connect вызван повторно, пока предыдущий ещё
	// идёт. Отличается от "уже подключено": там возвращается nil.
	ErrConnectInProgress = errors.New("session: connect already in progress")

	// ErrHandshakeRejected — сервер ответил {"ok":false}. Текст причины
	// оборачивается, но классификация идёт по этому sentinel.
	ErrHandshakeRejected = errors.New("session: handshake rejected by server")

	// ErrProtocolVersion — сервер объявил версию протокола, несовместимую с
	// ProtocolVersion. Отдельная ошибка, а не разновидность
	// ErrHandshakeRejected: это единственный механизм смены wire-формата
	// (ADR-034 п.5), и он обязан быть отличим программно.
	ErrProtocolVersion = errors.New("session: incompatible protocol version")

	// ErrModeNotAccepted — сервер подтвердил не тот режим, который был
	// запрошен. Отдельная ошибка, а не разновидность ErrHandshakeRejected:
	// сервер при этом ответил ok:true, и по всем остальным признакам
	// handshake состоялся.
	//
	// Сценарий не гипотетический. quic_listener.go:270-275 при ЛЮБОЙ ошибке
	// чтения mode-фрейма молча подставляет "action" и отвечает
	// {"ok":true,"data":{"status":"ready"}}. Клиент, запросивший subscribe и
	// не проверяющий status, уходит в AcceptPush на соединении, где сервер
	// UniStream'ов не откроет никогда, и висит там до отмены ctx. Симптом
	// («push не приходит») отстоит от причины (фрейм, отправленный секундой
	// раньше) настолько, что диагностируется часами.
	ErrModeNotAccepted = errors.New("session: server confirmed a different mode")

	// ErrUnknownAction — action-код отсутствует в enum Action
	// (photon-core/protobuf). Соответствует PHOTON_ERR_INVALID_ACTION на
	// границе ABI (ADR-026 п.5).
	ErrUnknownAction = errors.New("session: unknown action code")

	// ErrNoResourceForAction — код есть в enum Action, но не попадает ни в
	// один известный диапазон ресурсов. Не может произойти при текущей схеме
	// и обязан быть громким, если произойдёт: молчаливая отправка пустого
	// resource дала бы невнятную ошибку уже на сервере, по сети, вдали от
	// причины. См. bridge-session.md §4.
	ErrNoResourceForAction = errors.New("session: no resource range for action code")

	// ErrInvalidPayload — payload не является валидным JSON. Проверяется до
	// отправки: невалидный JSON, дописанный в стрим, ломает не текущий
	// запрос, а разбор всего последующего потока на стороне сервера.
	ErrInvalidPayload = errors.New("session: payload is not valid JSON")

	// ErrMalformedFrame — фрейм от сервера не разбирается.
	ErrMalformedFrame = errors.New("session: malformed frame from server")

	// ErrConnectionLost — связь была установлена и оборвалась. Отличается от
	// ErrNotReady («ещё не подключались») намеренно: это разные решения у
	// вызывающего, и сводить их к одной ошибке значит заставлять его гадать.
	//
	// Прецедент прямой, найден прогоном против живого Bridge, а не разбором
	// кода: subscribe-соединение умирает через 30 с простоя (idle-timeout
	// QUIC, DefaultIdleTimeout), сервер снимает клиента с регистрации, а
	// подписчик остаётся висеть в AcceptPush до отмены ctx, считая себя
	// подписанным. У ждущего события нет следующей операции, на ошибке
	// которой обрыв обнаружился бы — без явного сигнала он не узнает о нём
	// вообще.
	//
	// Реконнекта это НЕ вводит: сообщить об обрыве обязан транспортный слой,
	// потому что только он его видит; решать, восстанавливать ли связь, — слой
	// выше (bridge-session.md §6, п. 7).
	ErrConnectionLost = errors.New("session: connection lost")

	// ── потоковая передача (DoStream) ────────────────────────────────────
	//
	// Все четыре — про ГРАНИЦУ кадра, и каждая соответствует случаю, который
	// без явной ошибки прошёл бы тихо: длина правдоподобна, поток валиден,
	// данные неверны (media-transfer.md §2, §3).

	// ErrStreamDirection — заданы одновременно и body, и sink. Направление
	// тела задаётся действием, а не переговорами: стрим не может нести
	// двоичный хвост в обе стороны (media-transfer.md §1). Ошибка вызывающего,
	// проверяется ДО открытия стрима — ни один байт не отправляется.
	ErrStreamDirection = errors.New("session: body and sink are mutually exclusive")

	// ErrBodyLengthRequired — sink задан, сервер ответил ok, но длину тела не
	// объявил. Читать «до конца» здесь нельзя: конец потока неотличим от
	// обрыва (media-transfer.md §3).
	ErrBodyLengthRequired = errors.New("session: response declares no body length")

	// ErrBodyTruncated — тело кончилось раньше объявленной длины. Это ОШИБКА,
	// а не усечённый успех: частично полученное тело результатом не является
	// (media-transfer.md §7).
	ErrBodyTruncated = errors.New("session: body shorter than declared length")

	// ErrBodyLengthInvalid — объявленная длина отрицательна. Проверяется до
	// чтения: отрицательная длина превратила бы копирование в бесконечное.
	ErrBodyLengthInvalid = errors.New("session: declared body length is invalid")
)
