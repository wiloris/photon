package dispatcher

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/wiloris/photon/photon-core/services/session"
)

// Типы событий первой версии (event-delivery.md §7, ADR-045 §6).
//
// Имена берутся из замороженного перечня photon_core.h и новых не вводится:
// отдельного DISCONNECTED там нет, и заводить его прежде, чем появился
// потребитель, отличающий его от NETWORK_CHANGED, значило бы расширять ABI без
// основания.
//
// PLUGIN_EVENT и STORAGE_CHANGED не эмитируются: имена в заголовке есть,
// источников в ядре нет.
const (
	EventConnected      = "CONNECTED"
	EventMessage        = "MESSAGE"
	EventNetworkChanged = "NETWORK_CHANGED"
)

// stateDisconnected — значение поля state у NETWORK_CHANGED. Единственное на
// сегодня: событие эмитируется только при потере подписки.
const stateDisconnected = "disconnected"

// eventConnected — подписка подтверждена сервером.
//
// Область действия CONNECTED и NETWORK_CHANGED — ТОЛЬКО подписное соединение
// (event-delivery.md §7). Action-соединение подключается лениво, по вызову, и
// своего фонового жизненного цикла не имеет; сообщать о нём событиями значило
// бы обещать то, чего нет.
func eventConnected() []byte {
	return []byte(`{"type":"` + EventConnected + `"}`)
}

// eventNetworkChanged — подписка потеряна, дальше идёт восстановление.
func eventNetworkChanged() []byte {
	return []byte(`{"type":"` + EventNetworkChanged + `","state":"` + stateDisconnected + `"}`)
}

// eventMessage собирает event_json из принятого push (event-delivery.md §8).
//
// Две ловушки провода учтены здесь, а не оставлены вызывающему — обе найдены
// прогоном против живого Bridge (bridge-session.md §2.3):
//
//  1. Push.Event — СТРОКА с провода ("posted"), а не значение enum EventType из
//     схемы. Имя не переводится в число и не сверяется со списком: ядро не
//     обязано знать доменный словарь Bridge, а отбрасывать событие из-за
//     незнакомого имени значило бы терять его молча.
//  2. user_id верхнего уровня обычно ОТСУТСТВУЕТ: сервер объявил поле с
//     omitempty, и заполняет его один хук из восьми. Для остальных, включая
//     posted, идентификатор автора лежит внутри data. Поле просто опускается —
//     ничего не подставляется и ничего не достаётся из data.
//
// data вставляются СЫРЫМИ байтами, по тому же правилу, что в envelope: ни одной
// лишней сериализации, никакой потери точности чисел на float64.
func eventMessage(p session.Push) ([]byte, error) {
	// Сканирование без аллокаций, а не разбор с пересборкой: защищает контракт
	// границы — event_json обязан быть валидным JSON, и склейка непроверенного
	// куска отдала бы клиенту битую строку, причину которой он искал бы у себя.
	if len(p.Data) > 0 && !json.Valid(p.Data) {
		return nil, fmt.Errorf("dispatcher: %w: push data is not valid JSON", session.ErrMalformedFrame)
	}

	ev, err := json.Marshal(p.Event)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode event field: %w", err)
	}

	var b bytes.Buffer
	b.WriteString(`{"type":"` + EventMessage + `","event":`)
	b.Write(ev)
	if p.UserID != "" {
		uid, err := json.Marshal(p.UserID)
		if err != nil {
			return nil, fmt.Errorf("dispatcher: encode user_id field: %w", err)
		}
		b.WriteString(`,"user_id":`)
		b.Write(uid)
	}
	if len(p.Data) > 0 {
		b.WriteString(`,"data":`)
		b.Write(p.Data)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}
