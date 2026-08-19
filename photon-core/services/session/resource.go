package session

import (
	"fmt"

	photonpb "github.com/wiloris/photon-core/protobuf"
)

// ResourceFor выводит строку resource для header-фрейма из action-кода.
//
// Зачем это вообще нужно. ADR-026 п.1 убрал параметр resource из
// Photon_SendAction: на уровне ABI строка избыточна, потому что action-код
// определяет ресурс числовым диапазоном. На проводе header-фрейм строку
// содержит и ТРЕБУЕТ ({"action":100,"resource":"PostResource"}). Значит
// вывести её обязан кто-то между ABI и проводом — этот слой. Противоречия
// между ADR нет; это ровно то место, где «схема ≠ провод» становится кодом
// (ADR-034, «Следствие, требующее внимания в Phase 12.5»).
//
// Две РАЗНЫЕ проверки, и обе обязательны:
//
//  1. код существует в enum Action (photon-core/protobuf) — источник истины
//     тот же, что уже использует photon-bindings/c/photon.go;
//  2. код попадает в известный диапазон ресурсов.
//
// Первая без второй пропустила бы будущий код 700+, для которого ресурс не
// определён; вторая без первой пропустила бы, например, 150 — он лежит в
// диапазоне Post, но такого action'а не существует. Сервер отверг бы его
// невнятной ошибкой уже по сети, вдали от причины.
//
// Таблица диапазонов не выписана по памяти: подтверждена перекрёстной
// проверкой всех 48 вызовов action(...) в test-client/test_photon.go —
// клиенте, проверенном против живого сервера. Каждая сотня отображается
// ровно в один resource, исключений нет.
func ResourceFor(action int32) (string, error) {
	if _, known := photonpb.Action_name[action]; !known || action == 0 {
		return "", fmt.Errorf("%w: %d", ErrUnknownAction, action)
	}

	switch {
	case action >= 100 && action <= 199:
		return "PostResource", nil
	case action >= 200 && action <= 299:
		return "UserResource", nil
	case action >= 300 && action <= 399:
		return "ChannelResource", nil
	case action >= 400 && action <= 499:
		return "EngramResource", nil
	case action >= 500 && action <= 599:
		return "FileResource", nil
	case action >= 600 && action <= 699:
		return "WallResource", nil
	}

	// Достижимо только если в enum Action появится код вне известных
	// диапазонов. Тогда молчать нельзя: пустой resource на проводе — это
	// отказ сервера с текстом, по которому причину не найти.
	return "", fmt.Errorf("%w: %d", ErrNoResourceForAction, action)
}
