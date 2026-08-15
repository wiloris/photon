// Package ports — см. storage.go для общих правил пакета.
package ports

import "context"

// NotificationTarget — непрозрачный адрес доставки (APNs device
// token, FCM registration token, WebPush endpoint, email — конкретный
// смысл решает адаптер). Соответствие PublicKey ↔ NotificationTarget
// не входит в этот порт — это данные будущего Account/Profile сервиса
// (см. ADR-017/018, ADR-019), который резолвит target до вызова
// Notify. Notifier ничего не знает о PublicKey и не зависит от
// каталога пользователей.
type NotificationTarget string

// Notification — уведомление для доставки вне активного соединения
// ядра. Title/Body/Data — общеупотребимые понятия push-систем (APNs,
// FCM, WebPush и т.д.), не специфика одной из них: Data — payload
// для клиента (например, action code для перехода к нужному экрану).
type Notification struct {
	Title string
	Body  string
	Data  map[string]string
}

// Notifier — порт доставки уведомлений по уже известному адресу,
// когда прямого канала ядра (Transport/P2P) сейчас нет. Механизм
// доставки (APNs, FCM, WebPush, email) инкапсулирован в адаптере —
// порт этого не знает. Первая реализация будет определена по мере
// появления конкретного провайдера push-доставки.
//
// Notify — действие (аналог Encrypt/Sign), а не проверка условия
// (аналог Verify/Authorize), поэтому возвращает обычный error, а не
// (bool, error): либо уведомление принято к доставке, либо нет.
type Notifier interface {
	// Notify доставляет notification по адресу target.
	// ErrRecipientUnreachable — target недействителен или отозван
	// (не имеет смысла повторять попытку без его обновления); иначе
	// — технический сбой адаптера.
	Notify(ctx context.Context, target NotificationTarget, notification Notification) error
}
