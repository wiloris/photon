// Package ports — см. storage.go для общих правил пакета.
package ports

import "context"

// P2P — порт прямых соединений между узлами с обходом NAT.
// Первая реализация — adapters/pion (ICE/STUN/TURN, см. ARCHITECTURE.md,
// раздел 8).
//
// Peer — существующий PublicKey (identity.go), не отдельный тип: узел
// идентифицируется той же криптографической личностью, что и везде в
// Photon. Discovery (откуда взялся PublicKey собеседника, кто сейчас
// online) не входит в этот порт — это Account/Profile или контакт-лист
// приложения (см. ADR-017/018); P2P начинает работать, когда peer уже
// известен.
//
// Signaling-блобы (offer/answer) непрозрачны для порта — формат
// (SDP или иной) решает адаптер. Доставка блобов другой стороне —
// забота вызывающего кода через любой уже существующий канал
// (например, Transport к серверу-бриджу), не этого порта.
//
// Граница с Transport: Transport — dial на известный адрес; P2P —
// согласование до появления адреса (NAT traversal). Результат
// согласования — Session, встраивающая Connection (transport.go):
// после установления P2P-канал неотличим от обычного соединения,
// P2P не дублирует Transport, а производит его результат другим
// путём установления. См. ADR-011/013/016.
type P2P interface {
	// CreateOffer начинает согласование сессии с peer (вызывается
	// инициирующей стороной). Возвращает offer — блоб для доставки
	// peer'у любым существующим каналом.
	CreateOffer(ctx context.Context, peer PublicKey) (offer []byte, err error)

	// AcceptOffer принимает offer, полученный от peer (вызывается
	// принимающей стороной). Возвращает answer для отправки обратно и
	// Session, которая может быть возвращена до полного установления
	// соединения: методы открытия потоков (OpenStream, AcceptStream и
	// т.п.) блокируются до успешного завершения согласования либо
	// возвращают ошибку, если согласование завершилось неудачно.
	AcceptOffer(ctx context.Context, peer PublicKey, offer []byte) (answer []byte, session Session, err error)

	// CompleteSession завершает согласование, начатое CreateOffer,
	// используя answer от peer (вызывается инициирующей стороной).
	CompleteSession(ctx context.Context, peer PublicKey, answer []byte) (Session, error)

	// Close освобождает локальные ресурсы согласования (ICE-агент,
	// STUN/TURN-клиент), общие для всех сессий. Не закрывает уже
	// установленные Session — для этого Session.Close (унаследован от
	// Connection).
	Close() error
}

// Session — установленное P2P-соединение с конкретным peer.
// Расширяет Connection (transport.go): в P2P, в отличие от Transport,
// имеет значение не только КУДА подключено (Endpoint), но и С КЕМ —
// Connection этого не знает, поэтому Session добавляет Peer().
type Session interface {
	Connection

	// Peer возвращает публичный ключ собеседника этой сессии.
	Peer() PublicKey
}
