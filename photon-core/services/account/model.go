// Package account реализует services/account — связывает внешнюю
// OAuth-личность (ports.IdentityProvider), Photon Account и
// зарегистрированные устройства (ports.Identity для проверки подписи
// при Transport Authentication). Не порт — сервис поверх Storage,
// Identity, IdentityProvider (см. ARCHITECTURE.md, раздел 3;
// docs/account-storage-model.md).
package account

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// PhotonID — публичный постоянный идентификатор аккаунта.
// 9 цифр, "000000000"-"999999999", ведущие нули обязательны. Формат
// и свойства (назначается один раз, не изменяется, не переиспользуется)
// — см. ADR-018. Не часть ports/ — платформенный, не криптографический
// идентификатор (см. ADR-017).
type PhotonID string

const photonIDDigits = 9
const photonIDSpace = 1_000_000_000 // 10^9: "000000000"-"999999999"

// generatePhotonID возвращает случайный корректно отформатированный
// PhotonID. Не проверяет уникальность — это ответственность вызывающего
// кода (Storage знает про уже занятые значения, model — нет).
func generatePhotonID() (PhotonID, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(photonIDSpace))
	if err != nil {
		return "", fmt.Errorf("account: generate PhotonID: %w", err)
	}
	return PhotonID(fmt.Sprintf("%0*d", photonIDDigits, n.Int64())), nil
}

// Account — запись аккаунта. См. docs/account-storage-model.md.
type Account struct {
	PhotonID  PhotonID
	Provider  string
	Subject   string
	Email     string
	CreatedAt time.Time
}

// Device — запись устройства, привязанного к Account.
// PublicKey — ключ поиска в реестре (не DeviceID — DeviceID неизвестен
// вызывающей стороне на момент Transport Authentication, см.
// docs/account-storage-model.md).
type Device struct {
	DeviceID  string
	PhotonID  PhotonID
	PublicKey ports.PublicKey
	Name      string
	CreatedAt time.Time
	LastSeen  time.Time
}

// generateDeviceID возвращает случайный непрозрачный идентификатор
// устройства — для отображения/логов, не для поиска (см. Device.PublicKey).
func generateDeviceID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("account: generate DeviceID: %w", err)
	}
	return fmt.Sprintf("%x", b), nil
}
