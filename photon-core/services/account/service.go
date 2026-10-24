package account

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// Storage namespaces — см. docs/account-storage-model.md для полного
// обоснования схемы (почему четыре namespace, не два "умных").
const (
	nsAccounts           = "photon.accounts"
	nsAccountsByExternal = "photon.accounts.by_external"
	nsDevices            = "photon.devices"
	nsDevicesByPubKey    = "photon.devices.by_pubkey"
)

// Ошибки services/account. Не ports/errors.go — это сервис, не порт,
// у него собственное пространство ошибок (см. ARCHITECTURE.md,
// раздел 3 — сервисы не являются частью замороженного ports/ контракта).
var (
	ErrAccountNotFound         = errors.New("account: account not found")
	ErrDeviceNotFound          = errors.New("account: device not found")
	ErrDeviceAlreadyRegistered = errors.New("account: device already registered")
	ErrInvalidSignature        = errors.New("account: invalid signature")
	ErrPhotonIDSpaceExhausted  = errors.New("account: photon id space exhausted")
)

const maxPhotonIDGenerationAttempts = 10

// Service — services/account. Не порт: использует существующие
// ports.Storage, ports.Identity, ports.IdentityProvider — новый порт
// под это не заводится (см. ADR-022, "AccountService — сервис ядра,
// не порт").
type Service struct {
	storage  ports.Storage
	identity ports.Identity
	idp      ports.IdentityProvider
}

// New создаёт Service поверх уже существующих Storage/Identity/IdentityProvider.
// Не создаёт namespace самостоятельно — они должны быть подготовлены
// миграцией на этапе конструирования Storage (см. docs/account-storage-model.md,
// ADR-010: "Migrate — единственное место, где namespace появляется").
func New(storage ports.Storage, identity ports.Identity, idp ports.IdentityProvider) *Service {
	return &Service{storage: storage, identity: identity, idp: idp}
}

// LoginOrRegister проверяет providerToken через IdentityProvider и
// возвращает существующий Account (если ExternalIdentity уже привязана)
// или создаёт новый (сценарий 1, docs/account-device-flow.md).
func (s *Service) LoginOrRegister(ctx context.Context, providerToken string) (Account, error) {
	ext, err := s.idp.VerifyIDToken(ctx, providerToken)
	if err != nil {
		return Account{}, fmt.Errorf("account: verify token: %w", err)
	}

	var result Account
	err = s.storage.Update(ctx, func(tx ports.WriteTx) error {
		extKey := externalKey(ext.Provider, ext.Subject)

		// Уже привязан к существующему Account?
		if idBytes, getErr := tx.Get(nsAccountsByExternal, extKey); getErr == nil {
			acc, loadErr := s.loadAccount(tx, PhotonID(idBytes))
			if loadErr != nil {
				return loadErr
			}
			result = acc
			return nil
		} else if !errors.Is(getErr, ports.ErrNotFound) {
			return getErr
		}

		// Новый Account — генерируем PhotonID с проверкой на коллизию.
		var id PhotonID
		for attempt := 0; ; attempt++ {
			if attempt >= maxPhotonIDGenerationAttempts {
				return ErrPhotonIDSpaceExhausted
			}
			candidate, genErr := generatePhotonID()
			if genErr != nil {
				return genErr
			}
			_, getErr := tx.Get(nsAccounts, []byte(candidate))
			if errors.Is(getErr, ports.ErrNotFound) {
				id = candidate
				break
			}
			if getErr != nil {
				return getErr
			}
			// getErr == nil: candidate занят, пробуем снова.
		}

		acc := Account{
			PhotonID:  id,
			Provider:  ext.Provider,
			Subject:   ext.Subject,
			Email:     ext.Email,
			CreatedAt: time.Now().UTC(),
		}
		accBytes, marshalErr := json.Marshal(acc)
		if marshalErr != nil {
			return marshalErr
		}
		if putErr := tx.Put(nsAccounts, []byte(id), accBytes); putErr != nil {
			return putErr
		}
		if putErr := tx.Put(nsAccountsByExternal, extKey, []byte(id)); putErr != nil {
			return putErr
		}

		result = acc
		return nil
	})
	if err != nil {
		return Account{}, err
	}
	return result, nil
}

// RegisterDevice привязывает новое устройство (publicKey) к аккаунту
// photonID (сценарии 1 и 3, docs/account-device-flow.md — одна и та же
// операция: "первое устройство" и "ещё одно устройство" не различаются
// на уровне Service, только на уровне вызывающего UI-потока).
func (s *Service) RegisterDevice(ctx context.Context, photonID PhotonID, publicKey ports.PublicKey, name string) (Device, error) {
	deviceID, err := generateDeviceID()
	if err != nil {
		return Device{}, err
	}

	dev := Device{
		DeviceID:  deviceID,
		PhotonID:  photonID,
		PublicKey: publicKey,
		Name:      name,
		CreatedAt: time.Now().UTC(),
		LastSeen:  time.Now().UTC(),
	}

	err = s.storage.Update(ctx, func(tx ports.WriteTx) error {
		// Account обязан существовать — не создаём его неявно здесь.
		if _, getErr := tx.Get(nsAccounts, []byte(photonID)); getErr != nil {
			if errors.Is(getErr, ports.ErrNotFound) {
				return ErrAccountNotFound
			}
			return getErr
		}

		pubHex := hexKey(publicKey)
		if _, getErr := tx.Get(nsDevicesByPubKey, pubHex); getErr == nil {
			return ErrDeviceAlreadyRegistered
		} else if !errors.Is(getErr, ports.ErrNotFound) {
			return getErr
		}

		devBytes, marshalErr := json.Marshal(dev)
		if marshalErr != nil {
			return marshalErr
		}
		if putErr := tx.Put(nsDevices, deviceKey(photonID, publicKey), devBytes); putErr != nil {
			return putErr
		}
		if putErr := tx.Put(nsDevicesByPubKey, pubHex, []byte(photonID)); putErr != nil {
			return putErr
		}
		return nil
	})
	if err != nil {
		return Device{}, err
	}
	return dev, nil
}

// RevokeDevice удаляет устройство из реестра (сценарий 4,
// docs/account-device-flow.md). Атомарно из обоих namespace — см.
// docs/account-storage-model.md, "Отзыв устройства", про риск
// рассинхрона при неатомарном удалении.
func (s *Service) RevokeDevice(ctx context.Context, publicKey ports.PublicKey) error {
	return s.storage.Update(ctx, func(tx ports.WriteTx) error {
		pubHex := hexKey(publicKey)
		idBytes, getErr := tx.Get(nsDevicesByPubKey, pubHex)
		if errors.Is(getErr, ports.ErrNotFound) {
			return ErrDeviceNotFound
		}
		if getErr != nil {
			return getErr
		}
		photonID := PhotonID(idBytes)

		if delErr := tx.Delete(nsDevicesByPubKey, pubHex); delErr != nil {
			return delErr
		}
		return tx.Delete(nsDevices, deviceKey(photonID, publicKey))
	})
}

// ListDevices возвращает все устройства аккаунта photonID (Device List
// API, ROADMAP.md Phase 3.5).
func (s *Service) ListDevices(ctx context.Context, photonID PhotonID) ([]Device, error) {
	var devices []Device
	err := s.storage.View(ctx, func(tx ports.ReadTx) error {
		prefix := append([]byte(photonID), '|')
		return tx.ForEachPrefix(nsDevices, prefix, func(key, value []byte) error {
			var dev Device
			if err := json.Unmarshal(value, &dev); err != nil {
				return err
			}
			devices = append(devices, dev)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return devices, nil
}

// deviceAuthDomain — доменный литерал аутентификации устройства (ADR-037).
//
// Литерал САМ является границей версии схемы: менять правило кодирования —
// значит менять литерал (`photon-device-auth2:` и подобное). Отдельного поля
// версии нет намеренно, оно дублировало бы литерал.
const deviceAuthDomain = "photon-device-auth:"

// deviceAuthInput — то, что на самом деле подписывается при аутентификации
// устройства (ADR-037):
//
//	device_auth_input = "photon-device-auth:" ‖ challenge
//
// Без домена подпись, полученная плагином через `identity.sign` (право из
// каталога SDK), годилась бы как аутентификация устройства — достаточно, чтобы
// challenge совпал с подписанными данными. Вторая половина решения —
// `plugin-sign` в photon-plugin-runtime/coreops — уже реализована; домен
// только с одной стороны дыру не закрывает.
//
// Длина перед challenge не нужна ЗДЕСЬ и сейчас: за литералом одно поле
// переменной длины, последнему полю граница не требуется. Это свойство
// текущей формы, а не принцип: если в challenge появится структура из
// нескольких полей, границы придётся кодировать явно — иначе разбор молча
// станет неоднозначным (ADR-037).
//
// Не экспортируется: домен применяется внутри сервиса, вызывающих снаружи
// нет. Клиент (Phase 16) живёт в другом процессе и реализует формат по
// спецификации docs/account-device-flow.md, а не импортом Go-функции.
func deviceAuthInput(challenge []byte) []byte {
	input := make([]byte, 0, len(deviceAuthDomain)+len(challenge))
	input = append(input, deviceAuthDomain...)
	input = append(input, challenge...)
	return input
}

// AuthenticateDevice — Transport Authentication (сценарий 2,
// docs/account-device-flow.md): проверяет, что publicKey
// зарегистрирован и не отозван, и что signature — корректная подпись
// ДОМЕННОГО ПРЕДСТАВЛЕНИЯ challenge этим ключом (ADR-037; см. также
// ADR-021: challenge-response, не сертификаты). Обновляет LastSeen при
// успехе.
//
// Подпись под сырым challenge отвергается. Это ломающее изменение для
// устройств, зарегистрированных до ADR-037, и миграция не нужна: подписи не
// хранятся, а клиентов, подписывающих challenge, ещё не существует —
// обоснование в ADR-037.
func (s *Service) AuthenticateDevice(ctx context.Context, publicKey ports.PublicKey, challenge, signature []byte) (Device, error) {
	ok, err := s.identity.Verify(ctx, publicKey, deviceAuthInput(challenge), ports.Signature(signature))
	if err != nil {
		return Device{}, fmt.Errorf("account: verify signature: %w", err)
	}
	if !ok {
		return Device{}, ErrInvalidSignature
	}

	var dev Device
	err = s.storage.Update(ctx, func(tx ports.WriteTx) error {
		pubHex := hexKey(publicKey)
		idBytes, getErr := tx.Get(nsDevicesByPubKey, pubHex)
		if errors.Is(getErr, ports.ErrNotFound) {
			return ErrDeviceNotFound
		}
		if getErr != nil {
			return getErr
		}
		photonID := PhotonID(idBytes)

		key := deviceKey(photonID, publicKey)
		devBytes, getErr := tx.Get(nsDevices, key)
		if getErr != nil {
			return getErr
		}
		if unmarshalErr := json.Unmarshal(devBytes, &dev); unmarshalErr != nil {
			return unmarshalErr
		}

		dev.LastSeen = time.Now().UTC()
		updated, marshalErr := json.Marshal(dev)
		if marshalErr != nil {
			return marshalErr
		}
		return tx.Put(nsDevices, key, updated)
	})
	if err != nil {
		return Device{}, err
	}
	return dev, nil
}

func (s *Service) loadAccount(tx ports.ReadTx, id PhotonID) (Account, error) {
	b, err := tx.Get(nsAccounts, []byte(id))
	if err != nil {
		return Account{}, err
	}
	var acc Account
	if err := json.Unmarshal(b, &acc); err != nil {
		return Account{}, err
	}
	return acc, nil
}

func externalKey(provider, subject string) []byte {
	return []byte(provider + "|" + subject)
}

func deviceKey(id PhotonID, publicKey ports.PublicKey) []byte {
	return append([]byte(string(id)+"|"), hexKey(publicKey)...)
}

func hexKey(publicKey ports.PublicKey) []byte {
	return []byte(hex.EncodeToString(publicKey))
}
