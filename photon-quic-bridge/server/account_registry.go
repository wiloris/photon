package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// Server-side ENGRAM Account Registry.
//
// Реализует последовательность, зафиксированную спецификацией
// photon-core/docs/account-registry.md по решению ADR-077 (носитель —
// Mattermost plugin storage).
//
//	R1a claim → R1b allocate → R1c register → U(внешний) → R2 link
//
// Многоключевой транзакции у Plugin API нет, поэтому «атомарно» здесь
// означает не транзакцию, а то, что НИ ОДНО прерывание не оставляет
// состояния, из которого повторный вход не выводит. Единственный механизм
// восстановления — повторный Register по (Provider, Subject); отдельной
// процедуры починки нет.
//
// U (CreateUser) и S (CreateSession) остаются ВНЕ реестра: он отдаёт
// состояние, необходимое для идемпотентного продолжения, и принимает
// результат обратно через Link.

// kv — ровно те операции KV, которые нужны реестру, и ни одной сверх.
//
// Узкий интерфейс вместо plugin.API целиком: он делает возможной подмену
// настоящим in-memory хранилищем с НАСТОЯЩИМ compare-and-set, без которого
// конкурентные тесты проверяли бы мок, а не протокол. plugin.API ему
// удовлетворяет.
type kv interface {
	KVGet(key string) ([]byte, *model.AppError)
	KVCompareAndSet(key string, oldValue, newValue []byte) (bool, *model.AppError)
}

// AccountState — состояние записи. Именно оно отличает частичные состояния
// друг от друга, и повторный вход ветвится по нему (спецификация §5).
type AccountState string

const (
	StateClaimed    AccountState = "claimed"
	StateRegistered AccountState = "registered"
	StateLinked     AccountState = "linked"
)

// AccountRecord — значение ключа acct: (спецификация §2.1).
type AccountRecord struct {
	State     AccountState `json:"state"`
	Provider  string       `json:"provider"`
	Subject   string       `json:"subject"`
	Email     string       `json:"email,omitempty"`
	PhotonID  string       `json:"photon_id,omitempty"`
	LoginName string       `json:"login_name,omitempty"`
	MMUserID  string       `json:"mm_user_id,omitempty"`
	CreatedAt string       `json:"created_at"`
}

// pidOwner — значение ключа pid:. Побочно даёт обратную связь
// PhotonID → (Provider, Subject); функцией поиска не является (ADR-077 §4).
type pidOwner struct {
	Provider string `json:"provider"`
	Subject  string `json:"subject"`
}

const (
	photonIDDigits = 9
	photonIDSpace  = 1000000000 // 10^9, диапазон 000000000–999999999 (ADR-018)

	// maxAllocationAttempts — предел попыток подобрать свободный
	// идентификатор. Без него исчерпание пространства даёт бесконечный
	// цикл вместо ошибки.
	maxAllocationAttempts = 32

	// maxAdvanceAttempts — предел перечитываний после проигранного CAS.
	//
	// Состояние записи движется только вперёд (claimed → registered →
	// linked), поэтому цикл сходится сам; предел защищает от
	// патологического случая, а не от нормальной гонки.
	maxAdvanceAttempts = 16

	loginNamePrefix = "engram-"
)

var (
	ErrPhotonIDSpaceExhausted = errors.New("account registry: PhotonID space exhausted")
	ErrAccountNotFound        = errors.New("account registry: account not found")
	ErrNotRegistered          = errors.New("account registry: account is not in registered state")
	ErrRegistrationStalled    = errors.New("account registry: registration did not converge")
)

// AccountRegistry — реестр поверх KV плагина.
type AccountRegistry struct {
	kv kv
	// now и genID подменяются в тестах; в продакшне — time.Now и
	// generatePhotonID.
	//
	// genID существует ради одного свойства, которое иначе непроверяемо:
	// столкновение двух кандидатов в пространстве 10^9 случайно не
	// воспроизводится, и тест на «занятый идентификатор не выдаётся»
	// проходил бы, ни разу не столкнувшись. Поля неэкспортируемые —
	// публичная поверхность не меняется.
	now   func() time.Time
	genID func() (string, error)
}

func NewAccountRegistry(store kv) *AccountRegistry {
	return &AccountRegistry{
		kv:    store,
		now:   func() time.Time { return time.Now().UTC() },
		genID: generatePhotonID,
	}
}

// ── ключи (спецификация §2) ─────────────────────────────────────────

func acctKey(provider, subject string) string { return "acct:" + provider + ":" + subject }
func pidKey(photonID string) string           { return "pid:" + photonID }

// LoginName выводится из PhotonID и потому уникален вместе с ним.
//
// Записывается в acct: РАНЬШЕ вызова внешнего U — это единственное, что
// делает создание пользователя Mattermost повторяемым: осиротевшего
// пользователя ищут по нему, а не по Email, который ключом не является
// (ADR-023).
func LoginNameFor(photonID string) string { return loginNamePrefix + photonID }

// ── CAS (спецификация §1) ───────────────────────────────────────────

// compareAndSet различает ТРИ исхода, и это главное свойство слоя:
//
//	(true,  nil) — операция выполнена;
//	(false, nil) — CAS проигран, нормальная гонка;
//	(false, err) — отказ хранилища.
//
// Смешение двух последних было бы худшим дефектом реестра: продолжение
// «по существующей записи» после сбоя БД означало бы работу по данным,
// которых никто не читал.
func (r *AccountRegistry) compareAndSet(key string, oldValue, newValue []byte) (bool, error) {
	ok, appErr := r.kv.KVCompareAndSet(key, oldValue, newValue)
	if appErr != nil {
		return false, fmt.Errorf("account registry: CAS %s: %w", key, appErr)
	}
	return ok, nil
}

// get читает запись и возвращает СЫРЫЕ байты.
//
// Байты нужны как oldValue последующего CAS: повторная сериализация
// структуры не обязана быть побайтово равной хранимому значению, и сверка по
// ней молча проваливала бы CAS.
func (r *AccountRegistry) get(key string) (AccountRecord, []byte, bool, error) {
	raw, appErr := r.kv.KVGet(key)
	if appErr != nil {
		return AccountRecord{}, nil, false, fmt.Errorf("account registry: get %s: %w", key, appErr)
	}
	if len(raw) == 0 {
		// KVGet возвращает nil для отсутствующего ключа — это не ошибка.
		return AccountRecord{}, nil, false, nil
	}
	var rec AccountRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return AccountRecord{}, nil, false, fmt.Errorf("account registry: decode %s: %w", key, err)
	}
	return rec, raw, true, nil
}

// ── R1b: резервирование (спецификация §3.2) ─────────────────────────

func generatePhotonID() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(photonIDSpace))
	if err != nil {
		return "", fmt.Errorf("account registry: generate PhotonID: %w", err)
	}
	return fmt.Sprintf("%0*d", photonIDDigits, n.Int64()), nil
}

// allocatePhotonID резервирует идентификатор.
//
// Успешным резервированием считается ТОЛЬКО KVCompareAndSet(pid:N, nil, …),
// вернувшая (true, nil). Ни генерация числа, ни запись в acct: таковым не
// являются — поэтому запись аккаунта никогда не ссылается на
// незарезервированный идентификатор.
func (r *AccountRegistry) allocatePhotonID(provider, subject string) (string, error) {
	owner, err := json.Marshal(pidOwner{Provider: provider, Subject: subject})
	if err != nil {
		return "", err
	}
	for attempt := 0; attempt < maxAllocationAttempts; attempt++ {
		candidate, genErr := r.genID()
		if genErr != nil {
			return "", genErr
		}
		ok, casErr := r.compareAndSet(pidKey(candidate), nil, owner)
		if casErr != nil {
			return "", casErr
		}
		if ok {
			return candidate, nil
		}
		// (false, nil): идентификатор занят — пробуем следующий.
	}
	return "", ErrPhotonIDSpaceExhausted
}

// ── Register: R1a → R1b → R1c ───────────────────────────────────────

// Register приводит запись к состоянию registered и идемпотентен.
//
// Возвращает актуальную запись для ЛЮБОГО исходного состояния (§6):
// отсутствует → полный проход; claimed → продолжение; registered/linked →
// возврат как есть, без повторной регистрации.
//
// Никогда не откатывает сделанное конкурентом.
func (r *AccountRegistry) Register(provider, subject, email string) (AccountRecord, error) {
	key := acctKey(provider, subject)

	for attempt := 0; attempt < maxAdvanceAttempts; attempt++ {
		rec, raw, found, err := r.get(key)
		if err != nil {
			return AccountRecord{}, err
		}

		if !found {
			// R1a claim. Гонка двух входов одного (Provider, Subject)
			// разрешается здесь: (true, nil) достаётся ровно одному.
			claimed := AccountRecord{
				State:     StateClaimed,
				Provider:  provider,
				Subject:   subject,
				Email:     email,
				CreatedAt: r.now().Format(time.RFC3339Nano),
			}
			body, mErr := json.Marshal(claimed)
			if mErr != nil {
				return AccountRecord{}, mErr
			}
			ok, casErr := r.compareAndSet(key, nil, body)
			if casErr != nil {
				return AccountRecord{}, casErr
			}
			if !ok {
				continue // проиграл claim — перечитать и продолжить по чужой записи
			}
			rec, raw = claimed, body
		}

		switch rec.State {
		case StateRegistered, StateLinked:
			// Состояния C/D/E: регистрация уже выполнена. Ничего не
			// делаем — это и есть идемпотентность.
			return rec, nil

		case StateClaimed:
			// R1b: резерв. Выполняется ПОСЛЕ выигранного claim, поэтому
			// типичная гонка двух входов одного пользователя не теряет
			// ни одного идентификатора.
			photonID, allocErr := r.allocatePhotonID(provider, subject)
			if allocErr != nil {
				return AccountRecord{}, allocErr
			}

			// R1c: перевод записи в registered.
			registered := rec
			registered.State = StateRegistered
			registered.PhotonID = photonID
			registered.LoginName = LoginNameFor(photonID)
			body, mErr := json.Marshal(registered)
			if mErr != nil {
				return AccountRecord{}, mErr
			}
			ok, casErr := r.compareAndSet(key, raw, body)
			if casErr != nil {
				return AccountRecord{}, casErr
			}
			if !ok {
				// Проигран R1c. Зарезервированный photonID остаётся
				// занятым НАВСЕГДА и никому не выдаётся — освобождение
				// создало бы окно переиспользования, запрещённое ADR-018
				// (спецификация §4.3).
				continue
			}
			return registered, nil

		default:
			return AccountRecord{}, fmt.Errorf("account registry: unknown state %q", rec.State)
		}
	}
	return AccountRecord{}, ErrRegistrationStalled
}

// ── R2: link ────────────────────────────────────────────────────────

// Link записывает идентификатор пользователя Mattermost, созданного внешним
// шагом U. Идемпотентен: уже связанная запись возвращается как есть.
func (r *AccountRegistry) Link(provider, subject, mmUserID string) (AccountRecord, error) {
	key := acctKey(provider, subject)

	for attempt := 0; attempt < maxAdvanceAttempts; attempt++ {
		rec, raw, found, err := r.get(key)
		if err != nil {
			return AccountRecord{}, err
		}
		if !found {
			return AccountRecord{}, ErrAccountNotFound
		}
		switch rec.State {
		case StateLinked:
			return rec, nil // уже связана — повторный вход ничего не меняет
		case StateClaimed:
			return AccountRecord{}, ErrNotRegistered
		case StateRegistered:
			linked := rec
			linked.State = StateLinked
			linked.MMUserID = mmUserID
			body, mErr := json.Marshal(linked)
			if mErr != nil {
				return AccountRecord{}, mErr
			}
			ok, casErr := r.compareAndSet(key, raw, body)
			if casErr != nil {
				return AccountRecord{}, casErr
			}
			if !ok {
				continue // конкурент опередил — перечитать и продолжить по его записи
			}
			return linked, nil
		default:
			return AccountRecord{}, fmt.Errorf("account registry: unknown state %q", rec.State)
		}
	}
	return AccountRecord{}, ErrRegistrationStalled
}

// Lookup читает запись, ничего не изменяя.
func (r *AccountRegistry) Lookup(provider, subject string) (AccountRecord, bool, error) {
	rec, _, found, err := r.get(acctKey(provider, subject))
	return rec, found, err
}

// OwnerOfPhotonID отвечает, кем занят идентификатор.
//
// Сам по себе поиском пользователя не является: резерв может быть
// осиротевшим либо ещё не связанным. Поиск — FindByPhotonID (ADR-081).
func (r *AccountRegistry) OwnerOfPhotonID(photonID string) (pidOwner, bool, error) {
	raw, appErr := r.kv.KVGet(pidKey(photonID))
	if appErr != nil {
		return pidOwner{}, false, fmt.Errorf("account registry: get %s: %w", pidKey(photonID), appErr)
	}
	if len(raw) == 0 {
		return pidOwner{}, false, nil
	}
	var owner pidOwner
	if err := json.Unmarshal(raw, &owner); err != nil {
		return pidOwner{}, false, err
	}
	return owner, true, nil
}

// FindByPhotonID — поиск аккаунта по PhotonID (ADR-081 §2.2).
//
// Идёт по обратной связи pid: → (Provider, Subject) → acct: и возвращает
// запись, только если она связана с пользователем Mattermost и её photon_id
// совпадает с искомым. Осиротевший резерв, запись до linked и расхождение
// pid:↔acct: — «не найдено» (found=false), без различения (§2.3).
func (r *AccountRegistry) FindByPhotonID(photonID string) (AccountRecord, bool, error) {
	owner, found, err := r.OwnerOfPhotonID(photonID)
	if err != nil || !found {
		return AccountRecord{}, false, err
	}
	rec, found, err := r.Lookup(owner.Provider, owner.Subject)
	if err != nil || !found {
		return AccountRecord{}, false, err
	}
	if rec.PhotonID != photonID || rec.State != StateLinked || rec.MMUserID == "" {
		return AccountRecord{}, false, nil
	}
	return rec, true, nil
}

// jsonMarshal — тонкая обёртка, чтобы тесты собирали запись тем же
// кодеком, что и реестр.
func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
