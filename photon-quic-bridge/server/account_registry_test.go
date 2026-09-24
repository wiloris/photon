package main

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// ── in-memory KV с НАСТОЯЩИМ compare-and-set ────────────────────────
//
// Мок с заранее объявленными ожиданиями здесь не годится: он проверял бы
// сценарий, записанный автором теста, а не поведение протокола под гонкой.
// Семантика воспроизводит документированную у Mattermost:
//
//	CAS(key, nil, v) — вставка при отсутствии;
//	(false, nil)     — проигрыш;
//	KVGet            — nil для отсутствующего ключа, не ошибка.

type fakeKV struct {
	mu   sync.Mutex
	data map[string][]byte

	// failGet/failCAS включают отказ ХРАНИЛИЩА — путь (false, err),
	// который обязан отличаться от проигранной гонки.
	failGet *model.AppError
	failCAS *model.AppError

	casCalls int
	setCalls int

	// afterCAS вызывается ПОСЛЕ снятия блокировки, с ключом и исходом.
	// Нужен, чтобы воспроизвести окно §4.3 детерминированно: без него
	// тест надеется на планировщик и обычно не создаёт осиротевшего
	// резерва вовсе — то есть проходит, ничего не проверив.
	afterCAS func(key string, ok bool)
}

func newFakeKV() *fakeKV { return &fakeKV{data: map[string][]byte{}} }

func (f *fakeKV) KVGet(key string) ([]byte, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGet != nil {
		return nil, f.failGet
	}
	v, ok := f.data[key]
	if !ok {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (f *fakeKV) KVCompareAndSet(key string, oldValue, newValue []byte) (bool, *model.AppError) {
	ok, appErr := f.casLocked(key, oldValue, newValue)
	if f.afterCAS != nil && appErr == nil {
		f.afterCAS(key, ok)
	}
	return ok, appErr
}

func (f *fakeKV) casLocked(key string, oldValue, newValue []byte) (bool, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casCalls++
	if f.failCAS != nil {
		return false, f.failCAS
	}
	cur, exists := f.data[key]
	if oldValue == nil {
		if exists {
			return false, nil // ключ уже есть при вставке
		}
	} else if !exists || !bytes.Equal(cur, oldValue) {
		return false, nil
	}
	stored := make([]byte, len(newValue))
	copy(stored, newValue)
	f.data[key] = stored
	f.setCalls++
	return true, nil
}

func (f *fakeKV) countPrefix(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for k := range f.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func (f *fakeKV) put(key string, value []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
}

func kvFailure(msg string) *model.AppError {
	return model.NewAppError("test", msg, nil, "", 500)
}

func mustRegister(t *testing.T, r *AccountRegistry, provider, subject, email string) AccountRecord {
	t.Helper()
	rec, err := r.Register(provider, subject, email)
	if err != nil {
		t.Fatalf("Register(%s,%s): %v", provider, subject, err)
	}
	return rec
}

// ── базовый проход ──────────────────────────────────────────────────

func TestRegisterFullSequence(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)

	rec := mustRegister(t, r, "google", "sub-1", "a@example.com")
	if rec.State != StateRegistered {
		t.Fatalf("state %q, ожидалось %q", rec.State, StateRegistered)
	}
	if len(rec.PhotonID) != photonIDDigits {
		t.Fatalf("PhotonID %q — ожидалось %d цифр (ADR-018)", rec.PhotonID, photonIDDigits)
	}
	if rec.LoginName != LoginNameFor(rec.PhotonID) {
		t.Fatalf("LoginName %q не выведен из PhotonID", rec.LoginName)
	}
	// Резерв обязан существовать и принадлежать этой паре.
	owner, found, err := r.OwnerOfPhotonID(rec.PhotonID)
	if err != nil || !found {
		t.Fatalf("резерв pid: не создан (found=%v, err=%v)", found, err)
	}
	if owner.Provider != "google" || owner.Subject != "sub-1" {
		t.Fatalf("резерв принадлежит %+v", owner)
	}

	linked, err := r.Link("google", "sub-1", "mmuser1")
	if err != nil {
		t.Fatalf("Link: %v", err)
	}
	if linked.State != StateLinked || linked.MMUserID != "mmuser1" {
		t.Fatalf("после Link: %+v", linked)
	}
}

// ── I2 + конкурентный вход одного (Provider, Subject) ───────────────

func TestConcurrentRegistrationsOfSameIdentityYieldOneAccount(t *testing.T) {
	const goroutines = 16
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	// Фиксированное время делает тело claim у всех горутин ПОБАЙТОВО
	// одинаковым. Без этого проигравший claim маскируется: его R1c падает
	// на несовпадении меток времени, и защита R1a остаётся непроверенной.
	r.now = func() time.Time { return time.Unix(0, 0).UTC() }

	var wg sync.WaitGroup
	results := make([]AccountRecord, goroutines)
	errs := make([]error, goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = r.Register("google", "same-subject", "x@example.com")
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("горутина %d: %v", i, err)
		}
	}
	// I2: ровно одна запись acct: на эту пару.
	if n := kvs.countPrefix("acct:"); n != 1 {
		t.Fatalf("записей acct: %d, ожидалась 1 — второй Account для того же external identity", n)
	}
	// Все исходы указывают на ОДИН PhotonID.
	first := results[0].PhotonID
	for i, rec := range results {
		if rec.State != StateRegistered {
			t.Fatalf("горутина %d: состояние %q", i, rec.State)
		}
		if rec.PhotonID != first {
			t.Fatalf("горутина %d получила PhotonID %q, у первой %q — гонка дала два Account",
				i, rec.PhotonID, first)
		}
	}
}

// Проигравший claim не выделяет собственный PhotonID.
//
// Свойство отдельное от I1/I2: корректность (один Account, один
// идентификатор) держится на R1c и без этой проверки. Здесь проверяется то,
// ради чего §3.1 спецификации ставит claim РАНЬШЕ allocate — что типичная
// гонка двух входов одного пользователя не теряет ни одного идентификатора.
//
// Порядок задан детерминированно: проигравший claim не продолжает, пока
// победитель не завершил R1c. Без этого барьера оба успевают увидеть
// claimed, оба выделяют — то самое окно §4.3, которое здесь ни при чём.
func TestLostClaimDoesNotAllocateExtraReservation(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	r.now = func() time.Time { return time.Unix(0, 0).UTC() }

	// Барьер привязан к ИСХОДУ CAS, а не к номеру горутины: кто выиграл
	// claim, заранее неизвестно, и ожидание конкретной горутины давало бы
	// дедлок в половине прогонов.
	done := make(chan struct{})
	var once sync.Once
	kvs.afterCAS = func(key string, ok bool) {
		if ok || len(key) < 5 || key[:5] != "acct:" {
			return
		}
		<-done // проигравший claim ждёт, пока победитель завершит R1c
	}

	var wg sync.WaitGroup
	recs := make([]AccountRecord, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i], errs[i] = r.Register("google", "sub-claimrace", "")
			once.Do(func() { close(done) })
		}(i)
	}
	wg.Wait()
	kvs.afterCAS = nil

	for i, err := range errs {
		if err != nil {
			t.Fatalf("горутина %d: %v", i, err)
		}
	}
	if recs[0].PhotonID != recs[1].PhotonID {
		t.Fatalf("разные PhotonID: %q и %q", recs[0].PhotonID, recs[1].PhotonID)
	}
	if n := kvs.countPrefix("pid:"); n != 1 {
		t.Fatalf("резервов %d, ожидался 1 — проигравший claim выделил собственный "+
			"идентификатор вместо того, чтобы перечитать запись (§3.1)", n)
	}
	if n := kvs.countPrefix("acct:"); n != 1 {
		t.Fatalf("записей acct: %d", n)
	}
}

// ── I1: два разных external identity не получают один PhotonID ──────

func TestConcurrentRegistrationsOfDistinctIdentitiesGetDistinctPhotonIDs(t *testing.T) {
	const n = 64
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)

	var wg sync.WaitGroup
	recs := make([]AccountRecord, n)
	errs := make([]error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			recs[i], errs[i] = r.Register("google", fmt.Sprintf("sub-%d", i), "")
		}(i)
	}
	close(start)
	wg.Wait()

	seen := map[string]int{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("горутина %d: %v", i, err)
		}
		if prev, dup := seen[recs[i].PhotonID]; dup {
			t.Fatalf("PhotonID %q выдан дважды: subject-%d и subject-%d",
				recs[i].PhotonID, prev, i)
		}
		seen[recs[i].PhotonID] = i
	}
	if len(seen) != n {
		t.Fatalf("уникальных PhotonID %d, ожидалось %d", len(seen), n)
	}
	if got := kvs.countPrefix("acct:"); got != n {
		t.Fatalf("записей acct: %d, ожидалось %d", got, n)
	}
}

// ── I1 на уровне резерва: занятый кандидат не выдаётся ──────────────

// Занятый идентификатор не выдаётся — проверяется НА СТОЛКНОВЕНИИ, а не по
// счастливой случайности: генератор сначала отдаёт уже занятое значение.
func TestAllocationRejectsAlreadyReservedCandidate(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)

	first := mustRegister(t, r, "google", "sub-a", "")

	// Следующая регистрация дважды предложит занятый идентификатор и лишь
	// затем свободный. Принять первый — значит выдать один PhotonID двум
	// аккаунтам.
	calls := 0
	r.genID = func() (string, error) {
		calls++
		if calls <= 2 {
			return first.PhotonID, nil
		}
		return "000000777", nil
	}
	second := mustRegister(t, r, "google", "sub-b", "")
	if second.PhotonID == first.PhotonID {
		t.Fatal("выдан уже зарезервированный PhotonID — резерв принят без успешного CAS")
	}
	if second.PhotonID != "000000777" {
		t.Fatalf("PhotonID %q, ожидался 000000777", second.PhotonID)
	}
	if calls != 3 {
		t.Fatalf("генератор вызван %d раз — занятые кандидаты не отвергались", calls)
	}
	owner, _, _ := r.OwnerOfPhotonID(first.PhotonID)
	if owner.Subject != "sub-a" {
		t.Fatalf("резерв первого перешёл к %+v", owner)
	}
}

// Исчерпание пространства даёт ошибку, а не бесконечный цикл.
func TestAllocationExhaustionIsReported(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	first := mustRegister(t, r, "google", "sub-x", "")
	r.genID = func() (string, error) { return first.PhotonID, nil }

	if _, err := r.Register("google", "sub-y", ""); !errors.Is(err, ErrPhotonIDSpaceExhausted) {
		t.Fatalf("ошибка %v, ожидалась ErrPhotonIDSpaceExhausted", err)
	}
}

func TestAllocationSkipsReservedPhotonID(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)

	first := mustRegister(t, r, "google", "sub-a", "")
	// Второй регистрации тот же идентификатор достаться не может: ключ
	// pid: занят, и CAS(nil) вернёт (false, nil).
	second := mustRegister(t, r, "google", "sub-b", "")
	if second.PhotonID == first.PhotonID {
		t.Fatal("второй Account получил уже зарезервированный PhotonID")
	}
	owner, _, _ := r.OwnerOfPhotonID(first.PhotonID)
	if owner.Subject != "sub-a" {
		t.Fatalf("резерв первого перешёл к %+v", owner)
	}
}

// ── I3 + §4.3: осиротевший резерв остаётся занятым навсегда ─────────

// Воспроизводится сценарий спецификации §4.3: два входа застали claimed,
// оба зарезервировали РАЗНЫЕ идентификаторы, R1c выиграл один.
func TestOrphanedReservationStaysTakenForever(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)

	// Состояние B: запись claimed есть, регистрация не завершена.
	mustClaim(t, kvs, "google", "sub-race")

	// Окно §4.3 воспроизводится ДЕТЕРМИНИРОВАННО: обе горутины обязаны
	// завершить R1b (резерв pid:) ПРЕЖДЕ, чем любая начнёт R1c. Без этого
	// барьера планировщик почти всегда пропускает их по очереди, и
	// осиротевший резерв не возникает — тест проходит, не проверив
	// собственное утверждение.
	var (
		reserved  sync.WaitGroup
		released  = make(chan struct{})
		relOnce   sync.Once
		arrivals  int
		arrivalMu sync.Mutex
	)
	reserved.Add(2)
	kvs.afterCAS = func(key string, ok bool) {
		if !ok || len(key) < 4 || key[:4] != "pid:" {
			return
		}
		arrivalMu.Lock()
		arrivals++
		n := arrivals
		arrivalMu.Unlock()
		reserved.Done()
		if n <= 2 {
			<-released // держим обе до второго резерва
		}
	}
	go func() {
		reserved.Wait()
		relOnce.Do(func() { close(released) })
	}()

	var wg sync.WaitGroup
	recs := make([]AccountRecord, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i], errs[i] = r.Register("google", "sub-race", "")
		}(i)
	}
	wg.Wait()
	kvs.afterCAS = nil

	for i, err := range errs {
		if err != nil {
			t.Fatalf("горутина %d: %v", i, err)
		}
	}
	// Обе завершились ОДНИМ Account и одним PhotonID — проигравший R1c
	// продолжил по чужой записи, а не создал вторую.
	if recs[0].PhotonID != recs[1].PhotonID {
		t.Fatalf("две горутины завершились разными PhotonID: %q и %q",
			recs[0].PhotonID, recs[1].PhotonID)
	}
	winner := recs[0].PhotonID

	// Резервов ровно два: победивший и осиротевший.
	pids := collectPIDs(kvs)
	if len(pids) != 2 {
		t.Fatalf("резервов %d, ожидалось 2 — окно §4.3 не воспроизвелось, "+
			"тест не проверил собственное утверждение", len(pids))
	}
	if _, ok := pids[winner]; !ok {
		t.Fatalf("резерв победителя %s отсутствует", winner)
	}

	// I3: ОБА остаются занятыми навсегда, включая осиротевший.
	var orphan string
	for id := range pids {
		owner, found, err := r.OwnerOfPhotonID(id)
		if err != nil || !found {
			t.Fatalf("резерв %s исчез — идентификатор освобождён (ADR-018 запрещает)", id)
		}
		if owner.Subject != "sub-race" {
			t.Fatalf("резерв %s принадлежит %+v", id, owner)
		}
		if id != winner {
			orphan = id
		}
	}

	// Осиротевший не может быть выдан другому: ключ занят, CAS(nil) вернёт
	// (false, nil) — проверяем это прямо, а не по факту его наличия.
	if ok, err := kvs.KVCompareAndSet(pidKey(orphan), nil, []byte(`{"provider":"x","subject":"y"}`)); err != nil || ok {
		t.Fatalf("осиротевший резерв %s удалось перезанять (ok=%v)", orphan, ok)
	}
	t.Logf("победивший PhotonID %s; осиротевший %s остаётся занятым навсегда", winner, orphan)
}

func collectPIDs(f *fakeKV) map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]struct{}{}
	for k := range f.data {
		if len(k) > 4 && k[:4] == "pid:" {
			out[k[4:]] = struct{}{}
		}
	}
	return out
}

// mustClaim создаёт запись в состоянии claimed, минуя Register.
func mustClaim(t *testing.T, kvs *fakeKV, provider, subject string) AccountRecord {
	t.Helper()
	rec := AccountRecord{
		State: StateClaimed, Provider: provider, Subject: subject,
		CreatedAt: "2026-09-24T00:00:00Z",
	}
	body, err := jsonMarshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	kvs.put(acctKey(provider, subject), body)
	return rec
}

// ── recovery: прерывание на каждом этапе (спецификация §5, §6) ──────

func TestRecoveryFromClaimed(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	mustClaim(t, kvs, "google", "sub-B")

	rec := mustRegister(t, r, "google", "sub-B", "")
	if rec.State != StateRegistered {
		t.Fatalf("состояние %q — повторный вход не продолжил из claimed", rec.State)
	}
	if n := kvs.countPrefix("acct:"); n != 1 {
		t.Fatalf("записей acct: %d — создан второй Account вместо продолжения", n)
	}
}

func TestRecoveryFromRegisteredReturnsStateForExternalU(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	first := mustRegister(t, r, "google", "sub-C", "")

	// Прерывание после R1c: внешний U ещё не выполнялся.
	again := mustRegister(t, r, "google", "sub-C", "")
	if again.PhotonID != first.PhotonID {
		t.Fatalf("PhotonID изменился: %q → %q", first.PhotonID, again.PhotonID)
	}
	if again.State != StateRegistered || again.MMUserID != "" {
		t.Fatalf("состояние %+v", again)
	}
	// I4: всё необходимое для идемпотентного продолжения через внешний U.
	if again.LoginName == "" || again.LoginName != LoginNameFor(again.PhotonID) {
		t.Fatalf("LoginName %q не позволяет найти осиротевшего пользователя", again.LoginName)
	}
	// Второго резерва не появилось.
	if n := kvs.countPrefix("pid:"); n != 1 {
		t.Fatalf("резервов %d — повторный вход выделил новый идентификатор", n)
	}
}

func TestRecoveryAfterExternalUButBeforeLink(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	first := mustRegister(t, r, "google", "sub-C2", "")

	// Состояние C: внешний U выполнился, R2 — нет. Для реестра это
	// НЕОТЛИЧИМО от «U не вызывался», и он обязан вести себя одинаково.
	again := mustRegister(t, r, "google", "sub-C2", "")
	if again.State != StateRegistered || again.PhotonID != first.PhotonID {
		t.Fatalf("состояние %+v", again)
	}
	// Продолжение: связываем тем же пользователем, найденным по LoginName.
	linked, err := r.Link("google", "sub-C2", "mm-found-by-login")
	if err != nil {
		t.Fatalf("Link: %v", err)
	}
	if linked.State != StateLinked || linked.MMUserID != "mm-found-by-login" {
		t.Fatalf("после Link: %+v", linked)
	}
}

func TestRecoveryFromLinkedDoesNotRegisterAgain(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	first := mustRegister(t, r, "google", "sub-D", "")
	linked, err := r.Link("google", "sub-D", "mmuser-D")
	if err != nil {
		t.Fatal(err)
	}

	again := mustRegister(t, r, "google", "sub-D", "")
	if again.State != StateLinked {
		t.Fatalf("состояние %q — повторный вход начал регистрацию заново", again.State)
	}
	if again.PhotonID != first.PhotonID || again.MMUserID != linked.MMUserID {
		t.Fatalf("запись изменилась: %+v", again)
	}
	if n := kvs.countPrefix("pid:"); n != 1 {
		t.Fatalf("резервов %d — повторный вход выделил новый", n)
	}
}

func TestLinkIsIdempotent(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	mustRegister(t, r, "google", "sub-E", "")
	if _, err := r.Link("google", "sub-E", "mm-1"); err != nil {
		t.Fatal(err)
	}
	again, err := r.Link("google", "sub-E", "mm-2")
	if err != nil {
		t.Fatalf("повторный Link: %v", err)
	}
	if again.MMUserID != "mm-1" {
		t.Fatalf("повторный Link перезаписал связь на %q", again.MMUserID)
	}
}

// ── I5: идемпотентность повторного входа ────────────────────────────

func TestRepeatedLoginIsIdempotent(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)

	first := mustRegister(t, r, "google", "sub-idem", "e@example.com")
	if _, err := r.Link("google", "sub-idem", "mm-idem"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		rec := mustRegister(t, r, "google", "sub-idem", "e@example.com")
		if rec.PhotonID != first.PhotonID {
			t.Fatalf("вход #%d дал PhotonID %q, первый — %q", i, rec.PhotonID, first.PhotonID)
		}
		if rec.MMUserID != "mm-idem" {
			t.Fatalf("вход #%d потерял связь: %+v", i, rec)
		}
	}
	if n := kvs.countPrefix("acct:"); n != 1 {
		t.Fatalf("записей acct: %d после десяти входов", n)
	}
	if n := kvs.countPrefix("pid:"); n != 1 {
		t.Fatalf("резервов %d после десяти входов", n)
	}
}

// ── I6: (false, nil) и (false, err) обрабатываются РАЗЛИЧНО ─────────

func TestStorageFailureIsNotTreatedAsLostRace(t *testing.T) {
	kvs := newFakeKV()
	kvs.failCAS = kvFailure("kv down")
	r := NewAccountRegistry(kvs)

	failure := kvs.failCAS
	_, err := r.Register("google", "sub-fail", "")
	if err == nil {
		t.Fatal("отказ хранилища принят за проигранную гонку — Register вернул успех")
	}
	// Недостаточно «какой-нибудь ошибки»: приняв отказ за проигрыш, реестр
	// отработал бы предел повторов и вернул ErrRegistrationStalled — тоже
	// ошибку, но описывающую не то, что случилось.
	if errors.Is(err, ErrRegistrationStalled) {
		t.Fatalf("отказ хранилища подан как исчерпание попыток: %v", err)
	}
	if !errors.Is(err, failure) {
		t.Fatalf("ошибка не несёт причину отказа хранилища: %v", err)
	}
	// И он обязан прерывать работу сразу, а не долбить лежащее хранилище.
	if kvs.casCalls != 1 {
		t.Fatalf("CAS вызван %d раз при отказе хранилища — повторы по недоступному KV", kvs.casCalls)
	}
	if kvs.countPrefix("acct:") != 0 {
		t.Fatal("при отказе хранилища появилась запись")
	}
}

func TestStorageFailureOnGetIsReported(t *testing.T) {
	kvs := newFakeKV()
	kvs.failGet = kvFailure("kv down")
	r := NewAccountRegistry(kvs)

	if _, err := r.Register("google", "sub-fail2", ""); err == nil {
		t.Fatal("отказ чтения не привёл к ошибке")
	}
	if _, _, err := r.Lookup("google", "sub-fail2"); err == nil {
		t.Fatal("Lookup скрыл отказ чтения")
	}
}

// Проигранная гонка НЕ является ошибкой: тот же путь обязан завершиться
// успехом по чужой записи.
func TestLostRaceIsNotAnError(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	// Конкурент уже полностью зарегистрировал эту пару.
	mustRegister(t, r, "google", "sub-lost", "")

	rec, err := r.Register("google", "sub-lost", "")
	if err != nil {
		t.Fatalf("проигранная гонка отдана как ошибка: %v", err)
	}
	if rec.State != StateRegistered {
		t.Fatalf("состояние %q", rec.State)
	}
}

// ── границы ─────────────────────────────────────────────────────────

func TestLinkOnClaimedIsRejected(t *testing.T) {
	kvs := newFakeKV()
	r := NewAccountRegistry(kvs)
	mustClaim(t, kvs, "google", "sub-claimed")

	if _, err := r.Link("google", "sub-claimed", "mm-x"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("Link из claimed дал %v, ожидался ErrNotRegistered", err)
	}
}

func TestLinkOnMissingAccount(t *testing.T) {
	r := NewAccountRegistry(newFakeKV())
	if _, err := r.Link("google", "nobody", "mm-x"); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("Link несуществующего дал %v", err)
	}
}

func TestKeysFollowBridgeConvention(t *testing.T) {
	if got := acctKey("google", "sub"); got != "acct:google:sub" {
		t.Fatalf("acctKey = %q", got)
	}
	if got := pidKey("000000042"); got != "pid:000000042" {
		t.Fatalf("pidKey = %q", got)
	}
	if got := LoginNameFor("000000042"); got != "engram-000000042" {
		t.Fatalf("LoginNameFor = %q", got)
	}
}
