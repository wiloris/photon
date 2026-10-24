package account

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/wiloris/photon/photon-core/ports"
)

// --- LoginOrRegister ---------------------------------------------------

func TestLoginOrRegister_CreatesNewAccount(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	idp.addToken("valid-token", ports.ExternalIdentity{Provider: "google", Subject: "sub-1", Email: "user@example.com"})

	svc := New(storage, newTestIdentity(), idp)

	acc, err := svc.LoginOrRegister(ctx, "valid-token")
	if err != nil {
		t.Fatalf("LoginOrRegister: %v", err)
	}
	if acc.PhotonID == "" {
		t.Fatal("PhotonID is empty")
	}
	if len(acc.PhotonID) != 9 {
		t.Fatalf("PhotonID length = %d, want 9 (ADR-018)", len(acc.PhotonID))
	}
	if acc.Provider != "google" || acc.Subject != "sub-1" {
		t.Fatalf("unexpected Provider/Subject: %+v", acc)
	}
}

func TestLoginOrRegister_SameTokenTwice_ReturnsSameAccount(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	idp.addToken("valid-token", ports.ExternalIdentity{Provider: "google", Subject: "sub-1", Email: "user@example.com"})

	svc := New(storage, newTestIdentity(), idp)

	acc1, err := svc.LoginOrRegister(ctx, "valid-token")
	if err != nil {
		t.Fatalf("LoginOrRegister (1): %v", err)
	}
	acc2, err := svc.LoginOrRegister(ctx, "valid-token")
	if err != nil {
		t.Fatalf("LoginOrRegister (2): %v", err)
	}

	if acc1.PhotonID != acc2.PhotonID {
		t.Fatalf("same token produced different accounts: %q != %q", acc1.PhotonID, acc2.PhotonID)
	}
}

func TestLoginOrRegister_SameProviderDifferentSubject_DifferentAccounts(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	idp.addToken("token-a", ports.ExternalIdentity{Provider: "google", Subject: "sub-a"})
	idp.addToken("token-b", ports.ExternalIdentity{Provider: "google", Subject: "sub-b"})

	svc := New(storage, newTestIdentity(), idp)

	accA, err := svc.LoginOrRegister(ctx, "token-a")
	if err != nil {
		t.Fatalf("LoginOrRegister (a): %v", err)
	}
	accB, err := svc.LoginOrRegister(ctx, "token-b")
	if err != nil {
		t.Fatalf("LoginOrRegister (b): %v", err)
	}

	if accA.PhotonID == accB.PhotonID {
		t.Fatal("different Subject produced the same PhotonID")
	}
}

func TestLoginOrRegister_SameSubjectDifferentProvider_DifferentAccounts(t *testing.T) {
	// Прямая проверка ADR-023: Subject уникален только внутри Provider.
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	idp.addToken("token-google", ports.ExternalIdentity{Provider: "google", Subject: "12345"})
	idp.addToken("token-apple", ports.ExternalIdentity{Provider: "apple", Subject: "12345"})

	svc := New(storage, newTestIdentity(), idp)

	accGoogle, err := svc.LoginOrRegister(ctx, "token-google")
	if err != nil {
		t.Fatalf("LoginOrRegister (google): %v", err)
	}
	accApple, err := svc.LoginOrRegister(ctx, "token-apple")
	if err != nil {
		t.Fatalf("LoginOrRegister (apple): %v", err)
	}

	if accGoogle.PhotonID == accApple.PhotonID {
		t.Fatal("same Subject under different Provider collapsed into the same account — ADR-023 violated")
	}
}

func TestLoginOrRegister_InvalidToken(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()

	svc := New(storage, newTestIdentity(), idp)

	_, err := svc.LoginOrRegister(ctx, "no-such-token")
	if !errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid, got %v", err)
	}
}

func TestLoginOrRegister_ProviderUnavailable(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	idp.err = errors.New("provider unreachable")

	svc := New(storage, newTestIdentity(), idp)

	_, err := svc.LoginOrRegister(ctx, "any-token")
	if err == nil {
		t.Fatal("expected error when provider is unreachable, got nil")
	}
}

// --- RegisterDevice ------------------------------------------------------

func loginTestAccount(t *testing.T, svc *Service, idp *mockIdentityProvider) Account {
	t.Helper()
	idp.addToken("tok", ports.ExternalIdentity{Provider: "google", Subject: "sub-1"})
	acc, err := svc.LoginOrRegister(context.Background(), "tok")
	if err != nil {
		t.Fatalf("loginTestAccount: %v", err)
	}
	return acc
}

func TestRegisterDevice_FirstDevice(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	svc := New(storage, newTestIdentity(), idp)
	acc := loginTestAccount(t, svc, idp)

	pair, err := newTestIdentity().Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	dev, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "My Phone")
	if err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}
	if dev.PhotonID != acc.PhotonID {
		t.Fatalf("Device.PhotonID = %q, want %q", dev.PhotonID, acc.PhotonID)
	}
	if dev.DeviceID == "" {
		t.Fatal("DeviceID is empty")
	}
}

func TestRegisterDevice_SecondDeviceSameAccount(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	svc := New(storage, newTestIdentity(), idp)
	acc := loginTestAccount(t, svc, idp)

	pair1, _ := newTestIdentity().Generate(ctx)
	pair2, _ := newTestIdentity().Generate(ctx)

	if _, err := svc.RegisterDevice(ctx, acc.PhotonID, pair1.Public, "Phone"); err != nil {
		t.Fatalf("RegisterDevice (1): %v", err)
	}
	if _, err := svc.RegisterDevice(ctx, acc.PhotonID, pair2.Public, "Laptop"); err != nil {
		t.Fatalf("RegisterDevice (2): %v", err)
	}

	devices, err := svc.ListDevices(ctx, acc.PhotonID)
	if err != nil {
		t.Fatalf("ListDevices: %v", err)
	}
	if len(devices) != 2 {
		t.Fatalf("ListDevices returned %d devices, want 2", len(devices))
	}
}

func TestRegisterDevice_UnknownAccount(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	svc := New(storage, newTestIdentity(), idp)

	pair, _ := newTestIdentity().Generate(ctx)

	_, err := svc.RegisterDevice(ctx, PhotonID("000000001"), pair.Public, "Ghost Phone")
	if !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("expected ErrAccountNotFound, got %v", err)
	}
}

func TestRegisterDevice_DuplicatePublicKey(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	svc := New(storage, newTestIdentity(), idp)
	acc := loginTestAccount(t, svc, idp)

	pair, _ := newTestIdentity().Generate(ctx)

	if _, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "Phone"); err != nil {
		t.Fatalf("RegisterDevice (1): %v", err)
	}
	_, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "Phone Again")
	if !errors.Is(err, ErrDeviceAlreadyRegistered) {
		t.Fatalf("expected ErrDeviceAlreadyRegistered, got %v", err)
	}
}

// --- RevokeDevice ---------------------------------------------------------

func TestRevokeDevice_RemovesFromListAndBlocksAuthentication(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	identity := newTestIdentity()
	svc := New(storage, identity, idp)
	acc := loginTestAccount(t, svc, idp)

	pair, _ := identity.Generate(ctx)
	if _, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "Phone"); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}

	if err := svc.RevokeDevice(ctx, pair.Public); err != nil {
		t.Fatalf("RevokeDevice: %v", err)
	}

	devices, err := svc.ListDevices(ctx, acc.PhotonID)
	if err != nil {
		t.Fatalf("ListDevices: %v", err)
	}
	if len(devices) != 0 {
		t.Fatalf("device still present in list after revocation: %+v", devices)
	}

	// Согласованность двух namespace (ADR-024): устройство должно
	// исчезнуть и из обратного индекса, иначе Authenticate прошёл бы,
	// хотя устройство отозвано.
	// Подпись ВАЛИДНАЯ (доменная, ADR-037) — иначе Verify отсечёт вызов
	// раньше обращения к хранилищу, и тест перестанет проверять то, ради
	// чего написан: согласованность двух namespace, а не подпись.
	challenge := []byte("challenge")
	sig, err := identity.Sign(ctx, pair.Private, deviceAuthInput(challenge))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	_, err = svc.AuthenticateDevice(ctx, pair.Public, challenge, sig)
	if !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("expected ErrDeviceNotFound after revocation, got %v", err)
	}
}

func TestRevokeDevice_UnknownDevice(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	svc := New(storage, newTestIdentity(), idp)

	pair, _ := newTestIdentity().Generate(ctx)
	err := svc.RevokeDevice(ctx, pair.Public)
	if !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("expected ErrDeviceNotFound, got %v", err)
	}
}

// --- AuthenticateDevice ----------------------------------------------------

func TestAuthenticateDevice_ValidSignature(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	identity := newTestIdentity()
	svc := New(storage, identity, idp)
	acc := loginTestAccount(t, svc, idp)

	pair, _ := identity.Generate(ctx)
	if _, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "Phone"); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}

	challenge := []byte("server-issued-challenge")
	sig, err := identity.Sign(ctx, pair.Private, deviceAuthInput(challenge))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	dev, err := svc.AuthenticateDevice(ctx, pair.Public, challenge, sig)
	if err != nil {
		t.Fatalf("AuthenticateDevice: %v", err)
	}
	if dev.PhotonID != acc.PhotonID {
		t.Fatalf("authenticated device belongs to %q, want %q", dev.PhotonID, acc.PhotonID)
	}
}

func TestAuthenticateDevice_WrongSignature(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	identity := newTestIdentity()
	svc := New(storage, identity, idp)
	acc := loginTestAccount(t, svc, idp)

	pair, _ := identity.Generate(ctx)
	if _, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "Phone"); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}

	otherPair, _ := identity.Generate(ctx)
	// Домен тот же, ключ чужой: тест проверяет владение ключом, а не
	// доменное разделение (для него — TestAuthenticateDevice_RawChallengeRejected).
	challenge := []byte("server-issued-challenge")
	wrongSig, err := identity.Sign(ctx, otherPair.Private, deviceAuthInput(challenge))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	_, err = svc.AuthenticateDevice(ctx, pair.Public, challenge, wrongSig)
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestAuthenticateDevice_UnknownPublicKey(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	identity := newTestIdentity()
	svc := New(storage, identity, idp)

	pair, _ := identity.Generate(ctx)
	// Подпись ВАЛИДНАЯ: тест про незарегистрированный ключ, и он обязан
	// доходить до хранилища, а не отсекаться на проверке подписи.
	challenge := []byte("challenge")
	sig, _ := identity.Sign(ctx, pair.Private, deviceAuthInput(challenge))

	_, err := svc.AuthenticateDevice(ctx, pair.Public, challenge, sig)
	if !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("expected ErrDeviceNotFound, got %v", err)
	}
}

func TestAuthenticateDevice_UpdatesLastSeen(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	identity := newTestIdentity()
	svc := New(storage, identity, idp)
	acc := loginTestAccount(t, svc, idp)

	pair, _ := identity.Generate(ctx)
	registered, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "Phone")
	if err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}

	challenge := []byte("challenge")
	sig, _ := identity.Sign(ctx, pair.Private, deviceAuthInput(challenge))

	authenticated, err := svc.AuthenticateDevice(ctx, pair.Public, challenge, sig)
	if err != nil {
		t.Fatalf("AuthenticateDevice: %v", err)
	}

	if !authenticated.LastSeen.After(registered.LastSeen) && !authenticated.LastSeen.Equal(registered.LastSeen) {
		t.Fatalf("LastSeen not updated: before=%v after=%v", registered.LastSeen, authenticated.LastSeen)
	}
}

// --- ListDevices -----------------------------------------------------------

func TestListDevices_EmptyForAccountWithNoDevices(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	svc := New(storage, newTestIdentity(), idp)
	acc := loginTestAccount(t, svc, idp)

	devices, err := svc.ListDevices(ctx, acc.PhotonID)
	if err != nil {
		t.Fatalf("ListDevices: %v", err)
	}
	if len(devices) != 0 {
		t.Fatalf("expected 0 devices, got %d", len(devices))
	}
}

func TestListDevices_DoesNotLeakOtherAccountsDevices(t *testing.T) {
	// Прямая проверка того, что префиксный индекс photon.devices не
	// возвращает устройства другого PhotonID — критично, раз ключ
	// строится конкатенацией PhotonID+"|"+hex(PublicKey).
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	identity := newTestIdentity()
	svc := New(storage, identity, idp)

	idp.addToken("tok-1", ports.ExternalIdentity{Provider: "google", Subject: "sub-1"})
	acc1, err := svc.LoginOrRegister(ctx, "tok-1")
	if err != nil {
		t.Fatalf("LoginOrRegister (1): %v", err)
	}
	idp.addToken("tok-2", ports.ExternalIdentity{Provider: "google", Subject: "sub-2"})
	acc2, err := svc.LoginOrRegister(ctx, "tok-2")
	if err != nil {
		t.Fatalf("LoginOrRegister (2): %v", err)
	}

	pair1, _ := identity.Generate(ctx)
	pair2, _ := identity.Generate(ctx)
	if _, err := svc.RegisterDevice(ctx, acc1.PhotonID, pair1.Public, "Acc1 Phone"); err != nil {
		t.Fatalf("RegisterDevice (acc1): %v", err)
	}
	if _, err := svc.RegisterDevice(ctx, acc2.PhotonID, pair2.Public, "Acc2 Phone"); err != nil {
		t.Fatalf("RegisterDevice (acc2): %v", err)
	}

	devices1, err := svc.ListDevices(ctx, acc1.PhotonID)
	if err != nil {
		t.Fatalf("ListDevices (acc1): %v", err)
	}
	if len(devices1) != 1 || string(devices1[0].PublicKey) != string(pair1.Public) {
		t.Fatalf("ListDevices(acc1) leaked or missing devices: %+v", devices1)
	}
}

// --- ADR-037: доменное разделение подписей ---------------------------------
//
// Домен `photon-device-auth:` существует не ради формы, а потому что без него
// подпись, полученная плагином через `identity.sign` (право из каталога SDK),
// годится как аутентификация устройства: достаточно, чтобы challenge совпал с
// подписанными данными. Вторая половина решения — `plugin-sign` в
// `photon-plugin-runtime/coreops` — уже реализована; эти тесты закрывают
// первую.

// registeredDevice — зарегистрированное устройство и его пара ключей.
// Вынесено, потому что три теста ниже отличаются только тем, ЧТО подписано.
func registeredDevice(t *testing.T) (context.Context, *Service, ports.Identity, ports.IdentityPair) {
	t.Helper()
	ctx := context.Background()
	storage := newTestStorage(t)
	idp := newMockIdentityProvider()
	identity := newTestIdentity()
	svc := New(storage, identity, idp)

	acc := loginTestAccount(t, svc, idp)

	pair, err := identity.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, err := svc.RegisterDevice(ctx, acc.PhotonID, pair.Public, "device"); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}
	return ctx, svc, identity, pair
}

// Подпись под СЫРЫМ challenge не проходит. Это и есть дыра, которую закрывает
// ADR-037: до правки AuthenticateDevice принимал любые байты как challenge и
// любую подпись под ними.
func TestAuthenticateDevice_RawChallengeRejected(t *testing.T) {
	ctx, svc, identity, pair := registeredDevice(t)

	challenge := []byte("server-issued-challenge")
	rawSig, err := identity.Sign(ctx, pair.Private, challenge)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	_, err = svc.AuthenticateDevice(ctx, pair.Public, challenge, rawSig)
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("подпись под сырым challenge принята — домен не применён; err=%v", err)
	}

	// Контроль: доменное представление тем же ключом проходит. Без него тест
	// был бы зелёным и при полностью сломанной проверке подписи.
	domainSig, err := identity.Sign(ctx, pair.Private, deviceAuthInput(challenge))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := svc.AuthenticateDevice(ctx, pair.Public, challenge, domainSig); err != nil {
		t.Fatalf("подпись под доменным представлением отвергнута: %v", err)
	}
}

// Подпись, сделанная в домене плагина, не является аутентификацией устройства.
// Представление строится здесь ВРУЧНУЮ из байт, а не импортом coreops:
// photon-core не знает о плагинах (ADR-030), и знать не должен — проверяется
// непересечение входных пространств, а не чужой код.
func TestAuthenticateDevice_PluginSignatureIsNotDeviceAuth(t *testing.T) {
	ctx, svc, identity, pair := registeredDevice(t)

	challenge := []byte("server-issued-challenge")

	// "plugin-sign:" ‖ uint32be(len(plugin_id)) ‖ plugin_id ‖ ":" ‖ data,
	// где data — тот самый challenge (ADR-037, формат домена плагина).
	pluginID := "com.example.plugin"
	var pluginInput []byte
	pluginInput = append(pluginInput, "plugin-sign:"...)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(pluginID)))
	pluginInput = append(pluginInput, n[:]...)
	pluginInput = append(pluginInput, pluginID...)
	pluginInput = append(pluginInput, ':')
	pluginInput = append(pluginInput, challenge...)

	pluginSig, err := identity.Sign(ctx, pair.Private, pluginInput)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	_, err = svc.AuthenticateDevice(ctx, pair.Public, challenge, pluginSig)
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("подпись плагина принята как аутентификация устройства; err=%v", err)
	}
}

// Доменный литерал — граница версии схемы (ADR-037), поэтому он должен
// совпадать точно. Близкие варианты не проходят.
func TestAuthenticateDevice_DomainPrefixIsExact(t *testing.T) {
	ctx, svc, identity, pair := registeredDevice(t)

	challenge := []byte("server-issued-challenge")
	for _, prefix := range []string{
		"photon-device-auth",  // без двоеточия
		"photon-device-auth2", // иная версия схемы
		"device-auth:",        // без вендорной части
		"Photon-Device-Auth:", // иной регистр
	} {
		t.Run(prefix, func(t *testing.T) {
			input := append([]byte(prefix), challenge...)
			sig, err := identity.Sign(ctx, pair.Private, input)
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}
			if _, err := svc.AuthenticateDevice(ctx, pair.Public, challenge, sig); !errors.Is(err, ErrInvalidSignature) {
				t.Fatalf("принят префикс %q; err=%v", prefix, err)
			}
		})
	}
}

// Литерал домена закреплён побайтово.
//
// Найдено мутацией: подмена литерала на `photon-device-auth2:` не роняла ни
// одного теста — все они выводили ожидаемое из самой deviceAuthInput и были
// самосогласованы. Между тем литерал по ADR-037 САМ является границей версии
// схемы: его смена — смена протокола, и клиент (Phase 16) реализует формат по
// спецификации, а не по этой функции. Тест, не замечающий переименования,
// проходил бы всегда.
func TestDeviceAuthInput_FormatIsPinned(t *testing.T) {
	got := deviceAuthInput([]byte("challenge"))
	const want = "photon-device-auth:challenge"
	if string(got) != want {
		t.Fatalf("формат домена изменился:\n получено %q\n ожидалось %q\n"+
			"Смена литерала — смена версии схемы (ADR-037): нужен новый ADR и"+
			" правка docs/account-device-flow.md, а не правка этого теста.", got, want)
	}

	// Пустой challenge не превращает вход в пустые байты: домен остаётся.
	if string(deviceAuthInput(nil)) != "photon-device-auth:" {
		t.Fatalf("домен теряется на пустом challenge: %q", deviceAuthInput(nil))
	}
}
