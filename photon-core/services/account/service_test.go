package account

import (
	"context"
	"errors"
	"testing"

	"github.com/wiloris/photon-core/ports"
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
	challenge := []byte("challenge")
	sig, err := identity.Sign(ctx, pair.Private, challenge)
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
	sig, err := identity.Sign(ctx, pair.Private, challenge)
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
	challenge := []byte("server-issued-challenge")
	wrongSig, err := identity.Sign(ctx, otherPair.Private, challenge)
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
	challenge := []byte("challenge")
	sig, _ := identity.Sign(ctx, pair.Private, challenge)

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
	sig, _ := identity.Sign(ctx, pair.Private, challenge)

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
