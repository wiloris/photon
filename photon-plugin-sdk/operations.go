package sdk

import "context"

// Реализации интерфейсов API.
//
// Каждый метод — ровно один вызов c.call с операцией из каталога
// (sdk-boundary.md, раздел 3). Никакой логики между вызовом API и
// отправкой запроса: тело метода состоит из формирования аргументов и
// одного call. Появление здесь условий, циклов или обращений к
// нескольким операциям означало бы, что SDK завёл собственную
// семантику поверх runtime.

// ── Типы запросов и ответов ─────────────────────────────────────────
//
// JSON-структуры соответствуют payload-схемам каталога. Имена полей
// фиксированы: их меняет только изменение протокола, а не рефакторинг
// SDK.

type logRequest struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

// workCompleteRequest — доклад о завершении (ADR-074 §4).
//
// Payload объявлен []byte: encoding/json кодирует его в base64 — ровно
// так же, как Value у storagePutRequest. Механизм не новый, и предел
// (MaxWorkPayloadBytes) проверяется по СЫРЫМ байтам, до кодирования.
//
// omitempty у обоих полей: пустой доклад обязан быть неотличим от
// доклада старого плагина, не знавшего про эти поля (ADR-074 §12).
type workCompleteRequest struct {
	Status  WorkStatus `json:"status,omitempty"`
	Payload []byte     `json:"payload,omitempty"`
}

type storageGetRequest struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

type storageGetResponse struct {
	Value []byte `json:"value"`
}

type storagePutRequest struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value"`
}

type storageDeleteRequest struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

type storageListRequest struct {
	Namespace string `json:"namespace"`
	Prefix    string `json:"prefix"`
}

type storageListResponse struct {
	Keys []string `json:"keys"`
}

type identityPublicKeyResponse struct {
	PublicKey []byte `json:"public_key"`
}

type identitySignRequest struct {
	Data []byte `json:"data"`
}

type identitySignResponse struct {
	Signature []byte `json:"signature"`
}

type identityVerifyRequest struct {
	PublicKey []byte `json:"public_key"`
	Data      []byte `json:"data"`
	Signature []byte `json:"signature"`
}

type identityVerifyResponse struct {
	Valid bool `json:"valid"`
}

type cryptoEncryptRequest struct {
	Plaintext   []byte `json:"plaintext"`
	BindingData []byte `json:"binding_data"`
}

type cryptoEncryptResponse struct {
	Ciphertext []byte `json:"ciphertext"`
}

type cryptoDecryptRequest struct {
	Ciphertext  []byte `json:"ciphertext"`
	BindingData []byte `json:"binding_data"`
}

type cryptoDecryptResponse struct {
	Plaintext []byte `json:"plaintext"`
}

// ── Storage ─────────────────────────────────────────────────────────

type storageClient struct{ c *client }

var _ Storage = (*storageClient)(nil)

func (s *storageClient) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	var resp storageGetResponse
	err := s.c.call(ctx, "storage.get", storageGetRequest{Namespace: namespace, Key: key}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

func (s *storageClient) Put(ctx context.Context, namespace, key string, value []byte) error {
	return s.c.call(ctx, "storage.put",
		storagePutRequest{Namespace: namespace, Key: key, Value: value}, nil)
}

func (s *storageClient) Delete(ctx context.Context, namespace, key string) error {
	return s.c.call(ctx, "storage.delete",
		storageDeleteRequest{Namespace: namespace, Key: key}, nil)
}

func (s *storageClient) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	var resp storageListResponse
	err := s.c.call(ctx, "storage.list",
		storageListRequest{Namespace: namespace, Prefix: prefix}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Keys, nil
}

// ── Identity ────────────────────────────────────────────────────────

type identityClient struct{ c *client }

var _ Identity = (*identityClient)(nil)

func (i *identityClient) PublicKey(ctx context.Context) ([]byte, error) {
	var resp identityPublicKeyResponse
	if err := i.c.call(ctx, "identity.public_key", nil, &resp); err != nil {
		return nil, err
	}
	return resp.PublicKey, nil
}

func (i *identityClient) Sign(ctx context.Context, data []byte) ([]byte, error) {
	var resp identitySignResponse
	if err := i.c.call(ctx, "identity.sign", identitySignRequest{Data: data}, &resp); err != nil {
		return nil, err
	}
	return resp.Signature, nil
}

func (i *identityClient) Verify(ctx context.Context, publicKey, data, signature []byte) (bool, error) {
	var resp identityVerifyResponse
	err := i.c.call(ctx, "identity.verify",
		identityVerifyRequest{PublicKey: publicKey, Data: data, Signature: signature}, &resp)
	if err != nil {
		// Ошибка означает, что решение принять не удалось. Неверная
		// подпись — это (false, nil), и такой ответ приходит с
		// ErrorCode == "" (ports.Identity.Verify, та же семантика).
		return false, err
	}
	return resp.Valid, nil
}

// ── Crypto ──────────────────────────────────────────────────────────

type cryptoClient struct{ c *client }

var _ Crypto = (*cryptoClient)(nil)

func (cr *cryptoClient) Encrypt(ctx context.Context, plaintext, bindingData []byte) ([]byte, error) {
	var resp cryptoEncryptResponse
	err := cr.c.call(ctx, "crypto.encrypt",
		cryptoEncryptRequest{Plaintext: plaintext, BindingData: bindingData}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Ciphertext, nil
}

func (cr *cryptoClient) Decrypt(ctx context.Context, ciphertext, bindingData []byte) ([]byte, error) {
	var resp cryptoDecryptResponse
	err := cr.c.call(ctx, "crypto.decrypt",
		cryptoDecryptRequest{Ciphertext: ciphertext, BindingData: bindingData}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Plaintext, nil
}
