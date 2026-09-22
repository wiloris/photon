// Command idcheck — плагин, доказывающий identity.public_key и identity.verify
// на настоящей границе процесса.
//
// Зачем он нужен отдельно. Обе операции покрыты каталожным E2E, но тот идёт
// через net.Pipe и фиктивный обработчик: он доказывает, что SDK правильно
// формирует запрос, и ничего не говорит о том, что настоящий исполнитель
// вернёт осмысленный результат. Здесь путь целиком:
//
//	процесс плагина → SDK → IPC → Authorizer → coreops → adapters/ed25519
//
// Проверки семантические, а не «вызов не вернул ошибку»:
//
//  1. публичный ключ устройства доступен и непуст;
//  2. известная верная подпись (вектор RFC 8032) признаётся верной;
//  3. та же подпись с испорченным байтом признаётся НЕверной, причём это
//     (false, nil) — отрицательный результат, а не ошибка;
//  4. та же подпись против ДРУГОГО ключа — ключа устройства — тоже неверна.
//
// Третья и четвёртая — главные: реализация, отвечающая «верно» на что угодно,
// прошла бы первые две и провалилась здесь.
//
// ПОЧЕМУ ВЕКТОР, А НЕ СОБСТВЕННАЯ ПОДПИСЬ. Плагин не может проверить подпись,
// полученную через identity.sign: sign подписывает ДОМЕННО РАЗДЕЛЁННЫЙ вход
// ("plugin-sign:" ‖ len(id) ‖ id ‖ ":" ‖ data, ADR-037), а verify проверяет
// ровно переданные байты — это операция общего назначения, ею проверяют и
// подписи, сделанные вне Photon. Формула домена принадлежит доверенной
// стороне, и плагину недоступна намеренно. Выяснилось первым же прогоном
// этого теста: редакция с собственной подписью падала, и падала правильно.
//
// Результат плагин кладёт В ХРАНИЛИЩЕ, а не в журнал: тест читает файл базы и
// не зависит от разбора строк.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	sdk "github.com/wiloris/photon-plugin-sdk"
)

const (
	pluginID  = "dev.photon.idcheck"
	namespace = "result"
	resultKey = "identity"
	okValue   = "ok"
)

// Вектор RFC 8032 §7.1, TEST 1: пустое сообщение, известные ключ и подпись.
// Значения зафиксированы стандартом и от нашей реализации не зависят — именно
// поэтому годятся как независимый оракул для verify.
var (
	vectorPublicKey = mustHex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
	vectorMessage   = []byte{}
	vectorSignature = mustHex("e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b")
)

func main() {
	ctx := context.Background()

	p, err := sdk.Connect(ctx, sdk.Config{
		PluginID:        pluginID,
		ProtocolVersion: 1,
	})
	if err != nil {
		fail(2, "connect", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	if err := check(ctx, p); err != nil {
		fail(3, "identity", err)
	}
	if err := p.Storage().Put(ctx, namespace, resultKey, []byte(okValue)); err != nil {
		fail(4, "put result", err)
	}
	if err := p.WorkComplete(ctx); err != nil {
		fail(5, "work complete", err)
	}

	if err := <-runDone; err != nil {
		fail(6, "run", err)
	}
}

func check(ctx context.Context, p sdk.Plugin) error {
	pub, err := p.Identity().PublicKey(ctx)
	if err != nil {
		return fmt.Errorf("public_key: %w", err)
	}
	if len(pub) == 0 {
		return fmt.Errorf("public_key пуст")
	}

	ok, err := p.Identity().Verify(ctx, vectorPublicKey, vectorMessage, vectorSignature)
	if err != nil {
		return fmt.Errorf("verify(vector): %w", err)
	}
	if !ok {
		return fmt.Errorf("verify отверг подпись из вектора RFC 8032")
	}

	bad := append([]byte(nil), vectorSignature...)
	bad[0] ^= 0xFF
	ok, err = p.Identity().Verify(ctx, vectorPublicKey, vectorMessage, bad)
	if err != nil {
		return fmt.Errorf("verify(broken) вернул ошибку вместо false: %w", err)
	}
	if ok {
		return fmt.Errorf("verify принял испорченную подпись")
	}

	// Тот же вектор против ключа устройства. Связывает public_key с verify:
	// проверка идёт ИМЕННО переданным ключом, а не каким-то своим.
	ok, err = p.Identity().Verify(ctx, pub, vectorMessage, vectorSignature)
	if err != nil {
		return fmt.Errorf("verify(device key): %w", err)
	}
	if ok {
		return fmt.Errorf("verify принял чужую подпись под ключом устройства")
	}
	return nil
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func fail(code int, what string, err error) {
	fmt.Fprintf(os.Stderr, "idcheck: %s: %v\n", what, err)
	os.Exit(code)
}
