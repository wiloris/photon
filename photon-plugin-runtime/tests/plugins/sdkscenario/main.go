// Command sdkscenario is a Plugin Test Laboratory fixture
// (docs/test-laboratory.md, "sdkscenario"). It is the first fixture that
// runs against the REAL executor (coreops) instead of a fake handler, and
// the first that calls identity.* and crypto.* at all.
//
// It uses nothing but the public SDK. It reports every result through
// log.write as "name:value" lines and finishes each mode with "done:<mode>",
// so the driving test never races ahead of an unfinished call.
//
// What this fixture deliberately does NOT know: the ADR-037 signing domain.
// The trusted test builds the domain-separated input itself and passes it in
// PHOTON_TEST_DOMAIN_INPUT; building it here would let the oracle and the
// code under test share the same formatting mistake.
//
// Mode: PHOTON_TEST_MODE. Inputs (base64 where binary): PHOTON_TEST_VALUE,
// PHOTON_TEST_BINDING, PHOTON_TEST_DOMAIN_INPUT. Identity for the SDK
// handshake: PHOTON_TEST_PLUGIN_ID (same conventions as storage/permission).
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"sort"
	"strings"

	sdk "github.com/wiloris/photon-plugin-sdk"
)

const defaultPluginID = "com.engram.test.sdkscenario"

func pluginID() string {
	if id := os.Getenv("PHOTON_TEST_PLUGIN_ID"); id != "" {
		return id
	}
	return defaultPluginID
}

func main() {
	mode := os.Getenv("PHOTON_TEST_MODE")
	run, ok := map[string]func(context.Context, sdk.Plugin){
		"storage-roundtrip": storageRoundtrip,
		"storage-write":     storageWrite,
		"storage-read":      storageRead,
		"sign":              sign,
		"encrypt":           encrypt,
		"decrypt":           decrypt,
	}[mode]
	if !ok {
		os.Exit(2)
	}

	p, err := sdk.Connect(context.Background(), sdk.Config{PluginID: pluginID(), ProtocolVersion: 1})
	if err != nil {
		os.Exit(1)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(context.Background()) }()

	ctx := context.Background()
	run(ctx, p)
	logf(ctx, p, "done:"+mode)
	<-runDone
}

// ── modes ────────────────────────────────────────────────────────────────────

func storageRoundtrip(ctx context.Context, p sdk.Plugin) {
	s := p.Storage()
	value := []byte("roundtrip-value")
	if err := s.Put(ctx, "rt", "k", value); err != nil {
		logf(ctx, p, "roundtrip:put:"+class(err))
		return
	}
	got, err := s.Get(ctx, "rt", "k")
	if err != nil || string(got) != string(value) {
		logf(ctx, p, "roundtrip:get:"+class(err)+":"+string(got))
		return
	}
	keys, err := s.List(ctx, "rt", "")
	if err != nil || strings.Join(keys, ",") != "k" {
		logf(ctx, p, "roundtrip:list:"+class(err)+":"+strings.Join(keys, ","))
		return
	}
	if err := s.Delete(ctx, "rt", "k"); err != nil {
		logf(ctx, p, "roundtrip:delete:"+class(err))
		return
	}
	logf(ctx, p, "roundtrip:OK")
	_, err = s.Get(ctx, "rt", "k")
	logf(ctx, p, "after_delete:"+class(err))
}

func storageWrite(ctx context.Context, p sdk.Plugin) {
	err := p.Storage().Put(ctx, "data", "secret", []byte(os.Getenv("PHOTON_TEST_VALUE")))
	logf(ctx, p, "write:"+class(err))
}

func storageRead(ctx context.Context, p sdk.Plugin) {
	val, err := p.Storage().Get(ctx, "data", "secret")
	if err != nil {
		logf(ctx, p, "read:"+class(err))
	} else {
		logf(ctx, p, "read:value:"+string(val))
	}
	keys, err := p.Storage().List(ctx, "data", "")
	if err != nil {
		logf(ctx, p, "list:"+class(err))
		return
	}
	sort.Strings(keys)
	logf(ctx, p, "list:keys:"+strings.Join(keys, ","))
}

func sign(ctx context.Context, p sdk.Plugin) {
	id := p.Identity()
	pub, err := id.PublicKey(ctx)
	if err != nil {
		logf(ctx, p, "pub:"+class(err))
		return
	}
	logf(ctx, p, "pub:"+b64(pub))

	data := input("PHOTON_TEST_VALUE")
	sig, err := id.Sign(ctx, data)
	if err != nil {
		logf(ctx, p, "sig:"+class(err))
		return
	}
	logf(ctx, p, "sig:"+b64(sig))

	raw, err := id.Verify(ctx, pub, data, sig)
	logf(ctx, p, "verify_raw:"+boolOr(raw, err))
	domain, err := id.Verify(ctx, pub, input("PHOTON_TEST_DOMAIN_INPUT"), sig)
	logf(ctx, p, "verify_domain:"+boolOr(domain, err))
}

func encrypt(ctx context.Context, p sdk.Plugin) {
	ct, err := p.Crypto().Encrypt(ctx, input("PHOTON_TEST_VALUE"), input("PHOTON_TEST_BINDING"))
	if err != nil {
		logf(ctx, p, "ct:"+class(err))
		return
	}
	logf(ctx, p, "ct:"+b64(ct))
}

func decrypt(ctx context.Context, p sdk.Plugin) {
	pt, err := p.Crypto().Decrypt(ctx, input("PHOTON_TEST_VALUE"), input("PHOTON_TEST_BINDING"))
	if err != nil {
		logf(ctx, p, "pt:"+class(err))
		return
	}
	logf(ctx, p, "pt:"+b64(pt))
}

// ── helpers ──────────────────────────────────────────────────────────────────

// class names an SDK error by its sentinel, so the test compares stable
// words rather than message text.
func class(err error) string {
	switch {
	case err == nil:
		return "OK"
	case errors.Is(err, sdk.ErrForbidden):
		return "FORBIDDEN"
	case errors.Is(err, sdk.ErrNotFound):
		return "NOT_FOUND"
	case errors.Is(err, sdk.ErrUnsupported):
		return "UNSUPPORTED"
	case errors.Is(err, sdk.ErrUnauthorized):
		return "UNAUTHORIZED"
	default:
		return "ERROR"
	}
}

func boolOr(v bool, err error) string {
	if err != nil {
		return class(err)
	}
	if v {
		return "true"
	}
	return "false"
}

func input(name string) []byte {
	b, err := base64.StdEncoding.DecodeString(os.Getenv(name))
	if err != nil {
		os.Exit(4)
	}
	return b
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func logf(ctx context.Context, p sdk.Plugin, msg string) {
	_ = p.Log(ctx, sdk.LogInfo, msg)
}
