// Command storage is a Plugin Test Laboratory fixture
// (docs/test-laboratory.md, "storage"). Unlike permission, it does not
// attack anything — every call here is well-formed and authorized. The
// property under test is invisibility, not denial: this plugin reads
// its own "secret" and gets its own value back, never suspecting
// another plugin's identically-named key exists at all. Isolation here
// comes entirely from the runtime's namespace prefixing being
// transparent, not from anything this plugin does.
//
// Mode is selected by PHOTON_TEST_MODE; the value to write (where
// relevant) by PHOTON_TEST_VALUE; identity override by
// PHOTON_TEST_PLUGIN_ID (same conventions as crash/permission).
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	sdk "github.com/wiloris/photon/photon-plugin-sdk"
)

const defaultPluginID = "com.engram.test.storage"

func pluginID() string {
	if id := os.Getenv("PHOTON_TEST_PLUGIN_ID"); id != "" {
		return id
	}
	return defaultPluginID
}

func main() {
	switch os.Getenv("PHOTON_TEST_MODE") {
	case "write-secret":
		writeSecret()
	case "read-secret":
		readSecret()
	case "write-many":
		writeMany()
	case "list-keys":
		listKeys()
	case "delete-secret":
		deleteSecret()
	default:
		os.Exit(2)
	}
}

func writeSecret() {
	p, runDone := connect()
	ctx := context.Background()
	err := p.Storage().Put(ctx, "data", "secret", []byte(os.Getenv("PHOTON_TEST_VALUE")))
	report(p, ctx, "write", err)
	<-runDone
}

func readSecret() {
	p, runDone := connect()
	ctx := context.Background()
	val, err := p.Storage().Get(ctx, "data", "secret")
	if err != nil {
		report(p, ctx, "read", err)
	} else {
		logMessage(p, ctx, "read:"+string(val))
	}
	<-runDone
}

func writeMany() {
	p, runDone := connect()
	ctx := context.Background()
	value := os.Getenv("PHOTON_TEST_VALUE")
	for _, key := range []string{"k1", "k2", "k3"} {
		if err := p.Storage().Put(ctx, "data", key, []byte(value+":"+key)); err != nil {
			report(p, ctx, "write_many", err)
			<-runDone
			return
		}
	}
	report(p, ctx, "write_many", nil)
	<-runDone
}

func listKeys() {
	p, runDone := connect()
	ctx := context.Background()
	keys, err := p.Storage().List(ctx, "data", "")
	if err != nil {
		report(p, ctx, "list", err)
	} else {
		sort.Strings(keys)
		logMessage(p, ctx, "list:"+strings.Join(keys, ","))
	}
	<-runDone
}

func deleteSecret() {
	p, runDone := connect()
	ctx := context.Background()
	err := p.Storage().Delete(ctx, "data", "secret")
	report(p, ctx, "delete", err)
	<-runDone
}

func connect() (sdk.Plugin, chan error) {
	p, err := sdk.Connect(context.Background(), sdk.Config{
		PluginID:        pluginID(),
		ProtocolVersion: 1,
	})
	if err != nil {
		os.Exit(1)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(context.Background()) }()
	return p, runDone
}

func report(p sdk.Plugin, ctx context.Context, name string, err error) {
	status := "OK"
	if err != nil {
		status = fmt.Sprintf("ERROR:%v", err)
	}
	logMessage(p, ctx, name+":"+status)
}

func logMessage(p sdk.Plugin, ctx context.Context, msg string) {
	_ = p.Log(ctx, sdk.LogInfo, msg)
}
