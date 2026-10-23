//go:build android

package pion

import (
	"os"

	"github.com/wlynxg/anet"
)

// На Android сообщить anet версию системы до первого перечисления
// интерфейсов (см. androidsdk.go). /system/build.prop читаем приложением.
func init() {
	raw, _ := os.ReadFile("/system/build.prop")
	anet.SetAndroidVersion(parseAndroidSDK(string(raw)))
}
