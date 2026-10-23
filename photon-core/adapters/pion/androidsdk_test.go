package pion

import "testing"

// Регрессия S4 (ADR-116): без версии Android anet идёт запрещённым на API
// 30+ путём, и ICE на устройстве не собирает кандидатов.
func TestParseAndroidSDK(t *testing.T) {
	cases := []struct {
		prop string
		want uint
	}{
		{"# begin build properties\nro.build.version.release=14\nro.build.version.sdk=34\n", 34},
		{"ro.build.version.sdk = 29\n", 29},
		{"ro.build.version.sdk=26", 26},
		{"ro.build.version.sdk_full=35\nro.build.version.sdk=35\n", 35},
		// Не прочитали или не разобрали — путь без bind(), а не запрещённый.
		{"", androidAPI30},
		{"ro.build.version.sdk=abc\n", androidAPI30},
		{"ro.build.version.sdk=0\n", androidAPI30},
		{"ro.build.version.release=14\n", androidAPI30},
	}
	for _, c := range cases {
		if got := parseAndroidSDK(c.prop); got != c.want {
			t.Errorf("%q: %d, ожидалось %d", c.prop, got, c.want)
		}
	}
}
