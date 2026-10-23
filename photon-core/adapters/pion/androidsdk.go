package pion

import (
	"bufio"
	"strconv"
	"strings"
)

// Android 11 (API 30) запретил приложениям bind() на netlink-сокете
// (SELinux, b/155595000): стандартный net.Interfaces() там падает, ICE не
// собирает ни одного кандидата, и согласование P2P проваливается целиком.
// pion/transport перечисляет интерфейсы через github.com/wlynxg/anet, но
// anet обходит запрет, только если ему сообщили версию Android
// (anet.SetAndroidVersion); по умолчанию версия 0, и он сам падает обратно
// на запрещённый путь. Найдено сквозной приёмкой Chat Roulette на API 34
// (ADR-116, срез S4): avc denied { bind } netlink_route_socket.

// androidAPI30 — граница, по которой anet выбирает путь без bind().
const androidAPI30 = 30

// parseAndroidSDK находит ro.build.version.sdk в тексте build.prop. Не
// нашёл или не разобрал — API 30: путь anet без bind() работает и на
// старых версиях, а запрещённый на новых — нет.
func parseAndroidSDK(buildProp string) uint {
	sc := bufio.NewScanner(strings.NewReader(buildProp))
	for sc.Scan() {
		k, v, ok := strings.Cut(strings.TrimSpace(sc.Text()), "=")
		if !ok || strings.TrimSpace(k) != "ro.build.version.sdk" {
			continue
		}
		if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 32); err == nil && n > 0 {
			return uint(n)
		}
	}
	return androidAPI30
}
