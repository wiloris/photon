// turnlab — локальный TURN-сервер стенда Chat Roulette (ADR-116 §6, срез
// S4). pion/turn с LongTermTURNRESTAuthHandler — та же схема, что coturn
// use-auth-secret (photon-quic-bridge/docs/turn-rest.md). Это стенд, НЕ
// coturn: он доказывает путь relay с краткоживущими учётными данными
// Bridge, но не конфигурацию развёрнутого coturn.
//
// Журнал: каждая первая аутентификация (username, адрес клиента) — по нему
// видно, что relay получили ОБА участника, и под чьими PhotonID.
//
//	go run ./tests/turnlab -listen 192.0.2.2:3478 -relay 192.0.2.2 -secret-file <файл>
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pion/turn/v3"
)

func main() {
	listen := flag.String("listen", "192.0.2.2:3478", "UDP-адрес TURN")
	relay := flag.String("relay", "192.0.2.2", "адрес relay-кандидатов")
	secretFile := flag.String("secret-file", "", "файл с общим секретом (тот же, что TURNSharedSecret Bridge)")
	flag.Parse()
	raw, err := os.ReadFile(*secretFile)
	if err != nil {
		log.Fatalf("секрет: %v", err)
	}
	secret := strings.TrimSpace(string(raw))

	pc, err := net.ListenPacket("udp4", *listen)
	if err != nil {
		log.Fatal(err)
	}
	inner := turn.LongTermTURNRESTAuthHandler(secret, nil)
	var mu sync.Mutex
	seen := map[string]bool{}
	auth := func(username, realm string, src net.Addr) ([]byte, bool) {
		key, ok := inner(username, realm, src)
		k := username + "|" + src.String()
		mu.Lock()
		if !seen[k] {
			seen[k] = true
			fmt.Printf("%s AUTH user=%s src=%s accepted=%v\n", time.Now().Format("15:04:05.000"), username, src, ok)
		}
		mu.Unlock()
		return key, ok
	}
	srv, err := turn.NewServer(turn.ServerConfig{
		Realm:       "photon.lab",
		AuthHandler: auth,
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: pc,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
				RelayAddress: net.ParseIP(*relay),
				Address:      *relay,
			},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s turnlab ready listen=%s relay=%s\n", time.Now().Format("15:04:05.000"), *listen, *relay)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	_ = srv.Close()
}
