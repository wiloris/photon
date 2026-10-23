package main

import (
	"fmt"
	"net"
	"sync"
)

// udpProxy — UDP-ретранслятор между клиентом и Bridge.
//
// Нужен ровно для одного: оборвать транспорт по-настоящему. Первая попытка
// вызвать обрыв отодвинутым keep-alive провалилась, и провал содержательный —
// keep-alive шлёт САМ СЕРВЕР (правка Phase 12.5, KeepAlivePeriod 15 с), поэтому
// подписка простоем не умирает. Убить соединение со стороны Bridge без
// админ-доступа нечем, отозвать сессию нельзя — она единственная. Разрыв пути
// остаётся единственным способом получить настоящий обрыв, а не имитацию.
type udpProxy struct {
	ln     *net.UDPConn
	remote *net.UDPAddr

	mu     sync.Mutex
	broken bool
	up     *net.UDPConn
	client *net.UDPAddr
}

func newUDPProxy(remoteAddr string) (*udpProxy, error) {
	ra, err := net.ResolveUDPAddr("udp4", remoteAddr)
	if err != nil {
		return nil, err
	}
	ln, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return nil, err
	}
	p := &udpProxy{ln: ln, remote: ra}
	go p.pumpDown()
	return p, nil
}

func (p *udpProxy) addr() string {
	return fmt.Sprintf("127.0.0.1:%d", p.ln.LocalAddr().(*net.UDPAddr).Port)
}

// pumpDown — от клиента к серверу.
func (p *udpProxy) pumpDown() {
	buf := make([]byte, 2048)
	for {
		n, from, err := p.ln.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.broken {
			p.mu.Unlock()
			continue
		}
		if p.up == nil {
			up, derr := net.DialUDP("udp4", nil, p.remote)
			if derr != nil {
				p.mu.Unlock()
				continue
			}
			p.up = up
			p.client = from
			go p.pumpUp(up)
		}
		up := p.up
		p.mu.Unlock()
		_, _ = up.Write(buf[:n])
	}
}

// pumpUp — от сервера к клиенту.
func (p *udpProxy) pumpUp(up *net.UDPConn) {
	buf := make([]byte, 2048)
	for {
		n, err := up.Read(buf)
		if err != nil {
			return
		}
		p.mu.Lock()
		broken, client := p.broken, p.client
		p.mu.Unlock()
		if broken || client == nil {
			continue
		}
		_, _ = p.ln.WriteToUDP(buf[:n], client)
	}
}

// breakPath рвёт путь: пакеты перестают ходить в обе стороны. Клиент обязан
// обнаружить это по своему idle-timeout — то есть настоящим механизмом QUIC.
func (p *udpProxy) breakPath() {
	p.mu.Lock()
	p.broken = true
	up := p.up
	p.up = nil
	p.mu.Unlock()
	if up != nil {
		up.Close()
	}
}

// healPath восстанавливает путь.
func (p *udpProxy) healPath() {
	p.mu.Lock()
	p.broken = false
	p.mu.Unlock()
}

func (p *udpProxy) close() { p.ln.Close() }
