package transportlab

import (
	"net"
	"sync"
	"sync/atomic"
)

// relay — управляемый UDP-ретранслятор между ядром и сервером.
//
// На каждый исходный адрес клиента — отдельный сокет к серверу: сервер видит
// адрес ретранслятора, и смену этого сокета (rebind) он видит как смену
// адреса клиента — NAT rebinding.
type relay struct {
	frontMu sync.Mutex
	front   *net.UDPConn // слушает клиента
	addr    *net.UDPAddr // адрес front — тот же после reopenFront
	server  *net.UDPAddr

	dropUp   atomic.Bool // клиент → сервер теряется
	dropDown atomic.Bool // сервер → клиент теряется

	mu    sync.Mutex
	flows map[string]*flow
	total int // потоков за всё время
}

type flow struct {
	client *net.UDPAddr
	up     *net.UDPConn // к серверу
}

func startRelay(server string) (*relay, error) {
	sa, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return nil, err
	}
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	r := &relay{front: front, addr: front.LocalAddr().(*net.UDPAddr), server: sa, flows: map[string]*flow{}}
	go r.loop(front)
	return r, nil
}

func (r *relay) Addr() string { return r.addr.String() }

func (r *relay) Close() {
	r.frontMu.Lock()
	if r.front != nil {
		r.front.Close()
	}
	r.frontMu.Unlock()
	r.closeFlows()
}

// closeFront — закрыть принимающий сокет: пакеты клиента на этот порт
// получают от ядра этой машины ICMP port unreachable (S3-b, вариант 1 плана —
// только локально, сервер и сеть не затрагиваются).
func (r *relay) closeFront() {
	r.frontMu.Lock()
	defer r.frontMu.Unlock()
	if r.front != nil {
		r.front.Close()
		r.front = nil
	}
}

// reopenFront — снова слушать тот же порт; потоки к серверу сохраняются.
func (r *relay) reopenFront() error {
	front, err := net.ListenUDP("udp", r.addr)
	if err != nil {
		return err
	}
	r.frontMu.Lock()
	r.front = front
	r.frontMu.Unlock()
	go r.loop(front)
	return nil
}

func (r *relay) frontConn() *net.UDPConn {
	r.frontMu.Lock()
	defer r.frontMu.Unlock()
	return r.front
}

func (r *relay) closeFlows() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range r.flows {
		f.up.Close()
	}
	r.flows = map[string]*flow{}
}

// Flows — сколько потоков (исходных адресов клиента) было за всё время.
func (r *relay) Flows() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

func (r *relay) loop(front *net.UDPConn) {
	buf := make([]byte, 64<<10)
	for {
		n, from, err := front.ReadFromUDP(buf)
		if err != nil {
			return
		}
		f := r.flowFor(from)
		if f == nil || r.dropUp.Load() {
			continue
		}
		f.up.Write(buf[:n])
	}
}

func (r *relay) flowFor(client *net.UDPAddr) *flow {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.flows[client.String()]; ok {
		return f
	}
	f, err := r.newUpstream(client)
	if err != nil {
		return nil
	}
	r.flows[client.String()] = f
	r.total++
	return f
}

func (r *relay) newUpstream(client *net.UDPAddr) (*flow, error) {
	up, err := net.DialUDP("udp", nil, r.server)
	if err != nil {
		return nil, err
	}
	f := &flow{client: client, up: up}
	go r.downLoop(f, up)
	return f, nil
}

func (r *relay) downLoop(f *flow, up *net.UDPConn) {
	buf := make([]byte, 64<<10)
	for {
		n, err := up.Read(buf)
		if err != nil {
			return
		}
		if r.dropDown.Load() {
			continue
		}
		if front := r.frontConn(); front != nil {
			front.WriteToUDP(buf[:n], f.client)
		}
	}
}

// rebind — новый исходный порт к серверу для всех текущих потоков: сервер
// видит смену адреса клиента (NAT rebinding). Старые сокеты закрываются.
func (r *relay) rebind() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, f := range r.flows {
		nf, err := r.newUpstream(f.client)
		if err != nil {
			return err
		}
		f.up.Close()
		r.flows[k] = nf
	}
	return nil
}
