package pion

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pion/dtls/v2"
	"github.com/pion/dtls/v2/pkg/crypto/selfsign"
	"github.com/pion/ice/v3"
	"github.com/pion/sctp"
	"github.com/pion/stun/v2"
	"github.com/wiloris/photon-core/ports"
)

var _ ports.P2P = (*Adapter)(nil)

// Config — параметры адаптера.
type Config struct {
	// Identity и Self — ключ устройства, которым подписывается handshake
	// ADR-041. Приватный ключ не покидает адаптер и никуда не передаётся
	// (ADR-021).
	Identity ports.Identity
	Self     ports.IdentityPair

	// STUNTURN — адреса вида "stun:host:port" / "turn:host:port".
	// Учётные данные TURN задаются здесь же (TURNUser/TURNPass): конкретный
	// сервер — конфигурация развёртывания, а не предмет заморозки
	// (ADR-043 §1).
	STUNTURN []string
	TURNUser string
	TURNPass string

	// ForceRelay исключает host- и srflx-кандидатов, оставляя только relay.
	// Нужен для acceptance-проверки relay-пути: без него TURN может быть
	// настроен и ни разу не использован, а прогон выглядел бы проверкой
	// relay, не будучи ею (p2p-session.md §10.2).
	ForceRelay bool

	// EstablishTimeout — обязательный потолок согласования (ADR-044 §3).
	// Без него зависание неотличимо от медленной сети.
	EstablishTimeout time.Duration
}

const defaultEstablishTimeout = 30 * time.Second

// Adapter реализует ports.P2P.
//
// Владеет ресурсами СОГЛАСОВАНИЯ; ресурсами установленного соединения владеет
// session. Поэтому Close() не закрывает уже установленные сессии — прямое
// требование порта (ADR-044 §8).
type Adapter struct {
	cfg Config

	// dtlsCert — эфемерный самоподписанный сертификат ТОЛЬКО для DTLS.
	// К личности Photon отношения не имеет и ею не является: кто на другом
	// конце, устанавливает исключительно handshake ADR-041. Сертификат нужен
	// самому протоколу DTLS, у которого без него не с чем делать рукопожатие.
	dtlsCert tls.Certificate

	mu       sync.Mutex
	closed   bool
	pending  map[string]*negotiation // ключ — nonce инициатора
	sessions map[*session]struct{}
}

// negotiation — начатое CreateOffer и ещё не завершённое согласование.
//
// Живёт ОГРАНИЧЕННОЕ время. Между CreateOffer и CompleteSession стоит
// доставка блоба чужим каналом (ADR-042), и ответ может не прийти никогда:
// собеседник офлайн, канал потерял сообщение, вызывающий передумал. Без
// потолка такое согласование держало бы ICE-агент и его UDP-сокеты до
// Close() адаптера — то самое «зависание, неотличимое от медленной сети»,
// которое ADR-044 §3 запрещает, только заметное не по задержке, а по утечке.
type negotiation struct {
	agent *ice.Agent
	peer  ports.PublicKey
	nonce []byte
	// stop снимает сторож; вызывается тем, кто первым забрал согласование.
	stop func()
}

func New(cfg Config) (*Adapter, error) {
	if cfg.Identity == nil {
		return nil, errors.New("pion: Config.Identity is required")
	}
	if len(cfg.Self.Public) == 0 || len(cfg.Self.Private) == 0 {
		return nil, errors.New("pion: Config.Self must carry a key pair")
	}
	if cfg.EstablishTimeout <= 0 {
		cfg.EstablishTimeout = defaultEstablishTimeout
	}
	// ForceRelay без TURN — не «строгая настройка», а конфигурация, при
	// которой не соберётся ни один кандидат. Молча она даёт offer без
	// кандидатов и отказ по потолку через десятки секунд, причём с ошибкой,
	// не называющей причину. Отвергается на построении.
	if cfg.ForceRelay && !hasTURN(cfg.STUNTURN) {
		return nil, errors.New("pion: Config.ForceRelay requires at least one turn:/turns: URL in STUNTURN")
	}
	cert, err := selfsign.GenerateSelfSigned()
	if err != nil {
		return nil, fmt.Errorf("pion: dtls certificate: %w", err)
	}
	return &Adapter{
		cfg:      cfg,
		dtlsCert: cert,
		pending:  make(map[string]*negotiation),
		sessions: make(map[*session]struct{}),
	}, nil
}

func hasTURN(urls []string) bool {
	for _, raw := range urls {
		if u, err := stun.ParseURI(raw); err == nil &&
			(u.Scheme == stun.SchemeTypeTURN || u.Scheme == stun.SchemeTypeTURNS) {
			return true
		}
	}
	return false
}

func (a *Adapter) agentConfig() (*ice.AgentConfig, error) {
	urls := make([]*stun.URI, 0, len(a.cfg.STUNTURN))
	for _, raw := range a.cfg.STUNTURN {
		u, err := stun.ParseURI(raw)
		if err != nil {
			return nil, fmt.Errorf("pion: bad STUN/TURN url %q: %w", raw, err)
		}
		if u.Scheme == stun.SchemeTypeTURN || u.Scheme == stun.SchemeTypeTURNS {
			u.Username, u.Password = a.cfg.TURNUser, a.cfg.TURNPass
		}
		urls = append(urls, u)
	}
	cfg := &ice.AgentConfig{
		Urls: urls,
		// Логгер задаётся ЯВНО у каждого слоя. Оставленный нулевым, он
		// означает не «молчать», а NewDefaultLoggerFactory с Writer =
		// os.Stderr: библиотека начинает писать в поток чужого процесса.
		// Для Core, живущего за C ABI внутри приложения, это решение за
		// вызывающего — см. logging.go.
		LoggerFactory: nopLoggerFactory{},
		// UDP4 явно, а не по умолчанию: набор сетевых типов влияет на то,
		// какие кандидаты вообще собираются, и оставлять его неявным значит
		// зависеть от умолчания библиотеки в вопросе, который определяет
		// поведение адаптера.
		NetworkTypes: []ice.NetworkType{ice.NetworkTypeUDP4},
		// mDNS выключен: он подменяет host-кандидатов на .local-имена ради
		// приватности, что для нашего сценария не нужно и делает прогон
		// зависимым от многоадресной рассылки в сети.
		MulticastDNSMode: ice.MulticastDNSModeDisabled,
	}
	if a.cfg.ForceRelay {
		cfg.CandidateTypes = []ice.CandidateType{ice.CandidateTypeRelay}
	}
	return cfg, nil
}

// gather собирает кандидатов целиком: trickle ICE не поддерживается, потому
// что порт даёт ровно два обмена и третьего канала для дозагрузки нет
// (p2p-session.md §4.3).
func gather(ctx context.Context, agent *ice.Agent) ([]string, error) {
	done := make(chan struct{})
	var mu sync.Mutex
	var out []string
	if err := agent.OnCandidate(func(c ice.Candidate) {
		if c == nil {
			close(done)
			return
		}
		mu.Lock()
		out = append(out, c.Marshal())
		mu.Unlock()
	}); err != nil {
		return nil, err
	}
	if err := agent.GatherCandidates(); err != nil {
		return nil, err
	}
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	mu.Lock()
	defer mu.Unlock()
	// Ноль кандидатов — отказ здесь и сейчас, а не блоб, с которым
	// согласование заведомо не состоится. Без этой проверки причина
	// («TURN не ответил», «интерфейсов нет») подменялась бы истечением
	// потолка на противоположной стороне.
	if len(out) == 0 {
		return nil, fmt.Errorf("pion: no ICE candidates gathered: %w", ports.ErrSessionFailed)
	}
	return out, nil
}

// CreateOffer — шаг 1 инициатора.
func (a *Adapter) CreateOffer(ctx context.Context, peer ports.PublicKey) ([]byte, error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, ports.ErrSessionFailed
	}
	a.mu.Unlock()

	acfg, err := a.agentConfig()
	if err != nil {
		return nil, err
	}
	agent, err := ice.NewAgent(acfg)
	if err != nil {
		return nil, fmt.Errorf("pion: new ice agent: %w", err)
	}
	cands, err := gather(ctx, agent)
	if err != nil {
		_ = agent.Close()
		return nil, err
	}
	ufrag, pwd, err := agent.GetLocalUserCredentials()
	if err != nil {
		_ = agent.Close()
		return nil, fmt.Errorf("pion: local ice credentials: %w", err)
	}
	nonce, err := newNonce()
	if err != nil {
		_ = agent.Close()
		return nil, err
	}

	raw, err := (&blob{
		Version: blobVersion, ICEUfrag: ufrag, ICEPwd: pwd,
		Candidates: cands, DTLSRole: roleServer, Nonce: nonce,
	}).encode()
	if err != nil {
		_ = agent.Close()
		return nil, err
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		_ = agent.Close()
		return nil, ports.ErrSessionFailed
	}
	// Сторож: отмена контекста вызывающего ЛИБО истечение потолка снимает
	// согласование с учёта и освобождает агент. Контекст здесь тот же, что у
	// остальных шагов, и по ADR-044 §2 его отмена означает «согласование
	// прервано, локальные ресурсы этой попытки освобождаются» — а не «offer
	// уже отдан, дальше не моё дело».
	//
	// stop проставляется ДО публикации в pending и под тем же замком: иначе
	// CompleteSession успевал бы забрать согласование и прочитать поле, пока
	// оно записывается.
	stop := make(chan struct{})
	neg := &negotiation{agent: agent, peer: peer, nonce: nonce, stop: func() { close(stop) }}
	a.pending[string(nonce)] = neg
	a.mu.Unlock()

	go a.watchPending(ctx, nonce, stop)
	return raw, nil
}

// watchPending освобождает незавершённое согласование по отмене контекста или
// по истечении потолка.
//
// Снять сторож имеет право только тот, кто ЗАБРАЛ согласование из pending:
// удаление из карты под замком и есть передача владения, второго владельца у
// согласования не возникает.
func (a *Adapter) watchPending(ctx context.Context, nonce []byte, stop <-chan struct{}) {
	t := time.NewTimer(a.cfg.EstablishTimeout)
	defer t.Stop()
	select {
	case <-stop:
		return
	case <-ctx.Done():
	case <-t.C:
	}
	a.mu.Lock()
	neg, ok := a.pending[string(nonce)]
	if ok {
		delete(a.pending, string(nonce))
	}
	a.mu.Unlock()
	if ok {
		_ = neg.agent.Close()
	}
}

// AcceptOffer — шаг 2 отвечающего. Session возвращается ДО установления
// (ADR-044 §1): её методы блокируются до исхода.
func (a *Adapter) AcceptOffer(ctx context.Context, peer ports.PublicKey, offer []byte) ([]byte, ports.Session, error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, nil, ports.ErrSessionFailed
	}
	a.mu.Unlock()

	off, err := decodeBlob(offer, roleServer)
	if err != nil {
		return nil, nil, err
	}
	acfg, err := a.agentConfig()
	if err != nil {
		return nil, nil, err
	}
	agent, err := ice.NewAgent(acfg)
	if err != nil {
		return nil, nil, fmt.Errorf("pion: new ice agent: %w", err)
	}
	cands, err := gather(ctx, agent)
	if err != nil {
		_ = agent.Close()
		return nil, nil, err
	}
	ufrag, pwd, err := agent.GetLocalUserCredentials()
	if err != nil {
		_ = agent.Close()
		return nil, nil, fmt.Errorf("pion: local ice credentials: %w", err)
	}
	nonce, err := newNonce()
	if err != nil {
		_ = agent.Close()
		return nil, nil, err
	}
	answer, err := (&blob{
		Version: blobVersion, InReplyTo: off.Nonce, ICEUfrag: ufrag, ICEPwd: pwd,
		Candidates: cands, DTLSRole: roleClient, Nonce: nonce,
	}).encode()
	if err != nil {
		_ = agent.Close()
		return nil, nil, err
	}

	sess := a.track(peer)
	// Согласование продолжается в фоне: контракт разрешает вернуть Session
	// до установления, и именно поэтому её методы блокирующие.
	go a.establish(ctx, sess, agent, off, roleClient, peer, off.Nonce, nonce)
	return answer, sess, nil
}

// CompleteSession — шаг 3 инициатора.
func (a *Adapter) CompleteSession(ctx context.Context, peer ports.PublicKey, answer []byte) (ports.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ans, err := decodeBlob(answer, roleClient)
	if err != nil {
		return nil, err
	}
	if len(ans.InReplyTo) == 0 {
		return nil, errors.New("pion: answer without in_reply_to")
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, ports.ErrSessionFailed
	}
	neg, ok := a.pending[string(ans.InReplyTo)]
	if ok {
		delete(a.pending, string(ans.InReplyTo))
	}
	a.mu.Unlock()
	if ok && neg.stop != nil {
		neg.stop() // согласование забрано — сторожу больше нечего освобождать
	}

	// Повторный CompleteSession — ошибка, не вторая сессия и не та же самая
	// (ADR-044 §6): идемпотентность скрыла бы ошибку вызывающего.
	if !ok {
		return nil, fmt.Errorf("pion: no pending negotiation for this answer: %w", ports.ErrSessionFailed)
	}
	if string(neg.peer) != string(peer) {
		return nil, fmt.Errorf("pion: answer addressed to a different peer: %w", ports.ErrSessionFailed)
	}

	sess := a.track(peer)
	go a.establish(ctx, sess, neg.agent, ans, roleServer, peer, neg.nonce, ans.Nonce)
	return sess, nil
}

func (a *Adapter) track(peer ports.PublicKey) *session {
	var s *session
	s = newSession(peer, func() {
		a.mu.Lock()
		delete(a.sessions, s)
		a.mu.Unlock()
	})
	a.mu.Lock()
	a.sessions[s] = struct{}{}
	a.mu.Unlock()
	return s
}

// establish поднимает ICE → DTLS → SCTP → handshake ADR-041.
//
// Собственный контекст с обязательным потолком: вызывающий уже отменил свой
// на предыдущем шаге, а согласование не должно висеть бесконечно при
// бездействии противоположной стороны (ADR-044 §3).
func (a *Adapter) establish(
	caller context.Context, sess *session, agent *ice.Agent, remote *blob, selfRole string,
	peer ports.PublicKey, initiatorNonce, responderNonce []byte,
) {
	// Контекст НАСЛЕДУЕТСЯ от вызывающего, а не заводится с нуля: ADR-044 §2
	// требует, чтобы отмена AcceptOffer/CompleteSession прерывала
	// согласование. Свой потолок добавляется сверху (§3) — он обязателен и
	// без отмены вызывающего.
	//
	// Первая редакция брала context.Background() и отмену игнорировала.
	// Loopback этого не показал: там согласование успевает завершиться
	// раньше, чем кто-либо отменит. Нашёл сетевой прогон.
	ctx, cancel := context.WithTimeout(caller, a.cfg.EstablishTimeout)
	defer cancel()

	fail := func(err error) {
		_ = agent.Close()
		sess.fail(err)
	}

	for _, c := range remote.Candidates {
		cand, err := ice.UnmarshalCandidate(c)
		if err != nil {
			fail(fmt.Errorf("pion: bad remote candidate: %w: %w", err, ports.ErrSessionFailed))
			return
		}
		if err := agent.AddRemoteCandidate(cand); err != nil {
			fail(fmt.Errorf("pion: add remote candidate: %w: %w", err, ports.ErrSessionFailed))
			return
		}
	}

	var iceConn *ice.Conn
	var err error
	if selfRole == roleServer {
		iceConn, err = agent.Dial(ctx, remote.ICEUfrag, remote.ICEPwd) // controlling
	} else {
		iceConn, err = agent.Accept(ctx, remote.ICEUfrag, remote.ICEPwd) // controlled
	}
	if err != nil {
		fail(fmt.Errorf("pion: ice: %w: %w", err, ports.ErrSessionFailed))
		return
	}

	dtlsConn, binding, err := a.secureChannel(ctx, iceConn, selfRole)
	if err != nil {
		fail(err)
		return
	}

	assoc, hsStream, err := association(dtlsConn, selfRole)
	if err != nil {
		_ = dtlsConn.Close()
		fail(err)
		return
	}

	initPub, respPub := a.cfg.Self.Public, ports.PublicKey(peer)
	if selfRole == roleClient {
		initPub, respPub = ports.PublicKey(peer), a.cfg.Self.Public
	}
	confirmed, err := runHandshake(ctx, hsStream, a.cfg.Identity, a.cfg.Self, peer,
		initPub, respPub, initiatorNonce, responderNonce, binding)
	if err != nil {
		_ = assoc.Close()
		_ = dtlsConn.Close()
		fail(fmt.Errorf("%w: %w", err, ports.ErrSessionFailed))
		return
	}
	_ = hsStream.Close()

	ep := ports.Endpoint("")
	if pair, perr := agent.GetSelectedCandidatePair(); perr == nil && pair != nil {
		ep = ports.Endpoint(fmt.Sprintf("%s->%s", pair.Local.String(), pair.Remote.String()))
	}
	// Идентификаторы потоков: инициатор чётные, отвечающий нечётные — иначе
	// обе стороны заняли бы один и тот же id.
	first := uint16(2)
	if selfRole == roleClient {
		first = 3
	}
	sess.establish(confirmed, assoc, ep, first)
}

// secureChannel поднимает DTLS и извлекает channel-binding material.
//
// Материал берётся ExportKeyingMaterial (RFC 5705) — буквально то, что требует
// ADR-041 §4. Сертификатные отпечатки как замена запрещены ADR-043 §2.
func (a *Adapter) secureChannel(ctx context.Context, raw net.Conn, selfRole string) (*dtls.Conn, []byte, error) {
	cfg := a.dtlsConfig()
	var conn *dtls.Conn
	var err error
	if selfRole == roleServer {
		conn, err = dtls.ServerWithContext(ctx, raw, cfg)
	} else {
		conn, err = dtls.ClientWithContext(ctx, raw, cfg)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("pion: dtls: %w: %w", err, ports.ErrSessionFailed)
	}
	st := conn.ConnectionState()
	binding, err := st.ExportKeyingMaterial(bindingLabel, nil, bindingLength)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("pion: channel binding: %w: %w", err, ports.ErrSessionFailed)
	}
	return conn, binding, nil
}

// dtlsConfig вынесен отдельно, чтобы конфигурация слоя проверялась тестом, а
// не только глазами: пропущенный LoggerFactory ничего не ломает в работе и
// виден лишь по неожиданному выводу в stderr у вызывающего.
func (a *Adapter) dtlsConfig() *dtls.Config {
	return &dtls.Config{
		Certificates: []tls.Certificate{a.dtlsCert},
		// Сертификат не проверяется намеренно: он эфемерный и ничего не
		// удостоверяет. Личность даёт handshake ADR-041, а channel binding
		// ниже привязывает эту личность именно к этому каналу — поэтому
		// доверять сертификату не нужно и нечему.
		InsecureSkipVerify:   true,
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
		LoggerFactory:        nopLoggerFactory{},
	}
}

func sctpConfig(conn net.Conn) sctp.Config {
	return sctp.Config{NetConn: conn, LoggerFactory: nopLoggerFactory{}}
}

// association поднимает SCTP и открывает выделенный поток под handshake.
func association(conn net.Conn, selfRole string) (*sctp.Association, *sctp.Stream, error) {
	cfg := sctpConfig(conn)
	var assoc *sctp.Association
	var err error
	if selfRole == roleServer {
		assoc, err = sctp.Server(cfg)
	} else {
		assoc, err = sctp.Client(cfg)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("pion: sctp: %w: %w", err, ports.ErrSessionFailed)
	}
	// Поток 0 выделен под handshake; прикладные потоки его не используют
	// (p2p-session.md §5).
	st, err := assoc.OpenStream(0, sctp.PayloadTypeWebRTCBinary)
	if err != nil {
		_ = assoc.Close()
		return nil, nil, fmt.Errorf("pion: handshake stream: %w: %w", err, ports.ErrSessionFailed)
	}
	return assoc, st, nil
}

// Close прекращает НОВЫЕ согласования и освобождает их ресурсы. Уже
// установленные Session не закрывает — прямое требование порта; общий ресурс
// живёт, пока жива последняя сессия (ADR-044 §8). Идемпотентен.
func (a *Adapter) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	pending := a.pending
	a.pending = make(map[string]*negotiation)
	a.mu.Unlock()

	var errs []error
	for _, n := range pending {
		if n.stop != nil {
			n.stop()
		}
		if err := n.agent.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
