package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/quic-go/quic-go"
)

const (
	quicBindAddr = ":4443"
	caddyCertDir = "/var/lib/caddy/.local/share/caddy/certificates/acme-v02.api.letsencrypt.org-directory"
)

// ── certCache — кэш сертификата с автообновлением ────────────────────────────
// Перечитывает файлы с диска раз в 12 часов.
// Caddy обновляет сертификат за 30 дней до истечения (каждые ~60 дней).
// Никаких ручных операций не требуется.

type certCache struct {
	mu       sync.RWMutex
	cert     *tls.Certificate
	certFile string
	keyFile  string
	loadedAt time.Time
	// plugin нужен только для логирования неудачных обновлений: молча
	// отдавать старый сертификат — способ не заметить отказ до рестарта.
	plugin *Plugin
}

func (c *certCache) get() (*tls.Certificate, error) {
	c.mu.RLock()
	if c.cert != nil && time.Since(c.loadedAt) < 12*time.Hour {
		cert := c.cert
		c.mu.RUnlock()
		return cert, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check после захвата write lock.
	if c.cert != nil && time.Since(c.loadedAt) < 12*time.Hour {
		return c.cert, nil
	}

	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		if c.cert != nil {
			// Не удалось перечитать — продолжаем на старом сертификате.
			// Это ОТЛОЖЕННЫЙ ОТКАЗ, а не восстановление: Caddy продлевает
			// сертификат каждые ~60 дней и перезаписывает файлы с правами
			// 0600, после чего процесс плагина их не прочитает. Снаружи
			// всё работает — до первого рестарта, когда старого
			// сертификата в памяти уже не будет, getTLSConfig вернёт
			// ошибку и QUIC не поднимется. Без этой записи в логе
			// промежуток между причиной и симптомом — недели.
			if c.plugin != nil {
				c.plugin.API.LogError(
					"[Photon] Cert refresh FAILED — still serving the cached certificate; "+
						"QUIC will not start after the next restart until this is fixed",
					"cert", c.certFile,
					"key", c.keyFile,
					"loaded_at", c.loadedAt.Format(time.RFC3339),
					"error", err.Error(),
				)
			}
			return c.cert, nil
		}
		return nil, fmt.Errorf("loading cert %s: %w", c.certFile, err)
	}
	c.cert = &cert
	c.loadedAt = time.Now()
	return c.cert, nil
}

// ── getTLSConfig ──────────────────────────────────────────────────────────────

func getTLSConfig(p *Plugin, cfg *model.Config) (*tls.Config, error) {
	certFile, keyFile, cert, err := findCert(p, cfg)
	if err != nil {
		return nil, err
	}

	// Сертификат уже загружен findCert — повторно с диска не читаем,
	// кэш стартует прогретым.
	cache := &certCache{
		certFile: certFile,
		keyFile:  keyFile,
		cert:     cert,
		loadedAt: time.Now(),
		plugin:   p,
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"photon-quic-v1"},
		// GetCertificate вызывается при каждом TLS handshake.
		// Кэш автоматически перечитывает файл раз в 12 часов.
		GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return cache.get()
		},
	}, nil
}

// certCandidate — один вариант расположения сертификата.
// source — человекочитаемая метка для лога: без неё «permission denied»
// не отличить от «домен из SiteURL не совпал с каталогом Caddy».
type certCandidate struct {
	source   string
	certFile string
	keyFile  string
}

// certCandidates возвращает кандидатов в порядке приоритета:
// Caddy по домену из SiteURL → любой домен в каталоге Caddy → Mattermost config.
func certCandidates(cfg *model.Config) []certCandidate {
	var out []certCandidate
	seen := map[string]bool{} // пара .crt+.key уже в списке

	add := func(c certCandidate) {
		key := c.certFile + "\x00" + c.keyFile
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, c)
	}

	// 1. Caddy: домен из SiteURL.
	if domain := extractDomain(cfg); domain != "" {
		add(certCandidate{
			source:   "caddy/SiteURL domain " + domain,
			certFile: fmt.Sprintf("%s/%s/%s.crt", caddyCertDir, domain, domain),
			keyFile:  fmt.Sprintf("%s/%s/%s.key", caddyCertDir, domain, domain),
		})
	}

	// 2. Caddy: все домены в каталоге. Перечисляются все, а не первый
	//    попавшийся: если у одного домена права не те, следующий должен
	//    получить свой шанс.
	entries, err := os.ReadDir(caddyCertDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			domain := e.Name()
			add(certCandidate{
				source:   "caddy/dir scan " + domain,
				certFile: fmt.Sprintf("%s/%s/%s.crt", caddyCertDir, domain, domain),
				keyFile:  fmt.Sprintf("%s/%s/%s.key", caddyCertDir, domain, domain),
			})
		}
	}

	// 3. Mattermost ServiceSettings.
	if cfg.ServiceSettings.TLSCertFile != nil && cfg.ServiceSettings.TLSKeyFile != nil {
		cf, kf := *cfg.ServiceSettings.TLSCertFile, *cfg.ServiceSettings.TLSKeyFile
		if cf != "" && kf != "" {
			add(certCandidate{
				source:   "mattermost ServiceSettings",
				certFile: cf,
				keyFile:  kf,
			})
		}
	}

	return out
}

// findCert перебирает кандидатов и возвращает первого, который РЕАЛЬНО
// загружается.
//
// Раньше здесь проверялось os.Stat (существование файла), и порядок из трёх
// вариантов был фикцией: после выдачи процессу Mattermost прав на обход
// каталогов Caddy stat проходит всегда, первый кандидат выигрывает всегда, а
// нечитаемый или битый файл давал отказ вместо отката на следующий вариант.
// tls.LoadX509KeyPair проверяет разом существование, права, разбор PEM и
// совпадение пары .crt/.key — последнее до этого не проверялось нигде вплоть
// до подъёма листенера.
func findCert(p *Plugin, cfg *model.Config) (certFile, keyFile string, cert *tls.Certificate, err error) {
	candidates := certCandidates(cfg)
	if len(candidates) == 0 {
		return "", "", nil, fmt.Errorf(
			"no TLS cert candidates: Caddy dir (%s) unreadable or empty, "+
				"SiteURL gives no domain, ServiceSettings.TLSCertFile empty", caddyCertDir,
		)
	}

	for _, c := range candidates {
		pair, loadErr := tls.LoadX509KeyPair(c.certFile, c.keyFile)
		if loadErr != nil {
			// Отбраковка обязана быть видимой. Молчаливое падение на
			// следующий вариант и было тем, что скрывало проблему прав:
			// оба пути через Caddy проваливались беззвучно, оставался
			// третий, а при пустом TLSCertFile — ни одного.
			p.API.LogWarn("[Photon] Cert candidate rejected",
				"source", c.source,
				"cert", c.certFile,
				"key", c.keyFile,
				"error", loadErr.Error())
			continue
		}
		p.API.LogInfo("[Photon] Cert selected",
			"source", c.source, "cert", c.certFile, "key", c.keyFile)
		return c.certFile, c.keyFile, &pair, nil
	}

	return "", "", nil, fmt.Errorf(
		"none of %d TLS cert candidates could be loaded (see [Photon] Cert candidate rejected "+
			"in the log for the reason on each); Caddy dir: %s", len(candidates), caddyCertDir,
	)
}

func extractDomain(cfg *model.Config) string {
	if cfg.ServiceSettings.SiteURL == nil {
		return ""
	}
	s := *cfg.ServiceSettings.SiteURL
	for _, pfx := range []string{"https://", "http://"} {
		if len(s) > len(pfx) && s[:len(pfx)] == pfx {
			s = s[len(pfx):]
			break
		}
	}
	for i, ch := range s {
		if ch == ':' || ch == '/' {
			return s[:i]
		}
	}
	return s
}

func scanCaddyDir() (certFile, keyFile string, found bool) {
	entries, err := os.ReadDir(caddyCertDir)
	if err != nil {
		return "", "", false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		domain := e.Name()
		cf := fmt.Sprintf("%s/%s/%s.crt", caddyCertDir, domain, domain)
		kf := fmt.Sprintf("%s/%s/%s.key", caddyCertDir, domain, domain)
		if fileExists(cf) && fileExists(kf) {
			return cf, kf, true
		}
	}
	return "", "", false
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ── QuicListener ──────────────────────────────────────────────────────────────

type QuicListener struct {
	plugin     *Plugin
	tlsCfg     *tls.Config
	dispatcher *Dispatcher
	registry   *PushRegistry
	listener   *quic.Listener
	cancel     context.CancelFunc
}

func NewQuicListener(p *Plugin, tlsCfg *tls.Config, d *Dispatcher) *QuicListener {
	return &QuicListener{
		plugin:     p,
		tlsCfg:     tlsCfg,
		dispatcher: d,
		registry:   NewPushRegistry(),
	}
}

func (ql *QuicListener) Push(event PushEvent) {
	ql.registry.Broadcast(event)
}

func (ql *QuicListener) Serve() {
	defer func() {
		if r := recover(); r != nil {
			ql.plugin.API.LogError("[Photon] PANIC in Serve", "recover", fmt.Sprintf("%v", r))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	ql.cancel = cancel

	ln, err := quic.ListenAddr(quicBindAddr, ql.tlsCfg, &quic.Config{
		EnableDatagrams: false,
	})
	if err != nil {
		ql.plugin.API.LogError("[Photon] Failed to bind QUIC",
			"addr", quicBindAddr, "error", err.Error())
		return
	}
	ql.listener = ln
	ql.plugin.API.LogInfo("[Photon] QUIC listener bound", "addr", quicBindAddr)

	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				ql.plugin.API.LogInfo("[Photon] QUIC listener stopped")
			default:
				ql.plugin.API.LogError("[Photon] Accept error", "error", err.Error())
			}
			return
		}
		go ql.handleConnection(conn)
	}
}

func (ql *QuicListener) Shutdown() {
	if ql.cancel != nil {
		ql.cancel()
	}
	if ql.listener != nil {
		_ = ql.listener.Close()
	}
}

func (ql *QuicListener) handleConnection(conn *quic.Conn) {
	defer func() {
		if r := recover(); r != nil {
			ql.plugin.API.LogError("[Photon] PANIC in handleConnection",
				"recover", fmt.Sprintf("%v", r))
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	ql.plugin.API.LogDebug("[Photon] New connection", "remote", remoteAddr)

	stream, err := conn.AcceptStream(context.Background())
	if err != nil {
		return
	}

	dec := json.NewDecoder(stream)

	var auth AuthFrame
	if err := dec.Decode(&auth); err != nil {
		writeError(stream, "invalid auth frame")
		stream.Close()
		conn.CloseWithError(0, "bad auth")
		return
	}

	session, appErr := ql.plugin.API.GetSession(auth.SessionToken)
	if appErr != nil {
		ql.plugin.API.LogWarn("[Photon] Auth failed", "remote", remoteAddr)
		writeError(stream, "unauthorized")
		stream.Close()
		conn.CloseWithError(0, "unauthorized")
		return
	}
	userID := session.UserId
	ql.plugin.API.LogInfo("[Photon] Authenticated", "remote", remoteAddr, "user_id", userID)

	var modeFrame struct {
		Mode string `json:"mode"`
	}
	if err := dec.Decode(&modeFrame); err != nil {
		modeFrame.Mode = "action"
	}

	switch modeFrame.Mode {
	case "subscribe":
		writeResponse(stream, Response{OK: true, Data: map[string]string{"status": "subscribed"}})
		stream.Close()
		client := ql.registry.Register(userID, conn)
		ql.plugin.API.LogInfo("[Photon] Client subscribed", "user_id", userID, "remote", remoteAddr)
		<-client.done
		ql.plugin.API.LogInfo("[Photon] Client unsubscribed", "user_id", userID)

	default:
		writeResponse(stream, Response{OK: true, Data: map[string]string{"status": "ready"}})
		stream.Close()
		for {
			s, err := conn.AcceptStream(context.Background())
			if err != nil {
				ql.plugin.API.LogDebug("[Photon] Connection closed", "remote", remoteAddr)
				return
			}
			go ql.handleStream(s, userID, remoteAddr)
		}
	}
}

func (ql *QuicListener) handleStream(stream *quic.Stream, userID string, remoteAddr string) {
	defer func() {
		if r := recover(); r != nil {
			ql.plugin.API.LogError("[Photon] PANIC in handleStream",
				"recover", fmt.Sprintf("%v", r))
		}
		stream.Close()
	}()

	dec := json.NewDecoder(stream)

	var header HeaderFrame
	if err := dec.Decode(&header); err != nil {
		writeError(stream, "invalid header")
		return
	}

	var payload json.RawMessage
	if err := dec.Decode(&payload); err != nil && err != io.EOF {
		writeError(stream, "invalid payload")
		return
	}

	ql.plugin.API.LogInfo("[Photon] Action",
		"user_id", userID, "resource", header.Resource, "action", header.Action)

	resp := ql.dispatcher.Dispatch(userID, header, payload)
	writeResponse(stream, resp)
}

func writeResponse(w io.Writer, resp Response) { _ = json.NewEncoder(w).Encode(resp) }
func writeError(w io.Writer, msg string)       { writeResponse(w, Response{OK: false, Error: msg}) }
