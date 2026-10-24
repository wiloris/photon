package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	edadapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	pionadapter "github.com/wiloris/photon/photon-core/adapters/pion"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/dispatcher"
)

// Сквозной сценарий Phase 15.5: identity -> публикация ключей -> discovery ->
// offer -> push Bridge -> answer -> push Bridge -> CompleteSession -> Peer() ->
// двунаправленный payload -> Close.
//
// Всё, чего нет в Photon, живёт ЗДЕСЬ, в инструменте, а не в ядре: генерация
// ключа устройства, публикация его в профиле, распознавание служебных
// сообщений. Тот же приём, что TCP-рандеву внутри p2pcheck — иллюстрация
// ADR-042 («доставку блобов выполняет вызывающий»), а не часть платформы.
//
// Два процесса, две личности, два инстанса Photon. Signaling идёт настоящими
// действиями Bridge и приходит настоящим push через насос событий Phase 15.5.

const (
	actionPatchMe = 207 // ACTION_PATCH_ME
	actionGetMe   = 200 // ACTION_GET_ME
	actionGetUser = 201 // ACTION_GET_USER

	// sigMarker — способ отличить служебное сообщение от обычного.
	// CreatePostPayload не содержит ни type, ни props, поэтому пометить пост
	// на уровне протокола нечем. Маркер в теле — минимальный способ.
	sigMarker = "PHOTONSIG1"

	propPrefix = "photon_p2p_"
)

func publishKey(d *dispatcher.Dispatcher, self ports.PublicKey) error {
	key := propPrefix + hexShort(self)
	props := map[string]string{key: base64.StdEncoding.EncodeToString(self)}
	payload, _ := json.Marshal(map[string]any{"props": props})
	raw, err := d.SendAction(actionPatchMe, payload)
	if err != nil {
		return err
	}
	var env struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &env)
	if !env.OK {
		return fmt.Errorf("patchMe отверг: %s", env.Error)
	}
	return nil
}

// discoverPeer находит публичный ключ собеседника через Bridge.
//
// Два режима, и разница принципиальна:
//   - peerUser пуст — обе стороны под ОДНИМ аккаунтом Mattermost, ключи лежат
//     в props собственного профиля, и берётся тот, что не мой. Так выглядел
//     первый прогон: две личности Photon, один аккаунт;
//   - peerUser задан — РАЗНЫЕ аккаунты, и ключ читается из чужого профиля
//     действием getUser. Это настоящая форма discovery.
//
// Discovery вне P2P-порта по построению (ADR-017/018): порт начинает работать,
// когда peer уже известен.
func discoverPeer(d *dispatcher.Dispatcher, self ports.PublicKey) (ports.PublicKey, error) {
	action, payload := int32(actionGetMe), []byte(`{}`)
	if *peerUser != "" {
		action = actionGetUser
		payload, _ = json.Marshal(map[string]string{"user_id": *peerUser})
	}
	raw, err := d.SendAction(action, payload)
	if err != nil {
		return nil, err
	}
	var env struct {
		OK   bool `json:"ok"`
		Data struct {
			Props map[string]string `json:"props"`
		} `json:"data"`
	}
	if uerr := json.Unmarshal(raw, &env); uerr != nil || !env.OK {
		return nil, fmt.Errorf("getMe: %s", raw)
	}
	mine := hexShort(self)
	for k, v := range env.Data.Props {
		if !strings.HasPrefix(k, propPrefix) {
			continue
		}
		// Из ЧУЖОГО профиля годится любой ключ; из своего — любой, кроме
		// собственного.
		if *peerUser == "" && k == propPrefix+mine {
			continue
		}
		pk, derr := base64.StdEncoding.DecodeString(v)
		if derr != nil || len(pk) == 0 {
			continue
		}
		return ports.PublicKey(pk), nil
	}
	return nil, fmt.Errorf("ключ собеседника в профиле не найден (props: %d)", len(env.Data.Props))
}

func hexShort(k []byte) string {
	s := fmt.Sprintf("%x", k)
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func sendBlob(d *dispatcher.Dispatcher, to ports.PublicKey, kind string, blob []byte) error {
	body := sigMarker + hexShort(to) + "|" + kind + "|" + base64.StdEncoding.EncodeToString(blob)
	payload, _ := json.Marshal(map[string]string{"channel_id": *channelID, "message": body})
	raw, err := d.SendAction(actionCreatePost, payload)
	if err != nil {
		return err
	}
	var env struct {
		OK bool `json:"ok"`
	}
	_ = json.Unmarshal(raw, &env)
	if !env.OK {
		return fmt.Errorf("пост отвергнут: %s", raw)
	}
	return nil
}

func parseBlob(eventJSON string, me ports.PublicKey) (kind string, blob []byte, found bool) {
	if typeOf(eventJSON) != dispatcher.EventMessage {
		return "", nil, false
	}
	i := strings.Index(eventJSON, sigMarker)
	if i < 0 {
		return "", nil, false
	}
	rest := eventJSON[i+len(sigMarker):]
	if j := strings.IndexAny(rest, `"\`); j >= 0 {
		rest = rest[:j]
	}
	parts := strings.SplitN(rest, "|", 3)
	if len(parts) != 3 || parts[0] != hexShort(me) {
		return "", nil, false
	}
	b, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", nil, false
	}
	return parts[1], b, true
}

func waitBlob(inbox <-chan string, me ports.PublicKey, want string, d time.Duration) []byte {
	deadline := time.After(d)
	for {
		select {
		case ev := <-inbox:
			if kind, blob, found := parseBlob(ev, me); found && kind == want {
				return blob
			}
		case <-deadline:
			return nil
		}
	}
}

func runP2P(role string) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	id := edadapter.New()
	self, err := id.Generate(ctx)
	if err != nil {
		fail("Generate: %v", err)
		return
	}
	ok("[%s] identity создана: %s", role, hexShort(self.Public))

	d, err := newDisp(nil)
	if err != nil {
		fail("New: %v", err)
		return
	}
	defer d.Close()

	inbox := make(chan string, 64)
	r := &recorder{}
	sink := func(ev []byte) {
		r.sink(ev)
		select {
		case inbox <- string(ev):
		default:
		}
	}
	if err := d.StartEvents(sink, nil); err != nil {
		fail("StartEvents: %v", err)
		return
	}
	if !r.waitType(dispatcher.EventConnected, 1, 30*time.Second) {
		fail("[%s] подписка не поднялась", role)
		return
	}
	ok("[%s] подписка поднята (CONNECTED)", role)

	if err := publishKey(d, self.Public); err != nil {
		fail("[%s] публикация ключа: %v", role, err)
		return
	}
	ok("[%s] публичный ключ опубликован в профиле", role)

	var peer ports.PublicKey
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		peer, err = discoverPeer(d, self.Public)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if peer == nil {
		fail("[%s] discovery не нашёл собеседника: %v", role, err)
		return
	}
	ok("[%s] ключ собеседника получен через Bridge: %s", role, hexShort(peer))

	p2p, err := pionadapter.New(pionadapter.Config{
		Identity: id, Self: self, EstablishTimeout: 60 * time.Second,
	})
	if err != nil {
		fail("[%s] pion.New: %v", role, err)
		return
	}
	defer p2p.Close()

	var sess ports.Session
	if role == "a" {
		offer, cerr := p2p.CreateOffer(ctx, peer)
		if cerr != nil {
			fail("[a] CreateOffer: %v", cerr)
			return
		}
		ok("[a] offer создан, %d байт", len(offer))
		if serr := sendBlob(d, peer, "offer", offer); serr != nil {
			fail("[a] отправка offer: %v", serr)
			return
		}
		ok("[a] offer отправлен через Bridge")

		answer := waitBlob(inbox, self.Public, "answer", 120*time.Second)
		if answer == nil {
			fail("[a] answer не пришёл push за 120 с")
			return
		}
		ok("[a] answer получен ЧЕРЕЗ PUSH, %d байт", len(answer))

		sess, err = p2p.CompleteSession(ctx, peer, answer)
		if err != nil {
			fail("[a] CompleteSession: %v", err)
			return
		}
	} else {
		offer := waitBlob(inbox, self.Public, "offer", 120*time.Second)
		if offer == nil {
			fail("[b] offer не пришёл push за 120 с")
			return
		}
		ok("[b] offer получен ЧЕРЕЗ PUSH, %d байт", len(offer))

		answer, s, aerr := p2p.AcceptOffer(ctx, peer, offer)
		if aerr != nil {
			fail("[b] AcceptOffer: %v", aerr)
			return
		}
		sess = s
		if serr := sendBlob(d, peer, "answer", answer); serr != nil {
			fail("[b] отправка answer: %v", serr)
			return
		}
		ok("[b] answer отправлен через Bridge, %d байт", len(answer))
	}
	defer sess.Close()

	confirmed := sess.Peer()
	if confirmed == nil {
		fail("[%s] Peer() пуст — handshake ADR-041 не подтвердил ключ", role)
		return
	}
	if string(confirmed) != string(peer) {
		fail("[%s] Peer() вернул НЕ тот ключ", role)
		return
	}
	ok("[%s] Peer() подтверждён: %s", role, hexShort(confirmed))
	ok("[%s] выбранная пара: %s", role, sess.RemoteEndpoint())

	if role == "a" {
		st, oerr := sess.OpenStream(ctx)
		if oerr != nil {
			fail("[a] OpenStream: %v", oerr)
			return
		}
		if _, werr := st.Write([]byte("ping")); werr != nil {
			fail("[a] Write: %v", werr)
			return
		}
		buf := make([]byte, 4)
		if _, rerr := io.ReadFull(st, buf); rerr != nil {
			fail("[a] чтение эха: %v", rerr)
			return
		}
		if string(buf) != "pong" {
			fail("[a] эхо %q, ожидалось pong", buf)
			return
		}
		ok("[a] двунаправленный обмен: ping -> pong")
	} else {
		st, aerr := sess.AcceptStream(ctx)
		if aerr != nil {
			fail("[b] AcceptStream: %v", aerr)
			return
		}
		buf := make([]byte, 4)
		if _, rerr := io.ReadFull(st, buf); rerr != nil {
			fail("[b] чтение: %v", rerr)
			return
		}
		if _, werr := st.Write([]byte("pong")); werr != nil {
			fail("[b] запись эха: %v", werr)
			return
		}
		ok("[b] двунаправленный обмен: принято %q, отвечено pong", buf)
	}

	time.Sleep(3 * time.Second)
	ok("[%s] сценарий пройден целиком", role)
}
