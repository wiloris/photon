// Package pion реализует порт ports.P2P поверх ICE + DTLS + SCTP (ADR-043,
// путь B+: без pion/webrtc и без медиастека).
//
// Спецификация — photon-core/docs/p2p-session.md, написана до этого кода.
// Аутентификация peer'а — ADR-041, signaling — ADR-042, жизненный цикл —
// ADR-044, однонаправленные потоки — ADR-016.
//
// Ни один тип Pion не пересекает границу порта: наружу идут []byte,
// ports.PublicKey, ports.Session/Connection/Stream и ports.Endpoint
// (ADR-043 §0).
package pion

import (
	"encoding/json"
	"fmt"
)

// blobVersion — граница версии формата signaling-блоба. Несовпадение —
// терминальный отказ, а не попытка разобрать «как получится»
// (p2p-session.md §4.3).
const blobVersion = 1

// Роли нижних слоёв назначаются шагом согласования, а не выводятся из сети:
// инициатор — ICE controlling / DTLS server / SCTP server, отвечающий —
// наоборот. Поле в блобе дублирует правило явно, чтобы расхождение
// обнаруживалось при разборе, а не таймаутом (p2p-session.md §4.3).
const (
	roleServer = "server"
	roleClient = "client"
)

// blob — offer и answer имеют одну форму; answer дополнительно несёт
// in_reply_to. SDP не используется: он не нужен при выбранной архитектуре, и
// pion/sdp в графе зависимостей отсутствует (ADR-043).
type blob struct {
	Version    int      `json:"v"`
	InReplyTo  []byte   `json:"in_reply_to,omitempty"`
	ICEUfrag   string   `json:"ice_ufrag"`
	ICEPwd     string   `json:"ice_pwd"`
	Candidates []string `json:"candidates"`
	DTLSRole   string   `json:"dtls_role"`
	Nonce      []byte   `json:"nonce"`
}

func (b *blob) encode() ([]byte, error) { return json.Marshal(b) }

func decodeBlob(raw []byte, wantRole string) (*blob, error) {
	var b blob
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("pion: malformed signaling blob: %w", err)
	}
	if b.Version != blobVersion {
		return nil, fmt.Errorf("pion: unsupported blob version %d (want %d)", b.Version, blobVersion)
	}
	if b.DTLSRole != wantRole {
		// Роль назначается шагом согласования. Несовпадение означает, что обе
		// стороны считают себя одной и той же стороной, — ловим при разборе.
		return nil, fmt.Errorf("pion: unexpected dtls_role %q (want %q)", b.DTLSRole, wantRole)
	}
	if b.ICEUfrag == "" || b.ICEPwd == "" {
		return nil, fmt.Errorf("pion: blob without ICE credentials")
	}
	if len(b.Nonce) != nonceLen {
		return nil, fmt.Errorf("pion: blob nonce is %d bytes (want %d)", len(b.Nonce), nonceLen)
	}
	return &b, nil
}
