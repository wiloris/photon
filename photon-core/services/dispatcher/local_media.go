package dispatcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	photonpb "github.com/wiloris/photon/photon-core/protobuf"
	"github.com/wiloris/photon/photon-core/services/diag"
	"github.com/wiloris/photon/photon-core/services/media"
	"github.com/wiloris/photon/photon-core/services/session"
)

// Передача файла вложения разговора ENGRAM — локальные 815/816 (ADR-124 M1,
// ПРЕДЛОЖЕНО; контракт — docs/local-actions.md). Core читает файл по пути и
// отдаёт его действием 703, либо пишет тело 704 в файл по пути. Байты идут по
// МЕДИАСОЕДИНЕНИЮ — отдельному action-соединению к цели 814: подписка событий
// (насос) им не затрагивается, HTTP-запросы хоста остаются у хоста
// (ADR-094 §6.1). Использование цели 814 для медиа — поправка ADR-123 §2.3.1,
// предложенная ADR-124 (профиль M1, M1-12).

const (
	actionLocalMediaUploadFile   = int32(photonpb.Action_ACTION_LOCAL_MEDIA_UPLOAD_FILE)
	actionLocalMediaDownloadFile = int32(photonpb.Action_ACTION_LOCAL_MEDIA_DOWNLOAD_FILE)
	// 817/818 (ADR-127): объекты галереи профиля — 706/707 тем же путём.
	actionLocalProfileMediaUploadFile   = int32(photonpb.Action_ACTION_LOCAL_PROFILE_MEDIA_UPLOAD_FILE)
	actionLocalProfileMediaDownloadFile = int32(photonpb.Action_ACTION_LOCAL_PROFILE_MEDIA_DOWNLOAD_FILE)
)

// mediaTransferTimeout — верхний предел одной передачи. Обычный бюджет вызова
// (CallTimeout) для 100 МБ по медленной сети мал; зависшую передачу раньше
// этого предела обрывает дедлайн Bridge или keep-alive QUIC.
const mediaTransferTimeout = 60 * time.Minute

func isMediaFileAction(action int32) bool {
	switch action {
	case actionLocalMediaUploadFile, actionLocalMediaDownloadFile,
		actionLocalProfileMediaUploadFile, actionLocalProfileMediaDownloadFile:
		return true
	}
	return false
}

type mediaUploadFileRequest struct {
	ConversationID string `json:"conversation_id"`
	UploadKey      string `json:"upload_key"`
	Path           string `json:"path"`
	Name           string `json:"name"`
	Kind           string `json:"kind"`
}

type mediaDownloadFileRequest struct {
	MediaID string `json:"media_id"`
	Path    string `json:"path"`
}

type profileMediaUploadFileRequest struct {
	UploadKey string `json:"upload_key"`
	Path      string `json:"path"`
	Role      string `json:"role"`
	Mime      string `json:"mime"`
}

func invalidArg(format string, a ...any) error {
	return fmt.Errorf("%w: %s", session.ErrInvalidPayload, fmt.Sprintf(format, a...))
}

func (d *Dispatcher) mediaFileAction(ctx context.Context, action int32, payload []byte) ([]byte, error) {
	switch action {
	case actionLocalMediaUploadFile:
		return d.mediaUploadFile(ctx, payload)
	case actionLocalProfileMediaUploadFile:
		return d.profileMediaUploadFile(ctx, payload)
	case actionLocalProfileMediaDownloadFile:
		return d.profileMediaDownloadFile(ctx, payload)
	}
	return d.mediaDownloadFile(ctx, payload)
}

// mediaUploadFile — 815: первый проход по файлу считает размер и SHA-256,
// второй отдаёт ровно столько байт действием 703. Файл в память целиком не
// читается.
func (d *Dispatcher) mediaUploadFile(ctx context.Context, payload []byte) ([]byte, error) {
	var req mediaUploadFileRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.ConversationID == "" || req.UploadKey == "" || req.Path == "" || req.Name == "" {
		return nil, invalidArg("conversation_id, upload_key, path and name are required")
	}
	if req.Kind != "file" {
		return nil, invalidArg("kind must be \"file\"")
	}
	return d.uploadFile(ctx, req.Path, func(sess *session.Session, sum string, size int64, f io.Reader) (*media.Ref, error) {
		return media.New(sess).ConvUpload(ctx, media.ConvUploadRequest{
			ConversationID: req.ConversationID, UploadKey: req.UploadKey, Kind: req.Kind,
			Name: req.Name, SHA256: sum, Size: size,
		}, f)
	})
}

// profileMediaUploadFile — 817: то же, что 815, действием 706 (ADR-127).
func (d *Dispatcher) profileMediaUploadFile(ctx context.Context, payload []byte) ([]byte, error) {
	var req profileMediaUploadFileRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.UploadKey == "" || req.Path == "" || req.Mime == "" {
		return nil, invalidArg("upload_key, path and mime are required")
	}
	if req.Role != "original" && req.Role != "thumb" {
		return nil, invalidArg("role must be \"original\" or \"thumb\"")
	}
	return d.uploadFile(ctx, req.Path, func(sess *session.Session, sum string, size int64, f io.Reader) (*media.Ref, error) {
		return media.New(sess).ProfileUpload(ctx, media.ProfileUploadRequest{
			UploadKey: req.UploadKey, Role: req.Role, Mime: req.Mime, SHA256: sum, Size: size,
		}, f)
	})
}

// uploadFile — общий путь 815/817: первый проход по файлу считает размер и
// SHA-256, второй отдаёт ровно столько байт действием send. Файл в память
// целиком не читается.
func (d *Dispatcher) uploadFile(ctx context.Context, path string,
	send func(sess *session.Session, sum string, size int64, f io.Reader) (*media.Ref, error)) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, invalidArg("open file: %v", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return nil, invalidArg("path is not a regular file")
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: read file: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("dispatcher: rewind file: %w", err)
	}
	sum := hex.EncodeToString(h.Sum(nil))

	sess, err := d.mediaSession(ctx)
	if err != nil {
		return nil, err
	}
	ref, err := send(sess, sum, size, f)
	if rej := (*media.Rejection)(nil); errors.As(err, &rej) {
		return envelope(session.Response{OK: false, Error: rej.Code})
	}
	if err != nil {
		d.dropMediaSessionOn(sess, err)
		return nil, err
	}
	data, err := json.Marshal(ref)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode upload result: %w", err)
	}
	return envelope(session.Response{OK: true, Data: data})
}

// mediaDownloadFile — 816: тело во временный файл рядом с path; после полного
// тела и совпавшего SHA-256 — fsync и атомарное переименование в path. При
// любом отказе временный файл удаляется, path не создаётся и не меняется.
func (d *Dispatcher) mediaDownloadFile(ctx context.Context, payload []byte) ([]byte, error) {
	var req mediaDownloadFileRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.MediaID == "" || req.Path == "" {
		return nil, invalidArg("media_id and path are required")
	}
	return d.downloadFile(ctx, req.Path, func(sess *session.Session, f io.Writer) (any, error) {
		return media.New(sess).ConvDownload(ctx, req.MediaID, f)
	})
}

// profileMediaDownloadFile — 818: то же, что 816, действием 707 (ADR-127).
func (d *Dispatcher) profileMediaDownloadFile(ctx context.Context, payload []byte) ([]byte, error) {
	var req mediaDownloadFileRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.MediaID == "" || req.Path == "" {
		return nil, invalidArg("media_id and path are required")
	}
	return d.downloadFile(ctx, req.Path, func(sess *session.Session, f io.Writer) (any, error) {
		return media.New(sess).ProfileDownload(ctx, req.MediaID, f)
	})
}

// downloadFile — общий путь 816/818: тело во временный файл рядом с path;
// после полного тела и совпавшего SHA-256 — fsync и атомарное переименование
// в path. При любом отказе временный файл удаляется, path не создаётся и не
// меняется.
func (d *Dispatcher) downloadFile(ctx context.Context, path string,
	fetch func(sess *session.Session, f io.Writer) (any, error)) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, invalidArg("path must be absolute")
	}
	if fi, err := os.Stat(filepath.Dir(path)); err != nil || !fi.IsDir() {
		return nil, invalidArg("parent directory of path does not exist")
	}
	tmp := path + ".part"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, invalidArg("create temporary file: %v", err)
	}
	discard := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}

	sess, err := d.mediaSession(ctx)
	if err != nil {
		discard()
		return nil, err
	}
	info, err := fetch(sess, f)
	if rej := (*media.Rejection)(nil); errors.As(err, &rej) {
		discard()
		return envelope(session.Response{OK: false, Error: rej.Code})
	}
	if err != nil {
		discard()
		d.dropMediaSessionOn(sess, err)
		return nil, err
	}
	if err := f.Sync(); err != nil {
		discard()
		return nil, fmt.Errorf("dispatcher: sync downloaded file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("dispatcher: close downloaded file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("dispatcher: rename downloaded file: %w", err)
	}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode download result: %w", err)
	}
	return envelope(session.Response{OK: true, Data: data})
}

// mediaSession — медиасоединение текущей цели: переиспользуется, пока цель
// не сменилась и соединение живо; создаётся и подключается лениво.
func (d *Dispatcher) mediaSession(ctx context.Context) (*session.Session, error) {
	d.mediaMu.Lock()
	defer d.mediaMu.Unlock()

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, ErrClosed
	}
	t, gen := d.target, d.targetGen
	if !t.set() {
		d.mu.Unlock()
		return nil, ErrNoEndpoint
	}
	if d.mediaSess != nil && d.mediaGen == gen {
		s := d.mediaSess
		d.mu.Unlock()
		return s, nil
	}
	stale := d.mediaSess
	d.mediaSess = nil
	tr := d.tr
	if tr == nil {
		tr = d.subTr
	}
	d.mu.Unlock()
	if stale != nil {
		_ = stale.Close()
	}
	if tr == nil {
		return nil, ErrNoEndpoint
	}

	s, err := session.New(session.Config{
		Transport: diag.WrapTransport(tr, d.diag, diag.ConnMedia),
		Endpoint:  t.endpoint, SessionToken: t.token,
		Mode: session.ModeAction,
		Diag: d.diag, DiagConn: diag.ConnMedia,
	})
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, d.cfg.ConnectTimeout)
	t0 := d.diag.ConnStart(diag.ConnMedia)
	err = s.Connect(cctx)
	d.diag.ConnResult(diag.ConnMedia, t0, Classify(err).String(), err == nil)
	cancel()
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	d.mu.Lock()
	if d.closed || d.targetGen != gen {
		// Цель сменилась или инстанс закрыт, пока шло подключение: это
		// соединение уже не принадлежит текущей цели.
		closed := d.closed
		d.mu.Unlock()
		_ = s.Close()
		if closed {
			return nil, ErrClosed
		}
		return nil, fmt.Errorf("dispatcher: %w: цель 814 сменилась во время подключения", session.ErrConnectionLost)
	}
	d.mediaSess, d.mediaGen = s, gen
	d.mu.Unlock()
	return s, nil
}

// dropMediaSessionOn — после обрыва медиасоединение не переиспользуется:
// следующий вызов подключится заново (ADR-124 §7 M1-9).
func (d *Dispatcher) dropMediaSessionOn(s *session.Session, err error) {
	if !errors.Is(err, session.ErrConnectionLost) && !errors.Is(err, session.ErrSessionClosed) {
		return
	}
	d.mu.Lock()
	if d.mediaSess != s {
		d.mu.Unlock()
		return
	}
	d.mediaSess = nil
	d.mu.Unlock()
	_ = s.Close()
}
