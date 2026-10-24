package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	photonpb "github.com/wiloris/photon/photon-core/protobuf"
	"github.com/wiloris/photon/photon-core/services/session"
)

// Внутрипроцессный поддельный Bridge.
//
// Зачем он нужен и чем НЕ является. Он не проверяет провод: кадрирование,
// граница тела и конверты закрыты тестами services/session и тестами Bridge.
// Он воспроизводит СЕМАНТИКУ медийного среза — и служит ровно одному: доказать,
// что проверки прогона ловят нарушение. Каждый переключатель ниже — мутация
// сервера, а не украшение: прогон, который зеленеет и на исправном сервере, и
// на сломанном, ничего не измеряет.
type fakeBridge struct {
	mu      sync.Mutex
	channel string
	objects map[string]*fakeObject
	posts   map[string]*fakePost
	order   []string
	seq     int
	streams int // всего открыто стримов за прогон

	// ── мутации сервера ──────────────────────────────────────────
	corruptDownload bool // отдаёт искажённые байты
	relabelDownload bool // и объявляет хеш ИСКАЖЁННЫХ байт — клиент расхождения не увидит
	echoClientProps bool // строит props из присланного клиентом
	acceptTruncated bool // принимает недосланное тело
	neverRelease    bool // зависший стрим не освобождает никогда
	creditLimit     int  // после стольких стримов кредит кончается навсегда
	foreignAllowed  bool // чужой объект доступен постороннему
	ghostAllowed    bool // несуществующий объект «находится»
	allowReattach   bool // уже прикреплённый объект принимается повторно
	wrongInfo       bool // 702 сообщает не те размер и хеш
	attachedOnly    bool // неприкреплённый объект (STORED) не отдаётся

	// keepMediaAfterDelete — сервер БЕЗ уборки C: удаление поста не трогает
	// media. Ровно то поведение, что было до ADR-055, и ровно то, ради чего
	// заведены N6 и N7.
	keepMediaAfterDelete bool
	// deletedLooksForbidden — сервер, удаливший метаданные вместе с байтами:
	// удалённый объект становится неотличим от никогда не существовавшего.
	// Мутация против ослабленного oracle — проверка «пришла какая-то ошибка»
	// зеленела бы на ней.
	deletedLooksForbidden bool
	// cascadeRepliesReadable — сервер С уборкой C, но БЕЗ проверки живости
	// (ADR-056): корень убирается по событию, а media каскадно удалённых
	// ответов остаётся читаемой. Ровно то поведение, что измерено живым
	// прогоном уборки C, и ровно то, ради чего заведены N8 и N9.
	cascadeRepliesReadable bool

	// mediaDenyReason подменяет ПРИЧИНУ отказа в медийных путях. Мутация
	// против ослабленного oracle: сервер отвергает, но не по существу
	// запроса, а как непосвящённого — ровно то, что делал Bridge с умершим
	// RPC-соединением, отвечая unauthorized на всё подряд.
	mediaDenyReason string

	// denyGetMeAfter — после стольких успешных getMe субъект перестаёт
	// подтверждаться. Моделирует смерть RPC посреди прогона: положительный
	// вызов больше не проходит, и любые отказы рядом ничего не значат.
	denyGetMeAfter int
	getMeOK        int

	// stallHold — сколько сервер ДЕРЖИТ брошенный стрим, прежде чем
	// отвергнуть его. Это и есть дедлайн сервера в миниатюре: мгновенный
	// отказ (stallHold == 0) означает, что передача не принималась.
	stallHold time.Duration

	// stallReason подменяет причину отказа по брошенному стриму.
	stallReason string
}

type fakeObject struct {
	id, channel, uploader string
	name, mime, sum       string
	size                  int64
	postID                string
	body                  []byte
	// deleted — состояние DELETED: метаданные есть, байтов нет (ADR-055).
	// Именно надгробие, а не исчезновение объекта: иначе ответ был бы
	// forbidden, и N6 измеряла бы не то.
	deleted bool
	// inReply — объект прикреплён к ОТВЕТУ треда. Нужен ровно одной мутации:
	// сервер, у которого работает уборка C и не работает проверка живости,
	// теряет именно ответы.
	inReply bool
}

type fakePost struct {
	id, channel, message string
	rootID               string
	media                []feedMedia
}

func newFakeBridge() *fakeBridge {
	return &fakeBridge{
		channel: "chan-self",
		objects: map[string]*fakeObject{},
		posts:   map[string]*fakePost{},
		// Исправный сервер ждёт брошенный стрим дольше половины дедлайна,
		// заданного прогону (см. newRunner): иначе «освобождён» было бы
		// неотличимо от «не принят».
		stallHold: 150 * time.Millisecond,
	}
}

func (f *fakeBridge) actor(user string) *fakeActor { return &fakeActor{f: f, user: user} }

// seedOrphan — объект, чей пост УЖЕ мёртв: тот самый случай, который на стенде
// возник до появления проверки живости. Готовится напрямую, а не прогоном,
// потому что прогон такого объекта создать не может по определению — он
// создаёт свои, новые.
func (f *fakeBridge) seedOrphan(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	body := []byte(name)
	o := &fakeObject{
		id: f.nextID("media"), channel: f.channel, uploader: "user-A",
		name: name + ".bin", mime: "application/octet-stream",
		sum:  hex.EncodeToString(func() []byte { h := sha256.Sum256(body); return h[:] }()),
		size: int64(len(body)), body: body,
		// Пост, которого в f.posts нет: он «умер» до начала прогона.
		postID: "post-long-gone",
	}
	f.objects[o.id] = o
	return o.id
}

func (f *fakeBridge) nextID(prefix string) string {
	f.seq++
	return fmt.Sprintf("%s-%03d", prefix, f.seq)
}

// takeStream моделирует кредит MAX_STREAMS: каждый вызов открывает стрим, а
// утёкший кредит проявляется не отказом, а ожиданием.
func (f *fakeBridge) takeStream(ctx context.Context) error {
	f.mu.Lock()
	f.streams++
	over := f.creditLimit > 0 && f.streams > f.creditLimit
	f.mu.Unlock()
	if over {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

// mediaDeny — причина отказа в медийных путях. По умолчанию контрактная.
func (f *fakeBridge) mediaDeny() string {
	if f.mediaDenyReason != "" {
		return f.mediaDenyReason
	}
	return "forbidden"
}

// mediaGone — отдаётся ли объект. Два основания, и они РАЗНЫЕ:
//
//   - deleted: уборка C убрала байты по событию удаления (ADR-055);
//   - пост объекта мёртв: проверка живости на чтении (ADR-056) — она и
//     закрывает каскадно удалённые ответы, до которых событие не дошло.
//
// Неприкреплённый объект (пустой postID) не гасится никогда: это брошенная
// загрузка, уборка A, и N5 требует, чтобы он читался.
func (f *fakeBridge) mediaGone(o *fakeObject) bool {
	if o.deleted {
		return true
	}
	if o.postID == "" || f.keepMediaAfterDelete {
		return false
	}
	if _, alive := f.posts[o.postID]; alive {
		return false
	}
	// Мутация: сервер с уборкой C, но без проверки живости.
	return !(f.cascadeRepliesReadable && o.inReply)
}

// goneReason — причина отказа по объекту, чьи байты убраны. Контрактная —
// unavailable; deletedLooksForbidden подменяет её на forbidden, то есть
// моделирует сервер, снёсший метаданные вместе с байтами.
func (f *fakeBridge) goneReason() string {
	if f.deletedLooksForbidden {
		return "forbidden"
	}
	return "unavailable"
}

// visible — право субъекта на объект. В самопереписке владельца объект
// доступен только ему; foreignAllowed ломает ровно это.
func (f *fakeBridge) visible(o *fakeObject, user string) bool {
	return f.foreignAllowed || o.uploader == user
}

type fakeActor struct {
	f    *fakeBridge
	user string
}

func okResp(v any) session.Response {
	b, _ := json.Marshal(v)
	return session.Response{OK: true, Data: b}
}

func denyResp(text string) session.Response { return session.Response{OK: false, Error: text} }

func (a *fakeActor) Do(ctx context.Context, action int32, payload []byte) (session.Response, error) {
	if err := a.f.takeStream(ctx); err != nil {
		return session.Response{}, err
	}
	f := a.f
	f.mu.Lock()
	defer f.mu.Unlock()

	switch photonpb.Action(action) {
	case photonpb.Action_ACTION_GET_ME:
		if f.denyGetMeAfter > 0 && f.getMeOK >= f.denyGetMeAfter {
			return denyResp("unauthorized"), nil
		}
		f.getMeOK++
		return okResp(map[string]string{"id": a.user}), nil

	case photonpb.Action_ACTION_GET_OR_CREATE_DIRECT_CHANNEL:
		return okResp(map[string]string{"id": f.channel}), nil

	case photonpb.Action_ACTION_CREATE_POST:
		return f.createPost(a.user, payload), nil

	case photonpb.Action_ACTION_GET_FEED:
		posts := map[string]any{}
		for _, id := range f.order {
			p := f.posts[id]
			entry := map[string]any{"id": p.id, "message": p.message}
			if len(p.media) > 0 {
				entry["props"] = map[string]any{"photon_media": p.media}
			} else {
				entry["props"] = map[string]any{}
			}
			posts[id] = entry
		}
		return okResp(map[string]any{"order": f.order, "posts": posts}), nil

	case photonpb.Action_ACTION_DELETE_POST:
		var p struct {
			PostID string `json:"post_id"`
		}
		_ = json.Unmarshal(payload, &p)
		post, existed := f.posts[p.PostID]
		if !existed {
			// Пост уже мёртв: сервер отвергает — authz не резолвит удалённый
			// пост. Измерено на живом Bridge.
			return denyResp("forbidden"), nil
		}
		// КАСКАД: удаление корня уносит ответы. Это поведение сервера
		// Mattermost, а не наше: измерено — у всех постов треда один
		// delete_at, и хук приходит ТОЛЬКО по корню.
		dead := []*fakePost{post}
		if post.rootID == "" {
			for _, other := range f.posts {
				if other.rootID == p.PostID {
					dead = append(dead, other)
				}
			}
		}
		for _, d := range dead {
			delete(f.posts, d.id)
			for i, id := range f.order {
				if id == d.id {
					f.order = append(f.order[:i], f.order[i+1:]...)
					break
				}
			}
		}
		// Уборка C (ADR-055) — ПО СОБЫТИЮ, то есть только по названному посту.
		// Ответы события не получают, и их объекты остаются нетронутыми: их
		// гасит не уборка, а проверка живости на чтении (ADR-056, mediaGone).
		if !f.keepMediaAfterDelete {
			for _, ref := range post.media {
				if o, ok := f.objects[ref.MediaID]; ok && o.postID == p.PostID {
					o.deleted = true
					o.body = nil
				}
			}
		}
		return okResp(map[string]bool{"ok": true}), nil

	case photonpb.Action_ACTION_MEDIA_GET_INFO:
		var p struct {
			MediaID string `json:"media_id"`
		}
		_ = json.Unmarshal(payload, &p)
		o, ok := f.objects[p.MediaID]
		if !ok {
			if f.ghostAllowed {
				return okResp(map[string]any{"media": feedMedia{MediaID: p.MediaID, SHA256: "", Size: 0}}), nil
			}
			return denyResp(f.mediaDeny()), nil
		}
		if !f.visible(o, a.user) {
			return denyResp(f.mediaDeny()), nil
		}
		if f.mediaGone(o) {
			return denyResp(f.goneReason()), nil
		}
		info := feedMedia{MediaID: o.id, SHA256: o.sum, Size: o.size, Mime: o.mime, Name: o.name}
		if f.wrongInfo {
			info.SHA256 = "deadbeef" + info.SHA256[8:]
			info.Size++
		}
		return okResp(map[string]any{"media": info}), nil
	}
	return denyResp(fmt.Sprintf("unknown action %d", action)), nil
}

// createPost повторяет правило ADR-048 §3: из запроса берётся только
// media_id, всё остальное — из хранилища. echoClientProps отменяет это.
func (f *fakeBridge) createPost(user string, payload []byte) session.Response {
	var p struct {
		ChannelID string      `json:"channel_id"`
		Message   string      `json:"message"`
		RootID    string      `json:"root_id"`
		Media     []feedMedia `json:"photon_media"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return denyResp("invalid: " + err.Error())
	}
	if p.ChannelID == "" || p.Message == "" {
		return denyResp("invalid: channel_id and message are required")
	}

	refs := make([]feedMedia, 0, len(p.Media))
	seen := map[string]bool{}
	for _, w := range p.Media {
		if w.MediaID == "" || seen[w.MediaID] {
			return denyResp("invalid: media_id is empty or duplicated")
		}
		seen[w.MediaID] = true
		o, ok := f.objects[w.MediaID]
		if !ok || !f.visible(o, user) || o.channel != p.ChannelID {
			return denyResp("forbidden")
		}
		if o.postID != "" && !f.allowReattach {
			return denyResp("invalid: media already attached")
		}
		if f.echoClientProps {
			refs = append(refs, w)
			continue
		}
		refs = append(refs, feedMedia{
			MediaID: o.id, SHA256: o.sum, Size: o.size, Mime: o.mime, Name: o.name,
		})
	}

	post := &fakePost{id: f.nextID("post"), channel: p.ChannelID, message: p.Message,
		rootID: p.RootID, media: refs}
	f.posts[post.id] = post
	f.order = append([]string{post.id}, f.order...)
	for _, ref := range refs {
		if o, ok := f.objects[ref.MediaID]; ok {
			o.postID = post.id
			o.inReply = p.RootID != ""
		}
	}

	entry := map[string]any{"id": post.id, "message": post.message}
	if len(refs) > 0 {
		entry["props"] = map[string]any{"photon_media": refs}
	}
	return okResp(entry)
}

func (a *fakeActor) DoStream(ctx context.Context, action int32, payload []byte,
	body io.Reader, sink io.Writer) (session.Response, error) {

	if err := a.f.takeStream(ctx); err != nil {
		return session.Response{}, err
	}
	switch photonpb.Action(action) {
	case photonpb.Action_ACTION_MEDIA_UPLOAD:
		return a.upload(ctx, payload, body)
	case photonpb.Action_ACTION_MEDIA_DOWNLOAD:
		return a.download(payload, sink)
	}
	return denyResp(fmt.Sprintf("unknown action %d", action)), nil
}

func (a *fakeActor) upload(ctx context.Context, payload []byte, body io.Reader) (session.Response, error) {
	var p struct {
		ChannelID  string `json:"channel_id"`
		Name       string `json:"name"`
		Mime       string `json:"mime"`
		SHA256     string `json:"sha256"`
		BodyLength int64  `json:"body_length"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return denyResp("invalid: " + err.Error()), nil
	}

	// Тело собирается в память НАМЕРЕННО: это тестовый двойник, а не
	// продакшн-путь. Запрет на буферизацию относится к серверу, и он
	// проверяется структурным тестом Bridge, а не здесь.
	var buf bytes.Buffer
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(&buf, h), body)
	if err != nil {
		return denyResp("invalid: " + err.Error()), nil
	}
	f := a.f
	if n != p.BodyLength && !f.acceptTruncated {
		if f.neverRelease {
			// Сервер, который не освобождает брошенный стрим: ждёт вечно.
			<-ctx.Done()
			return session.Response{}, ctx.Err()
		}
		// Дедлайн: сервер ДЕРЖИТ брошенную передачу, а потом отвергает.
		// Держать обязательно — мгновенный отказ означал бы, что стрим не
		// был принят, и проверка S1 такое обязана считать провалом.
		if hold := f.stallHold; hold > 0 {
			select {
			case <-time.After(hold):
			case <-ctx.Done():
				return session.Response{}, ctx.Err()
			}
		}
		reason := "invalid: body length mismatch"
		if f.stallReason != "" {
			reason = f.stallReason
		}
		return denyResp(reason), nil
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if sum != p.SHA256 && !f.acceptTruncated {
		return denyResp("integrity"), nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	o := &fakeObject{
		id: f.nextID("media"), channel: p.ChannelID, uploader: a.user,
		name: p.Name, mime: p.Mime, sum: p.SHA256, size: p.BodyLength,
		body: buf.Bytes(),
	}
	f.objects[o.id] = o
	return okResp(map[string]any{"media_id": o.id, "sha256": o.sum, "size": o.size}), nil
}

func (a *fakeActor) download(payload []byte, sink io.Writer) (session.Response, error) {
	var p struct {
		MediaID string `json:"media_id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return denyResp("invalid: " + err.Error()), nil
	}
	f := a.f
	f.mu.Lock()
	o, ok := f.objects[p.MediaID]
	corrupt, relabel, ghost, visible := f.corruptDownload, f.relabelDownload, f.ghostAllowed, false
	attachedOnly := f.attachedOnly
	gone, goneReason := false, f.goneReason()
	if ok {
		visible = f.visible(o, a.user)
		gone = f.mediaGone(o)
	}
	f.mu.Unlock()

	if !ok {
		if !ghost {
			return denyResp(f.mediaDeny()), nil
		}
		// Мутация: несуществующий объект «находится». Тело и хеш
		// согласованы — иначе отказ пришёл бы от проверки целостности у
		// клиента, и проверка прогона снова ничего не измеряла бы.
		fabricated := []byte("fabricated-object")
		sum := sha256.Sum256(fabricated)
		if sink != nil {
			if _, err := sink.Write(fabricated); err != nil {
				return session.Response{}, err
			}
		}
		return session.Response{OK: true, BodyLength: int64(len(fabricated)), Data: mustJSON(map[string]any{
			"media_id": p.MediaID, "sha256": hex.EncodeToString(sum[:]), "size": int64(len(fabricated)),
		})}, nil
	}
	if !visible {
		return denyResp(f.mediaDeny()), nil
	}
	if gone {
		// Байтов нет, метаданные есть: отказ приходит ДО кадра ответа,
		// поэтому клиент получает причину, а не оборванное тело.
		return denyResp(goneReason), nil
	}
	// Мутация: сервер отдаёт только ПРИКРЕПЛЁННОЕ, то есть STORED
	// недостижим до attach. Это нарушение §11: STORED означает «байты
	// записаны и проверены», и брошенную загрузку иначе не найти —
	// обнаружима она по пустому post_id и возрасту, а не по недоступности.
	if attachedOnly && o.postID == "" {
		return denyResp("unavailable"), nil
	}

	out := append([]byte(nil), o.body...)
	if corrupt && len(out) > 0 {
		out[0] ^= 0xFF
	}
	// Объявленный хеш: либо хранимый, либо — при relabelDownload — хеш того,
	// что реально отдано. Второй случай существеннее первого: клиент media
	// сверяет принятое с ЗАЯВЛЕННЫМ, и согласованная подмена проходит его
	// проверку целиком. Обнаружить её может только независимая сверка с
	// исходником, ради которой прогон и считает хеши сам.
	sum := o.sum
	if relabel {
		s := sha256.Sum256(out)
		sum = hex.EncodeToString(s[:])
	}
	if sink != nil {
		if _, err := sink.Write(out); err != nil {
			return session.Response{}, err
		}
	}
	return session.Response{OK: true, BodyLength: int64(len(out)), Data: mustJSON(map[string]any{
		"media_id": o.id, "sha256": sum, "size": int64(len(out)), "mime": o.mime, "name": o.name,
	})}, nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
