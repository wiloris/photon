package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	serverAddr   = "anichavpn.ru:4443"
	sessionToken = "REDACTED-MATTERMOST-SESSION-TOKEN"
)

type Response struct {
	OK    bool        `json:"ok"`
	Data  interface{} `json:"data"`
	Error string      `json:"error"`
}

// obj безопасно приводит Data к map[string]interface{}.
// Если Data — массив (список) или другой не-объектный тип, возвращает nil-мапу:
// индексирование nil-мапы в Go не паникует и просто даёт нулевое значение.
// Это заменяет старое жёсткое поле `Data map[string]interface{}`, которое падало
// с ошибкой декодирования на любом ответе-списке (getUsers, searchUsers,
// getUsersByIds, getReactions и т.п.).
func (r Response) obj() map[string]interface{} {
	m, _ := r.Data.(map[string]interface{})
	return m
}

// ── handshake ─────────────────────────────────────────────────────────────────
// Auth + Mode в ОДНОМ стриме — именно так работает сервер.
// Возвращает соединение готовое к action-стримам.

func handshake(ctx context.Context, mode string) *quic.Conn {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"photon-quic-v1"},
	}
	conn, err := quic.DialAddr(ctx, serverAddr, tlsConf, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		log.Fatal("open handshake stream:", err)
	}

	enc := json.NewEncoder(stream)
	dec := json.NewDecoder(stream)

	// Фрейм 1: auth
	if err := enc.Encode(map[string]string{"session_token": sessionToken}); err != nil {
		log.Fatal("send auth:", err)
	}

	// Фрейм 2: mode
	if err := enc.Encode(map[string]string{"mode": mode}); err != nil {
		log.Fatal("send mode:", err)
	}

	// Читаем подтверждение
	var resp Response
	if err := dec.Decode(&resp); err != nil {
		log.Fatal("read handshake resp:", err)
	}
	if !resp.OK {
		log.Fatal("handshake failed:", resp.Error)
	}

	stream.Close()
	return conn
}

// ── action ────────────────────────────────────────────────────────────────────
// Каждый action — отдельный стрим.

func action(ctx context.Context, conn *quic.Conn, code int, resource string, payload interface{}) Response {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return Response{OK: false, Error: "open stream: " + err.Error()}
	}
	defer s.Close()

	enc := json.NewEncoder(s)
	enc.Encode(map[string]interface{}{"action": code, "resource": resource})
	if payload == nil {
		enc.Encode(map[string]interface{}{})
	} else {
		enc.Encode(payload)
	}

	var resp Response
	if err := json.NewDecoder(s).Decode(&resp); err != nil {
		return Response{OK: false, Error: "decode: " + err.Error()}
	}
	return resp
}

func result(label string, resp Response) {
	if resp.OK {
		fmt.Printf("  ✅ %-42s ok\n", label)
	} else {
		fmt.Printf("  ❌ %-42s FAIL: %s\n", label, resp.Error)
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	log.SetFlags(log.Ltime)
	ctx := context.Background()

	fmt.Println("\n╔════════════════════════════════════════════╗")
	fmt.Println("║     PHOTON QUIC — FULL TEST SUITE          ║")
	fmt.Println("╚════════════════════════════════════════════╝")

	conn := handshake(ctx, "action")
	defer conn.CloseWithError(0, "test done")
	fmt.Println("\n🔗 Action connection ready\n")

	var myUserID, myChannelID, myPostID, myFileID, myTeamID string

	// ══════════════════════════════════════════════════════════════════════════
	fmt.Println("── UserResource (200-210) ───────────────────────")

	r := action(ctx, conn, 200, "UserResource", nil)
	result("getMe (200)", r)
	if r.OK {
		myUserID, _ = r.obj()["id"].(string)
		fmt.Printf("     id=%s  username=%s\n", myUserID, r.obj()["username"])
	}

	r = action(ctx, conn, 201, "UserResource", map[string]string{"user_id": myUserID})
	result("getUser (201)", r)

	r = action(ctx, conn, 202, "UserResource", map[string]interface{}{"page": 0, "per_page": 5})
	result("getUsers (202)", r)

	r = action(ctx, conn, 203, "UserResource", map[string]string{"user_id": myUserID})
	result("getUserStatus (203)", r)
	if r.OK {
		fmt.Printf("     status=%v\n", r.obj()["status"])
	}

	r = action(ctx, conn, 204, "UserResource", map[string]string{"user_id": myUserID})
	result("getUserProfile (204)", r)

	r = action(ctx, conn, 205, "UserResource", map[string]interface{}{"term": "a", "limit": 3})
	result("searchUsers (205)", r)

	r = action(ctx, conn, 206, "UserResource", map[string]interface{}{"user_ids": []string{myUserID}})
	result("getUsersByIds (206)", r)

	r = action(ctx, conn, 207, "UserResource", map[string]interface{}{
		"nickname": fmt.Sprintf("PhotonTest_%d", time.Now().Unix()%1000),
	})
	result("patchMe (207)", r)

	r = action(ctx, conn, 208, "UserResource", map[string]string{"status": "online"})
	result("setStatus (208)", r)

	r = action(ctx, conn, 210, "UserResource", map[string]string{"user_id": myUserID})
	result("getUserAvatar (210)", r)

	// ══════════════════════════════════════════════════════════════════════════
	fmt.Println("\n── ChannelResource (300-314) ────────────────────")

	// createDirectChannel с собой — не требует team_id, даёт channel_id
	r = action(ctx, conn, 306, "ChannelResource", map[string]string{"target_user_id": myUserID})
	result("createDirectChannel self (306)", r)
	if r.OK {
		myChannelID, _ = r.obj()["id"].(string)
		myTeamID, _ = r.obj()["team_id"].(string)
		fmt.Printf("     channel_id=%s  team_id=%s\n", myChannelID, myTeamID)
	}

	if myTeamID != "" {
		r = action(ctx, conn, 300, "ChannelResource", map[string]interface{}{
			"team_id": myTeamID, "page": 0, "per_page": 10,
		})
		result("listChannels (300)", r)

		r = action(ctx, conn, 302, "ChannelResource", map[string]string{"team_id": myTeamID})
		result("getMyChannels (302)", r)
	}

	if myChannelID != "" {
		r = action(ctx, conn, 303, "ChannelResource", map[string]string{"channel_id": myChannelID})
		result("getChannel (303)", r)

		r = action(ctx, conn, 301, "ChannelResource", map[string]interface{}{
			"channel_id": myChannelID, "page": 0, "per_page": 10,
		})
		result("getChannelMembers (301)", r)

		r = action(ctx, conn, 312, "ChannelResource", map[string]string{"channel_id": myChannelID})
		result("getUnreadForChannel (312)", r)
		if r.OK {
			fmt.Printf("     unread=%v  mentions=%v\n", r.obj()["unread_count"], r.obj()["mention_count"])
		}

		r = action(ctx, conn, 313, "ChannelResource", map[string]string{"channel_id": myChannelID})
		result("markAsRead (313)", r)

		r = action(ctx, conn, 314, "ChannelResource", map[string]interface{}{
			"channel_id": myChannelID, "user_id": myUserID,
		})
		result("getChannelMember (314)", r)
	}

	// ══════════════════════════════════════════════════════════════════════════
	fmt.Println("\n── PostResource (100-114) ───────────────────────")

	if myChannelID != "" {
		r = action(ctx, conn, 100, "PostResource", map[string]interface{}{
			"channel_id": myChannelID,
			"message":    fmt.Sprintf("🤖 Photon test %d", time.Now().Unix()),
		})
		result("createPost (100)", r)
		if r.OK {
			myPostID, _ = r.obj()["id"].(string)
			fmt.Printf("     post_id=%s\n", myPostID)
		}

		r = action(ctx, conn, 101, "PostResource", map[string]interface{}{
			"channel_id": myChannelID, "page": 0, "per_page": 5,
		})
		result("getFeed (101)", r)

		if myPostID != "" {
			r = action(ctx, conn, 105, "PostResource", map[string]interface{}{
				"root_id": myPostID, "message": "🤖 Test reply",
			})
			result("replyToPost (105)", r)

			r = action(ctx, conn, 104, "PostResource", map[string]string{"post_id": myPostID})
			result("getThread (104)", r)

			r = action(ctx, conn, 110, "PostResource", map[string]interface{}{
				"post_id": myPostID, "emoji_name": "thumbsup",
			})
			result("addReaction (110)", r)

			r = action(ctx, conn, 112, "PostResource", map[string]string{"post_id": myPostID})
			result("getReactions (112)", r)

			r = action(ctx, conn, 113, "PostResource", map[string]string{"post_id": myPostID})
			result("toggleLike → on (113)", r)
			if r.OK {
				fmt.Printf("     liked=%v\n", r.obj()["liked"])
			}

			r = action(ctx, conn, 113, "PostResource", map[string]string{"post_id": myPostID})
			result("toggleLike → off (113)", r)
			if r.OK {
				fmt.Printf("     liked=%v\n", r.obj()["liked"])
			}

			r = action(ctx, conn, 114, "PostResource", map[string]string{"post_id": myPostID})
			result("getPostSocialInfo (114)", r)
			if r.OK {
				fmt.Printf("     total_reactions=%v\n", r.obj()["total"])
			}

			r = action(ctx, conn, 106, "PostResource", map[string]string{"post_id": myPostID})
			result("pinPost (106)", r)

			r = action(ctx, conn, 108, "PostResource", map[string]string{"channel_id": myChannelID})
			result("getPinnedPosts (108)", r)

			r = action(ctx, conn, 107, "PostResource", map[string]string{"post_id": myPostID})
			result("unpinPost (107)", r)

			r = action(ctx, conn, 103, "PostResource", map[string]interface{}{
				"post_id": myPostID, "message": "🤖 Updated",
			})
			result("updatePost (103)", r)

			if myTeamID != "" {
				r = action(ctx, conn, 109, "PostResource", map[string]interface{}{
					"team_id": myTeamID, "query": "Photon test",
				})
				result("searchPosts (109)", r)
			}

			r = action(ctx, conn, 111, "PostResource", map[string]interface{}{
				"post_id": myPostID, "emoji_name": "thumbsup",
			})
			result("removeReaction (111)", r)

			// Cleanup пост
			defer func(pid string) {
				r := action(ctx, conn, 102, "PostResource", map[string]string{"post_id": pid})
				result("deletePost cleanup (102)", r)
			}(myPostID)
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	fmt.Println("\n── FileResource (500-505) ───────────────────────")

	if myChannelID != "" {
		pngBytes := []byte{
			0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
			0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
			0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
			0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
			0xde, 0x00, 0x00, 0x00, 0x0c, 0x49, 0x44, 0x41,
			0x54, 0x08, 0xd7, 0x63, 0xf8, 0xcf, 0xc0, 0x00,
			0x00, 0x00, 0x02, 0x00, 0x01, 0xe2, 0x21, 0xbc,
			0x33, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e,
			0x44, 0xae, 0x42, 0x60, 0x82,
		}

		r = action(ctx, conn, 500, "FileResource", map[string]interface{}{
			"channel_id": myChannelID,
			"file_name":  "test.png",
			"file_bytes": pngBytes,
		})
		result("uploadFile (500)", r)
		if r.OK {
			myFileID, _ = r.obj()["id"].(string)
			fmt.Printf("     file_id=%s\n", myFileID)
		}

		if myFileID != "" {
			r = action(ctx, conn, 502, "FileResource", map[string]string{"file_id": myFileID})
			result("getFileInfo (502)", r)

			r = action(ctx, conn, 503, "FileResource", map[string]string{"file_id": myFileID})
			result("getThumbnail URL (503)", r)
			if r.OK {
				fmt.Printf("     url=%v\n", r.obj()["url"])
			}

			r = action(ctx, conn, 504, "FileResource", map[string]string{"file_id": myFileID})
			result("getPreview URL (504)", r)

			r = action(ctx, conn, 505, "FileResource", map[string]interface{}{
				"file_id": myFileID, "file_type": "FULL",
			})
			result("getImageURL FULL (505)", r)

			r = action(ctx, conn, 505, "FileResource", map[string]interface{}{
				"file_id": myFileID, "file_type": "THUMB",
			})
			result("getImageURL THUMB (505)", r)

			r = action(ctx, conn, 501, "FileResource", map[string]string{"file_id": myFileID})
			result("downloadFile (501)", r)
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	fmt.Println("\n── EngramResource (400-405) ─────────────────────")

	r = action(ctx, conn, 400, "EngramResource", map[string]interface{}{
		"key":      "test:session",
		"value":    map[string]interface{}{"device": "photon-test", "ts": time.Now().Unix()},
		"ttl_secs": 300,
	})
	result("engramStore (400)", r)
	if r.OK {
		fmt.Printf("     version=%v\n", r.obj()["version"])
	}

	r = action(ctx, conn, 400, "EngramResource", map[string]interface{}{
		"key":          "test:session",
		"value":        map[string]interface{}{"device": "updated"},
		"preserve_ttl": true,
	})
	result("engramStore preserveTTL (400)", r)
	if r.OK {
		fmt.Printf("     version=%v (ожидается 2)\n", r.obj()["version"])
	}

	r = action(ctx, conn, 401, "EngramResource", map[string]string{"key": "test:session"})
	result("engramQuery (401)", r)

	r = action(ctx, conn, 400, "EngramResource", map[string]interface{}{
		"key":   "test:prefs",
		"value": map[string]interface{}{"theme": "dark"},
	})
	result("engramStore second key (400)", r)

	r = action(ctx, conn, 403, "EngramResource", map[string]interface{}{
		"prefix": "test:", "page": 0, "limit": 50,
	})
	result("engramList (403)", r)
	if r.OK {
		fmt.Printf("     keys=%v\n", r.obj()["keys"])
	}

	r = action(ctx, conn, 404, "EngramResource", map[string]interface{}{
		"entries": []map[string]interface{}{
			{"key": "batch:1", "value": map[string]interface{}{"n": 1}},
			{"key": "batch:2", "value": map[string]interface{}{"n": 2}},
			{"key": "batch:3", "value": map[string]interface{}{"n": 3}},
		},
	})
	result("engramBatchStore (404)", r)
	if r.OK {
		fmt.Printf("     stored=%v\n", r.obj()["stored"])
	}

	r = action(ctx, conn, 405, "EngramResource", map[string]interface{}{
		"keys": []string{"batch:1", "batch:2", "batch:3", "missing:key"},
	})
	result("engramBatchQuery (405)", r)
	if r.OK {
		if entries, ok := r.obj()["entries"].(map[string]interface{}); ok {
			fmt.Printf("     found=%d  missing=%v\n", len(entries), r.obj()["missing"])
		}
	}

	for _, k := range []string{"test:session", "test:prefs", "batch:1", "batch:2", "batch:3"} {
		action(ctx, conn, 402, "EngramResource", map[string]string{"key": k})
	}
	fmt.Println("  🧹 энграммы очищены")

	// ══════════════════════════════════════════════════════════════════════════
	fmt.Println("\n── WallResource (600-602) ───────────────────────")

	if myTeamID != "" {
		r = action(ctx, conn, 600, "WallResource", map[string]interface{}{
			"team_id": myTeamID, "user_id": myUserID,
		})
		result("getOrCreateWall (600)", r)
		wallChanID := ""
		if r.OK {
			wallChanID, _ = r.obj()["id"].(string)
			fmt.Printf("     wall_channel_id=%s\n", wallChanID)
		}

		if wallChanID != "" {
			r = action(ctx, conn, 601, "WallResource", map[string]interface{}{
				"channel_id": wallChanID, "page": 0, "per_page": 5,
			})
			result("getWallPosts (601)", r)

			r = action(ctx, conn, 602, "WallResource", map[string]interface{}{
				"channel_id": wallChanID,
				"message":    "🤖 Wall test post",
			})
			result("publishWallPost (602)", r)
		}
	} else {
		fmt.Println("  ⚠️  WallResource пропущен (DM канал не имеет team_id)")
	}

	// ══════════════════════════════════════════════════════════════════════════
	fmt.Println("\n── Subscribe / Push events ──────────────────────")

	subConn := handshake(ctx, "subscribe")
	defer subConn.CloseWithError(0, "sub done")
	fmt.Println("  📡 Subscribe соединение активно")
	fmt.Println("  ⏳ Ждём push (5 сек)...")

	eventCh := make(chan string, 1)
	pushCtx, pushCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pushCancel()

	go func() {
		stream, err := subConn.AcceptUniStream(pushCtx)
		if err != nil {
			eventCh <- "timeout: " + err.Error()
			return
		}
		defer stream.CancelRead(0)
		var ev map[string]interface{}
		json.NewDecoder(stream).Decode(&ev)
		eventCh <- fmt.Sprintf("event=%v", ev["event"])
	}()

	// Провоцируем push
	if myChannelID != "" {
		time.Sleep(200 * time.Millisecond)
		action(ctx, conn, 100, "PostResource", map[string]interface{}{
			"channel_id": myChannelID,
			"message":    "🤖 push trigger",
		})
	}

	select {
	case ev := <-eventCh:
		fmt.Printf("  ✅ %-42s %s\n", "push received", ev)
	case <-time.After(6 * time.Second):
		fmt.Printf("  ⚠️  %-42s timeout\n", "push event")
	}

	fmt.Println("\n╔════════════════════════════════════════════╗")
	fmt.Println("║           TESTS COMPLETE                   ║")
	fmt.Println("╚════════════════════════════════════════════╝\n")
}
