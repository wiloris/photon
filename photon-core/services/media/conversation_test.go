package media

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	photonpb "github.com/wiloris/photon/photon-core/protobuf"
	"github.com/wiloris/photon/photon-core/services/session"
)

// 703/704 — вложения разговоров (ADR-124 M1, contract §8.3–§8.4).

func convReq(b []byte) ConvUploadRequest {
	return ConvUploadRequest{ConversationID: "conv1", UploadKey: "op:0", Kind: "file", Name: "a.bin",
		SHA256: sum(b), Size: int64(len(b))}
}

func TestConvUploadSendsPayloadAndExactLength(t *testing.T) {
	body := payload(5000)
	f := &fakeStreamer{}
	f.serve = func(_ context.Context, _ int32, _ []byte, b io.Reader, _ io.Writer) (session.Response, error) {
		n, _ := io.Copy(io.Discard, b)
		f.readFromBody = n
		return okResp(Ref{MediaID: "m1", SHA256: sum(body), Size: int64(len(body))}), nil
	}
	// Источник длиннее объявленного: уйти должно ровно Size байт.
	ref, err := New(f).ConvUpload(context.Background(), convReq(body), io.MultiReader(bytes.NewReader(body), bytes.NewReader([]byte("tail"))))
	if err != nil || ref.MediaID != "m1" {
		t.Fatalf("ConvUpload: %v %v", ref, err)
	}
	if f.gotAction != int32(photonpb.Action_ACTION_CONV_MEDIA_UPLOAD) || f.readFromBody != int64(len(body)) {
		t.Fatalf("action %d, тело %d байт", f.gotAction, f.readFromBody)
	}
	var p map[string]any
	_ = json.Unmarshal(f.gotPayload, &p)
	for k, want := range map[string]any{"conversation_id": "conv1", "upload_key": "op:0", "kind": "file",
		"name": "a.bin", "sha256": sum(body), "body_length": float64(len(body))} {
		if p[k] != want {
			t.Errorf("payload[%s] = %v, ожидалось %v", k, p[k], want)
		}
	}
}

func TestConvUploadRejectionCarriesCode(t *testing.T) {
	f := &fakeStreamer{serve: drain(session.Response{OK: false, Error: "forbidden"})}
	_, err := New(f).ConvUpload(context.Background(), convReq([]byte("x")), bytes.NewReader([]byte("x")))
	var rej *Rejection
	if !errors.As(err, &rej) || rej.Code != "forbidden" || !errors.Is(err, ErrRejected) {
		t.Fatalf("вердикт: %v", err)
	}
}

func TestConvUploadServerMismatchIsError(t *testing.T) {
	body := []byte("data")
	for name, ref := range map[string]Ref{
		"хэш":   {MediaID: "m", SHA256: sum([]byte("other")), Size: 4},
		"длина": {MediaID: "m", SHA256: sum(body), Size: 3},
	} {
		f := &fakeStreamer{serve: drain(okResp(ref))}
		if _, err := New(f).ConvUpload(context.Background(), convReq(body), bytes.NewReader(body)); err == nil {
			t.Errorf("%s: несовпадение принято как успех", name)
		}
	}
}

func convServe(info ConvMedia, blob []byte) func(context.Context, int32, []byte, io.Reader, io.Writer) (session.Response, error) {
	return func(_ context.Context, _ int32, _ []byte, _ io.Reader, sink io.Writer) (session.Response, error) {
		_, _ = sink.Write(blob)
		return okResp(info), nil
	}
}

func TestConvDownloadVerifiesLengthAndHash(t *testing.T) {
	blob := payload(3000)
	ok := ConvMedia{MediaID: "m1", Kind: "file", Mime: "application/octet-stream", Name: "a", Size: 3000, SHA256: sum(blob)}
	var got bytes.Buffer
	info, err := New(&fakeStreamer{serve: convServe(ok, blob)}).ConvDownload(context.Background(), "m1", &got)
	if err != nil || info.Size != 3000 || !bytes.Equal(got.Bytes(), blob) {
		t.Fatalf("ConvDownload: %v %v", info, err)
	}
	bad := ok
	bad.SHA256 = sum([]byte("x"))
	if _, err := New(&fakeStreamer{serve: convServe(bad, blob)}).ConvDownload(context.Background(), "m1", io.Discard); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("хэш: %v", err)
	}
	short := ok
	short.Size = 4000
	if _, err := New(&fakeStreamer{serve: convServe(short, blob)}).ConvDownload(context.Background(), "m1", io.Discard); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("недобор: %v", err)
	}
	other := ok
	other.MediaID = "m2"
	if _, err := New(&fakeStreamer{serve: convServe(other, blob)}).ConvDownload(context.Background(), "m1", io.Discard); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("чужой объект: %v", err)
	}
	rej := &fakeStreamer{serve: drain(session.Response{OK: false, Error: "unavailable"})}
	var r *Rejection
	if _, err := New(rej).ConvDownload(context.Background(), "m1", io.Discard); !errors.As(err, &r) || r.Code != "unavailable" {
		t.Fatalf("вердикт: %v", err)
	}
}
