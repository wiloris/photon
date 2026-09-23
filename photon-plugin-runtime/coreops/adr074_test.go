package coreops

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-core/adapters/aesgcm"
	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	ed25519adapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"

	"github.com/wiloris/photon-plugin-runtime/ipc"
)

/*
 * Проверка конверта доклада на стороне RUNTIME (ADR-074 §4, §7, §12).
 *
 * Здесь проверяется именно доверенная сторона, отдельно от SDK, и это не
 * дублирование: SDK исполняется в недоверенном процессе, и его проверки
 * ничего не гарантируют (ADR-031). Кадр, обошедший SDK, приходит сюда — а
 * составить такой кадр в тесте можно только вызвав Handle напрямую, что и
 * делается ниже.
 */

// reportSink — исполнитель с записью докладов.
type reportSink struct {
	h    *Handler
	got  []WorkReportForTest
	done func()
}

// WorkReportForTest — то, что увидел потребитель колбэка.
type WorkReportForTest struct {
	PluginID string
	Status   WorkStatus
	Payload  []byte
}

func newReportSink(t *testing.T) *reportSink {
	t.Helper()

	st, err := bboltadapter.New(t.TempDir()+"/adr074.db", []bboltadapter.Migration{{
		Version: 1,
		Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(testNS))
			return e
		},
	}})
	if err != nil {
		t.Fatalf("bbolt.New: %v", err)
	}
	if err := st.Migrate(context.Background(), 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	id := ed25519adapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	cr := aesgcm.New()
	master, err := cr.GenerateKey(context.Background())
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	sink := &reportSink{done: func() { st.Close(context.Background()) }}
	h, err := New(Config{
		Storage:          st,
		StorageNamespace: testNS,
		Identity:         id,
		DeviceKey:        pair.Private,
		Crypto:           cr,
		DeriveKey: func(ctx context.Context, domain string) (ports.SymmetricKey, error) {
			return cr.DeriveKey(ctx, master, domain)
		},
		Log: func(string, string, string) {},
		OnWorkComplete: func(pluginID string, status WorkStatus, payload []byte) {
			sink.got = append(sink.got, WorkReportForTest{pluginID, status, payload})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sink.h = h
	t.Cleanup(sink.done)
	return sink
}

// raw отправляет доклад ГОТОВЫМ телом запроса, минуя любую сборку на стороне
// плагина: только так проверяется кадр, обошедший SDK.
func (s *reportSink) raw(t *testing.T, body []byte) ([]byte, error) {
	t.Helper()
	return s.h.Handle(context.Background(), ipc.CallerInfo{PluginID: "dev.photon.generic"}, "work.complete", body)
}

// envelope собирает тело так же, как его собрал бы SDK.
func envelope(t *testing.T, status string, payload []byte) []byte {
	t.Helper()
	b, err := json.Marshal(struct {
		Status  string `json:"status,omitempty"`
		Payload []byte `json:"payload,omitempty"`
	}{status, payload})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// T7 — нагрузка сверх предела ОТВЕРГАЕТСЯ, и потребитель не видит ничего.
//
// Две половины утверждения проверяются раздельно, потому что первая без
// второй ничего не стоит: отказ, при котором обрезанный доклад всё же дошёл
// до потребителя, был бы худшим из исходов — правдоподобным, но ложным
// результатом.
func TestADR074_T7_OverLimitPayloadRejectedNotTruncated(t *testing.T) {
	s := newReportSink(t)

	over := make([]byte, maxWorkPayloadBytes+1)
	for i := range over {
		over[i] = byte(i % 251)
	}

	_, err := s.raw(t, envelope(t, "success", over))
	if err == nil {
		t.Fatal("нагрузка сверх предела принята — обязан быть отказ")
	}
	if code := opCode(t, err); code != ipc.ErrInvalidRequest {
		t.Fatalf("код ошибки %q, ожидался %q", code, ipc.ErrInvalidRequest)
	}
	if len(s.got) != 0 {
		t.Fatalf("потребитель получил %d докладов при отказе, ожидалось 0 (длина нагрузки %d)",
			len(s.got), len(s.got[0].Payload))
	}
}

// T6 на стороне runtime — нагрузка РОВНО в предел проходит целиком.
//
// Парная к T7: без неё «отказ сверх предела» проходил бы и у реализации,
// отвергающей вообще всё.
func TestADR074_T6_PayloadExactlyAtLimitAccepted(t *testing.T) {
	s := newReportSink(t)

	exact := make([]byte, maxWorkPayloadBytes)
	for i := range exact {
		exact[i] = byte(i % 251)
	}

	if _, err := s.raw(t, envelope(t, "success", exact)); err != nil {
		t.Fatalf("нагрузка ровно в предел отвергнута: %v", err)
	}
	if len(s.got) != 1 {
		t.Fatalf("докладов %d, ожидался 1", len(s.got))
	}
	if !bytes.Equal(s.got[0].Payload, exact) {
		t.Fatalf("нагрузка изменилась: %d байт вместо %d", len(s.got[0].Payload), len(exact))
	}
}

// T10 — ПУСТОЕ тело запроса: ровно то, что слал плагин до ADR-074.
//
// Старый SDK отправлял work.complete без полезной нагрузки вовсе
// (call(ctx, "work.complete", nil, nil) → payload == nil). Такой доклад
// обязан значить ровно то же, что значил раньше: успех без нагрузки.
func TestADR074_T10_LegacyEmptyEnvelopeIsSuccess(t *testing.T) {
	s := newReportSink(t)

	if _, err := s.raw(t, nil); err != nil {
		t.Fatalf("доклад старого образца отвергнут: %v", err)
	}
	if len(s.got) != 1 {
		t.Fatalf("докладов %d, ожидался 1", len(s.got))
	}
	if s.got[0].Status != WorkSuccess {
		t.Fatalf("статус %q, ожидался %q", s.got[0].Status, WorkSuccess)
	}
	if len(s.got[0].Payload) != 0 {
		t.Fatalf("нагрузка непуста: %d байт", len(s.got[0].Payload))
	}
}

// Неизвестный статус отвергается — и потребитель о нём не узнаёт.
//
// Без этой проверки «статус» был бы свободной строкой, а не двумя
// значениями: реализация, передающая наружу что угодно, прошла бы все
// остальные тесты.
func TestADR074_UnknownStatusRejected(t *testing.T) {
	s := newReportSink(t)

	_, err := s.raw(t, envelope(t, "maybe", []byte("x")))
	if err == nil {
		t.Fatal("неизвестный статус принят")
	}
	if code := opCode(t, err); code != ipc.ErrInvalidRequest {
		t.Fatalf("код ошибки %q, ожидался %q", code, ipc.ErrInvalidRequest)
	}
	if len(s.got) != 0 {
		t.Fatalf("потребитель получил доклад с негодным статусом: %+v", s.got)
	}
}

// T2 на стороне runtime — байты доходят без изменений.
//
// Вектор подобран так, чтобы поймать подмену текстом: нулевой байт, 0xFF,
// одиночный continuation-байт и overlong-последовательность. Сравнение
// идёт по байтам, а не по разобранному JSON: разбор скрыл бы и
// перекодировку, и потерю хвоста.
func TestADR074_T2_PayloadSurvivesByteForByte(t *testing.T) {
	s := newReportSink(t)

	vector := []byte{0x00, 0xFF, 0xFE, 0x01, 0x80, 0x81, 0xC0, 0xAF, 0x00, 'z'}
	if _, err := s.raw(t, envelope(t, "failure", vector)); err != nil {
		t.Fatalf("доклад отвергнут: %v", err)
	}
	if len(s.got) != 1 {
		t.Fatalf("докладов %d, ожидался 1", len(s.got))
	}
	if s.got[0].Status != WorkFailure {
		t.Fatalf("статус %q, ожидался %q", s.got[0].Status, WorkFailure)
	}
	if !bytes.Equal(s.got[0].Payload, vector) {
		t.Fatalf("байты изменились:\n получено %v\n ожидалось %v", s.got[0].Payload, vector)
	}
}

// T8 на стороне runtime — повторные доклады НЕ ЗАМЕНЯЮТ друг друга.
//
// Исполнитель не хранит «последний результат» и хранить его не должен: он
// передаёт каждый доклад отдельно, а накопление — дело потребителя
// (ADR-074 §10).
func TestADR074_T8_RepeatedReportsAreIndependent(t *testing.T) {
	s := newReportSink(t)

	first := []byte("первая единица работы")
	second := []byte("вторая единица работы")

	if _, err := s.raw(t, envelope(t, "success", first)); err != nil {
		t.Fatalf("первый доклад: %v", err)
	}
	if _, err := s.raw(t, envelope(t, "failure", second)); err != nil {
		t.Fatalf("второй доклад: %v", err)
	}

	if len(s.got) != 2 {
		t.Fatalf("докладов %d, ожидалось 2 — второй не заменяет первый", len(s.got))
	}
	if s.got[0].Status != WorkSuccess || !bytes.Equal(s.got[0].Payload, first) {
		t.Fatalf("первый доклад испорчен: %+v", s.got[0])
	}
	if s.got[1].Status != WorkFailure || !bytes.Equal(s.got[1].Payload, second) {
		t.Fatalf("второй доклад испорчен: %+v", s.got[1])
	}
}

// Негодный JSON конверта отвергается — форма проверяется, содержимое нет.
func TestADR074_MalformedEnvelopeRejected(t *testing.T) {
	s := newReportSink(t)

	if _, err := s.raw(t, []byte(`{"status":`)); err == nil {
		t.Fatal("негодный JSON принят")
	}
	if len(s.got) != 0 {
		t.Fatalf("потребитель получил доклад с негодным конвертом: %+v", s.got)
	}
}
