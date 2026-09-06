package dispatcher

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	quicadapter "github.com/wiloris/photon-core/adapters/quic"
	"github.com/wiloris/photon-core/ports"
)

// Значения по умолчанию.
//
// Ноль в конфиге означает «по умолчанию», а НЕ «без предела»: вызов ABI без
// предела времени — это зависание клиента без способа его прервать, потому что
// отменить Photon_SendAction снаружи нечем (в сигнатуре нет ни ctx, ни
// таймаута). См. action-routing.md §3.5.
const (
	DefaultConnectTimeout = 15 * time.Second
	DefaultCallTimeout    = 30 * time.Second

	// DefaultKeepAlive — половина DefaultIdleTimeout quic-go (30 с), то же
	// значение, что выставлено на сервере после находки Phase 12.5:
	// subscribe-соединение умирало через 30 с простоя, потому что keep-alive
	// не слала ни одна сторона (bridge-session.md §9 п. 3).
	DefaultKeepAlive = 15 * time.Second

	// Backoff восстановления подписки (event-delivery.md §6.3, §11).
	//
	// Число попыток НЕ ограничено — у подписки нет вызывающего, который решил
	// бы, что пора прекратить; исчерпание попыток означало бы тихо
	// переставший работать канал событий, неотличимый для клиента от
	// «событий нет». Ограничен только интервал.
	DefaultEventBackoffMin = 500 * time.Millisecond
	DefaultEventBackoffMax = 30 * time.Second
)

// TLSOptions — то немногое из TLS, что имеет смысл выставлять из config_json.
type TLSOptions struct {
	// InsecureSkipVerify отключает проверку сертификата. Осознанно
	// присутствует: без него нельзя проверить работу против стенда с
	// самоподписанным сертификатом. Осознанно не имеет значения по
	// умолчанию — adapters/quic не включает его сам, и это правило здесь не
	// ослабляется.
	InsecureSkipVerify bool
	ServerName         string
}

// Config — параметры маршрутизации действий.
type Config struct {
	// Endpoint — адрес Bridge. Пустой означает «инстанс без адресата»: это
	// законное состояние, а не ошибка конфигурации (action-routing.md §4.2).
	// SendAction на таком инстансе возвращает ErrNoEndpoint, а проверка
	// action-кода происходит РАНЬШЕ и от наличия адресата не зависит.
	Endpoint ports.Endpoint

	// SessionToken — Session ID Mattermost, а НЕ токен из /users/login: имя
	// поля на проводе историческое (bridge-session.md §2.1). Значения
	// различаются, и перепутать их стоит часа.
	SessionToken string

	ConnectTimeout time.Duration
	CallTimeout    time.Duration
	KeepAlive      time.Duration

	// EventBackoffMin/Max — пауза между попытками восстановить подписку.
	// Растёт удвоением от Min до Max и сбрасывается после успешного
	// подключения. Пауза обязана быть отменяемой: неотменяемая заставила бы
	// Photon_Destroy ждать её длину (event-delivery.md §6.3).
	EventBackoffMin time.Duration
	EventBackoffMax time.Duration

	TLS TLSOptions

	// Transport — nil означает adapters/quic с параметрами выше. Тот же
	// приём, что у session.Config.Codec (nil → JSONCodec), и по той же
	// причине: тесты подставляют фиктивный транспорт, а config_json его
	// выразить не может и не должен.
	//
	// Это же поле — точка, куда однажды встанет autodial.Dialer: он
	// реализует ports.Transport, и подключение автоматического выбора
	// маршрута не потребует правок ни здесь, ни в bindings.
	Transport ports.Transport
}

// withDefaults подставляет значения по умолчанию. Идемпотентна.
func (c Config) withDefaults() Config {
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = DefaultCallTimeout
	}
	if c.KeepAlive <= 0 {
		c.KeepAlive = DefaultKeepAlive
	}
	if c.EventBackoffMin <= 0 {
		c.EventBackoffMin = DefaultEventBackoffMin
	}
	if c.EventBackoffMax < c.EventBackoffMin {
		c.EventBackoffMax = DefaultEventBackoffMax
	}
	if c.EventBackoffMax < c.EventBackoffMin {
		c.EventBackoffMax = c.EventBackoffMin
	}
	return c
}

// wireConfig — форма config_json. Отдельный тип, а не Config: у Config есть
// поля, которые JSON выразить не может и не должен (Transport), и единицы
// времени на границе ABI — миллисекунды, а не time.Duration.
type wireConfig struct {
	Bridge *struct {
		Endpoint         string `json:"endpoint"`
		SessionToken     string `json:"session_token"`
		ConnectTimeoutMS int64  `json:"connect_timeout_ms"`
		CallTimeoutMS    int64  `json:"call_timeout_ms"`
		KeepAliveMS      int64  `json:"keepalive_ms"`
		// Восстановление подписки. Ноль означает «по умолчанию», как и у
		// таймаутов выше: инстанс, созданный минимальным config_json, обязан
		// получать события без дополнительной настройки.
		EventBackoffMinMS int64 `json:"event_backoff_min_ms"`
		EventBackoffMaxMS int64 `json:"event_backoff_max_ms"`
		TLS               *struct {
			InsecureSkipVerify bool   `json:"insecure_skip_verify"`
			ServerName         string `json:"server_name"`
		} `json:"tls"`
	} `json:"bridge"`
}

// ParseConfig разбирает config_json границы ABI.
//
// Разбор живёт здесь, а не в bindings, по той же причине, по какой там не
// живёт валидация action-кода: bindings отвечает за гигиену ABI, схема
// конфигурации — доменное знание. Оставить её в bindings значит переписать её
// заново в каждом биндинге (JNI, Desktop, будущие Rust/Swift).
//
// Два правила, каждое из которых легко нарушить «улучшением»:
//
//  1. Неизвестные поля ИГНОРИРУЮТСЯ. Иначе конфиг перестанет быть
//     расширяемым: добавление секции для будущей подсистемы ломало бы старое
//     ядро. Поэтому здесь нет DisallowUnknownFields.
//  2. Секция "bridge" НЕОБЯЗАТЕЛЬНА. Инстанс без адреса Bridge законен —
//     `{}` и `{"storage_path":"/tmp/photon"}` уже принимаются Photon_Create,
//     и сделать endpoint обязательным значило бы изменить поведение
//     замороженной функции, то есть завести ADR (ADR-008).
func ParseConfig(raw []byte) (Config, error) {
	var wc wireConfig
	if err := json.Unmarshal(raw, &wc); err != nil {
		return Config{}, fmt.Errorf("dispatcher: config_json: %w", err)
	}
	if wc.Bridge == nil {
		return Config{}.withDefaults(), nil
	}

	for _, f := range []struct {
		name string
		ms   int64
	}{
		{"connect_timeout_ms", wc.Bridge.ConnectTimeoutMS},
		{"call_timeout_ms", wc.Bridge.CallTimeoutMS},
		{"keepalive_ms", wc.Bridge.KeepAliveMS},
		{"event_backoff_min_ms", wc.Bridge.EventBackoffMinMS},
		{"event_backoff_max_ms", wc.Bridge.EventBackoffMaxMS},
	} {
		if f.ms < 0 {
			return Config{}, fmt.Errorf("dispatcher: config_json: bridge.%s is negative: %d", f.name, f.ms)
		}
	}

	cfg := Config{
		Endpoint:       ports.Endpoint(wc.Bridge.Endpoint),
		SessionToken:   wc.Bridge.SessionToken,
		ConnectTimeout: time.Duration(wc.Bridge.ConnectTimeoutMS) * time.Millisecond,
		CallTimeout:    time.Duration(wc.Bridge.CallTimeoutMS) * time.Millisecond,
		KeepAlive:      time.Duration(wc.Bridge.KeepAliveMS) * time.Millisecond,

		EventBackoffMin: time.Duration(wc.Bridge.EventBackoffMinMS) * time.Millisecond,
		EventBackoffMax: time.Duration(wc.Bridge.EventBackoffMaxMS) * time.Millisecond,
	}
	if wc.Bridge.TLS != nil {
		cfg.TLS = TLSOptions{
			InsecureSkipVerify: wc.Bridge.TLS.InsecureSkipVerify,
			ServerName:         wc.Bridge.TLS.ServerName,
		}
	}
	return cfg.withDefaults(), nil
}

// newQUICTransport собирает транспорт по умолчанию.
func newQUICTransport(cfg Config) ports.Transport {
	return quicadapter.New(quicadapter.Config{
		TLSConfig: &tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify, //nolint:gosec // явное решение вызывающего, см. TLSOptions
			ServerName:         cfg.TLS.ServerName,
		},
		HandshakeTimeout: cfg.ConnectTimeout,
		KeepAlivePeriod:  cfg.KeepAlive,
	})
}
