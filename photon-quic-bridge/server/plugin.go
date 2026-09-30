package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin"
	"github.com/mattermost/mattermost/server/public/pluginapi"
	"github.com/mattermost/mattermost/server/public/pluginapi/cluster"
	"github.com/pkg/errors"
	"github.com/wiloris/photon-quic-bridge/server/command"
	"github.com/wiloris/photon-quic-bridge/server/store/kvstore"
)

// Plugin implements the interface expected by the Mattermost server to communicate between the
// server and plugin processes.
type Plugin struct {
	plugin.MattermostPlugin

	// kvstore is the client used to read/write KV records for this plugin.
	kvstore kvstore.KVStore

	// googleVerifier — проверяющий Google ID token, пересобираемый при
	// смене GoogleClientID (ADR-078 §2.2).
	googleVerifier verifierCache

	// client is the Mattermost server API client.
	client *pluginapi.Client

	// commandClient is the client used to register and execute slash commands.
	commandClient command.Command

	// router is the HTTP router for handling API requests.
	router *mux.Router

	// backgroundJob — фоновый джоб планировщика.
	backgroundJob *cluster.Job

	// configurationLock synchronizes access to the configuration.
	configurationLock sync.RWMutex

	// configuration is the active plugin configuration.
	configuration *configuration

	// ── Photon QUIC Bridge ────────────────────────────────────────────────────

	// quicListener управляет QUIC-сервером на порту :4443.
	quicListener *QuicListener

	// wg позволяет дождаться завершения QUIC-горутины при деактивации.
	wg sync.WaitGroup

	// dmStore — прямое чтение Posts для личных сообщений v2 (ADR-092 ред. 3,
	// dm_sql.go). nil — v2 выключен, причина — dmStoreErr; 213/214 при этом
	// работают. Пишется один раз в OnActivate до начала обслуживания.
	dmStore    dmStore
	dmStoreErr error
}

// OnActivate is invoked when the plugin is activated.
func (p *Plugin) OnActivate() error {
	p.client = pluginapi.NewClient(p.API, p.Driver)
	p.initDMv2()
	p.kvstore = kvstore.NewKVStore(p.client)
	p.commandClient = command.NewCommandHandler(p.client)
	p.router = p.initRouter()

	job, err := cluster.Schedule(
		p.API,
		"BackgroundJob",
		cluster.MakeWaitForRoundedInterval(1*time.Hour),
		p.runJob,
	)
	if err != nil {
		return errors.Wrap(err, "failed to schedule background job")
	}
	p.backgroundJob = job

	// ── Photon QUIC Bridge — запуск ───────────────────────────────────────────
	// Версия — в первой же строке Photon в логе. После переустановки бандла
	// иначе нечем отличить работающую сборку от предыдущей: id плагина не
	// меняется, а System Console показывает версию из манифеста, но не то,
	// что реально загрузилось в процесс.
	p.API.LogInfo("[Photon] Initialising QUIC bridge", "version", manifest.Version)

	cfg := p.API.GetConfig()
	tlsCfg, err := getTLSConfig(p, cfg)
	if err != nil {
		// F7: раньше здесь возвращался nil — плагин считался активированным,
		// System Console показывал его включённым, а quicListener оставался
		// nil. Первое же сообщение любого пользователя роняло процесс плагина
		// на разыменовании в хуках. Симптом («падает при отправке сообщений»)
		// отстоит от причины («не нашёлся сертификат») настолько, что искать
		// будут не там. Отказ активации виден сразу и в нужном месте.
		p.API.LogError("[Photon] Failed to build TLS config — activation aborted",
			"error", err.Error())
		return errors.Wrap(err, "photon: failed to build TLS config")
	}

	dispatcher := NewDispatcher(p)
	p.quicListener = NewQuicListener(p, tlsCfg, dispatcher)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.quicListener.Serve()
	}()

	p.API.LogInfo("[Photon] Plugin activated successfully",
		"version", manifest.Version, "quic_addr", quicBindAddr)
	return nil
}

// OnDeactivate is invoked when the plugin is deactivated.
func (p *Plugin) OnDeactivate() error {
	// ── Photon QUIC Bridge — остановка ────────────────────────────────────────
	if p.quicListener != nil {
		p.API.LogInfo("[Photon] Shutting down QUIC bridge")
		p.quicListener.Shutdown()
		p.wg.Wait()
		p.API.LogInfo("[Photon] QUIC bridge stopped")
	}

	// Останавливаем фоновый джоб.
	if p.backgroundJob != nil {
		if err := p.backgroundJob.Close(); err != nil {
			p.API.LogError("[Photon] Failed to close background job", "error", err)
		}
	}

	return nil
}

// ExecuteCommand executes slash commands registered in NewCommandHandler.
func (p *Plugin) ExecuteCommand(c *plugin.Context, args *model.CommandArgs) (*model.CommandResponse, *model.AppError) {
	response, err := p.commandClient.Handle(args)
	if err != nil {
		return nil, model.NewAppError(
			"ExecuteCommand",
			"plugin.command.execute_command.app_error",
			nil,
			err.Error(),
			http.StatusInternalServerError,
		)
	}
	return response, nil
}
