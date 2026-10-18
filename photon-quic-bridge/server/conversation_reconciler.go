package main

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Фоновая сверка разговоров — запуск методов conversation_reconcile.go
// (contract §3.6, §4.5; ADR-101 §5.10, §5.11).
//
// Без неё брошенный резерв 930 — неопределённый исход CreatePost, смерть
// процесса плагина между резервом и итогом — оставался reserved навсегда, а
// самая ранняя reserved-позиция — барьер 931 для ВСЕХ читателей разговора.
// Живой прогон M1 (2026-10-18) упёрся ровно в это.
//
// Кто и когда запускает проход и порог — по контракту эксплуатационный
// параметр (§4.5, §7: Q2), поэтому оба — настройки плагина. Один исполнитель
// на кластер — cluster.Schedule (мьютекс в KV), запуск заново при каждой
// активации: перезапуск Bridge сверку не теряет, состояние резервов — в БД.

const (
	convReconcileIntervalDefault = 60 * time.Second
	convReconcileIntervalMin     = 10 * time.Second
	convReconcileAbandonDefault  = 5 * time.Minute
	// Порог не меньше двух бюджетов вызова 930: владелец, ещё ждущий
	// CreatePost, не должен увидеть свой резерв закрытым в норме.
	convReconcileAbandonMin = 2 * convCallTimeout
	// convReconcileBatch — разговоров за один проход; остальные — следующим
	// проходом, по кругу.
	convReconcileBatch = 100
	// convReconcileBudget — предел одного прохода.
	convReconcileBudget = 5 * time.Minute
)

// convReconcileSettings — период и порог из конфигурации; ноль — значение
// по умолчанию, меньше минимума — минимум.
func convReconcileSettings(c *configuration) (interval, abandon time.Duration) {
	interval, abandon = convReconcileIntervalDefault, convReconcileAbandonDefault
	if c != nil && c.ConvReconcileIntervalSec > 0 {
		interval = time.Duration(c.ConvReconcileIntervalSec) * time.Second
	}
	if c != nil && c.ConvReconcileAbandonSec > 0 {
		abandon = time.Duration(c.ConvReconcileAbandonSec) * time.Second
	}
	if interval < convReconcileIntervalMin {
		interval = convReconcileIntervalMin
	}
	if abandon < convReconcileAbandonMin {
		abandon = convReconcileAbandonMin
	}
	return interval, abandon
}

// convReconciler — проход по кругу: курсор помнит, где остановился прошлый
// проход; выборка короче пакета — круг замкнулся, следующий проход с начала.
type convReconciler struct {
	svc     *convService
	abandon func() time.Duration
	batch   int
	logInfo func(msg string, keyValuePairs ...any)
	logErr  func(msg string, keyValuePairs ...any)

	mu     sync.Mutex
	cursor string
}

// run — один проход. Вызовы не перекрываются (mu): cluster.Schedule их и
// так не перекрывает, но тесты и ручной запуск — могут.
func (r *convReconciler) run(ctx context.Context) convReconcilePass {
	r.mu.Lock()
	defer r.mu.Unlock()
	before := r.svc.now() - r.abandon().Milliseconds()
	pass, err := r.svc.reconcileOpenAfter(ctx, before, r.batch, r.cursor)
	if pass.Selected < r.batch {
		r.cursor = ""
	} else {
		r.cursor = pass.Last
	}
	if err != nil && r.logErr != nil {
		r.logErr("[Photon][Conv] reconcile pass incomplete", "selected", pass.Selected, "done", pass.Done, "error", err.Error())
	}
	if (pass.EventsWritten > 0 || pass.Published > 0 || pass.Aborted > 0) && r.logInfo != nil {
		r.logInfo("[Photon][Conv] reconcile pass", "events_written", pass.EventsWritten,
			"published", pass.Published, "aborted", pass.Aborted, "conversations", pass.Done)
	}
	return pass
}

// startConvReconciler — при активации, если разговоры включены.
func (p *Plugin) startConvReconciler(svc *convService) {
	if svc == nil {
		return
	}
	interval, abandon := convReconcileSettings(p.getConfiguration())
	r := &convReconciler{
		svc: svc,
		abandon: func() time.Duration {
			_, a := convReconcileSettings(p.getConfiguration())
			return a
		},
		batch:   convReconcileBatch,
		logInfo: p.API.LogInfo,
		logErr:  p.API.LogError,
	}
	// Деактивация отменяет идущий проход: Job.Close ждёт конца обратного
	// вызова, и без отмены OnDeactivate держался бы до convReconcileBudget.
	// Прерванный проход безопасен — каждый переход условный, транзакция
	// откатывается, следующий проход начнёт заново.
	base, stop := context.WithCancel(context.Background())
	job, err := scheduleClusterJob(p, "PhotonConvReconcile", interval, func() {
		ctx, cancel := context.WithTimeout(base, convReconcileBudget)
		defer cancel()
		r.run(ctx)
	})
	if err != nil {
		stop()
		// Разговоры работают, но брошенные резервы закрываться не будут —
		// это должно быть видно сразу.
		p.API.LogError("[Photon][Conv] reconcile not scheduled", "error", err.Error())
		return
	}
	p.convReconcileJob = job
	p.convReconcileStop = stop
	p.API.LogInfo("[Photon][Conv] reconcile scheduled", "interval", interval.String(), "abandon_after", abandon.String())
}

// settingSeconds — число секунд из настройки плагина. System Console может
// сохранить значение поля "number" и числом, и строкой; LoadPluginConfiguration
// разбирает JSON целиком, и неподходящий тип поля молча оставил бы его
// нулём. Принимаются целое число и строка с целым числом; прочее — 0
// (значение по умолчанию), без ошибки разбора: ошибка не должна задевать
// остальные настройки.
type settingSeconds int

func (s *settingSeconds) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		*s = 0
		return nil
	}
	switch x := v.(type) {
	case float64:
		if x >= 0 && x <= math.MaxInt32 {
			*s = settingSeconds(x)
			return nil
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(x)); err == nil && n >= 0 {
			*s = settingSeconds(n)
			return nil
		}
	}
	*s = 0
	return nil
}
