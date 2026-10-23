package permissions

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Три состояния прав держатся раздельно с самого начала
// (docs/manifest.md, docs/threat-model.md):
//
//	requested  — что попросил плагин (манифест, недоверенный вход)
//	granted    — что разрешила политика
//	effective  — что разрешено конкретному запросу сейчас
//
// requested живёт в manifest и сюда не импортируется: policy принимает
// уже разобранный список строк. Отсутствие зависимости
// permissions -> manifest намеренно — оно позволяет выдавать права из
// других источников (решение пользователя, trust policy), не трогая
// манифест вообще.
//
// granted не обязан быть отдельной хранимой структурой: это результат
// вычисления политики. Важно, что архитектурно понятия не смешаны.

// Permission — capability-строка вида "домен.действие".
//
// Соответствует ports.Permission из photon-core. Отдельный тип здесь не
// заводится: контракт заморожен (ADR-020), и дублирование типа привело
// бы к конвертациям на каждой границе.

// Decision — результат вычисления политики для одной пары
// (субъект, право).
type Decision int

const (
	// DecisionDeny — право не выдано. Значение по умолчанию: политика,
	// не сказавшая ничего, означает запрет, а не разрешение.
	DecisionDeny Decision = iota

	// DecisionAllow — право выдано.
	DecisionAllow
)

func (d Decision) String() string {
	if d == DecisionAllow {
		return "ALLOW"
	}
	return "DENY"
}

// Subject — кому выданы права.
//
// Идентифицируется PluginID, а не самозаявленным значением из запроса:
// Runtime знает, какой процесс находится на конкретном IPC-канале,
// потому что сам его запустил (docs/ipc.md, раздел 2).
type Subject struct {
	PluginID string
}

var (
	// ErrUnknownSubject — политика для этого субъекта не загружена.
	//
	// Это техническая ошибка, а не отказ в доступе: разница
	// принципиальна. "Политика не загружена" и "право не выдано" —
	// разные ситуации, и смешивать их означало бы позволить сбою
	// загрузки выглядеть как обычный DENY (та же логика, что в
	// ports.Authorizer: (false, nil) против error).
	ErrUnknownSubject = errors.New("permissions: no policy loaded for subject")

	// ErrInvalidPermission — синтаксически некорректное имя права.
	ErrInvalidPermission = errors.New("permissions: invalid permission name")
)

// Policy хранит и вычисляет решения по правам.
//
// Потокобезопасна: Authorize вызывается из обработчиков IPC-запросов,
// которые могут выполняться параллельно.
type Policy struct {
	mu sync.RWMutex
	// granted[pluginID][permission] — выданные права.
	//
	// Вложенная карта, а не общий набор строк: у каждого субъекта
	// СВОЙ набор. Глобальный набор означал бы, что право, выданное
	// одному плагину, доступно всем — ровно та ошибка, ради проверки
	// которой написан TestIsolation_PluginsDoNotShareGrants.
	granted map[string]map[string]bool
}

func NewPolicy() *Policy {
	return &Policy{granted: make(map[string]map[string]bool)}
}

// Grant выдаёт субъекту перечисленные права, заменяя предыдущий набор.
//
// Замена, а не добавление: политика описывает полное состояние прав
// субъекта. Инкрементальное добавление потребовало бы отдельного
// механизма отзыва каждого права и делало бы неочевидным, что именно
// выдано в данный момент.
func (p *Policy) Grant(subject Subject, perms []string) error {
	for _, perm := range perms {
		if !isValidPermission(perm) {
			return fmt.Errorf("%w: %q", ErrInvalidPermission, perm)
		}
	}

	set := make(map[string]bool, len(perms))
	for _, perm := range perms {
		set[perm] = true
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.granted[subject.PluginID] = set
	return nil
}

// Revoke отзывает одно право.
//
// Действует немедленно: следующий Authorize уже вернёт DENY. Права не
// кэшируются как "выданные навсегда" — иначе отзыв требовал бы
// перезапуска плагина (docs/ipc.md, раздел 4).
func (p *Policy) Revoke(subject Subject, perm string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if set, ok := p.granted[subject.PluginID]; ok {
		delete(set, perm)
	}
}

// RevokeAll удаляет политику субъекта целиком.
//
// После этого Evaluate вернёт ErrUnknownSubject, а не DENY: субъект без
// загруженной политики и субъект с пустым набором прав — разные
// состояния.
func (p *Policy) RevokeAll(subject Subject) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.granted, subject.PluginID)
}

// Evaluate вычисляет решение для пары (субъект, право).
//
// Проверка ТОЧНАЯ: наличие "storage.read" не даёт "storage.write".
// Wildcard'ы ("storage.*") не поддерживаются намеренно — они не были
// частью спецификации, а их введение создало бы неявную иерархию прав,
// которую сложно формально проверить и легко неверно понять при
// выдаче.
func (p *Policy) Evaluate(subject Subject, perm string) (Decision, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	set, ok := p.granted[subject.PluginID]
	if !ok {
		return DecisionDeny, fmt.Errorf("%w: %q", ErrUnknownSubject, subject.PluginID)
	}
	if set[perm] {
		return DecisionAllow, nil
	}
	return DecisionDeny, nil
}

// GrantedPermissions возвращает копию выданных прав субъекта — для
// диагностики и UI. Копия, а не ссылка: вызывающий не должен иметь
// возможности изменить политику мимо Grant/Revoke.
func (p *Policy) GrantedPermissions(subject Subject) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	set, ok := p.granted[subject.PluginID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSubject, subject.PluginID)
	}
	out := make([]string, 0, len(set))
	for perm := range set {
		out = append(out, perm)
	}
	return out, nil
}

// EvaluateAll — вспомогательный метод для проверки списка прав разом
// (например, при выдаче прав по манифесту).
func (p *Policy) EvaluateAll(ctx context.Context, subject Subject, perms []string) (map[string]Decision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	out := make(map[string]Decision, len(perms))
	for _, perm := range perms {
		d, err := p.Evaluate(subject, perm)
		if err != nil {
			return nil, err
		}
		out[perm] = d
	}
	return out, nil
}
