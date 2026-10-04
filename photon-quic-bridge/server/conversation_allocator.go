package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/mattermost/mattermost/server/public/model"
)

// Распределитель носителей по служебным командам — conversation-storage.md
// §6.0. Главное правило: своей Bridge считает только команду, которую вернул
// успешный CreateTeam; найденную по зарезервированному имени команду не
// принимает никогда (§6.0.5).

// carrierCapacity — §6.0.6. unlimited=true — предел снят (MaxChannelsPerTeam
// < 0). Значение читается при каждом выборе: администратор может его менять.
func (s *convService) carrierCapacity() (capacity int64, unlimited bool, err error) {
	cfg := s.mm.GetConfig()
	if cfg == nil || cfg.TeamSettings.MaxChannelsPerTeam == nil {
		return 0, false, fmt.Errorf("%w: нет TeamSettings.MaxChannelsPerTeam", errConvNoCapacity)
	}
	maxCh := *cfg.TeamSettings.MaxChannelsPerTeam
	switch {
	case maxCh < 0:
		return 0, true, nil
	case maxCh <= 2:
		return 0, false, fmt.Errorf("%w: MaxChannelsPerTeam = %d", errConvNoCapacity, maxCh)
	}
	return maxCh - 2, false, nil
}

// pickCarrierTeam — §6.0.8, внутри транзакции резерва носителя: команда
// created с местом, FOR UPDATE. "" — подходящей нет.
func pickCarrierTeam(ctx context.Context, tx *sql.Tx, capacity int64, unlimited bool) (string, error) {
	var name string
	err := tx.QueryRowContext(ctx, `SELECT team_name FROM engram_service_team
WHERE state = 'created' AND ($1 OR carriers < $2)
ORDER BY created_at, team_name LIMIT 1 FOR UPDATE`, unlimited, capacity).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return name, err
}

// hasCarrierTeam — есть ли команда с местом (без блокировки строк).
func (s *convService) hasCarrierTeam(ctx context.Context, capacity int64, unlimited bool) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM engram_service_team
WHERE state = 'created' AND ($1 OR carriers < $2)`, unlimited, capacity).Scan(&n)
	return n > 0, err
}

// maxProvisionNames — сколько занятых имён один вызов пропускает подряд,
// прежде чем ответить temporarily_unavailable (защита от бесконечного цикла).
const maxProvisionNames = 16

// ensureCarrierTeam — есть команда с местом или она создаётся (§6.0.1, 6.0.4).
func (s *convService) ensureCarrierTeam(ctx context.Context) error {
	capacity, unlimited, err := s.carrierCapacity()
	if err != nil {
		return err
	}
	if ok, err := s.hasCarrierTeam(ctx, capacity, unlimited); err != nil || ok {
		return err
	}
	if s.botID == "" {
		return errConvNoBot
	}
	release, err := s.holdLock(ctx, carrierTeamsLockKey)
	if err != nil {
		return err
	}
	defer release()
	for i := 0; i < maxProvisionNames; i++ {
		// Под блокировкой: другой исполнитель мог создать команду, пока мы ждали.
		if ok, err := s.hasCarrierTeam(ctx, capacity, unlimited); err != nil || ok {
			return err
		}
		err := s.provisionCarrierTeam(ctx)
		if errors.Is(err, errConvRetryNextName) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: %d занятых имён подряд", errConvUnavailable, maxProvisionNames)
}

// provisionCarrierTeam — один проход P1–P4 (§6.0.4) под блокировкой
// photon:carrier-teams. errConvRetryNextName — имя занято, строка переведена
// в full, нужен следующий N.
func (s *convService) provisionCarrierTeam(ctx context.Context) error {
	// P1 — продолжить незавершённую строку или зарезервировать следующее имя.
	var name string
	var attempted sql.NullInt64
	var ownTeamID sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT team_name, create_attempted_at, team_id FROM engram_service_team
WHERE state = 'reserved' ORDER BY created_at LIMIT 1`).Scan(&name, &attempted, &ownTeamID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		var maxN sql.NullInt64
		if err := s.db.QueryRowContext(ctx, `SELECT max(substring(team_name FROM '^engram-carriers-([0-9]+)$')::bigint)
FROM engram_service_team`).Scan(&maxN); err != nil {
			return err
		}
		name = fmt.Sprintf("%s%d", carrierTeamPrefix, maxN.Int64+1)
		if _, err := s.db.ExecContext(ctx, `INSERT INTO engram_service_team (team_name, state, created_at)
VALUES ($1, 'reserved', $2)`, name, s.now()); err != nil {
			return err
		}
	case err != nil:
		return err
	}

	// Своя команда: team_id вернул успешный CreateTeam этой строки, P3 не
	// завершён. Это продолжение, а не приём найденной команды (§6.0.5).
	if ownTeamID.Valid {
		return s.finishCarrierTeam(ctx, name, ownTeamID.String)
	}

	if !attempted.Valid {
		// P1 — имя должно быть свободно: существующая команда (любая, в том
		// числе архивная) занимает N и Bridge не принимается.
		found, err := s.teamExists(name)
		if err != nil {
			return err
		}
		if found {
			return s.markTeamNameTaken(ctx, name)
		}
		// P2, первый вызов: отметка попытки — до вызова.
		if _, err := s.db.ExecContext(ctx, `UPDATE engram_service_team SET create_attempted_at = $2
WHERE team_name = $1 AND state = 'reserved' AND create_attempted_at IS NULL`, name, s.now()); err != nil {
			return err
		}
	} else {
		// P2, повтор после неопределённого исхода: найденная команда не
		// принимается (§6.0.5), даже если её создал наш прежний вызов.
		found, err := s.teamExists(name)
		if err != nil {
			return err
		}
		if found {
			return s.markTeamNameTaken(ctx, name)
		}
	}
	team, appErr := s.mm.CreateTeam(&model.Team{
		Name:            name,
		DisplayName:     "ENGRAM carriers",
		Type:            model.TeamInvite,
		AllowOpenInvite: false,
	})
	switch {
	case appErr == nil && team != nil:
	case appErr != nil && appErr.Id == mmErrTeamNameExists:
		// Конфликт — команда уже существует; Bridge её не принимает.
		return s.markTeamNameTaken(ctx, name)
	default:
		// Неопределённый исход: строка остаётся reserved с отметкой попытки.
		return fmt.Errorf("%w: CreateTeam %s: %v", errConvUnavailable, name, appErr)
	}
	// team_id успешного CreateTeam — сразу в строку (§6.0.4 P2): сбой P3
	// дальше не превратит свою команду в «найденную».
	if _, err := s.db.ExecContext(ctx, `UPDATE engram_service_team SET team_id = $2
WHERE team_name = $1 AND state = 'reserved' AND team_id IS NULL`, name, team.Id); err != nil {
		return err
	}
	return s.finishCarrierTeam(ctx, name, team.Id)
}

// finishCarrierTeam — P3 и P4 для своей команды (team_id из своей строки).
func (s *convService) finishCarrierTeam(ctx context.Context, name, teamID string) error {
	// P3 — бот по team_id своей строки; GetTeamByName не используется.
	if _, appErr := s.mm.CreateTeamMember(teamID, s.botID); appErr != nil {
		if appErr.StatusCode == http.StatusNotFound {
			// Своя команда исчезла вне Bridge — новых носителей не примет.
			if _, err := s.db.ExecContext(ctx, `UPDATE engram_service_team SET state = 'full'
WHERE team_name = $1 AND state = 'reserved'`, name); err != nil {
				return err
			}
			s.logger("[Photon][Conv] carrier team disappeared before bot joined", "team_name", name)
			return errConvRetryNextName
		}
		return fmt.Errorf("%w: бот в команду %s: %v", errConvUnavailable, name, appErr)
	}
	// P4 — подтверждение.
	if _, err := s.db.ExecContext(ctx, `UPDATE engram_service_team SET state = 'created', team_id = $2
WHERE team_name = $1 AND state = 'reserved'`, name, teamID); err != nil {
		return err
	}
	s.logger("[Photon][Conv] carrier team created", "team_name", name)
	return nil
}

// teamExists — GetTeamByName: найдена (любая, в том числе архивная) / нет.
// Иной сбой — неопределённость, а не «свободно».
func (s *convService) teamExists(name string) (bool, error) {
	t, appErr := s.mm.GetTeamByName(name)
	if appErr == nil && t != nil {
		return true, nil
	}
	if appErr != nil && appErr.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("%w: GetTeamByName %s: %v", errConvUnavailable, name, appErr)
}

func (s *convService) markTeamNameTaken(ctx context.Context, name string) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE engram_service_team SET state = 'full'
WHERE team_name = $1 AND state = 'reserved'`, name); err != nil {
		return err
	}
	s.logger("[Photon][Conv] carrier team name taken, skipped", "team_name", name)
	return errConvRetryNextName
}

// markTeamFull — created → full (§6.0.3): исчерпан предел.
func (s *convService) markTeamFull(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE engram_service_team SET state = 'full'
WHERE team_name = $1 AND state = 'created'`, name)
	return err
}
