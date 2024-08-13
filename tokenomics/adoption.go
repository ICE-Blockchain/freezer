// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	stdlibtime "time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	"github.com/ice-blockchain/freezer/model"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

func (r *repository) GetAdoptionSummary(ctx context.Context, userID string) (as *AdoptionSummary, err error) {
	if as = new(AdoptionSummary); ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "context failed")
	}
	if as.TotalActiveUsers, err = r.db.Get(ctx, r.totalActiveUsersKey(*time.Now().Time)).Uint64(); err != nil && !errors.Is(err, redis.Nil) {
		return nil, errors.Wrap(err, "failed to get current totalActiveUsers")
	}
	as.Milestones = make([]*Adoption[string], 0, r.cfg.Adoption.Milestones)
	id, err := GetOrInitInternalID(ctx, r.db, userID, r.cfg.WelcomeBonusV2Amount)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", userID)
	}
	res, err := storage.Get[struct{ model.CreatedAtField }](ctx, r.db, model.SerializedUsersKey(id))
	if err != nil || len(res) == 0 {
		if err == nil {
			err = errors.Wrapf(ErrRelationNotFound, "missing state for id:%v", id)
		}

		return nil, errors.Wrapf(err, "failed to get GetAdoptionSummary for id:%v", id)
	}
	createdAt := res[0].CreatedAt
	if createdAt.IsNil() {
		createdAt = time.Now()
	}
	for mi := range r.cfg.Adoption.Milestones {
		achievedAt := time.New(createdAt.Add(stdlibtime.Duration(mi) * r.cfg.Adoption.DurationBetweenMilestones))
		as.Milestones = append(as.Milestones, &Adoption[string]{
			AchievedAt:     achievedAt,
			BaseMiningRate: strconv.FormatFloat(BaseMiningRate(achievedAt, createdAt, r.cfg.Adoption.StartingBaseMiningRate, r.cfg.Adoption.Milestones, r.cfg.Adoption.DurationBetweenMilestones), 'f', 20, 64),
			Milestone:      uint64(mi + 1),
		})
	}

	return
}

func (r *repository) totalActiveUsersKey(date stdlibtime.Time) string {
	return fmt.Sprintf("%v:%v", totalActiveUsersGlobalKey, date.Format(r.cfg.globalAggregationIntervalChildDateFormat()))
}

func (r *repository) extractTimeFromTotalActiveUsersKey(key string) *time.Time {
	parseTime, err := stdlibtime.Parse(r.cfg.globalAggregationIntervalChildDateFormat(), strings.ReplaceAll(key, totalActiveUsersGlobalKey+":", ""))
	log.Panic(err)

	return time.New(parseTime)
}

func (r *repository) incrementTotalActiveUsers(ctx context.Context, ms *MiningSession) (err error) { //nolint:funlen // .
	duplGuardKey := ms.duplGuardKey(r, "incr_total_active_users")
	if set, dErr := r.db.SetNX(ctx, duplGuardKey, "", r.cfg.MiningSessionDuration.Min).Result(); dErr != nil || !set {
		if dErr == nil {
			dErr = ErrDuplicate
		}

		return errors.Wrapf(dErr, "SetNX failed for mining_session_dupl_guard, miningSession: %#v", ms)
	}
	defer func() {
		if err != nil {
			undoCtx, cancelUndo := context.WithTimeout(context.Background(), requestDeadline)
			defer cancelUndo()
			err = multierror.Append( //nolint:wrapcheck // .
				err,
				errors.Wrapf(r.db.Del(undoCtx, duplGuardKey).Err(), "failed to del mining_session_dupl_guard key"),
			).ErrorOrNil()
		}
	}()
	keys := ms.detectIncrTotalActiveUsersKeys(r)
	responses, err := r.db.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for _, key := range keys {
			if err = pipeliner.Incr(ctx, key).Err(); err != nil {
				return err
			}
		}

		return nil
	})
	if err == nil {
		errs := make([]error, 0, len(responses))
		for _, response := range responses {
			errs = append(errs, errors.Wrapf(response.Err(), "failed to `%v`", response.FullName()))
		}
		err = multierror.Append(nil, errs...).ErrorOrNil()
	}

	return errors.Wrapf(err, "failed to incr total active users for keys:%#v", keys)
}

func (ms *MiningSession) detectIncrTotalActiveUsersKeys(repo *repository) []string {
	keys := make([]string, 0, int(repo.cfg.MiningSessionDuration.Max/repo.cfg.GlobalAggregationInterval.Child))
	start, end := ms.EndedAt.Add(-ms.Extension), *ms.EndedAt.Time
	if !ms.LastNaturalMiningStartedAt.Equal(*ms.StartedAt.Time) ||
		(!ms.PreviouslyEndedAt.IsNil() &&
			repo.totalActiveUsersKey(*ms.StartedAt.Time) == repo.totalActiveUsersKey(*ms.PreviouslyEndedAt.Time)) {
		start = start.Add(repo.cfg.GlobalAggregationInterval.Child)
	}
	start = start.Truncate(repo.cfg.GlobalAggregationInterval.Child)
	end = end.Truncate(repo.cfg.GlobalAggregationInterval.Child)
	for start.Before(end) {
		keys = append(keys, repo.totalActiveUsersKey(start))
		start = start.Add(repo.cfg.GlobalAggregationInterval.Child)
	}
	if ms.PreviouslyEndedAt.IsNil() || repo.totalActiveUsersKey(end) != repo.totalActiveUsersKey(*ms.PreviouslyEndedAt.Time) {
		keys = append(keys, repo.totalActiveUsersKey(end))
	}

	return keys
}

func (c *Config) BaseMiningRate(now, createdAt *time.Time) float64 {
	return BaseMiningRate(now, createdAt, c.Adoption.StartingBaseMiningRate, c.Adoption.Milestones, c.Adoption.DurationBetweenMilestones)
}

func BaseMiningRate(now, createdAt *time.Time, startingBaseMiningRate float64, milestones uint8, durationBetweenMilestones stdlibtime.Duration) float64 {
	if createdAt.IsNil() || createdAt.Equal(*now.Time) || createdAt.After(*now.Time) {
		return startingBaseMiningRate
	}

	return startingBaseMiningRate / (math.Pow(2, math.Min(float64(milestones-1), float64(now.Sub(*createdAt.Time)/durationBetweenMilestones))))
}
