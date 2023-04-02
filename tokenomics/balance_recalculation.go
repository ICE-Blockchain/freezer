// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	stdlibtime "time"

	"github.com/cenkalti/backoff/v4"
	"github.com/goccy/go-json"
	"github.com/pkg/errors"

	"github.com/ice-blockchain/eskimo/users"
	"github.com/ice-blockchain/wintr/coin"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v2"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

func (r *repository) initializeBalanceRecalculationWorker(ctx context.Context, usr *users.User) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}

	return errors.Wrapf(retry(ctx, func() error {
		if err := r.initializeWorker(ctx, "balance_recalculation_worker", usr.ID, usr.HashCode); err != nil {
			if errors.Is(err, storage.ErrRelationNotFound) {
				return err
			}

			return errors.Wrapf(backoff.Permanent(err), "failed to initializeBalanceRecalculationWorker for userID:%v", usr.ID)
		}

		return nil
	}), "permanently failed to initializeBalanceRecalculationWorker for userID:%v", usr.ID)
}

func (s *balanceRecalculationTriggerStreamSource) start(ctx context.Context) {
	log.Info("balanceRecalculationTriggerStreamSource started")
	defer log.Info("balanceRecalculationTriggerStreamSource stopped")
	workerIndexes := make([]int16, s.cfg.WorkerCount) //nolint:makezero // Intended.
	for i := 0; i < int(s.cfg.WorkerCount); i++ {
		workerIndexes[i] = int16(i)
	}
	for ctx.Err() == nil {
		stdlibtime.Sleep(balanceCalculationProcessingSeedingStreamEmitFrequency)
		before := time.Now()
		log.Error(errors.Wrap(executeBatchConcurrently(ctx, s.process, workerIndexes), "failed to executeBatchConcurrently[balanceRecalculationTriggerStreamSource.process]")) //nolint:lll // .
		log.Info(fmt.Sprintf("balanceRecalculationTriggerStreamSource.process took: %v", stdlibtime.Since(*before.Time)))
	}
}

func (s *balanceRecalculationTriggerStreamSource) process(ignoredCtx context.Context, workerIndex int16) (err error) {
	if ignoredCtx.Err() != nil {
		return errors.Wrap(ignoredCtx.Err(), "unexpected deadline while processing message")
	}
	const deadline = 5 * stdlibtime.Minute
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	now := time.Now()
	before := time.Now()
	batch, err := s.getLatestBalancesNewBatch(ctx, now, workerIndex) //nolint:contextcheck // Intended.
	log.Info(fmt.Sprintf("balanceRecalculationTriggerStreamSource.getLatestBalancesNewBatch[%v] took: %v", workerIndex, stdlibtime.Since(*before.Time)))
	if err != nil || len(batch) == 0 {
		return errors.Wrapf(err, "failed to getLatestBalancesNewBatch for workerIndex:%v,time:%v", workerIndex, now)
	}
	if err = s.updateBalances(ctx, now, workerIndex, batch); err != nil { //nolint:contextcheck // Intended.
		return errors.Wrapf(err, "failed to updateBalances for workerIndex:%v,time:%v,batch:%#v", workerIndex, now, batch)
	}

	return nil
}

type (
	BalanceRecalculationDetails struct {
		LastNaturalMiningStartedAt, LastMiningStartedAt, T0LastMiningStartedAt, TMinus1LastMiningStartedAt,
		LastMiningEndedAt, T0LastMiningEndedAt, TMinus1LastMiningEndedAt,
		PreviousMiningEndedAt, T0PreviousMiningEndedAt, TMinus1PreviousMiningEndedAt,
		RollbackUsedAt, T0RollbackUsedAt, TMinus1RollbackUsedAt *time.Time
		BaseMiningRate                         *coin.ICEFlake
		UUserID, T0UserID, TMinus1UserID       string
		T0, T1, T2, ExtraBonus                 uint64
		UHashCode, T0HashCode, TMinus1HashCode int64
	}
	B                       = balance
	balanceRecalculationRow struct {
		*B
		*BalanceRecalculationDetails
	}
)

func (s *balanceRecalculationTriggerStreamSource) getLatestBalancesNewBatch( //nolint:funlen // Big SQL.
	ctx context.Context, now *time.Time, workerIndex int16,
) ([]*balanceRecalculationRow, error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "unexpected deadline while processing message")
	}
	sql := fmt.Sprintf(`
SELECT  b.updated_at,
    	b.amount,
    	coalesce(b.user_id,'') AS user_id,
    	coalesce(b.type_detail,'') AS type_detail,
    	coalesce(b.hash_code,0) AS hash_code,
    	coalesce(b.worker_index,0) AS worker_index ,
    	coalesce(b.type,0) AS type,
    	coalesce(b.negative,false) AS negative,
	   u.last_natural_mining_started_at,
	   u.last_mining_started_at,
	   t0.last_mining_started_at AS t0_last_mining_started_at,
	   tminus1.last_mining_started_at AS t_minus1_last_mining_started_at,
	   u.last_mining_ended_at,
	   t0.last_mining_ended_at AS t0_last_mining_ended_at,
	   tminus1.last_mining_ended_at AS t_minus1_last_mining_ended_at,
	   u.previous_mining_ended_at,
	   t0.previous_mining_ended_at AS t0_previous_mining_ended_at,
	   tminus1.previous_mining_ended_at AS t_minus1_previous_mining_ended_at,
	   u.rollback_used_at,
	   t0.rollback_used_at AS t0_rollback_used_at,
	   tminus1.rollback_used_at AS t_minus1_rollback_used_at,
	   current_adoption.base_mining_rate,
	   u.user_id AS u_user_id,
	   coalesce(t0.user_id,'') AS t0_user_id,
	   coalesce(tminus1.user_id,'') AS t_minus1_user_id,
	   (CASE 
	   		WHEN 1 = 1
	   			 AND t0.last_mining_ended_at IS NOT NULL 
	   			 AND t0.last_mining_ended_at  > $1 
		   				THEN 1
		    ELSE 0 
	   END) AS t0,
	   coalesce(ar_worker.t1,0) AS t1,
	   coalesce(ar_worker.t2,0) AS t2,
	   (CASE WHEN coalesce(eb_worker.extra_bonus_ended_at, '1999-01-08 04:05:06'::timestamp) > $1 THEN eb_worker.extra_bonus ELSE 0 END) AS extra_bonus,
	   coalesce(u.hash_code,0) AS u_hash_code,	
	   coalesce(t0.hash_code,0) AS t0_hash_code,	
	   coalesce(tminus1.hash_code,0) AS t_minus1_hash_code
FROM ( SELECT user_id
	   FROM balance_recalculation_worker
	   WHERE worker_index = $3 AND enabled = TRUE
	   ORDER BY last_iteration_finished_at
	   LIMIT $2 ) x
		JOIN (%[1]v) current_adoption 
		  ON 1=1
	    JOIN users u
		  ON u.user_id = x.user_id
   LEFT JOIN extra_bonus_processing_worker eb_worker
		  ON eb_worker.worker_index = $3
		 AND eb_worker.user_id = x.user_id
   LEFT JOIN active_referrals ar_worker
		  ON ar_worker.worker_index = $3
		 AND ar_worker.user_id = x.user_id
   LEFT	JOIN users t0
	  	  ON t0.user_id = u.referred_by
         AND t0.user_id != x.user_id
   LEFT JOIN users tminus1
	  	  ON tminus1.user_id = t0.referred_by
         AND tminus1.user_id != x.user_id
   LEFT JOIN balances_worker b
		  ON b.worker_index = $3
		 AND b.user_id = u.user_id
	     AND POSITION('@' IN b.type_detail) = 0
	     AND (CASE 
	     		WHEN POSITION('/' IN b.type_detail) = 1 AND POSITION('&' IN b.type_detail) = 0
	              THEN b.type_detail = ANY($4)
             	ELSE 1 = 1
              END)`, currentAdoptionSQL())
	args := append(make([]any, 0, 4), //nolint:gomnd // There are 4 elements.
		*now.Time,
		balanceRecalculationBatchSize,
		workerIndex,
		[]string{
			fmt.Sprintf("/%v", now.Add(-1*s.cfg.GlobalAggregationInterval.Child).Format(s.cfg.globalAggregationIntervalChildDateFormat())),
			fmt.Sprintf("/%v", now.Format(s.cfg.globalAggregationIntervalChildDateFormat())),
			fmt.Sprintf("/%v", now.Add(s.cfg.GlobalAggregationInterval.Child).Format(s.cfg.globalAggregationIntervalChildDateFormat())),
		})
	resp, err := storage.Select[balanceRecalculationRow](ctx, s.db, sql, args...)

	return resp, errors.Wrapf(err, "failed to select new balance recalculation batch for workerIndex:%v,now:%#v", workerIndex, now)
}

func (s *balanceRecalculationTriggerStreamSource) updateBalances(
	ctx context.Context, now *time.Time, workerIndex int16, batch []*balanceRecalculationRow,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "context failed")
	}
	balancesForReplace, balancesForDelete, processingStoppedForUserIDs, dayOffStartedEvents, userIDs, usersThatStoppedMining := s.recalculateBalances(now, workerIndex, batch) //nolint:lll // .
	if err := s.decrementActiveReferralCountForT0AndTMinus1(ctx, usersThatStoppedMining...); err != nil {
		return errors.Wrapf(err, "failed to decrementActiveReferralCountForT0AndTMinus1 for:%#v", usersThatStoppedMining)
	}
	if err := executeBatchConcurrently(ctx, s.sendFreeMiningSessionStartedMessage, dayOffStartedEvents); err != nil {
		return errors.Wrapf(err, "failed to executeBatchConcurrently[sendFreeMiningSessionStartedMessage] for dayOffStartedEvents:%#v", dayOffStartedEvents)
	}
	if err := s.replaceBalances(ctx, nil, workerIndex, now, balancesForReplace...); err != nil {
		return errors.Wrapf(err, "failed to replaceBalances: %#v", balancesForReplace)
	}
	if err := s.deleteBalances(ctx, workerIndex, balancesForDelete...); err != nil {
		return errors.Wrapf(err, "failed to deleteBalances: %#v", balancesForDelete)
	}
	if err := s.updateLastIterationFinishedAt(ctx, workerIndex, userIDs); err != nil {
		return errors.Wrapf(err, "failed to updateLastIterationFinishedAt, workerIndex:%v,userIDs:%#v", workerIndex, userIDs)
	}
	if err := s.stopWorkerForUsers(ctx, workerIndex, processingStoppedForUserIDs); err != nil {
		return errors.Wrapf(err, "failed to stopWorkerForUsers, workerIndex:%v,userIDs:%#v", workerIndex, processingStoppedForUserIDs)
	}

	return nil
}

//nolint:funlen,gocognit,gocritic,gocyclo,revive,cyclop,maintidx // .
func (s *balanceRecalculationTriggerStreamSource) recalculateBalances(
	now *time.Time, workerIndex int16, rows []*balanceRecalculationRow,
) (balancesForReplace, balancesForDelete []*balance, processingStoppedForUserIDs map[string]*time.Time, dayOffStartedEvents []*FreeMiningSessionStarted, userIDs []string, usersThatStoppedMining []*userThatStoppedMining) { //nolint:lll // .
	balancesForReplace = make([]*balance, 0, len(rows))
	balancesForDelete = make([]*balance, 0, 0) //nolint:gosimple // Nope.
	processingStoppedForUserIDs = make(map[string]*time.Time)
	dayOffStartedEvents = make([]*FreeMiningSessionStarted, 0, 0) //nolint:gosimple // Nope.
	userIDs = make([]string, 0, len(rows))
	usersThatStoppedMining = make([]*userThatStoppedMining, 0, len(rows))
	var (
		thisDurationTypeDetail                = fmt.Sprintf("/%v", now.Format(s.cfg.globalAggregationIntervalChildDateFormat()))
		untilThisDurationTypeDetail           = fmt.Sprintf("@%v", now.Format(s.cfg.globalAggregationIntervalChildDateFormat()))
		balancesPerUser                       = make(map[string]map[string]*balance, balanceRecalculationBatchSize)
		aggregatedPendingTotalBalancesPerUser = make(map[string]map[bool]*balance, balanceRecalculationBatchSize)
		aggregatedPendingT1BalancesPerUser    = make(map[string]map[bool]*balance, balanceRecalculationBatchSize)
		aggregatedPendingT2BalancesPerUser    = make(map[string]map[bool]*balance, balanceRecalculationBatchSize)
		balanceRecalculationDetailsPerUser    = make(map[string]*BalanceRecalculationDetails, balanceRecalculationBatchSize)
	)
	for _, row := range rows {
		userID := row.BalanceRecalculationDetails.UUserID
		if _, found := balanceRecalculationDetailsPerUser[userID]; !found {
			balanceRecalculationDetailsPerUser[userID] = row.BalanceRecalculationDetails
		}
		if _, found := balancesPerUser[userID]; !found {
			balancesPerUser[userID] = make(map[string]*balance)
			userIDs = append(userIDs, userID)
		}
		if row.B == nil || row.B.UserID == "" {
			continue
		}
		if row.B.Type == pendingXBalanceType { //nolint:nestif // It's fine.
			switch {
			case strings.HasPrefix(row.B.TypeDetail, t1BalanceTypeDetail):
				if _, found := aggregatedPendingT1BalancesPerUser[userID]; !found {
					aggregatedPendingT1BalancesPerUser[userID] = make(map[bool]*balance, 1+1)
				}
				if existing, found := aggregatedPendingT1BalancesPerUser[userID][row.B.Negative]; !found {
					aggregatedPendingT1BalancesPerUser[userID][row.B.Negative] = row.B
				} else {
					existing.add(row.B.Amount)
				}
			case strings.HasPrefix(row.B.TypeDetail, t2BalanceTypeDetail):
				if _, found := aggregatedPendingT2BalancesPerUser[userID]; !found {
					aggregatedPendingT2BalancesPerUser[userID] = make(map[bool]*balance, 1+1)
				}
				if existing, found := aggregatedPendingT2BalancesPerUser[userID][row.B.Negative]; !found {
					aggregatedPendingT2BalancesPerUser[userID][row.B.Negative] = row.B
				} else {
					existing.add(row.B.Amount)
				}
			case strings.HasPrefix(row.B.TypeDetail, rootBalanceTypeDetail):
				if _, found := aggregatedPendingTotalBalancesPerUser[userID]; !found {
					aggregatedPendingTotalBalancesPerUser[userID] = make(map[bool]*balance, 1+1)
				}
				if existing, found := aggregatedPendingTotalBalancesPerUser[userID][row.Negative]; !found {
					aggregatedPendingTotalBalancesPerUser[userID][row.B.Negative] = row.B
				} else {
					existing.add(row.B.Amount)
				}
			default:
				log.Panic(fmt.Sprintf("unknown typeDetail `%v`", row.B.TypeDetail))
			}
			clone := *row.B
			clone.UpdatedAt = now
			clone.Amount = coin.ZeroICEFlakes()
			balancesForReplace = append(balancesForReplace, &clone)
			balancesForDelete = append(balancesForDelete, &clone)
		} else {
			balancesPerUser[userID][fmt.Sprint(row.B.Negative, row.B.Type, row.B.TypeDetail)] = row.B
		}
	}
	for userID, balancesByPK := range balancesPerUser {
		var (
			details                                                                 = balanceRecalculationDetailsPerUser[userID]
			aggregatedPendingTotalBalances                                          = aggregatedPendingTotalBalancesPerUser[userID]
			aggregatedPendingT1Balances                                             = aggregatedPendingT1BalancesPerUser[userID]
			aggregatedPendingT2Balances                                             = aggregatedPendingT2BalancesPerUser[userID]
			previousDurationTypeDetail, previousElapsedDuration, nowElapsedDuration = s.calculateElapsedDurations(balancesByPK, details, now)
			previousT0ElapsedDuration, nowT0ElapsedDuration                         = s.calculateElapsedT0ReverseDurations(balancesByPK, details, now)
			previousTMinus1ElapsedDuration, nowTMinus1ElapsedDuration               = s.calculateElapsedTMinus1ReverseDurations(balancesByPK, details, now)
		)
		if previousDurationTypeDetail == "" {
			previousDurationTypeDetail = thisDurationTypeDetail
		}
		if dayOffStarted := s.didANewFreeMiningSessionJustStart(balancesByPK, details, now); dayOffStarted != nil {
			dayOffStartedEvents = append(dayOffStartedEvents, dayOffStarted)
		}

		s.processDegradationForTotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.processDegradationForT0TotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.processDegradationForT1TotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.processDegradationForT2TotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.processDegradationForT0ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.processDegradationForTMinus1ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now)

		s.processPreviousIncompleteTotalNoPreStakingBonusBalanceType(balancesByPK, details, now, previousElapsedDuration)
		s.processPreviousIncompleteTMinus1ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now, previousTMinus1ElapsedDuration)
		s.processPreviousIncompleteT0ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now, previousT0ElapsedDuration)
		s.processPreviousIncompleteT0TotalNoPreStakingBonusBalanceType(balancesByPK, details, now, previousElapsedDuration)
		s.processPreviousIncompleteT1TotalNoPreStakingBonusBalanceType(balancesByPK, details, now, previousElapsedDuration)
		s.processPreviousIncompleteT2TotalNoPreStakingBonusBalanceType(balancesByPK, details, now, previousElapsedDuration)
		s.processPreviousIncompleteThisDurationTotalNoPreStakingBonusBalanceType(balancesByPK, details, now, previousElapsedDuration, previousDurationTypeDetail)

		s.rollbackTotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.rollbackTMinus1ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.rollbackT0ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.rollbackT0TotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.rollbackT1TotalNoPreStakingBonusBalanceType(balancesByPK, details, now)
		s.rollbackT2TotalNoPreStakingBonusBalanceType(balancesByPK, details, now)

		s.processTotalNoPreStakingBonusBalanceType(balancesByPK, aggregatedPendingTotalBalances, details, now, nowElapsedDuration)
		s.processTMinus1ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now, nowTMinus1ElapsedDuration)
		s.processT0ReverseTotalNoPreStakingBonusBalanceType(balancesByPK, details, now, nowT0ElapsedDuration)
		s.processT0TotalNoPreStakingBonusBalanceType(balancesByPK, details, now, nowElapsedDuration)
		s.processT1TotalNoPreStakingBonusBalanceType(balancesByPK, aggregatedPendingT1Balances, details, now, nowElapsedDuration)
		s.processT2TotalNoPreStakingBonusBalanceType(balancesByPK, aggregatedPendingT2Balances, details, now, nowElapsedDuration)
		s.processThisDurationTotalNoPreStakingBonusBalanceType(balancesByPK, aggregatedPendingTotalBalances, details, now, nowElapsedDuration, thisDurationTypeDetail)

		s.processTotalNoPreStakingBonusUntilThisDurationBalanceType(balancesByPK, details, untilThisDurationTypeDetail, userID)

		zeroBalancesRequiredToStop := make(map[string]*coin.ICEFlake, 3) //nolint:gomnd // There's only 3, untilThisDuration, revT0, revT-1
		for balPK, bal := range balancesByPK {
			if bal == nil || bal.Amount.IsNil() {
				delete(balancesByPK, balPK)

				continue
			}
			bal.UpdatedAt = now
			if bal.Type == totalNoPreStakingBonusBalanceType &&
				(details.t0Changed(bal.TypeDetail) || details.reverseT0Changed(bal.TypeDetail) || details.reverseTMinus1Changed(bal.TypeDetail)) {
				bal.Amount = coin.ZeroICEFlakes()
			}
			if bal.Type == totalNoPreStakingBonusBalanceType &&
				!bal.Negative &&
				(bal.TypeDetail == untilThisDurationTypeDetail ||
					bal.TypeDetail == details.reverseT0TypeDetail() ||
					bal.TypeDetail == details.reverseTMinus1TypeDetail()) {
				zeroBalancesRequiredToStop[balPK] = bal.Amount
			}
			bal.WorkerIndex = workerIndex
			bal.HashCode = details.UHashCode
			if bal.Amount.IsZero() {
				balancesForDelete = append(balancesForDelete, bal)
			}
			balancesForReplace = append(balancesForReplace, bal)
		}
		shouldStop := true
		for _, bal := range zeroBalancesRequiredToStop {
			if !bal.IsZero() {
				shouldStop = false

				break
			}
		}
		if shouldStop {
			processingStoppedForUserIDs[userID] = details.LastMiningEndedAt
		}
		if details.LastMiningEndedAt.Before(*now.Time) && previousElapsedDuration > 0 {
			usersThatStoppedMining = append(usersThatStoppedMining, &userThatStoppedMining{
				LastMiningEndedAt: details.LastMiningEndedAt,
				UserID:            userID,
				T0UserID:          details.T0UserID,
				TMinus1UserID:     details.TMinus1UserID,
				T0HashCode:        details.T0HashCode,
				TMinus1HashCode:   details.TMinus1HashCode,
			})
		}
	}

	return balancesForReplace, balancesForDelete, processingStoppedForUserIDs, dayOffStartedEvents, userIDs, usersThatStoppedMining
}

func (s *balanceRecalculationTriggerStreamSource) calculateElapsedDurations(
	balancesByPK map[string]*balance, details *BalanceRecalculationDetails, now *time.Time,
) (previousDurationTypeDetail string, previousElapsedDuration, nowElapsedDuration stdlibtime.Duration) {
	totalBalance := s.getOrInitBalance(false, "", details.UUserID, balancesByPK)
	if totalBalance.UpdatedAt == nil {
		return "", 0, now.Sub(*details.LastMiningStartedAt.Time)
	}
	if details.LastMiningEndedAt.Before(*now.Time) && totalBalance.UpdatedAt.Before(*details.LastMiningEndedAt.Time) {
		previousDurationTypeDetail = fmt.Sprintf("/%v", details.LastMiningEndedAt.Format(s.cfg.globalAggregationIntervalChildDateFormat()))
		previousElapsedDuration = details.LastMiningEndedAt.Sub(*totalBalance.UpdatedAt.Time)
		nowElapsedDuration = now.Sub(*details.LastMiningEndedAt.Time)
	}
	if details.PreviousMiningEndedAt != nil &&
		details.PreviousMiningEndedAt.Before(*totalBalance.UpdatedAt.Time) &&
		details.LastMiningEndedAt.After(*now.Time) &&
		details.LastMiningStartedAt.Before(*now.Time) &&
		totalBalance.UpdatedAt.Before(*details.LastMiningStartedAt.Time) {
		previousDurationTypeDetail = fmt.Sprintf("/%v", details.LastMiningStartedAt.Format(s.cfg.globalAggregationIntervalChildDateFormat()))
		previousElapsedDuration = details.LastMiningStartedAt.Sub(*totalBalance.UpdatedAt.Time)
		nowElapsedDuration = now.Sub(*details.LastMiningStartedAt.Time)
	}
	if nowElapsedDuration == 0 {
		nowElapsedDuration = now.Sub(*totalBalance.UpdatedAt.Time)
	}

	return previousDurationTypeDetail, previousElapsedDuration, nowElapsedDuration
}

func (s *balanceRecalculationTriggerStreamSource) didANewFreeMiningSessionJustStart(
	balancesByPK map[string]*balance, details *BalanceRecalculationDetails, now *time.Time,
) *FreeMiningSessionStarted {
	if details.LastMiningEndedAt.Before(*now.Time) {
		return nil
	}
	totalBalance := s.getOrInitBalance(false, "", details.UUserID, balancesByPK)
	if totalBalance.UpdatedAt == nil {
		return nil
	}
	ms := s.calculateMiningSession(now, details.LastNaturalMiningStartedAt, details.LastMiningEndedAt)
	if ms == nil || ms.Free == nil || !*ms.Free || totalBalance.UpdatedAt.After(*ms.StartedAt.Time) {
		return nil
	}

	return &FreeMiningSessionStarted{
		StartedAt:                   ms.StartedAt,
		EndedAt:                     ms.EndedAt,
		UserID:                      details.UUserID,
		ID:                          fmt.Sprint(ms.StartedAt.UnixNano() / s.cfg.MiningSessionDuration.Max.Nanoseconds()),
		RemainingFreeMiningSessions: s.calculateRemainingFreeMiningSessions(now, details.LastMiningEndedAt),
		MiningStreak:                s.calculateMiningStreak(now, details.LastMiningStartedAt, details.LastMiningEndedAt),
	}
}

func (*balanceRecalculationTriggerStreamSource) getOrInitBalance(
	negative bool, typeDetail, userID string, balancesByPK map[string]*balance,
) *balance {
	if val, found := balancesByPK[fmt.Sprint(negative, totalNoPreStakingBonusBalanceType, typeDetail)]; !found {
		val = &balance{
			UserID:     userID,
			TypeDetail: typeDetail,
			Type:       totalNoPreStakingBonusBalanceType,
			Negative:   negative,
		}
		balancesByPK[fmt.Sprint(negative, totalNoPreStakingBonusBalanceType, typeDetail)] = val

		return val
	} else { //nolint:revive // Nope.
		return val
	}
}

func (*balanceRecalculationTriggerStreamSource) getBalance(
	negative bool, typeDetail string, balancesByPK map[string]*balance,
) *balance {
	return balancesByPK[fmt.Sprint(negative, totalNoPreStakingBonusBalanceType, typeDetail)]
}

const (
	degradationPrecision = 1.005
)

//nolint:revive // Not a problem here.
func (r *repository) calculateDegradation(
	elapsedDuration stdlibtime.Duration, referenceAmount *coin.ICEFlake, aggressive bool,
) *coin.ICEFlake {
	if elapsedDuration < 0 {
		return nil
	}

	if aggressive {
		return referenceAmount.
			MultiplyUint64(uint64(float64(elapsedDuration) * degradationPrecision)).
			DivideUint64(uint64(r.cfg.RollbackNegativeMining.Available.Until - r.cfg.RollbackNegativeMining.AggressiveDegradationStartsAfter))
	}

	return referenceAmount.
		MultiplyUint64(uint64(float64(elapsedDuration) * degradationPrecision)).
		DivideUint64(uint64(r.cfg.RollbackNegativeMining.AggressiveDegradationStartsAfter))
}

func (s *balanceRecalculationTriggerStreamSource) processLastXPositiveMiningSessions( //nolint:revive // Not an issue here.
	balancesByPK map[string]*balance, shouldTransformNegative bool, dateExtractionSeparator, lastXMiningTypeDetail, userID string,
) {
	type datedBalance struct {
		b    *balance
		date *time.Time
	}
	actualLastXMiningSessionBalances := make([]*datedBalance, 0, 0) //nolint:gosimple // Prefer to be more descriptive.
	for _, bal := range balancesByPK {
		if parts := strings.Split(bal.TypeDetail, dateExtractionSeparator); len(parts) == 1+1 && parts[0] == "" { //nolint:revive,gocritic // Nope.
			date, err := stdlibtime.Parse(s.cfg.lastXMiningSessionsCollectingIntervalDateFormat(), parts[1])
			log.Panic(err) //nolint:revive // Intended.
			if shouldTransformNegative && bal.Negative {
				bal.Negative = false
			}
			actualLastXMiningSessionBalances = append(actualLastXMiningSessionBalances, &datedBalance{b: bal, date: time.New(date)})
		}
	}
	if len(actualLastXMiningSessionBalances) > int(s.cfg.RollbackNegativeMining.AggressiveDegradationStartsAfter/s.cfg.MiningSessionDuration.Max) {
		sort.SliceStable(actualLastXMiningSessionBalances, func(i, j int) bool {
			return actualLastXMiningSessionBalances[i].date.Before(*actualLastXMiningSessionBalances[j].date.Time)
		})
		actualLastXMiningSessionBalances[0].b.Amount = coin.ZeroICEFlakes()
	}
	totalPositiveLastXMiningSessions := s.getOrInitBalance(false, lastXMiningTypeDetail, userID, balancesByPK)
	for _, bal := range actualLastXMiningSessionBalances {
		totalPositiveLastXMiningSessions.add(bal.b.Amount)
	}
}

func (s *balanceRecalculationTriggerStreamSource) updateLastIterationFinishedAt(
	ctx context.Context, workerIndex int16, userIDs []string,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	const table = "balance_recalculation_worker"
	params := make(map[string]any, 1)
	params["last_iteration_finished_at"] = *time.Now().Time
	err := s.updateWorkerFields(ctx, workerIndex, table, params, userIDs...)

	return errors.Wrapf(err, "failed to updateWorkerFields for workerIndex:%v,table:%q,params:%#v,userIDs:%#v", workerIndex, table, params, userIDs)
}

func (s *balanceRecalculationTriggerStreamSource) stopWorkerForUsers(
	ctx context.Context, workerIndex int16, lastMiningEndedAtPerUserID map[string]*time.Time,
) error {
	if ctx.Err() != nil || len(lastMiningEndedAtPerUserID) == 0 {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	const fieldCount = 2
	ix := 1
	args := make([]any, 0, 1+len(lastMiningEndedAtPerUserID)*fieldCount)
	args = append(args, workerIndex)
	conditions := make([]string, 0, len(lastMiningEndedAtPerUserID))
	for userID, lastMiningEndedAt := range lastMiningEndedAtPerUserID {
		args = append(args, userID, *lastMiningEndedAt.Time)
		conditions = append(conditions, fmt.Sprintf("(user_id = $%[1]v AND last_mining_ended_at = $%[2]v)", ix+1, ix+1+1))
		ix += fieldCount
	}
	sql := fmt.Sprintf(`UPDATE balance_recalculation_worker
					    SET enabled = FALSE
					    WHERE worker_index = $1 AND (%v)`, strings.Join(conditions, " OR "))

	if _, err := storage.Exec(ctx, s.db, sql, args...); err != nil {
		return errors.Wrapf(err, "failed to update balance_recalculation_worker_%v SET enabled = FALSE for conditions:%#v", workerIndex, conditions)
	}

	return nil
}

func (s *balanceRecalculationTriggerStreamSource) sendFreeMiningSessionStartedMessage(ctx context.Context, fmss *FreeMiningSessionStarted) error {
	valueBytes, err := json.MarshalContext(ctx, fmss)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal %#v", fmss)
	}

	msg := &messagebroker.Message{
		Headers: map[string]string{"producer": "freezer"},
		Key:     fmss.UserID,
		Topic:   s.cfg.MessageBroker.Topics[8].Name,
		Value:   valueBytes,
	}

	responder := make(chan error, 1)
	defer close(responder)
	s.mb.SendMessage(ctx, msg, responder)

	return errors.Wrapf(<-responder, "failed to send %v message to broker, msg:%#v", msg.Topic, fmss)
}
