// SPDX-License-Identifier: ice License 1.0

package miner

import (
	"context"
	"fmt"
	"math"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	balancesynchronizer "github.com/ice-blockchain/freezer/balance-synchronizer"
	dwh "github.com/ice-blockchain/freezer/bookkeeper/storage"
	"github.com/ice-blockchain/freezer/model"
	"github.com/ice-blockchain/freezer/tokenomics"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

type (
	ResettableFields struct {
		BalanceLastUpdatedAtResettableField
		MiningSessionSoloLastStartedAtResettableField
		MiningSessionSoloStartedAtResettableField
		MiningSessionSoloEndedAtResettableField
		MiningSessionSoloPreviouslyEndedAtResettableField
		ResurrectSoloUsedAtResettableField
		ResurrectT0UsedAtResettableField
		ResurrectTMinus1UsedAtResettableField
		MiningSessionSoloDayOffLastAwardedAtResettableField
		ExtraBonusLastClaimAvailableAtResettableField
		ReferralsCountChangeGuardUpdatedAtResettableField
		BalanceT1PendingResettableField
		BalanceT1PendingAppliedResettableField
		BalanceT1WelcomeBonusPendingResettableField
		BalanceT1WelcomeBonusPendingAppliedResettableField
		BalanceSoloPendingAppliedResettableField
		ActiveT1ReferralsResettableField
		model.BalanceTotalStandardField
		model.BalanceTotalPreStakingField
		model.BalanceTotalMintedField
		model.BalanceTotalSlashedField
		model.BalanceSoloField
		model.BalanceT0Field
		model.BalanceT1Field
		model.BalanceForT0Field
		model.BalanceForTMinus1Field
		model.SlashingRateSoloField
		model.SlashingRateT0Field
		model.SlashingRateT1Field
		model.SlashingRateForT0Field
		model.SlashingRateForTMinus1Field
		model.WelcomeBonusV2AppliedField
	}
	ReadOnlyFields struct {
		model.CreatedAtField
		model.UserIDField
		model.DeserializedUsersKey
		model.PreStakingAllocationField
		model.PreStakingBonusField
		model.TotalT1ReferralsField
	}
	UserResettable struct {
		ReadOnlyFields
		ResettableFields
	}
	BalanceLastUpdatedAtResettableField struct {
		BalanceLastUpdatedAt *time.Time `redis:"balance_last_updated_at"`
	}
	MiningSessionSoloLastStartedAtResettableField struct {
		MiningSessionSoloLastStartedAt *time.Time `redis:"mining_session_solo_last_started_at"`
	}
	MiningSessionSoloStartedAtResettableField struct {
		MiningSessionSoloStartedAt *time.Time `redis:"mining_session_solo_started_at"`
	}
	MiningSessionSoloEndedAtResettableField struct {
		MiningSessionSoloEndedAt *time.Time `redis:"mining_session_solo_ended_at"`
	}
	MiningSessionSoloPreviouslyEndedAtResettableField struct {
		MiningSessionSoloPreviouslyEndedAt *time.Time `redis:"mining_session_solo_previously_ended_at"`
	}
	ResurrectSoloUsedAtResettableField struct {
		ResurrectSoloUsedAt *time.Time `redis:"resurrect_solo_used_at"`
	}
	ResurrectT0UsedAtResettableField struct {
		ResurrectT0UsedAt *time.Time `redis:"resurrect_t0_used_at"`
	}
	ResurrectTMinus1UsedAtResettableField struct {
		ResurrectTMinus1UsedAt *time.Time `redis:"resurrect_tminus1_used_at"`
	}
	MiningSessionSoloDayOffLastAwardedAtResettableField struct {
		MiningSessionSoloDayOffLastAwardedAt *time.Time `redis:"mining_session_solo_day_off_last_awarded_at"`
	}
	ExtraBonusLastClaimAvailableAtResettableField struct {
		ExtraBonusLastClaimAvailableAt *time.Time `redis:"extra_bonus_last_claim_available_at"`
	}
	ReferralsCountChangeGuardUpdatedAtResettableField struct {
		ReferralsCountChangeGuardUpdatedAt *time.Time `redis:"referrals_count_change_guard_updated_at"`
	}
	ActiveT1ReferralsResettableField struct {
		ActiveT1Referrals int32 `redis:"active_t1_referrals"`
	}
	ActiveT2ReferralsResettableField struct {
		ActiveT2Referrals int32 `redis:"active_t2_referrals"`
	}
	BalanceSoloPendingAppliedResettableField struct {
		BalanceSoloPendingApplied float64 `redis:"balance_solo_pending_applied"`
	}
	BalanceT1WelcomeBonusPendingResettableField struct {
		BalanceT1WelcomeBonusPending float64 `redis:"balance_t1_welcome_bonus_pending"`
	}
	BalanceT1WelcomeBonusPendingAppliedResettableField struct {
		BalanceT1WelcomeBonusPendingApplied float64 `redis:"balance_t1_welcome_bonus_pending_applied"`
	}
	BalanceT1PendingResettableField struct {
		BalanceT1Pending float64 `redis:"balance_t1_pending"`
	}
	BalanceT1PendingAppliedResettableField struct {
		BalanceT1PendingApplied float64 `redis:"balance_t1_pending_applied"`
	}
)

func (u *UserResettable) baseMiningRate(now *time.Time) float64 {
	if u == nil {
		return 0
	}

	return cfg.BaseMiningRate(now, u.CreatedAt)
}

func (m *miner) reset(ctx context.Context, workerNumber int64) {
	dwhClient := dwh.MustConnect(context.Background(), applicationYamlKey)
	defer func() {
		if err := recover(); err != nil {
			log.Error(dwhClient.Close())
			panic(err)
		}
		log.Error(dwhClient.Close())
	}()
	var (
		batchNumber                  int64
		totalBatches                 uint64
		iteration                    uint64
		now, lastIterationStartedAt  = time.Now(), time.Now()
		workers                      = cfg.Workers
		batchSize                    = cfg.BatchSize
		userKeys                     = make([]string, 0, batchSize)
		userResults                  = make([]*UserResettable, 0, batchSize)
		msgResponder                 = make(chan error, 3*batchSize)
		msgs                         = make([]*messagebroker.Message, 0, 3*batchSize)
		errs                         = make([]error, 0, 3*batchSize)
		updatedUsers                 = make([]*UserResettable, 0, batchSize)
		userGlobalRanks              = make([]redis.Z, 0, batchSize)
		shouldSynchronizeBalanceFunc = func(batchNumberArg uint64) bool { return false }
	)
	resetVars := func(success bool) {
		if success && len(userKeys) == int(batchSize) && len(userResults) == 0 {
			go m.telemetry.collectElapsed(0, *lastIterationStartedAt.Time)
			lastIterationStartedAt = time.Now()
			iteration++
			if batchNumber < 1 {
				panic("unexpected batch number: " + fmt.Sprint(batchNumber))
			}
			totalBatches = uint64(batchNumber - 1)
			if totalBatches != 0 && iteration > 2 {
				shouldSynchronizeBalanceFunc = m.telemetry.shouldSynchronizeBalanceFunc(uint64(workerNumber), totalBatches, iteration)
			}
			batchNumber = 0
		} else if success {
			go m.telemetry.collectElapsed(1, *now.Time)
		}
		now = time.Now()
		userKeys = userKeys[:0]
		userResults = userResults[:0]
		msgs, errs = msgs[:0], errs[:0]
		updatedUsers = updatedUsers[:0]
		userGlobalRanks = userGlobalRanks[:0]
	}
	for ctx.Err() == nil {
		/******************************************************************************************************************************************************
			1. Fetching a new batch of users.
		******************************************************************************************************************************************************/
		if len(userKeys) == 0 {
			for ix := batchNumber * batchSize; ix < (batchNumber+1)*batchSize; ix++ {
				userKeys = append(userKeys, model.SerializedUsersKey((workers*ix)+workerNumber))
			}
		}
		before := time.Now()
		reqCtx, reqCancel := context.WithTimeout(context.Background(), requestDeadline)
		if err := storage.Bind(reqCtx, m.db, userKeys, &userResults); err != nil {
			log.Error(errors.Wrapf(err, "[miner reset] failed to get users for batchNumber:%v,workerNumber:%v", batchNumber, workerNumber))
			reqCancel()
			now = time.Now()

			continue
		}
		reqCancel()
		if len(userKeys) > 0 {
			go m.telemetry.collectElapsed(2, *before.Time)
		}

		/******************************************************************************************************************************************************
			2. Reset the info.
		******************************************************************************************************************************************************/

		shouldSynchronizeBalance := shouldSynchronizeBalanceFunc(uint64(batchNumber))
		for _, usr := range userResults {
			if usr.UserID == "" {
				continue
			}

			if usr == nil || usr.MiningSessionSoloStartedAt.IsNil() || usr.MiningSessionSoloEndedAt.IsNil() {
				continue
			}

			clonedUser1 := *usr
			updatedUser := &clonedUser1
			maxT1Referrals := (*cfg.miningBoostLevels.Load())[len(*cfg.miningBoostLevels.Load())-1].MaxT1Referrals

			updatedUser.BalanceLastUpdatedAt = nil
			updatedUser.MiningSessionSoloLastStartedAt = nil
			updatedUser.MiningSessionSoloStartedAt = nil
			updatedUser.MiningSessionSoloEndedAt = nil
			updatedUser.MiningSessionSoloPreviouslyEndedAt = nil
			updatedUser.ResurrectSoloUsedAt = nil
			updatedUser.ResurrectT0UsedAt = nil
			updatedUser.ResurrectTMinus1UsedAt = nil
			updatedUser.MiningSessionSoloDayOffLastAwardedAt = nil
			updatedUser.ExtraBonusLastClaimAvailableAt = nil
			updatedUser.ReferralsCountChangeGuardUpdatedAt = nil
			updatedUser.BalanceTotalStandard, updatedUser.BalanceTotalPreStaking = tokenomics.ApplyPreStaking(cfg.WelcomeBonusV2Amount, updatedUser.PreStakingAllocation, updatedUser.PreStakingBonus)
			updatedUser.BalanceTotalMinted = cfg.WelcomeBonusV2Amount
			updatedUser.BalanceTotalSlashed = 0
			updatedUser.BalanceSolo = cfg.WelcomeBonusV2Amount
			updatedUser.BalanceT0 = 0
			updatedUser.BalanceT1 = 0
			updatedUser.BalanceForT0 = 0
			updatedUser.BalanceForTMinus1 = 0
			updatedUser.SlashingRateSolo = 0
			updatedUser.SlashingRateT0 = 0
			updatedUser.SlashingRateT1 = 0
			updatedUser.SlashingRateForT0 = 0
			updatedUser.SlashingRateForTMinus1 = 0
			updatedUser.ActiveT1Referrals = 0
			updatedUser.BalanceT1WelcomeBonusPending = float64(min(maxT1Referrals, uint64(usr.TotalT1Referrals))) * cfg.WelcomeBonusV2Amount
			updatedUser.BalanceT1WelcomeBonusPendingApplied = 0
			updatedUser.BalanceT1Pending = 0
			updatedUser.BalanceT1PendingApplied = 0
			updatedUser.BalanceSoloPendingApplied = 0
			trueVal := model.FlexibleBool(true)
			updatedUser.WelcomeBonusV2Applied = &trueVal

			updatedUsers = append(updatedUsers, updatedUser)

			totalStandardBalance, totalPreStakingBalance := updatedUser.BalanceTotalStandard, updatedUser.BalanceTotalPreStaking
			if shouldSynchronizeBalance {
				userGlobalRanks = append(userGlobalRanks, balancesynchronizer.GlobalRank(usr.ID, cfg.WelcomeBonusV2Amount))
				if math.IsNaN(totalStandardBalance) || math.IsNaN(totalPreStakingBalance) {
					log.Info(fmt.Sprintf("bmr[%#v],before[%+v], after[%+v]", updatedUser.baseMiningRate(now), usr, updatedUser))
				}
				msgs = append(msgs, balancesynchronizer.BalanceUpdatedMessage(reqCtx, usr.UserID, totalStandardBalance, totalPreStakingBalance))
			}
		}

		/******************************************************************************************************************************************************
			3. Sending messages to the broker.
		******************************************************************************************************************************************************/

		before = time.Now()
		reqCtx, reqCancel = context.WithTimeout(context.Background(), requestDeadline)
		for _, message := range msgs {
			m.mb.SendMessage(reqCtx, message, msgResponder)
		}
		for (len(msgs) > 0 && len(errs) < len(msgs)) || len(msgResponder) > 0 {
			errs = append(errs, <-msgResponder)
		}
		if err := multierror.Append(reqCtx.Err(), errs...).ErrorOrNil(); err != nil {
			log.Error(errors.Wrapf(err, "[miner reset] failed to send messages to broker for batchNumber:%v,workerNumber:%v", batchNumber, workerNumber))
			reqCancel()
			resetVars(false)

			continue
		}
		reqCancel()
		if len(msgs) > 0 {
			go m.telemetry.collectElapsed(4, *before.Time)
		}

		/******************************************************************************************************************************************************
			4. Persisting the progress for the users.
		******************************************************************************************************************************************************/

		var pipeliner redis.Pipeliner
		var transactional bool
		if len(userGlobalRanks) > 0 {
			pipeliner = m.db.TxPipeline()
			transactional = true
		} else {
			pipeliner = m.db.Pipeline()
		}

		before = time.Now()
		reqCtx, reqCancel = context.WithTimeout(context.Background(), requestDeadline)
		if responses, err := pipeliner.Pipelined(reqCtx, func(pipeliner redis.Pipeliner) error {
			for _, value := range updatedUsers {
				if err := pipeliner.HSet(reqCtx, value.Key(), storage.SerializeValue(value)...).Err(); err != nil {
					return err
				}
			}
			if len(userGlobalRanks) > 0 {
				if err := pipeliner.ZAdd(reqCtx, "top_miners", userGlobalRanks...).Err(); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			log.Error(errors.Wrapf(err, "[miner reset] [1]failed to persist mining process for batchNumber:%v,workerNumber:%v", batchNumber, workerNumber))
			reqCancel()
			resetVars(false)

			continue
		} else {
			if len(errs) != 0 {
				errs = errs[:0]
			}
			for _, response := range responses {
				if err = response.Err(); err != nil {
					errs = append(errs, errors.Wrapf(err, "failed to `%v`", response.FullName()))
				}
			}
			if err = multierror.Append(nil, errs...).ErrorOrNil(); err != nil {
				log.Error(errors.Wrapf(err, "[miner reset] [2]failed to persist mining progress for batchNumber:%v,workerNumber:%v", batchNumber, workerNumber))
				reqCancel()
				resetVars(false)

				continue
			}
		}
		if transactional || len(updatedUsers) > 0 {
			go m.telemetry.collectElapsed(9, *before.Time)
		}

		batchNumber++
		reqCancel()
		resetVars(true)
	}
}
