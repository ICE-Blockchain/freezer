// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	stdlibtime "time"

	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	"github.com/ice-blockchain/eskimo/users"
	"github.com/ice-blockchain/freezer/model"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/terror"
	"github.com/ice-blockchain/wintr/time"
)

type (
	StartOrExtendMiningSession struct {
		model.MiningBoostLevelIndexField
		model.ResurrectSoloUsedAtField
		model.MiningSessionSoloLastStartedAtField
		model.MiningSessionSoloStartedAtField
		model.MiningSessionSoloEndedAtField
		model.MiningSessionSoloDayOffLastAwardedAtField
		model.MiningSessionSoloPreviouslyEndedAtField
		model.ReferralsCountChangeGuardUpdatedAtField
		model.DeserializedUsersKey
	}
	getCurrentMiningSession struct {
		model.ExtraBonusStartedAtField
		StartOrExtendMiningSession
		model.KYCState
		model.LatestDeviceField
		model.UsernameField
		model.UserIDField
		model.SlashingRateSoloField
		model.SlashingRateT0Field
		model.SlashingRateT1Field
		model.SlashingRateT2Field
		model.BalanceForTMinus1Field
		model.BalanceSoloEthereumField
		model.BalanceSoloField
		model.UTCOffsetField
		model.IDT0Field
		model.IDTMinus1Field
		model.PreStakingAllocationField
		model.PreStakingBonusField
		model.BalanceT1WelcomeBonusPendingField
	}
)

func (r *repository) StartNewMiningSession( //nolint:funlen,gocognit // A lot of handling.
	ctx context.Context, ms *MiningSummary, rollbackNegativeMiningProgress *bool, skipKYCSteps []users.KYCStep,
) error {
	userID := *ms.MiningSession.UserID
	id, err := GetOrInitInternalID(ctx, r.db, userID, r.cfg.WelcomeBonusV2Amount)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", userID)
	}
	now := time.Now()
	old, err := storage.Get[getCurrentMiningSession](ctx, r.db, model.SerializedUsersKey(id))
	if err != nil || len(old) == 0 {
		if err == nil {
			err = errors.Wrapf(ErrRelationNotFound, "missing state for id:%v", id)
		}

		return errors.Wrapf(err, "failed to get miningSummary for id:%v", id)
	}
	var miningBoostLevel uint8
	if old[0].MiningBoostLevelIndex != nil {
		miningBoostLevel = uint8(*old[0].MiningBoostLevelIndex) + 1
	}
	maxMiningSessionDuration := r.cfg.maxMiningSessionDuration(old[0].MiningBoostLevelIndexField)
	if !old[0].MiningSessionSoloEndedAt.IsNil() &&
		!old[0].MiningSessionSoloLastStartedAt.IsNil() &&
		old[0].MiningSessionSoloEndedAt.After(*now.Time) {
		durationIncludingDayOffs := old[0].MiningSessionSoloEndedAt.Sub(*old[0].MiningSessionSoloLastStartedAt.Time)
		if dayOffIsUsed := durationIncludingDayOffs > maxMiningSessionDuration && now.Sub(*old[0].MiningSessionSoloLastStartedAt.Time) > maxMiningSessionDuration; dayOffIsUsed {
			maxMiningSessionDuration = r.cfg.MiningSessionDuration.Max
		}
		if (now.Sub(*old[0].MiningSessionSoloLastStartedAt.Time)/r.cfg.MiningSessionDuration.Min)%(maxMiningSessionDuration/r.cfg.MiningSessionDuration.Min) == 0 {
			return ErrDuplicate
		}
	}
	if !old[0].MiningSessionSoloEndedAt.IsNil() &&
		!old[0].ReferralsCountChangeGuardUpdatedAt.IsNil() &&
		now.After(*old[0].MiningSessionSoloEndedAt.Time) &&
		old[0].MiningSessionSoloStartedAt.Equal(*old[0].ReferralsCountChangeGuardUpdatedAt.Time) {
		return ErrDuplicate
	}
	if err = r.validateKYC(ctx, userID, old[0], skipKYCSteps); err != nil {
		return err
	}
	shouldRollback, err := r.validateRollbackNegativeMiningProgress(old[0].PreStakingAllocation, old[0].PreStakingBonus, old[0].SlashingRateSolo, old[0].SlashingRateT0, old[0].SlashingRateT1, old[0].SlashingRateT2, old[0].MiningSessionSoloEndedAt, old[0].ResurrectSoloUsedAt, now, rollbackNegativeMiningProgress, old[0].BalanceSoloEthereum > 0 && old[0].BalanceSoloEthereum >= old[0].BalanceSolo) //nolint:lll // .
	if err != nil {
		return err
	}
	if err = r.updateTMinus1(ctx, id, old[0].IDT0, old[0].IDTMinus1); err != nil {
		return errors.Wrapf(err, "failed to updateTMinus1 for id:%v", id)
	}
	if r.isExtraBonusAvailable(old[0]) {
		if err = r.ClaimExtraBonus(ctx, &ExtraBonusSummary{UserID: userID}); err != nil && !errors.Is(err, ErrNotFound) {
			return errors.Wrapf(err, "failed to ClaimExtraBonus for:%v", userID)
		}
	}
	newMS, extension := r.newStartOrExtendMiningSession(&old[0].StartOrExtendMiningSession, now)
	newMS.ID = id
	if shouldRollback != nil && *shouldRollback && old[0].ResurrectSoloUsedAt.IsNil() {
		newMS.ResurrectSoloUsedAt = time.New(stdlibtime.Date(3000, 0, 0, 0, 0, 0, 0, stdlibtime.UTC)) //nolint:gomnd // .
	}
	startedAt, previouslyEndedAt := newMS.MiningSessionSoloStartedAt, newMS.MiningSessionSoloPreviouslyEndedAt
	if startedAt.IsNil() {
		startedAt = old[0].MiningSessionSoloStartedAt
	}
	if previouslyEndedAt.IsNil() {
		previouslyEndedAt = old[0].MiningSessionSoloEndedAt
	}
	sess := &MiningSession{
		LastNaturalMiningStartedAt:    newMS.MiningSessionSoloLastStartedAt,
		StartedAt:                     startedAt,
		EndedAt:                       newMS.MiningSessionSoloEndedAt,
		PreviouslyEndedAt:             previouslyEndedAt,
		Extension:                     extension,
		MiningStreak:                  r.calculateMiningStreak(now, startedAt, newMS.MiningSessionSoloEndedAt),
		UserID:                        &userID,
		ResettableStartingAt:          time.New(newMS.MiningSessionSoloLastStartedAt.Add(r.cfg.MiningSessionDuration.Min)),
		WarnAboutExpirationStartingAt: time.New(newMS.MiningSessionSoloLastStartedAt.Add(maxMiningSessionDuration - r.cfg.MiningSessionDuration.Max).Add(r.cfg.MiningSessionDuration.WarnAboutExpirationAfter)),
		MiningBoostLevel:              miningBoostLevel,
	}
	if err = r.sendMiningSessionMessage(ctx, sess); err != nil {
		return errors.Wrapf(err, "failed to sendMiningSessionMessage:%#v", sess)
	}
	if err = storage.Set(ctx, r.db, newMS); err != nil {
		return errors.Wrapf(err, "failed to insertNewMiningSession:%#v", newMS)
	}

	return errors.Wrapf(retry(ctx, func() error {
		summary, gErr := r.GetMiningSummary(ctx, userID)
		if gErr == nil {
			if summary.MiningSession == nil || summary.MiningSession.StartedAt.IsNil() || !summary.MiningSession.StartedAt.Equal(*now.Time) {
				gErr = ErrNotFound
			} else {
				*ms = *summary
			}
		}

		return gErr
	}), "permanently failed to GetMiningSummary for userID:%v", userID)
}

func (r *repository) updateTMinus1(ctx context.Context, id, idT0, idTMinus1 int64) error {
	if idTMinus1 == 0 || idT0 == 0 {
		return nil
	}
	if idTMinus1 < 0 {
		idTMinus1 *= -1
	}
	if oldTminus1Data, err := storage.Get[struct{ model.UserIDField }](ctx, r.db, model.SerializedUsersKey(idTMinus1)); err != nil || len(oldTminus1Data) != 0 {
		return errors.Wrapf(err, "failed to get state for t-1:%v", idTMinus1)
	}
	idTMinus1 = 0
	if idT0 < 0 {
		idT0 *= -1
	}
	if t0Data, err := storage.Get[struct{ model.IDT0Field }](ctx, r.db, model.SerializedUsersKey(idT0)); err != nil {
		return errors.Wrapf(err, "failed to get state for t0:%v", idT0)
	} else if len(t0Data) != 0 {
		idTMinus1 = t0Data[0].IDT0
		if idTMinus1 > 0 {
			idTMinus1 *= -1
		}
	}

	return errors.Wrapf(storage.Set(ctx, r.db, &struct {
		model.DeserializedUsersKey
		model.IDTMinus1ResettableField
	}{
		DeserializedUsersKey:     model.DeserializedUsersKey{ID: id},
		IDTMinus1ResettableField: model.IDTMinus1ResettableField{IDTMinus1: idTMinus1},
	}), "failed to replaceIDTMinus1, id:%v, newIDTMinus1:%v", id, idTMinus1)
}

func (r *repository) validateRollbackNegativeMiningProgress(
	preStakingAllocation, preStakingBonus,
	slashingRateSolo, slashingRateT0, slashingRateT1, slashingRateT2 float64,
	miningSessionSoloEndedAt, resurrectSoloUsedAt, now *time.Time,
	rollbackNegativeMiningProgress *bool,
	allBalanceDistributed bool,
) (*bool, error) {
	if !resurrectSoloUsedAt.IsNil() || miningSessionSoloEndedAt.IsNil() ||
		(now.Sub(*miningSessionSoloEndedAt.Time) < r.cfg.RollbackNegativeMining.Available.After ||
			now.Sub(*miningSessionSoloEndedAt.Time) > r.cfg.RollbackNegativeMining.Available.Until) {
		return nil, nil //nolint:nilnil // Nope.
	}
	rateUnit := r.cfg.GlobalAggregationInterval.Child
	slashingDuration := now.Sub(*miningSessionSoloEndedAt.Time)
	delta := float64(slashingDuration/rateUnit) + float64(slashingDuration%rateUnit)/float64(rateUnit)
	amountLost := (slashingRateSolo + slashingRateT0 + slashingRateT1 + slashingRateT2) * delta
	amountLost = ((amountLost * (100 - preStakingAllocation)) / 100) + ((amountLost * (preStakingAllocation * (preStakingBonus + 100))) / (100 * 100))
	if amountLost == 0.0 {
		return nil, nil //nolint:nilnil // Nope.
	}
	if rollbackNegativeMiningProgress == nil {
		if allBalanceDistributed {
			return nil, nil //nolint:nilnil // Nope.
		}
		return nil, terror.New(ErrNegativeMiningProgressDecisionRequired, map[string]any{
			"amount":                fmt.Sprintf(floatToStringFormatter, amountLost),
			"duringTheLastXSeconds": uint64(slashingDuration.Seconds()),
		})
	}
	if rollbackNegativeMiningProgress != nil && *rollbackNegativeMiningProgress && allBalanceDistributed {
		*rollbackNegativeMiningProgress = false
	}
	return rollbackNegativeMiningProgress, nil
}

func (r *repository) isExtraBonusAvailable(state *getCurrentMiningSession) bool {
	// This is just a hack so that we can differentiate between a failed/skipped Social 2 and a successful one:
	// Social2KYCStep is a failed/skipped Social 2 outcome
	// Social3KYCStep is a completed Social 2 outcome
	// And, in actuality, there is no Social 3
	if (users.QuizKYCStep == state.KYCStepPassed || users.Social3KYCStep == state.KYCStepPassed) &&
		!state.DelayPassedSinceLastKYCStepAttempt(state.KYCStepPassed, r.cfg.MiningSessionDuration.Min) &&
		(state.ExtraBonusStartedAt.IsNil() || state.ExtraBonusStartedAt.Add(r.cfg.ExtraBonuses.Duration).Before(*time.Now().Time)) {
		return true
	}

	return false
}

func (r *repository) newStartOrExtendMiningSession(old *StartOrExtendMiningSession, now *time.Time) (*StartOrExtendMiningSession, stdlibtime.Duration) {
	resp := new(StartOrExtendMiningSession)
	resp.MiningSessionSoloStartedAt = now
	resp.MiningSessionSoloLastStartedAt = now
	maxMiningSessionDuration := r.cfg.maxMiningSessionDuration(old.MiningBoostLevelIndexField)
	resp.MiningSessionSoloEndedAt = time.New(now.Add(maxMiningSessionDuration))
	old.MiningBoostLevelIndex = nil
	resp.MiningSessionSoloPreviouslyEndedAt = old.MiningSessionSoloEndedAt
	resp.MiningSessionSoloDayOffLastAwardedAt = new(time.Time)
	if old.MiningSessionSoloEndedAt.IsNil() || old.MiningSessionSoloEndedAt.Before(*now.Time) {
		resp.ReferralsCountChangeGuardUpdatedAt = now
	}

	if old.MiningSessionSoloEndedAt.IsNil() || old.MiningSessionSoloStartedAt.IsNil() || old.MiningSessionSoloEndedAt.Before(*now.Time) {
		return resp, maxMiningSessionDuration
	}
	resp.MiningSessionSoloPreviouslyEndedAt, resp.MiningSessionSoloStartedAt, resp.MiningSessionSoloDayOffLastAwardedAt = nil, nil, nil
	var durationSinceLastFreeMiningSessionAwarded stdlibtime.Duration
	if old.MiningSessionSoloDayOffLastAwardedAt.IsNil() {
		durationSinceLastFreeMiningSessionAwarded = now.Sub(*old.MiningSessionSoloStartedAt.Time)
	} else {
		durationSinceLastFreeMiningSessionAwarded = now.Sub(*old.MiningSessionSoloDayOffLastAwardedAt.Time)
	}
	freeMiningSession := uint64(0)
	minimumDurationForAwardingFreeMiningSession := stdlibtime.Duration(r.cfg.ConsecutiveNaturalMiningSessionsRequiredFor1ExtraFreeArtificialMiningSession.Max) * r.cfg.MiningSessionDuration.Max //nolint:lll // .
	if durationSinceLastFreeMiningSessionAwarded >= minimumDurationForAwardingFreeMiningSession {
		resp.MiningSessionSoloDayOffLastAwardedAt = now
		freeMiningSession++
	}
	if freeSessions := stdlibtime.Duration(r.calculateRemainingFreeMiningSessions(now, old.MiningSessionSoloLastStartedAt, old.MiningSessionSoloEndedAt, maxMiningSessionDuration) + freeMiningSession); freeSessions > 0 {
		resp.MiningSessionSoloEndedAt = time.New(resp.MiningSessionSoloEndedAt.Add(freeSessions * r.cfg.MiningSessionDuration.Max))
	}

	return resp, resp.MiningSessionSoloEndedAt.Sub(*old.MiningSessionSoloEndedAt.Time)
}

func (r *repository) sendMiningSessionMessage(ctx context.Context, ms *MiningSession) error {
	valueBytes, err := json.MarshalContext(ctx, ms)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal %#v", ms)
	}
	msg := &messagebroker.Message{
		Timestamp: *ms.LastNaturalMiningStartedAt.Time,
		Headers:   map[string]string{"producer": "freezer"},
		Key:       *ms.UserID,
		Topic:     r.cfg.MessageBroker.Topics[2].Name,
		Value:     valueBytes,
	}
	responder := make(chan error, 1)
	defer close(responder)
	r.mb.SendMessage(ctx, msg, responder)

	return errors.Wrapf(<-responder, "failed to send `%v` message to broker", msg.Topic)
}

func (s *miningSessionsTableSource) Process(ctx context.Context, msg *messagebroker.Message) error {
	if ctx.Err() != nil || len(msg.Value) == 0 {
		return errors.Wrap(ctx.Err(), "unexpected deadline while processing message")
	}
	ms := new(MiningSession)
	if err := json.UnmarshalContext(ctx, msg.Value, ms); err != nil || ms.UserID == nil || ms.StartedAt.IsNil() {
		return errors.Wrapf(err, "process: cannot unmarshall %v into %#v", string(msg.Value), ms)
	}

	return multierror.Append( //nolint:wrapcheck // Not needed.
		errors.Wrapf(s.incrementTotalActiveUsers(ctx, ms), "failed to incrementTotalActiveUsers for %#v", ms),
		errors.Wrapf(s.incrementActiveReferralCountForT0AndTMinus1(ctx, ms), "failed to incrementActiveReferralCountForT0AndTMinus1 for %#v", ms),
	).ErrorOrNil()
}

//nolint:funlen,revive,gocognit // .
func (s *miningSessionsTableSource) incrementActiveReferralCountForT0AndTMinus1(ctx context.Context, ms *MiningSession) (err error) {
	if ctx.Err() != nil || !ms.LastNaturalMiningStartedAt.Equal(*ms.StartedAt.Time) {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	duplGuardKey := ms.duplGuardKey(s.repository, "incr_active_ref")
	if set, dErr := s.db.SetNX(ctx, duplGuardKey, "", s.cfg.MiningSessionDuration.Min).Result(); dErr != nil || !set {
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
				errors.Wrapf(s.db.Del(undoCtx, duplGuardKey).Err(), "failed to del mining_session_dupl_guard key"),
			).ErrorOrNil()
		}
	}()
	id, err := GetOrInitInternalID(ctx, s.db, *ms.UserID, s.cfg.WelcomeBonusV2Amount)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", *ms.UserID)
	}
	referees, err := storage.Get[struct {
		model.UserIDField
		model.DeserializedUsersKey
		model.IDT0Field
		model.IDTMinus1Field
	}](ctx, s.db, model.SerializedUsersKey(id))
	if err != nil || len(referees) == 0 || (referees[0].IDT0 == 0 && referees[0].IDTMinus1 == 0) {
		return errors.Wrapf(err, "failed to get referees for id:%v, userID:%v", id, *ms.UserID)
	}
	if referees[0].IDT0 < 0 {
		referees[0].IDT0 *= -1
	}
	if referees[0].IDTMinus1 < 0 {
		referees[0].IDTMinus1 *= -1
	}
	if referees[0].IDT0 == 0 || referees[0].IDTMinus1 == 0 {
		if referees[0].IDT0 >= 1 {
			err = s.db.HIncrBy(ctx, model.SerializedUsersKey(referees[0].IDT0), "active_t1_referrals", 1).Err()
		}
		if referees[0].IDTMinus1 >= 1 {
			err = s.db.HIncrBy(ctx, model.SerializedUsersKey(referees[0].IDTMinus1), "active_t2_referrals", 1).Err()
		}
	} else {
		responses, txErr := s.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
			return multierror.Append( //nolint:wrapcheck // .
				pipeliner.HIncrBy(ctx, model.SerializedUsersKey(referees[0].IDT0), "active_t1_referrals", 1).Err(),
				pipeliner.HIncrBy(ctx, model.SerializedUsersKey(referees[0].IDTMinus1), "active_t2_referrals", 1).Err(),
			).ErrorOrNil()
		})
		if txErr == nil {
			errs := make([]error, 0, len(responses))
			for _, response := range responses {
				errs = append(errs, errors.Wrapf(response.Err(), "failed to `%v`", response.FullName()))
			}
			txErr = multierror.Append(nil, errs...).ErrorOrNil()
		}
		err = txErr
	}

	return errors.Wrapf(err, "failed to increment active referrals for t0&t-1, id:%v, userID:%v, ref:%#v", id, *ms.UserID, referees[0])
}

func (r *repository) sessionNumber(date *time.Time) uint64 {
	return SessionNumber(date, r.cfg.MiningSessionDuration.Min)
}

func SessionNumber(date *time.Time, miningSessionResetDeadline stdlibtime.Duration) uint64 {
	return uint64(date.Unix()) / uint64(miningSessionResetDeadline/stdlibtime.Second)
}

func (ms *MiningSession) duplGuardKey(repo *repository, guardType string) string {
	return fmt.Sprintf("mining_session_dupl_guards:%v~%v~%v", guardType, *ms.UserID, repo.sessionNumber(ms.LastNaturalMiningStartedAt))
}

func MiningSessionProcessedDuplGuardKey(lastNaturalMiningStartedAt *time.Time, miningResetDuration stdlibtime.Duration, userID string) string {
	return fmt.Sprintf("mining_session_dupl_guards:incr_active_ref~%v~%v", userID, SessionNumber(lastNaturalMiningStartedAt, miningResetDuration))
}
