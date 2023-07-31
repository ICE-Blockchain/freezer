// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	"strings"
	stdlibtime "time"

	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	extrabonusnotifier "github.com/ice-blockchain/freezer/extra-bonus-notifier"
	"github.com/ice-blockchain/freezer/model"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/time"
)

type (
	availableExtraBonus struct {
		model.ExtraBonusLastClaimAvailableAtField
		model.ExtraBonusStartedAtField
		model.DeserializedUsersKey
		model.ExtraBonusField
		model.NewsSeenField
		model.ExtraBonusDaysClaimNotAvailableResettableField
	}
)

func (r *repository) ClaimExtraBonus(ctx context.Context, ebs *ExtraBonusSummary) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	id, err := GetOrInitInternalID(ctx, r.db, ebs.UserID)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", ebs.UserID)
	}
	now := time.Now()
	stateForUpdate, err := r.detectAvailableExtraBonus(ctx, now, id)
	if err != nil {
		return errors.Wrapf(err, "failed to getAvailableExtraBonus for userID:%v", ebs.UserID)
	}
	ebs.AvailableExtraBonus = stateForUpdate.ExtraBonus

	return errors.Wrapf(storage.Set(ctx, r.db, stateForUpdate), "failed to claim extra bonus:%#v", stateForUpdate)
}

func (r *repository) detectAvailableExtraBonus(ctx context.Context, now *time.Time, id int64) (*availableExtraBonus, error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	usr, err := storage.Get[struct {
		model.MiningSessionSoloStartedAtField
		model.MiningSessionSoloEndedAtField
		model.ExtraBonusLastClaimAvailableAtField
		model.ExtraBonusStartedAtField
		model.ExtraBonusDaysClaimNotAvailableResettableField
		model.NewsSeenField
		model.UTCOffsetField
	}](ctx, r.db, model.SerializedUsersKey(id))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get extra bonus state before claiming it for id:%v", id)
	}
	if len(usr) == 0 {
		return nil, ErrNotFound
	}

	return r.getAvailableExtraBonus(now, id, usr[0].ExtraBonusStartedAtField, usr[0].ExtraBonusLastClaimAvailableAtField, usr[0].MiningSessionSoloStartedAtField, usr[0].MiningSessionSoloEndedAtField, usr[0].ExtraBonusDaysClaimNotAvailableResettableField, usr[0].UTCOffsetField, usr[0].NewsSeenField) //nolint:lll // .
}

//nolint:funlen,lll // .
func (r *repository) getAvailableExtraBonus(
	now *time.Time,
	id int64,
	extraBonusStartedAtField model.ExtraBonusStartedAtField,
	extraBonusLastClaimAvailableAtField model.ExtraBonusLastClaimAvailableAtField,
	miningSessionSoloStartedAtField model.MiningSessionSoloStartedAtField,
	miningSessionSoloEndedAtField model.MiningSessionSoloEndedAtField,
	extraBonusDaysClaimNotAvailableField model.ExtraBonusDaysClaimNotAvailableResettableField,
	utcOffsetField model.UTCOffsetField,
	newsSeenField model.NewsSeenField,
) (*availableExtraBonus, error) {
	var (
		extraBonusIndex     uint16
		extraBonus          float64
		calculateExtraBonus = func() float64 {
			return extrabonusnotifier.CalculateExtraBonus(newsSeenField.NewsSeen, extraBonusDaysClaimNotAvailableField.ExtraBonusDaysClaimNotAvailable, extraBonusIndex-1, now, extraBonusLastClaimAvailableAtField.ExtraBonusLastClaimAvailableAt, miningSessionSoloStartedAtField.MiningSessionSoloStartedAt, miningSessionSoloEndedAtField.MiningSessionSoloEndedAt) //nolint:lll // .
		}
	)
	if !extraBonusStartedAtField.ExtraBonusStartedAt.IsNil() &&
		now.After(*extraBonusLastClaimAvailableAtField.ExtraBonusLastClaimAvailableAt.Time) &&
		extraBonusStartedAtField.ExtraBonusStartedAt.After(*extraBonusLastClaimAvailableAtField.ExtraBonusLastClaimAvailableAt.Time) &&
		extraBonusStartedAtField.ExtraBonusStartedAt.Before(extraBonusLastClaimAvailableAtField.ExtraBonusLastClaimAvailableAt.Add(r.cfg.ExtraBonuses.ClaimWindow)) &&
		now.Before(extraBonusLastClaimAvailableAtField.ExtraBonusLastClaimAvailableAt.Add(r.cfg.ExtraBonuses.ClaimWindow)) {
		return nil, ErrDuplicate
	}
	if bonusAvailable, bonusClaimable := extrabonusnotifier.IsExtraBonusAvailable(now, r.extraBonusStartDate, extraBonusStartedAtField.ExtraBonusStartedAt, r.extraBonusIndicesDistribution, id, int16(utcOffsetField.UTCOffset), &extraBonusIndex, &extraBonusDaysClaimNotAvailableField.ExtraBonusDaysClaimNotAvailable, &extraBonusLastClaimAvailableAtField.ExtraBonusLastClaimAvailableAt); bonusAvailable { //nolint:lll // .
		if extraBonus = calculateExtraBonus(); extraBonus == 0 {
			return nil, ErrNotFound
		} else {
			return &availableExtraBonus{
				ExtraBonusLastClaimAvailableAtField: extraBonusLastClaimAvailableAtField,
				ExtraBonusStartedAtField:            model.ExtraBonusStartedAtField{ExtraBonusStartedAt: now},
				DeserializedUsersKey:                model.DeserializedUsersKey{ID: id},
				ExtraBonusField:                     model.ExtraBonusField{ExtraBonus: extraBonus},
			}, nil
		}
	} else if !bonusClaimable {
		return nil, ErrNotFound
	} else {
		if extraBonus = calculateExtraBonus(); extraBonus == 0 {
			return nil, ErrNotFound
		} else {
			extraBonusLastClaimAvailableAtField.ExtraBonusLastClaimAvailableAt = nil
		}
	}

	return &availableExtraBonus{
		ExtraBonusLastClaimAvailableAtField: extraBonusLastClaimAvailableAtField,
		ExtraBonusStartedAtField:            model.ExtraBonusStartedAtField{ExtraBonusStartedAt: now},
		DeserializedUsersKey:                model.DeserializedUsersKey{ID: id},
		ExtraBonusField:                     model.ExtraBonusField{ExtraBonus: extraBonus},
	}, nil
}

func (s *deviceMetadataTableSource) Process(ctx context.Context, msg *messagebroker.Message) error { //nolint:funlen // .
	if ctx.Err() != nil || len(msg.Value) == 0 {
		return errors.Wrap(ctx.Err(), "unexpected deadline while processing message")
	}
	type (
		deviceMetadata struct {
			Before *deviceMetadata `json:"before,omitempty"`
			UserID string          `json:"userId,omitempty" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
			TZ     string          `json:"tz,omitempty" example:"+03:00"`
		}
	)
	var dm deviceMetadata
	if err := json.UnmarshalContext(ctx, msg.Value, &dm); err != nil || dm.UserID == "" || dm.TZ == "" || (dm.Before != nil && dm.Before.TZ == dm.TZ) {
		return errors.Wrapf(err, "process: cannot unmarshall %v into %#v", string(msg.Value), &dm)
	}
	duration, err := stdlibtime.ParseDuration(strings.Replace(dm.TZ+"m", ":", "h", 1))
	if err != nil {
		return errors.Wrapf(err, "invalid timezone:%#v", &dm)
	}
	id, err := GetOrInitInternalID(ctx, s.db, dm.UserID)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for %#v", &dm)
	}
	val := &struct {
		model.DeserializedUsersKey
		model.UTCOffsetField
	}{
		DeserializedUsersKey: model.DeserializedUsersKey{ID: id},
		UTCOffsetField:       model.UTCOffsetField{UTCOffset: int64(duration / stdlibtime.Minute)},
	}

	return errors.Wrapf(storage.Set(ctx, s.db, val), "failed to update users' timezone for %#v", &dm)
}

func (s *viewedNewsSource) Process(ctx context.Context, msg *messagebroker.Message) (err error) { //nolint:funlen // .
	if ctx.Err() != nil || len(msg.Value) == 0 {
		return errors.Wrap(ctx.Err(), "unexpected deadline while processing message")
	}
	var vn struct {
		UserID string `json:"userId,omitempty" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
		NewsID string `json:"newsId,omitempty" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
	}
	if err = json.UnmarshalContext(ctx, msg.Value, &vn); err != nil || vn.UserID == "" {
		return errors.Wrapf(err, "process: cannot unmarshall %v into %#v", string(msg.Value), &vn)
	}
	duplGuardKey := fmt.Sprintf("news_seen_dupl_guards:%v", vn.UserID)
	if set, dErr := s.db.SetNX(ctx, duplGuardKey, "", s.cfg.MiningSessionDuration.Min).Result(); dErr != nil || !set {
		if dErr == nil {
			dErr = ErrDuplicate
		}

		return errors.Wrapf(dErr, "SetNX failed for news_seen_dupl_guard, %#v", vn)
	}
	defer func() {
		if err != nil {
			undoCtx, cancelUndo := context.WithTimeout(context.Background(), requestDeadline)
			defer cancelUndo()
			err = multierror.Append( //nolint:wrapcheck // .
				err,
				errors.Wrapf(s.db.Del(undoCtx, duplGuardKey).Err(), "failed to del news_seen_dupl_guard key"),
			).ErrorOrNil()
		}
	}()
	id, err := GetOrInitInternalID(ctx, s.db, vn.UserID)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for %#v", &vn)
	}

	return errors.Wrapf(s.db.HIncrBy(ctx, model.SerializedUsersKey(id), "news_seen", 1).Err(),
		"failed to increment news_seen for userID:%v,id:%v", vn.UserID, id)
}
