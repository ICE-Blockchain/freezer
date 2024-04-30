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

	"github.com/ice-blockchain/freezer/model"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/time"
)

type (
	availableExtraBonus struct {
		model.ExtraBonusStartedAtField
		model.DeserializedUsersKey
		model.ExtraBonusField
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
	if r.cfg.ExtraBonuses.KycPassedExtraBonus == 0 {
		return ErrNotFound
	}
	stateForUpdate := &availableExtraBonus{
		ExtraBonusStartedAtField: model.ExtraBonusStartedAtField{ExtraBonusStartedAt: now},
		DeserializedUsersKey:     model.DeserializedUsersKey{ID: id},
		ExtraBonusField:          model.ExtraBonusField{ExtraBonus: r.cfg.ExtraBonuses.KycPassedExtraBonus},
	}
	ebs.AvailableExtraBonus = stateForUpdate.ExtraBonus

	return errors.Wrapf(storage.Set(ctx, r.db, stateForUpdate), "failed to claim extra bonus:%#v", stateForUpdate)
}

func (s *deviceMetadataTableSource) Process(ctx context.Context, msg *messagebroker.Message) error { //nolint:funlen // .
	if ctx.Err() != nil || len(msg.Value) == 0 {
		return errors.Wrap(ctx.Err(), "unexpected deadline while processing message")
	}
	type (
		deviceMetadata struct {
			UserID          string `json:"userId,omitempty" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
			TZ              string `json:"tz,omitempty" example:"+03:00"`
			SystemName      string `json:"systemName,omitempty" example:"Android"`
			ReadableVersion string `json:"readableVersion,omitempty" example:"9.9.9.2637"`
		}
	)
	var dm deviceMetadata
	if err := json.UnmarshalContext(ctx, msg.Value, &dm); err != nil || dm.UserID == "" {
		return errors.Wrapf(err, "process: cannot unmarshall %v into %#v", string(msg.Value), &dm)
	}
	if dm.TZ == "" {
		dm.TZ = "+00:00"
	}
	duration, err := stdlibtime.ParseDuration(strings.Replace(dm.TZ+"m", ":", "h", 1))
	if err != nil {
		return errors.Wrapf(err, "invalid timezone:%#v", &dm)
	}
	id, err := GetOrInitInternalID(ctx, s.db, dm.UserID)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for %#v", &dm)
	}
	sanitizedDeviceSystemName := strings.ReplaceAll(strings.ToLower(dm.SystemName), " ", "")
	val := &struct {
		model.LatestDeviceField
		model.DeserializedUsersKey
		model.UTCOffsetField
	}{
		DeserializedUsersKey: model.DeserializedUsersKey{ID: id},
		UTCOffsetField:       model.UTCOffsetField{UTCOffset: int64(duration / stdlibtime.Minute)},
		LatestDeviceField:    model.LatestDeviceField{LatestDevice: fmt.Sprintf("%v:%v", sanitizedDeviceSystemName, dm.ReadableVersion)},
	}
	if val.LatestDevice == ":" {
		val.LatestDevice = ""
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
	duplGuardKey := fmt.Sprintf("news_seen_dupl_guards:%v~%v", vn.UserID, vn.NewsID)
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
