// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"strconv"
	"strings"
	stdlibtime "time"

	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	"github.com/ice-blockchain/eskimo/users"
	"github.com/ice-blockchain/freezer/model"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

func (s *usersTableSource) Process(ctx context.Context, msg *messagebroker.Message) error { //nolint:gocognit // .
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline while processing message")
	}
	if len(msg.Value) == 0 {
		return nil
	}
	var usr users.UserSnapshot
	if err := json.UnmarshalContext(ctx, msg.Value, &usr); err != nil {
		return errors.Wrapf(err, "process: cannot unmarshall %v into %#v", string(msg.Value), &usr)
	}
	if (usr.User == nil || usr.User.ID == "") && (usr.Before == nil || usr.Before.ID == "") {
		return nil
	}

	if usr.User == nil || usr.User.ID == "" {
		return errors.Wrapf(s.deleteUser(ctx, usr.Before), "failed to delete user:%#v", usr.Before)
	}

	if err := s.replaceUser(ctx, usr.User); err != nil {
		return errors.Wrapf(err, "failed to replace user:%#v", usr.User)
	}

	return nil
}

func (s *usersTableSource) deleteUser(ctx context.Context, usr *users.User) error { //nolint:funlen // .
	id, err := GetOrInitInternalID(ctx, s.db, usr.ID, s.cfg.WelcomeBonusV2Amount)
	if err != nil {
		return errors.Wrapf(err, "failed to getInternalID for user:%#v", usr)
	}
	dbUserBeforeMiningStopped, err := storage.Get[struct {
		model.MiningSessionSoloEndedAtField
		model.UserIDField
	}](ctx, s.db, model.SerializedUsersKey(id))
	if err != nil || len(dbUserBeforeMiningStopped) == 0 {
		if err == nil && len(dbUserBeforeMiningStopped) == 0 {
			err = ErrNotFound
		}

		return errors.Wrapf(err, "[1]failed to get current state for user:%#v", usr)
	}
	if err = storage.Set(ctx, s.db, &struct {
		model.MiningSessionSoloStartedAtField
		model.MiningSessionSoloEndedAtField
		model.MiningSessionSoloPreviouslyEndedAtField
		model.DeserializedUsersKey
	}{
		MiningSessionSoloStartedAtField:         model.MiningSessionSoloStartedAtField{MiningSessionSoloStartedAt: new(time.Time)},
		MiningSessionSoloEndedAtField:           model.MiningSessionSoloEndedAtField{MiningSessionSoloEndedAt: new(time.Time)},
		MiningSessionSoloPreviouslyEndedAtField: model.MiningSessionSoloPreviouslyEndedAtField{MiningSessionSoloPreviouslyEndedAt: time.Now()},
		DeserializedUsersKey:                    model.DeserializedUsersKey{ID: id},
	}); err != nil {
		return errors.Wrapf(err, "failed to manually stop mining due to user deletion message for user:%#v", usr)
	}
	stdlibtime.Sleep(stdlibtime.Second)
	dbUserAfterMiningStopped, err := storage.Get[struct {
		model.UserIDField
		model.IDT0Field
		model.IDTMinus1Field
		model.BalanceForT0Field
		model.BalanceForTMinus1Field
		model.ActiveT1ReferralsField
	}](ctx, s.db, model.SerializedUsersKey(id))
	if err != nil || len(dbUserAfterMiningStopped) == 0 {
		if err == nil && len(dbUserAfterMiningStopped) == 0 {
			err = ErrNotFound
		}

		return errors.Wrapf(err, "[2]failed to get current state for user:%#v", usr)
	}
	results, err := s.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		if dbUserAfterMiningStopped[0].IDT0 < 0 {
			dbUserAfterMiningStopped[0].IDT0 *= -1
		}
		if dbUserAfterMiningStopped[0].IDTMinus1 < 0 {
			dbUserAfterMiningStopped[0].IDTMinus1 *= -1
		}
		if idT0Key := model.SerializedUsersKey(dbUserAfterMiningStopped[0].IDT0); idT0Key != "" {
			if !dbUserBeforeMiningStopped[0].MiningSessionSoloEndedAt.IsNil() &&
				dbUserBeforeMiningStopped[0].MiningSessionSoloEndedAt.After(*time.Now().Time) {
				if err = pipeliner.HIncrBy(ctx, idT0Key, "active_t1_referrals", -1).Err(); err != nil {
					return err
				}
			}
			if dbUserAfterMiningStopped[0].ActiveT1Referrals > 0 {
				if err = pipeliner.HIncrBy(ctx, idT0Key, "active_t2_referrals", -int64(dbUserAfterMiningStopped[0].ActiveT1Referrals)).Err(); err != nil {
					return err
				}
			}
		}
		if idTMinus1Key := model.SerializedUsersKey(dbUserAfterMiningStopped[0].IDTMinus1); idTMinus1Key != "" {
			if amount := dbUserAfterMiningStopped[0].BalanceForTMinus1; amount > 0.0 {
				if err = pipeliner.HIncrByFloat(ctx, idTMinus1Key, "balance_t2_pending", -amount).Err(); err != nil {
					return err
				}
			}
			if !dbUserBeforeMiningStopped[0].MiningSessionSoloEndedAt.IsNil() &&
				dbUserBeforeMiningStopped[0].MiningSessionSoloEndedAt.After(*time.Now().Time) {
				if err = pipeliner.HIncrBy(ctx, idTMinus1Key, "active_t2_referrals", -1).Err(); err != nil {
					return err
				}
			}
		}
		toRemove, _ := s.usernameKeywords(usr.Username, "")
		for _, usernameKeyword := range toRemove {
			if err = pipeliner.SRem(ctx, "lookup:"+usernameKeyword, model.SerializedUsersKey(id)).Err(); err != nil {
				return err
			}
		}
		if err = pipeliner.ZRem(ctx, "top_miners", model.SerializedUsersKey(id)).Err(); err != nil {
			return err
		}
		if err = pipeliner.Del(ctx, model.SerializedUsersKey(id), model.SerializedUsersKey(usr.ID)).Err(); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "failed to delete userID:%v,id:%v", usr.ID, id)
	}
	errs := make([]error, 0, len(results)+1)
	for _, result := range results {
		if err = result.Err(); err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to run `%#v`", result.FullName()))
		}
	}
	errs = append(errs, errors.Wrapf(s.dwh.DeleteUserInfo(ctx, id), "failed to delete clickhouse information for user id:%v,id:%v", usr.ID, id))

	return errors.Wrapf(multierror.Append(nil, errs...).ErrorOrNil(), "failed to delete userID:%v,id:%v", usr.ID, id)
}

func (s *usersTableSource) replaceUser(ctx context.Context, usr *users.User) error { //nolint:funlen // .
	internalID, err := GetOrInitInternalID(ctx, s.db, usr.ID, s.cfg.WelcomeBonusV2Amount)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for user:%#v", usr)
	}
	type (
		LocalUser struct {
			model.CreatedAtField
			model.KYCStepsCreatedAtField
			model.KYCStepsLastUpdatedAtField
			model.UserIDField
			model.ProfilePictureNameField
			model.UsernameField
			model.CountryField
			model.MiningBlockchainAccountAddressField
			model.BlockchainAccountAddressField
			model.VerifiedT1ReferralsField
			model.DeserializedUsersKey
			model.KYCStepPassedField
			model.KYCStepBlockedField
			model.HideRankingField
		}
		readOnlyUser struct {
			LocalUser
			model.BalanceForTMinus1Field
			model.IDT0Field
			model.IDTMinus1Field
		}
	)
	dbUser, err := storage.Get[readOnlyUser](ctx, s.db, model.SerializedUsersKey(internalID))
	if err != nil || len(dbUser) == 0 {
		if err == nil && len(dbUser) == 0 {
			err = errors.Errorf("missing state for user:%#v", usr)
		}

		return errors.Wrapf(err, "failed to get current user for internalID:%v", internalID)
	}
	newPartialState := new(LocalUser)
	newPartialState.ID = internalID
	newPartialState.CreatedAt = usr.CreatedAt
	newPartialState.ProfilePictureName = s.pictureClient.StripDownloadURL(usr.ProfilePictureURL)
	newPartialState.Username = usr.Username
	newPartialState.Country = usr.Country
	newPartialState.MiningBlockchainAccountAddress = usr.MiningBlockchainAccountAddress
	newPartialState.BlockchainAccountAddress = usr.BlockchainAccountAddress
	if usr.KYCStepPassed != nil {
		newPartialState.KYCStepPassed = *usr.KYCStepPassed
	}
	if usr.KYCStepBlocked != nil {
		newPartialState.KYCStepBlocked = *usr.KYCStepBlocked
	}
	if usr.KYCStepsLastUpdatedAt != nil {
		val := model.TimeSlice(*usr.KYCStepsLastUpdatedAt)
		newPartialState.KYCStepsLastUpdatedAt = &val
	}
	if usr.KYCStepsCreatedAt != nil {
		val := model.TimeSlice(*usr.KYCStepsCreatedAt)
		newPartialState.KYCStepsCreatedAt = &val
	}
	newPartialState.HideRanking = buildHideRanking(usr.HiddenProfileElements)
	newPartialState.VerifiedT1Referrals = usr.VerifiedT1ReferralCount
	if newPartialState.ProfilePictureName != dbUser[0].ProfilePictureName ||
		newPartialState.Username != dbUser[0].Username ||
		!strings.EqualFold(newPartialState.Country, dbUser[0].Country) ||
		newPartialState.MiningBlockchainAccountAddress != dbUser[0].MiningBlockchainAccountAddress ||
		newPartialState.BlockchainAccountAddress != dbUser[0].BlockchainAccountAddress ||
		newPartialState.HideRanking != dbUser[0].HideRanking ||
		newPartialState.VerifiedT1Referrals != dbUser[0].VerifiedT1Referrals ||
		(dbUser[0].CreatedAt.IsNil() || !newPartialState.CreatedAt.Equal(*dbUser[0].CreatedAt.Time)) ||
		!newPartialState.KYCStepsCreatedAt.Equals(dbUser[0].KYCStepsCreatedAt) ||
		!newPartialState.KYCStepsLastUpdatedAt.Equals(dbUser[0].KYCStepsLastUpdatedAt) ||
		newPartialState.KYCStepBlocked != dbUser[0].KYCStepBlocked ||
		newPartialState.KYCStepPassed != dbUser[0].KYCStepPassed {
		err = storage.Set(ctx, s.db, newPartialState)
	}

	return multierror.Append( //nolint:wrapcheck // Not Needed.
		errors.Wrapf(err, "failed to replace user:%#v", usr),
		errors.Wrapf(s.updateReferredBy(ctx, internalID, &dbUser[0].IDT0, &dbUser[0].IDTMinus1, usr.ID, usr.ReferredBy, dbUser[0].BalanceForTMinus1), "failed to updateReferredBy for user:%#v", usr),
		errors.Wrapf(s.updateUsernameKeywords(ctx, internalID, dbUser[0].Username, usr.Username), "failed to updateUsernameKeywords for oldUser:%#v, user:%#v", dbUser, usr), //nolint:lll // .
	).ErrorOrNil()
}

func (r *repository) updateReferredBy(ctx context.Context, id int64, oldIDT0, oldTMinus1 *int64, userID, referredBy string, balanceForTMinus1 float64) error {
	if referredBy == userID ||
		referredBy == "" ||
		referredBy == r.cfg.DefaultReferralName ||
		referredBy == "bogus" ||
		referredBy == "icenetwork" {
		return nil
	}
	idT0, err := GetOrInitInternalID(ctx, r.db, referredBy, r.cfg.WelcomeBonusV2Amount)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for referredBy:%v", referredBy)
	} else if (*oldIDT0 == idT0) || (*oldIDT0*-1 == idT0) {
		return nil
	}
	type (
		user struct {
			model.UserIDField
			model.DeserializedUsersKey
			model.IDT0ResettableField
			model.IDTMinus1ResettableField
		}
	)
	newPartialState := &user{DeserializedUsersKey: model.DeserializedUsersKey{ID: id}}
	if t0Referral, err2 := storage.Get[user](ctx, r.db, model.SerializedUsersKey(idT0)); err2 != nil {
		return errors.Wrapf(err2, "failed to get users entry for idT0:%v", idT0)
	} else if len(t0Referral) == 1 {
		newPartialState.IDT0 = -t0Referral[0].ID
		if t0Referral[0].IDT0 != 0 {
			if t0Referral[0].IDT0 < 0 {
				t0Referral[0].IDT0 *= -1
			}
			if tMinus1Referral, err3 := storage.Get[user](ctx, r.db, model.SerializedUsersKey(t0Referral[0].IDT0)); err3 != nil {
				return errors.Wrapf(err3, "failed to get users entry for tMinus1ID:%v", t0Referral[0].IDT0)
			} else if len(tMinus1Referral) == 1 {
				newPartialState.IDTMinus1 = -tMinus1Referral[0].ID
				if balanceForTMinus1 > 0.0 {
					results, err4 := r.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
						if *oldTMinus1 < 0 {
							*oldTMinus1 *= -1
						}
						if oldIdTMinus1Key := model.SerializedUsersKey(oldTMinus1); oldIdTMinus1Key != "" {
							if err = pipeliner.HIncrByFloat(ctx, oldIdTMinus1Key, "balance_t2_pending", -balanceForTMinus1).Err(); err != nil {
								return err
							}
						}
						newTMinus1 := tMinus1Referral[0].ID
						if newTMinus1 < 0 {
							newTMinus1 *= -1
						}
						if newIdTMinus1Key := model.SerializedUsersKey(newTMinus1); newIdTMinus1Key != "" {
							if err = pipeliner.HIncrByFloat(ctx, newIdTMinus1Key, "balance_t2_pending", balanceForTMinus1).Err(); err != nil {
								return err
							}
						}

						return nil
					})
					if err4 != nil {
						return errors.Wrapf(err4, "failed to move t2 balance from:%v to:%v", oldTMinus1, newPartialState.IDTMinus1)
					}
					errs := make([]error, 0, len(results))
					for _, result := range results {
						if err = result.Err(); err != nil {
							errs = append(errs, errors.Wrapf(err, "failed to run `%#v`", result.FullName()))
						}
					}
					if mErrs := multierror.Append(nil, errs...); mErrs.ErrorOrNil() != nil {
						return errors.Wrapf(mErrs.ErrorOrNil(), "failed to move t2 balances for id:%v,id:%v", userID, id)
					}
				}
			}
		}
	}
	if localIDT0 := newPartialState.IDT0; localIDT0 != 0 {
		if localIDT0 < 0 {
			localIDT0 *= -1
		}
		results, err4 := r.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
			if innerErr := pipeliner.HIncrBy(ctx, model.SerializedUsersKey(localIDT0), "balance_t1_welcome_bonus_pending", int64(r.cfg.WelcomeBonusV2Amount)).Err(); innerErr != nil {
				return innerErr
			}
			if innerErr := pipeliner.HIncrBy(ctx, model.SerializedUsersKey(localIDT0), "total_t1_referrals", 1).Err(); innerErr != nil {
				return innerErr
			}

			return pipeliner.HSet(ctx, newPartialState.Key(), storage.SerializeValue(newPartialState)...).Err()
		})
		if err4 != nil {
			return errors.Wrapf(err4, "failed to run TxPipelined for userID:%v", userID)
		}
		errs := make([]error, 0, len(results))
		for _, result := range results {
			if innerErr := result.Err(); innerErr != nil {
				errs = append(errs, errors.Wrapf(innerErr, "failed to run `%#v`", result.FullName()))
			}
		}
		if mErrs := multierror.Append(nil, errs...); mErrs.ErrorOrNil() != nil {
			return errors.Wrapf(mErrs.ErrorOrNil(), "failed to run TxPipelined for id:%v,id:%v", userID, id)
		}

		*oldIDT0 = newPartialState.IDT0
		*oldTMinus1 = newPartialState.IDTMinus1

		return nil
	}
	*oldIDT0 = newPartialState.IDT0
	*oldTMinus1 = newPartialState.IDTMinus1

	return errors.Wrapf(storage.Set(ctx, r.db, newPartialState), "failed to replace newPartialState:%#v", newPartialState)
}

func (r *repository) updateUsernameKeywords(
	ctx context.Context, id int64, oldUsername, newUsername string,
) error {
	if oldUsername == newUsername {
		return nil
	}
	toRemove, toAdd := r.usernameKeywords(oldUsername, newUsername)
	if len(toRemove)+len(toAdd) == 0 {
		return nil
	}
	results, err := r.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for _, keyword := range toAdd {
			if cmdErr := pipeliner.SAdd(ctx, "lookup:"+keyword, model.SerializedUsersKey(id)).Err(); cmdErr != nil {
				return cmdErr
			}
		}
		for _, keyword := range toRemove {
			if cmdErr := pipeliner.SRem(ctx, "lookup:"+keyword, model.SerializedUsersKey(id)).Err(); cmdErr != nil {
				return cmdErr
			}
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "failed to move username keywords for internalUserID:%#v", id)
	}
	errs := make([]error, 0, len(results))
	for _, result := range results {
		if err = result.Err(); err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to `%#v` for username keyword", result.FullName()))
		}
	}

	return multierror.Append(nil, errs...).ErrorOrNil()
}

func (*repository) usernameKeywords(before, after string) (toRemove, toAdd []string) {
	beforeKeywords, afterKeywords := generateUsernameKeywords(before), generateUsernameKeywords(after)
	for beforeKeyword := range beforeKeywords {
		if _, found := afterKeywords[beforeKeyword]; !found {
			toRemove = append(toRemove, beforeKeyword)
		}
	}
	for afterKeyword := range afterKeywords {
		if _, found := beforeKeywords[afterKeyword]; !found {
			toAdd = append(toAdd, afterKeyword)
		}
	}

	return toRemove, toAdd
}

func generateUsernameKeywords(username string) map[string]struct{} {
	if username == "" {
		return nil
	}
	keywords := make(map[string]struct{})
	for _, part := range append(strings.Split(username, "."), username) {
		for i := 0; i < len(part); i++ {
			keywords[part[:i+1]] = struct{}{}
			keywords[part[len(part)-1-i:]] = struct{}{}
		}
	}

	return keywords
}

func buildHideRanking(elems *users.Enum[users.HiddenProfileElement]) (hideRanking bool) {
	if elems != nil {
		for _, element := range *elems {
			if users.GlobalRankHiddenProfileElement == element {
				hideRanking = true

				break
			}
		}
	}

	return hideRanking
}

var (
	initInternalIDScript = redis.NewScript(`
local new_id = redis.call('INCR', KEYS[1])
local set_nx_reply = redis.pcall('SETNX', KEYS[2], tostring(new_id))
if type(set_nx_reply) == "table" and set_nx_reply['err'] ~= nil then
	redis.call('DECR', KEYS[1])
	return set_nx_reply
elseif set_nx_reply == 0 then
	redis.call('DECR', KEYS[1])
	return redis.error_reply('[1]race condition')
end
return new_id
`)
	initUserScript = redis.NewScript(`
local hlen_reply = redis.call('HLEN', KEYS[1])
if hlen_reply ~= 0 then
	return redis.error_reply('[2]race condition')
end
redis.call('HSETNX', KEYS[1], 'balance_total_standard', ARGV[2])
redis.call('HSETNX', KEYS[1], 'balance_total_minted', ARGV[2])
redis.call('HSETNX', KEYS[1], 'balance_solo', ARGV[2])
redis.call('HSETNX', KEYS[1], 'welcome_bonus_v2_applied', 'true')
redis.call('HSETNX', KEYS[1], 'user_id', ARGV[1])
`)
)

func GetOrInitInternalID(ctx context.Context, db storage.DB, userID string, welcomeBonus float64) (int64, error) {
	if ctx.Err() != nil {
		return 0, errors.Wrapf(ctx.Err(), "context expired")
	}
	id, err := GetInternalID(ctx, db, userID)
	if err != nil && errors.Is(err, ErrNotFound) {
		accessibleKeys := append(make([]string, 0, 1+1), "users_serial", model.SerializedUsersKey(userID))
		id, err = initInternalIDScript.EvalSha(ctx, db, accessibleKeys).Int64()
		if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
			log.Error(errors.Wrap(initInternalIDScript.Load(ctx, db).Err(), "failed to load initInternalIDScript"))

			return GetOrInitInternalID(ctx, db, userID, welcomeBonus)
		}
		if err == nil {
			accessibleKeys = append(make([]string, 0, 1), model.SerializedUsersKey(id))
			for err = errors.New("init"); ctx.Err() == nil && err != nil; {
				if err = initUserScript.EvalSha(ctx, db, accessibleKeys, userID, strconv.FormatFloat(welcomeBonus, 'f', 1, 64)).Err(); err == nil || errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "race condition") {
					if err != nil && strings.Contains(err.Error(), "race condition") {
						log.Error(errors.Wrapf(err, "[2]race condition while evaling initUserScript for userID:%v", userID))
					}
					err = nil
					break
				} else if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
					log.Error(errors.Wrap(initUserScript.Load(ctx, db).Err(), "failed to load initUserScript"))
				}
			}
		}
		err = errors.Wrapf(err, "failed to generate internalID for userID:%#v", userID)
	}
	if err != nil {
		log.Error(err)
		stdlibtime.Sleep(2 * stdlibtime.Second)

		return GetOrInitInternalID(ctx, db, userID, welcomeBonus)
	}

	return id, errors.Wrapf(err, "failed to getInternalID for userID:%#v", userID)
}

func GetInternalID(ctx context.Context, db storage.DB, userID string) (int64, error) {
	idAsString, err := db.Get(ctx, model.SerializedUsersKey(userID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, errors.Wrapf(err, "failed to get internal id for external userID:%v", userID)
	}
	if idAsString == "" {
		return 0, ErrNotFound
	}
	id, err := strconv.ParseInt(idAsString, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "internalID:%v is not numeric", idAsString)
	}

	return id, nil
}
