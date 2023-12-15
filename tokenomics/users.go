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
	id, err := GetOrInitInternalID(ctx, s.db, usr.ID)
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
			if amount := dbUserAfterMiningStopped[0].BalanceForT0; amount > 0.0 {
				if err = pipeliner.HIncrByFloat(ctx, idT0Key, "balance_t1_pending", -amount).Err(); err != nil {
					return err
				}
			}
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
	errs := make([]error, 0, len(results))
	for _, result := range results {
		if err = result.Err(); err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to run `%#v`", result.FullName()))
		}
	}

	return errors.Wrapf(multierror.Append(nil, errs...).ErrorOrNil(), "failed to delete userID:%v,id:%v", usr.ID, id)
}

func (s *usersTableSource) replaceUser(ctx context.Context, usr *users.User) error { //nolint:funlen // .
	internalID, err := GetOrInitInternalID(ctx, s.db, usr.ID)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for user:%#v", usr)
	}
	type (
		user struct {
			KYCState
			model.UserIDField
			model.ProfilePictureNameField
			model.UsernameField
			model.MiningBlockchainAccountAddressField
			model.BlockchainAccountAddressField
			model.BalanceForTMinus1Field
			model.DeserializedUsersKey
			model.IDT0Field
			model.IDTMinus1Field
			model.HideRankingField
		}
	)
	dbUser, err := storage.Get[user](ctx, s.db, model.SerializedUsersKey(internalID))
	if err != nil || len(dbUser) == 0 {
		if err == nil && len(dbUser) == 0 {
			err = errors.Errorf("missing state for user:%#v", usr)
		}

		return errors.Wrapf(err, "failed to get current user for internalID:%v", internalID)
	}
	newPartialState := new(user)
	newPartialState.ID = internalID
	newPartialState.ProfilePictureName = s.pictureClient.StripDownloadURL(usr.ProfilePictureURL)
	newPartialState.Username = usr.Username
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
	newPartialState.HideRanking = s.hideRanking(usr)
	if newPartialState.ProfilePictureName != dbUser[0].ProfilePictureName ||
		newPartialState.Username != dbUser[0].Username ||
		newPartialState.MiningBlockchainAccountAddress != dbUser[0].MiningBlockchainAccountAddress ||
		newPartialState.BlockchainAccountAddress != dbUser[0].BlockchainAccountAddress ||
		newPartialState.HideRanking != dbUser[0].HideRanking ||
		!newPartialState.KYCStepsCreatedAt.Equals(dbUser[0].KYCStepsCreatedAt) ||
		!newPartialState.KYCStepsLastUpdatedAt.Equals(dbUser[0].KYCStepsLastUpdatedAt) ||
		newPartialState.KYCStepBlocked != dbUser[0].KYCStepBlocked ||
		newPartialState.KYCStepPassed != dbUser[0].KYCStepPassed {
		err = storage.Set(ctx, s.db, newPartialState)
	}

	return multierror.Append( //nolint:wrapcheck // Not Needed.
		errors.Wrapf(err, "failed to replace user:%#v", usr),
		errors.Wrapf(s.updateReferredBy(ctx, internalID, dbUser[0].IDT0, dbUser[0].IDTMinus1, usr.ID, usr.ReferredBy, dbUser[0].BalanceForTMinus1), "failed to updateReferredBy for user:%#v", usr),
		errors.Wrapf(s.updateUsernameKeywords(ctx, internalID, dbUser[0].Username, usr.Username), "failed to updateUsernameKeywords for oldUser:%#v, user:%#v", dbUser, usr), //nolint:lll // .
	).ErrorOrNil()
}

func (s *usersTableSource) updateReferredBy(ctx context.Context, id, oldIDT0, oldTMinus1 int64, userID, referredBy string, balanceForTMinus1 float64) error {
	if referredBy == userID ||
		referredBy == "" ||
		referredBy == "bogus" ||
		referredBy == "icenetwork" {
		return nil
	}
	idT0, err := GetOrInitInternalID(ctx, s.db, referredBy)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for referredBy:%v", referredBy)
	} else if (oldIDT0 == idT0) || (oldIDT0*-1 == idT0) {
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
	if t0Referral, err2 := storage.Get[user](ctx, s.db, model.SerializedUsersKey(idT0)); err2 != nil {
		return errors.Wrapf(err2, "failed to get users entry for idT0:%v", idT0)
	} else if len(t0Referral) == 1 {
		newPartialState.IDT0 = -t0Referral[0].ID
		if t0Referral[0].IDT0 != 0 {
			if t0Referral[0].IDT0 < 0 {
				t0Referral[0].IDT0 *= -1
			}
			if tMinus1Referral, err3 := storage.Get[user](ctx, s.db, model.SerializedUsersKey(t0Referral[0].IDT0)); err3 != nil {
				return errors.Wrapf(err3, "failed to get users entry for tMinus1ID:%v", t0Referral[0].IDT0)
			} else if len(tMinus1Referral) == 1 {
				newPartialState.IDTMinus1 = -tMinus1Referral[0].ID
				if balanceForTMinus1 > 0.0 {
					results, err4 := s.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
						if oldTMinus1 < 0 {
							oldTMinus1 *= -1
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
					if errs := multierror.Append(nil, errs...); errs.ErrorOrNil() != nil {
						return errors.Wrapf(errs.ErrorOrNil(), "failed to move t2 balances for id:%v,id:%v", userID, id)
					}
				}
			}
		}
	}

	return errors.Wrapf(storage.Set(ctx, s.db, newPartialState), "failed to replace newPartialState:%#v", newPartialState)
}

func (s *usersTableSource) updateUsernameKeywords(
	ctx context.Context, id int64, oldUsername, newUsername string,
) error {
	if oldUsername == newUsername {
		return nil
	}
	toRemove, toAdd := s.usernameKeywords(oldUsername, newUsername)
	if len(toRemove)+len(toAdd) == 0 {
		return nil
	}
	results, err := s.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
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

func (*usersTableSource) usernameKeywords(before, after string) (toRemove, toAdd []string) {
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

func (*usersTableSource) hideRanking(usr *users.User) (hideRanking bool) {
	if usr.HiddenProfileElements != nil {
		for _, element := range *usr.HiddenProfileElements {
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
redis.call('HSETNX', KEYS[1], 'balance_total_standard', 10.0)
redis.call('HSETNX', KEYS[1], 'balance_total_minted', 10.0)
redis.call('HSETNX', KEYS[1], 'balance_solo', 10.0)
redis.call('HSETNX', KEYS[1], 'user_id', ARGV[1])
`)
)

func GetOrInitInternalID(ctx context.Context, db storage.DB, userID string) (int64, error) {
	if ctx.Err() != nil {
		return 0, errors.Wrapf(ctx.Err(), "context expired")
	}
	id, err := GetInternalID(ctx, db, userID)
	if err != nil && errors.Is(err, ErrNotFound) {
		accessibleKeys := append(make([]string, 0, 1+1), "users_serial", model.SerializedUsersKey(userID))
		id, err = initInternalIDScript.EvalSha(ctx, db, accessibleKeys).Int64()
		if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
			log.Error(errors.Wrap(initInternalIDScript.Load(ctx, db).Err(), "failed to load initInternalIDScript"))

			return GetOrInitInternalID(ctx, db, userID)
		}
		if err == nil {
			accessibleKeys = append(make([]string, 0, 1), model.SerializedUsersKey(id))
			for err = errors.New("init"); ctx.Err() == nil && err != nil; {
				if err = initUserScript.EvalSha(ctx, db, accessibleKeys, userID).Err(); err == nil || errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "race condition") {
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

		return GetOrInitInternalID(ctx, db, userID)
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
