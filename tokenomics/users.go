// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/ice-blockchain/eskimo/users"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
	stdlibtime "time"
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
	id, err := s.getInternalID(ctx, usr.ID)
	if err != nil {
		return errors.Wrapf(err, "failed to getInternalID for user:%#v", usr)
	}
	dbUserBeforeMiningStopped, err := storage.Get[struct {
		MiningSessionSoloEndedAt *time.Time `redis:"mining_session_solo_ended_at"`
		UserID                   string     `redis:"user_id"`
	}](ctx, s.db, serializedUsersKey(id))
	if err != nil || len(dbUserBeforeMiningStopped) == 0 {
		if err == nil && len(dbUserBeforeMiningStopped) == 0 {
			err = ErrNotFound
		}

		return errors.Wrapf(err, "failed to get current state for user:%#v", usr)
	}
	if err = storage.Set(ctx, s.db, &struct {
		MiningSessionSoloStartedAt       *time.Time `redis:"mining_session_solo_started_at"`
		MiningSessionSoloEndedAt         *time.Time `redis:"mining_session_solo_ended_at"`
		PreviousMiningSessionSoloEndedAt *time.Time `redis:"previous_mining_session_solo_ended_at"`
		deserializedUsersKey
	}{
		deserializedUsersKey:             deserializedUsersKey{ID: id},
		PreviousMiningSessionSoloEndedAt: time.Now(),
	}); err != nil {
		return errors.Wrapf(err, "failed to manually stop mining due to user deletion message for user:%#v", usr)
	}
	stdlibtime.Sleep(stdlibtime.Second)
	dbUserAfterMiningStopped, err := storage.Get[struct {
		UserID            string  `redis:"user_id"`
		IDT0              int64   `redis:"id_t0"`
		IDTMinus1         int64   `redis:"id_tminus1"`
		BalanceForT0      float64 `redis:"balance_for_t0"`
		BalanceForTMinus1 float64 `redis:"balance_for_tminus1"`
	}](ctx, s.db, serializedUsersKey(id))
	if err != nil || len(dbUserAfterMiningStopped) == 0 {
		if err == nil && len(dbUserAfterMiningStopped) == 0 {
			err = ErrNotFound
		}

		return errors.Wrapf(err, "failed to get current state for user:%#v", usr)
	}
	results, err := s.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		if idT0Key := serializedUsersKey(dbUserAfterMiningStopped[0].IDT0); idT0Key != "" {
			if dbUserAfterMiningStopped[0].BalanceForT0 > 0.0 {
				if err = pipeliner.HIncrByFloat(ctx, idT0Key, "balance_t1", -dbUserAfterMiningStopped[0].BalanceForT0).Err(); err != nil {
					return err
				}
			}
			if !dbUserBeforeMiningStopped[0].MiningSessionSoloEndedAt.IsNil() &&
				dbUserBeforeMiningStopped[0].MiningSessionSoloEndedAt.After(*time.Now().Time) {
				if err = pipeliner.HIncrBy(ctx, idT0Key, "active_t1_referrals", -1).Err(); err != nil {
					return err
				}
			}
		}
		if idTMinus1Key := serializedUsersKey(dbUserAfterMiningStopped[0].IDTMinus1); idTMinus1Key != "" {
			if dbUserAfterMiningStopped[0].BalanceForTMinus1 > 0.0 {
				if err = pipeliner.HIncrByFloat(ctx, idTMinus1Key, "balance_t2", -dbUserAfterMiningStopped[0].BalanceForTMinus1).Err(); err != nil {
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
		_, toAdd := s.usernameKeywords(usr.Username, "")
		for _, usernameKeyword := range toAdd {
			if err = pipeliner.SRem(ctx, usernameKeyword, id).Err(); err != nil {
				return err
			}
		}
		if err = pipeliner.ZRem(ctx, "top_miners", id).Err(); err != nil {
			return err
		}
		if err = pipeliner.Del(ctx, serializedUsersKey(id), serializedUsersKey(usr.ID)).Err(); err != nil {
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
	internalID, err := s.getOrInitInternalID(ctx, usr.ID)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for user:%#v", usr)
	}
	type (
		userPartialState struct {
			UserID                         string `redis:"user_id"`
			ProfilePictureName             string `redis:"profile_picture_name"`
			Username                       string `redis:"username"`
			MiningBlockchainAccountAddress string `redis:"mining_blockchain_account_address"`
			BlockchainAccountAddress       string `redis:"blockchain_account_address"`
			deserializedUsersKey
			IDT0        int64 `redis:"id_t0"`
			HideRanking bool  `redis:"hide_ranking"`
		}
	)
	dbUser, err := storage.Get[userPartialState](ctx, s.db, serializedUsersKey(internalID))
	if err != nil || len(dbUser) == 0 {
		if err == nil && len(dbUser) == 0 {
			err = errors.Errorf("missing state for user:%#v", usr)
		}

		return errors.Wrapf(err, "failed to get current user for internalID:%v", internalID)
	}
	newPartialState := &userPartialState{
		deserializedUsersKey:           deserializedUsersKey{ID: internalID},
		IDT0:                           dbUser[0].IDT0,
		UserID:                         usr.ID,
		ProfilePictureName:             s.pictureClient.StripDownloadURL(usr.ProfilePictureURL),
		Username:                       usr.Username,
		MiningBlockchainAccountAddress: usr.MiningBlockchainAccountAddress,
		BlockchainAccountAddress:       usr.BlockchainAccountAddress,
		HideRanking:                    s.hideRanking(usr),
	}

	return multierror.Append( //nolint:wrapcheck // Not Needed.
		errors.Wrapf(storage.Set(ctx, s.db, newPartialState), "failed to replace user:%#v", usr),
		errors.Wrapf(s.updateReferredBy(ctx, dbUser[0].ID, dbUser[0].IDT0, usr.ID, usr.ReferredBy), "failed to updateReferredBy for user:%#v", usr),
		errors.Wrapf(s.updateUsernameKeywords(ctx, dbUser[0].ID, dbUser[0].Username, usr.Username), "failed to updateUsernameKeywords for oldUser:%#v, user:%#v", dbUser, usr), //nolint:lll // .
	).ErrorOrNil()
}

func (s *usersTableSource) updateReferredBy(ctx context.Context, id, oldIDT0 int64, userID, referredBy string) error {
	if referredBy == userID ||
		referredBy == "" ||
		referredBy == "bogus" ||
		referredBy == "icenetwork" {
		return nil
	}
	idT0, err := s.getOrInitInternalID(ctx, referredBy)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for referredBy:%v", referredBy)
	} else if oldIDT0 == idT0 {
		return nil
	}
	type (
		t0Changed struct {
			MiningSessionT0StartedAt            *time.Time `redis:"mining_session_t0_started_at"`
			MiningSessionTMinus1StartedAt       *time.Time `redis:"mining_session_tminus1_started_at"`
			MiningSessionT0EndedAt              *time.Time `redis:"mining_session_t0_ended_at"`
			MiningSessionTMinus1EndedAt         *time.Time `redis:"mining_session_tminus1_ended_at"`
			PreviousMiningSessionT0EndedAt      *time.Time `redis:"previous_mining_session_t0_ended_at"`
			PreviousMiningSessionTMinus1EndedAt *time.Time `redis:"previous_mining_session_tminus1_ended_at"`
			ResurrectT0UsedAt                   *time.Time `redis:"resurrect_t0_used_at"`
			ResurrectTMinus1UsedAt              *time.Time `redis:"resurrect_tminus1_used_at"`
			deserializedUsersKey
			IDT0                   int64   `redis:"id_t0"`
			IDTMinus1              int64   `redis:"id_tminus1"`
			BalanceT0              int64   `redis:"balance_t0"`
			BalanceForT0           float64 `redis:"balance_for_t0"`
			BalanceForTMinus1      float64 `redis:"balance_for_tminus1"`
			SlashingRateT0         float64 `redis:"slashing_rate_t0"`
			SlashingRateForT0      float64 `redis:"slashing_rate_for_t0"`
			SlashingRateForTMinus1 float64 `redis:"slashing_rate_for_tminus1"`
		}
		referral struct {
			MiningSessionSoloStartedAt       *time.Time `redis:"mining_session_solo_started_at"`
			MiningSessionSoloEndedAt         *time.Time `redis:"mining_session_solo_ended_at"`
			PreviousMiningSessionSoloEndedAt *time.Time `redis:"previous_mining_session_solo_ended_at"`
			ResurrectSoloUsedAt              *time.Time `redis:"resurrect_solo_used_at"`
			deserializedUsersKey
			IDT0 int64 `redis:"id_t0"`
		}
	)
	newPartialState := &t0Changed{deserializedUsersKey: deserializedUsersKey{ID: id}}
	if t0Referral, err2 := storage.Get[referral](ctx, s.db, serializedUsersKey(idT0)); err2 != nil {
		return errors.Wrapf(err2, "failed to get users entry for idT0:%v", idT0)
	} else if len(t0Referral) == 1 {
		newPartialState.IDT0 = t0Referral[0].ID
		newPartialState.ResurrectT0UsedAt = t0Referral[0].ResurrectSoloUsedAt
		newPartialState.MiningSessionT0StartedAt = t0Referral[0].MiningSessionSoloStartedAt
		newPartialState.MiningSessionT0EndedAt = t0Referral[0].MiningSessionSoloEndedAt
		newPartialState.PreviousMiningSessionT0EndedAt = t0Referral[0].PreviousMiningSessionSoloEndedAt
		if t0Referral[0].IDT0 > 0 {
			if tMinus1Referral, err3 := storage.Get[referral](ctx, s.db, serializedUsersKey(t0Referral[0].IDT0)); err3 != nil {
				return errors.Wrapf(err3, "failed to get users entry for tMinus1ID:%v", t0Referral[0].IDT0)
			} else if len(tMinus1Referral) == 1 {
				newPartialState.IDTMinus1 = tMinus1Referral[0].ID
				newPartialState.ResurrectTMinus1UsedAt = tMinus1Referral[0].ResurrectSoloUsedAt
				newPartialState.MiningSessionTMinus1StartedAt = tMinus1Referral[0].MiningSessionSoloStartedAt
				newPartialState.MiningSessionTMinus1EndedAt = tMinus1Referral[0].MiningSessionSoloEndedAt
				newPartialState.PreviousMiningSessionTMinus1EndedAt = tMinus1Referral[0].PreviousMiningSessionSoloEndedAt
			}
		}
	}
	if err = storage.Set(ctx, s.db, newPartialState); err != nil {
		return errors.Wrapf(err, "failed to replace newPartialState:%#v", newPartialState)
	}
	if oldIDT0 > 0 {
		stdlibtime.Sleep(stdlibtime.Second)
	}
	dbUserAfterT0Changed, err := storage.Get[struct {
		IDT0                   int64   `redis:"id_t0"`
		IDTMinus1              int64   `redis:"id_tminus1"`
		BalanceT0              int64   `redis:"balance_t0"`
		BalanceForT0           float64 `redis:"balance_for_t0"`
		BalanceForTMinus1      float64 `redis:"balance_for_tminus1"`
		SlashingRateT0         float64 `redis:"slashing_rate_t0"`
		SlashingRateForT0      float64 `redis:"slashing_rate_for_t0"`
		SlashingRateForTMinus1 float64 `redis:"slashing_rate_for_tminus1"`
	}](ctx, s.db, serializedUsersKey(id))
	if err != nil || len(dbUserAfterT0Changed) == 0 {
		if err == nil && len(dbUserAfterT0Changed) == 0 {
			err = errors.Errorf("missing state for userID:%v", id)
		}

		return errors.Wrapf(err, "failed to get[2] current user for internalID:%v", id)
	}
	if dbUserAfterT0Changed[0].IDT0 != newPartialState.IDT0 ||
		dbUserAfterT0Changed[0].IDTMinus1 != newPartialState.IDTMinus1 ||
		dbUserAfterT0Changed[0].BalanceT0 != 0.0 ||
		dbUserAfterT0Changed[0].BalanceForT0 != 0.0 ||
		dbUserAfterT0Changed[0].BalanceForTMinus1 != 0.0 ||
		dbUserAfterT0Changed[0].SlashingRateForT0 != 0.0 ||
		dbUserAfterT0Changed[0].SlashingRateT0 != 0.0 ||
		dbUserAfterT0Changed[0].SlashingRateForTMinus1 != 0.0 {
		return errors.Wrapf(storage.Set(ctx, s.db, newPartialState), "failed[2] to replace newPartialState:%#v", newPartialState)
	}

	return nil
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
			if cmdErr := pipeliner.SAdd(ctx, keyword, id).Err(); cmdErr != nil {
				return cmdErr
			}
		}
		for _, keyword := range toRemove {
			if cmdErr := pipeliner.SRem(ctx, keyword, id).Err(); cmdErr != nil {
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
	return redis.error_reply('race condition')
end
return new_id
`)
	initUserScript = redis.NewScript(`
local hlen_reply = redis.call('HLEN', KEYS[1])
if hlen_reply ~= 0 then
	return redis.error_reply('race condition')
end
redis.call('HSETNX', KEYS[1], 'balance_total', 10.0)
redis.call('HSETNX', KEYS[1], 'balance_total_minted', 10.0)
redis.call('HSETNX', KEYS[1], 'balance_solo', 10.0)
redis.call('HSETNX', KEYS[1], 'user_id', ARGV[1])
redis.call('ZADD', 'top_miners', 'NX', 10.0, KEYS[1])
`)
)

func (r *repository) getOrInitInternalID(ctx context.Context, userID string) (int64, error) {
	if ctx.Err() != nil {
		return 0, errors.Wrapf(ctx.Err(), "context expired")
	}
	id, err := r.getInternalID(ctx, userID)
	if err != nil && errors.Is(err, ErrNotFound) {
		accessibleKeys := append(make([]string, 0, 1+1), "users_serial", serializedUsersKey(userID))
		id, err = initInternalIDScript.EvalSha(ctx, r.db, accessibleKeys).Int64()
		if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
			log.Error(errors.Wrap(initInternalIDScript.Load(ctx, r.db).Err(), "failed to load initInternalIDScript"))

			return r.getOrInitInternalID(ctx, userID)
		}
		if err == nil {
			accessibleKeys = append(make([]string, 0, 1), serializedUsersKey(id))
			for ctx.Err() == nil {
				if err = initUserScript.EvalSha(ctx, r.db, accessibleKeys, userID).Err(); err == nil || errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "race condition") {
					if err != nil && strings.Contains(err.Error(), "race condition") {
						log.Error(errors.Wrapf(err, "race condition while evaling initUserScript for userID:%v", userID))
					}
					err = nil
					break
				} else if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
					log.Error(errors.Wrap(initUserScript.Load(ctx, r.db).Err(), "failed to load initUserScript"))
				}
			}
		}
		err = errors.Wrapf(err, "failed to generate internalID for userID:%#v", userID)
	}
	if err != nil {
		log.Error(err)

		return r.getOrInitInternalID(ctx, userID)
	}

	return id, errors.Wrapf(err, "failed to getInternalID for userID:%#v", userID)
}

func (r *repository) getInternalID(ctx context.Context, userID string) (int64, error) {
	idAsString, err := r.db.Get(ctx, serializedUsersKey(userID)).Result()
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

func (k *deserializedUsersKey) Key() string {
	if k == nil || k.ID == 0 {
		return ""
	}

	return serializedUsersKey(k.ID)
}

func (k *deserializedUsersKey) SetKey(val string) {
	if val == "" || val == "users:" {
		return
	}
	if val[0] == 'u' {
		val = val[6:]
	}
	var err error
	k.ID, err = strconv.ParseInt(val, 10, 64)
	log.Panic(err)
}

func serializedUsersKey(val any) string {
	switch typedVal := val.(type) {
	case string:
		if typedVal == "" {
			return ""
		}

		return "users:" + typedVal
	case int64:
		if typedVal == 0 {
			return ""
		}

		return "users:" + strconv.FormatInt(typedVal, 10)
	default:
		panic(fmt.Sprintf("%#v cannot be used as users key", val))
	}
}
