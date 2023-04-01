// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/ice-blockchain/eskimo/users"
	"github.com/ice-blockchain/wintr/coin"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage"
	storagev2 "github.com/ice-blockchain/wintr/connectors/storage/v2"
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

func (s *usersTableSource) deleteUser(ctx context.Context, usr *users.User) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	if err := s.removeBalanceFromT0AndTMinus1(ctx, usr); err != nil {
		return errors.Wrapf(err, "failed to removeBalanceFromT0AndTMinus1 for user:%#v", usr)
	}
	affectedRows, err := storagev2.Exec(ctx, s.dbV2, `DELETE FROM users WHERE user_id = $1`, usr.ID)
	if err != nil || affectedRows == 0 {
		return errors.Wrapf(err, "failed to delete userID:%v", usr.ID)
	}

	return nil
}

func (s *usersTableSource) removeBalanceFromT0AndTMinus1(ctx context.Context, usr *users.User) error { //nolint:funlen // .
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	sql := fmt.Sprintf(`SELECT reverse_t0_balance.amount,
								reverse_tminus1_balance.amount,
								negative_t0_balance.amount,
								negative_tminus1_balance.amount,
				   			    t0.user_id,
								tminus1.user_id 
						FROM users u
							 JOIN users t0
							   ON t0.user_id = u.referred_by
							  AND t0.user_id != u.user_id
							 JOIN users tminus1
							   ON tminus1.user_id = t0.referred_by
							  AND tminus1.user_id != t0.user_id
						LEFT JOIN balances_%[1]v reverse_t0_balance
							   ON reverse_t0_balance.user_id = u.user_id
							  AND reverse_t0_balance.negative = FALSE
							  AND reverse_t0_balance.type = %[2]v
							  AND reverse_t0_balance.type_detail =  '%[3]v_' || t0.user_id
						LEFT JOIN balances_%[1]v reverse_tminus1_balance
							   ON reverse_tminus1_balance.user_id = u.user_id
							  AND reverse_tminus1_balance.negative = FALSE
							  AND reverse_tminus1_balance.type = %[2]v
							  AND reverse_tminus1_balance.type_detail =  '%[4]v_' || tminus1.user_id
						LEFT JOIN balances_%[1]v negative_t0_balance
							   ON negative_t0_balance.user_id = u.user_id
							  AND negative_t0_balance.negative = TRUE
							  AND negative_t0_balance.type = %[2]v
							  AND negative_t0_balance.type_detail =  '%[3]v_' || t0.user_id
						LEFT JOIN balances_%[1]v negative_tminus1_balance
							   ON negative_tminus1_balance.user_id = u.user_id
							  AND negative_tminus1_balance.negative = TRUE
							  AND negative_tminus1_balance.type = %[2]v
							  AND negative_tminus1_balance.type_detail =  '%[4]v_' || tminus1.user_id
					    WHERE u.user_id = $1`,
		usr.HashCode%uint64(s.cfg.WorkerCount),
		totalNoPreStakingBonusBalanceType,
		reverseT0BalanceTypeDetail,
		reverseTMinus1BalanceTypeDetail)
	type resp struct {
		TotalReverseT0Amount, TotalReverseTMinus1Amount,
		NegativeReverseT0Amount, NegativeReverseTMinus1Amount *coin.ICEFlake
		T0UserID, TMinus1UserID string
	}
	res, err := storagev2.Select[resp](ctx, s.dbV2, sql, usr.ID)
	if err != nil {
		return errors.Wrapf(err, "failed to get reverse t0 and t-1 balance information for userID:%v", usr.ID)
	}
	if len(res) == 0 {
		return nil
	}
	cmds := make([]*AddBalanceCommand, 0, 1+1+1+1)
	if !res[0].TotalReverseT0Amount.IsZero() {
		cmds = append(cmds, &AddBalanceCommand{
			Balances: &Balances[coin.ICEFlake]{
				T1:     res[0].TotalReverseT0Amount,
				UserID: res[0].T0UserID,
			},
			EventID: fmt.Sprintf("t1_referral_account_deletion_positive_balance_%v", usr.ID),
		})
	}
	if !res[0].NegativeReverseT0Amount.IsZero() {
		negative := true
		cmds = append(cmds, &AddBalanceCommand{
			Balances: &Balances[coin.ICEFlake]{
				T1:     res[0].NegativeReverseT0Amount,
				UserID: res[0].T0UserID,
			},
			EventID:  fmt.Sprintf("t1_referral_account_deletion_negative_balance_%v", usr.ID),
			Negative: &negative,
		})
	}
	if !res[0].TotalReverseTMinus1Amount.IsZero() {
		cmds = append(cmds, &AddBalanceCommand{
			Balances: &Balances[coin.ICEFlake]{
				T2:     res[0].TotalReverseTMinus1Amount,
				UserID: res[0].TMinus1UserID,
			},
			EventID: fmt.Sprintf("t2_referral_account_deletion_positive_balance_%v", usr.ID),
		})
	}
	if !res[0].NegativeReverseTMinus1Amount.IsZero() {
		negative := true
		cmds = append(cmds, &AddBalanceCommand{
			Balances: &Balances[coin.ICEFlake]{
				T2:     res[0].NegativeReverseTMinus1Amount,
				UserID: res[0].TMinus1UserID,
			},
			EventID:  fmt.Sprintf("t2_referral_account_deletion_negative_balance_%v", usr.ID),
			Negative: &negative,
		})
	}

	return errors.Wrapf(executeBatchConcurrently(ctx, s.sendAddBalanceCommandMessage, cmds), "failed to sendAddBalanceCommandMessages for %#v", cmds)
}

func (s *usersTableSource) replaceUser(ctx context.Context, usr *users.User) (err error) {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	if err = s.updateUser(ctx, usr); err != nil {
		if errors.Is(err, storagev2.ErrNotFound) {
			err = errors.Wrapf(s.insertUser(ctx, usr), "failed to insert user:%#v", usr)
		}
	}

	return errors.Wrapf(err, "failed to update user:%#v", usr)
}

func (s *usersTableSource) updateUser(ctx context.Context, usr *users.User) (err error) { //nolint:funlen // .
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	sql := `UPDATE users SET updated_at = $1, referred_by = $2, username = $3, first_name = $4, last_name = $5, 
						     profile_picture_name = $6, mining_blockchain_account_address = $7,
							 blockchain_account_address = $8, hide_ranking = $9, verified = $10
				   WHERE user_id = $11`
	verified := false
	if usr.Verified != nil && *usr.Verified {
		verified = true
	}
	affectedRows, err := storagev2.Exec(ctx, s.dbV2, sql, usr.UpdatedAt.Time, usr.ReferredBy, usr.Username, usr.FirstName, usr.LastName,
		usr.ProfilePictureURL, usr.MiningBlockchainAccountAddress, usr.BlockchainAccountAddress, s.hideRanking(usr), verified, usr.ID)
	if err == nil && affectedRows == 0 {
		err = storagev2.ErrNotFound
	}
	if err == nil {
		if err = s.updateBlockchainBalanceSynchronizationWorkerBlockchainAccountAddress(ctx, usr); err != nil {
			err = errors.Wrapf(err, "failed to updateBlockchainBalanceSynchronizationWorkerBlockchainAccountAddress for usr:%#v", usr)
		}
	}

	return err
}

func (s *usersTableSource) insertUser(ctx context.Context, usr *users.User) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	sql := `INSERT INTO users(created_at, updated_at, user_id, referred_by, username,
							 first_name, last_name, profile_picture_name, mining_blockchain_account_address, blockchain_account_address,
							 hash_code, hide_ranking, verified)
					    VALUES($1, $2, $3, $4,  $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	u := s.user(usr)
	affectedRows, err := storagev2.Exec(ctx, s.dbV2, sql, u.CreatedAt.Time, u.UpdatedAt.Time, u.UserID, u.ReferredBy, u.Username, u.FirstName, u.LastName,
		u.ProfilePictureURL, u.MiningBlockchainAccountAddress, u.BlockchainAccountAddress, int64(u.HashCode), u.HideRanking, u.Verified)
	if err != nil || affectedRows == 0 {
		if errors.Is(err, storage.ErrDuplicate) {
			return s.updateUser(ctx, usr)
		}

		return errors.Wrapf(err, "failed to insert user %#v", usr)
	}
	if err := s.doAfterCreate(ctx, usr); err != nil {
		revertCtx, cancel := context.WithTimeout(context.Background(), requestDeadline)
		defer cancel()
		revertErr := errors.Wrapf(s.deleteUser(revertCtx, usr), //nolint:contextcheck // It might be cancelled.
			"failed to delete userID:%v as a rollback for failed doAfterCreate", usr.ID)
		if revertErr != nil && errors.Is(revertErr, storagev2.ErrNotFound) {
			revertErr = nil
		}

		return multierror.Append( //nolint:wrapcheck // Not needed.
			errors.Wrapf(err, "failed to run doAfterCreate for:%#v", usr),
			revertErr,
		).ErrorOrNil()
	}

	return nil
}

func (s *usersTableSource) user(usr *users.User) *user {
	verified := false
	if usr.Verified != nil && *usr.Verified {
		verified = true
	}

	return &user{
		CreatedAt:                      usr.CreatedAt,
		UpdatedAt:                      usr.UpdatedAt,
		UserID:                         usr.ID,
		ReferredBy:                     usr.ReferredBy,
		Username:                       usr.Username,
		FirstName:                      usr.FirstName,
		LastName:                       usr.LastName,
		ProfilePictureURL:              s.pictureClient.StripDownloadURL(usr.ProfilePictureURL),
		MiningBlockchainAccountAddress: usr.MiningBlockchainAccountAddress,
		BlockchainAccountAddress:       usr.BlockchainAccountAddress,
		HashCode:                       usr.HashCode,
		HideRanking:                    s.hideRanking(usr),
		Verified:                       verified,
	}
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

func (s *usersTableSource) doAfterCreate(ctx context.Context, usr *users.User) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	if err := s.initializeBalanceRecalculationWorker(ctx, usr); err != nil {
		return errors.Wrapf(err, "failed to initializeBalanceRecalculationWorker for %#v", usr)
	}

	if err := s.initializeMiningRatesRecalculationWorker(ctx, usr); err != nil {
		return errors.Wrapf(err, "failed to initializeMiningRatesRecalculationWorker for %#v", usr)
	}

	if err := s.initializeBlockchainBalanceSynchronizationWorker(ctx, usr); err != nil {
		return errors.Wrapf(err, "failed to initializeBlockchainBalanceSynchronizationWorker for %#v", usr)
	}

	if err := s.initializeExtraBonusProcessingWorker(ctx, usr); err != nil {
		return errors.Wrapf(err, "failed to initializeExtraBonusProcessingWorker for %#v", usr)
	}

	return errors.Wrapf(s.awardRegistrationICECoinsBonus(ctx, usr), "failed to awardRegistrationBonus for %#v", usr)
}

func (s *usersTableSource) awardRegistrationICECoinsBonus(ctx context.Context, usr *users.User) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	cmd := &AddBalanceCommand{
		Balances: &Balances[coin.ICEFlake]{
			Total:  coin.NewAmountUint64(registrationICEFlakeBonusAmount),
			UserID: usr.ID,
		},
		EventID: "registration_ice_bonus",
	}

	return errors.Wrapf(s.sendAddBalanceCommandMessage(ctx, cmd), "failed to sendAddBalanceCommandMessage for %#v", cmd)
}
