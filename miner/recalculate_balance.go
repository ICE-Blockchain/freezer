// SPDX-License-Identifier: ice License 1.0

package miner

import (
	"context"
	"sort"
	stdlibtime "time"

	"github.com/pkg/errors"

	"github.com/ice-blockchain/eskimo/users"
	"github.com/ice-blockchain/freezer/tokenomics"
	storagePG "github.com/ice-blockchain/wintr/connectors/storage/v2"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

const (
	maxLimit int64 = 10000
)

var (
	errWrongInternalID = errors.New("can't get internal id")
)

type (
	UserID string
	pgUser struct {
		Active         *users.NotExpired
		ID, ReferredBy UserID
		ReferralType   string
	}
	pgUserCreated struct {
		ID        UserID
		CreatedAt *time.Time
	}

	splittedAdoptionByRange struct {
		TimePoint      *time.Time
		BaseMiningRate float64
	}

	historyRangeTime struct {
		MiningSessionSoloStartedAt         *time.Time
		MiningSessionSoloEndedAt           *time.Time
		MiningSessionSoloLastStartedAt     *time.Time
		MiningSessionSoloPreviouslyEndedAt *time.Time
		CreatedAt                          *time.Time
		ResurrectSoloUsedAt                *time.Time
		SlashingRateSolo                   float64
		BalanceSolo                        float64
		BalanceT1Pending                   float64
		BalanceT1PendingApplied            float64
		BalanceT2Pending                   float64
		BalanceT2PendingApplied            float64
	}
)

func (m *miner) getUsers(ctx context.Context, users []*user) (map[int64]*pgUserCreated, error) {
	var (
		userIDs []string
		offset  int64 = 0
		result        = make(map[int64]*pgUserCreated, len(users))
	)
	for _, val := range users {
		userIDs = append(userIDs, val.UserID)
	}
	for {
		sql := `SELECT
					id,
					created_at
				FROM users
				WHERE id = ANY($1)
				LIMIT $2 OFFSET $3`
		rows, err := storagePG.Select[pgUserCreated](ctx, m.dbPG, sql, userIDs, maxLimit, offset)
		if err != nil {
			return nil, errors.Wrap(err, "can't get users from pg")
		}
		if len(rows) == 0 {
			break
		}
		offset += maxLimit
		for _, row := range rows {
			id, err := tokenomics.GetInternalID(ctx, m.db, string(row.ID))
			if err != nil {
				log.Error(errWrongInternalID, row.ID)

				continue
			}
			result[id] = row
		}
	}

	return result, nil
}

func (m *miner) collectTiers(ctx context.Context, users []*user) (map[int64][]int64, map[int64][]int64, map[int64]uint64, map[int64]uint64, error) {
	var (
		referredByIDs                  []string
		offset                         int64 = 0
		now                                  = time.Now()
		t1ActiveCounts, t2ActiveCounts       = make(map[int64]uint64), make(map[int64]uint64)
		t1Referrals, t2Referrals             = make(map[int64][]int64), make(map[int64][]int64)
	)
	for _, val := range users {
		referredByIDs = append(referredByIDs, val.UserID)
	}
	for {
		sql := `SELECT * FROM(
					SELECT
						id,
						referred_by,
						'T1' AS referral_type,
						(CASE 
							WHEN COALESCE(last_mining_ended_at, to_timestamp(1)) > $1
								THEN COALESCE(last_mining_ended_at, to_timestamp(1))
								ELSE NULL
						END) 														  AS active
					FROM users
					WHERE referred_by = ANY($2)
						AND referred_by != id
						AND username != id
					UNION
					SELECT
						t2.id AS id,
						t0.id AS referred_by,
						'T2'  AS referral_type,
						(CASE 
							WHEN COALESCE(t2.last_mining_ended_at, to_timestamp(1)) > $1
								THEN COALESCE(t2.last_mining_ended_at, to_timestamp(1))
								ELSE NULL
						END) 														  AS active
					FROM users t0
						JOIN users t1
							ON t1.referred_by = t0.id
						JOIN users t2
							ON t2.referred_by = t1.id
					WHERE t0.id = ANY($2)
						AND t2.referred_by != t2.id
						AND t2.username != t2.id
				) X
				LIMIT $3 OFFSET $4`
		rows, err := storagePG.Select[pgUser](ctx, m.dbPG, sql, now.Time, referredByIDs, maxLimit, offset)
		if err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "can't get referrals from pg for showing actual data")
		}
		if len(rows) == 0 {
			break
		}
		offset += maxLimit
		for _, row := range rows {
			if row.ReferredBy != "bogus" && row.ReferredBy != "icenetwork" && row.ID != "bogus" && row.ID != "icenetwork" {
				referredByID, err := tokenomics.GetInternalID(ctx, m.db, string(row.ReferredBy))
				if err != nil {
					log.Error(errWrongInternalID, referredByID)

					continue
				}
				id, err := tokenomics.GetInternalID(ctx, m.db, string(row.ID))
				if err != nil {
					log.Error(errWrongInternalID, row.ID)

					continue
				}
				if row.ReferralType == "T1" {
					t1Referrals[referredByID] = append(t1Referrals[referredByID], id)
					if row.Active != nil && *row.Active {
						t1ActiveCounts[referredByID]++
					}
				} else if row.ReferralType == "T2" {
					t2Referrals[referredByID] = append(t2Referrals[referredByID], id)
					if row.Active != nil && *row.Active {
						t2ActiveCounts[referredByID]++
					}
				} else {
					log.Panic("wrong tier type")
				}
			}
		}
	}

	return t1Referrals, t2Referrals, t1ActiveCounts, t2ActiveCounts, nil
}

func splitByAdoptionTimeRanges(adoptions []*tokenomics.Adoption[float64], startedAt, endedAt *time.Time) []splittedAdoptionByRange {
	var result []splittedAdoptionByRange

	currentMBR := adoptions[0].BaseMiningRate
	lastAchievedAt := adoptions[0].AchievedAt
	currentAchievedAtIdx := 0

	for idx, adptn := range adoptions {
		if adptn.AchievedAt.IsNil() {
			continue
		}
		if adptn.AchievedAt.Before(*startedAt.Time) {
			currentMBR = adptn.BaseMiningRate
		}
		if (adptn.AchievedAt.After(*startedAt.Time) || adptn.AchievedAt.Equal(*startedAt.Time)) &&
			adptn.AchievedAt.Before(*endedAt.Time) {
			result = append(result, splittedAdoptionByRange{
				TimePoint:      adptn.AchievedAt,
				BaseMiningRate: adptn.BaseMiningRate,
			})
		}
		if adptn.AchievedAt.After(*lastAchievedAt.Time) {
			currentAchievedAtIdx = idx
			lastAchievedAt = adptn.AchievedAt
		}
	}
	result = append(result,
		splittedAdoptionByRange{
			TimePoint:      startedAt,
			BaseMiningRate: currentMBR,
		},
	)
	if endedAt.After(*adoptions[currentAchievedAtIdx].AchievedAt.Time) {
		result = append(result,
			splittedAdoptionByRange{
				TimePoint:      endedAt,
				BaseMiningRate: adoptions[currentAchievedAtIdx].BaseMiningRate,
			})
	} else {
		result = append(result,
			splittedAdoptionByRange{
				TimePoint:      endedAt,
				BaseMiningRate: currentMBR,
			})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TimePoint.Before(*result[j].TimePoint.Time)
	})

	return result
}

func calculateTimeBounds(refTimeRange, usrRange *historyRangeTime) (*time.Time, *time.Time) {
	if refTimeRange.MiningSessionSoloStartedAt.After(*usrRange.MiningSessionSoloEndedAt.Time) || refTimeRange.MiningSessionSoloEndedAt.Before(*usrRange.MiningSessionSoloStartedAt.Time) || refTimeRange.SlashingRateSolo > 0 {
		return nil, nil
	}
	var startedAt, endedAt *time.Time
	if refTimeRange.MiningSessionSoloStartedAt.After(*usrRange.MiningSessionSoloStartedAt.Time) || refTimeRange.MiningSessionSoloStartedAt.Equal(*usrRange.MiningSessionSoloStartedAt.Time) {
		startedAt = refTimeRange.MiningSessionSoloStartedAt
	} else {
		startedAt = usrRange.MiningSessionSoloStartedAt
	}
	if refTimeRange.MiningSessionSoloEndedAt.Before(*usrRange.MiningSessionSoloEndedAt.Time) || refTimeRange.MiningSessionSoloEndedAt.Equal(*usrRange.MiningSessionSoloEndedAt.Time) {
		endedAt = refTimeRange.MiningSessionSoloEndedAt
	} else {
		endedAt = usrRange.MiningSessionSoloEndedAt
	}

	return startedAt, endedAt
}

func initializeEmptyUser(updatedUser, usr *user) *user {
	var newUser user
	newUser.ID = usr.ID
	newUser.UserID = usr.UserID
	newUser.IDT0 = usr.IDT0
	newUser.IDTMinus1 = usr.IDTMinus1
	newUser.BalanceLastUpdatedAt = nil

	return &newUser
}

func (m *miner) recalculateTiersBalances(ctx context.Context, users []*user, tMinus1Referrals map[int64]*referral, t0Referrals map[int64]*referral) (map[int64]*user, error) {
	var (
		needToBeRecalculatedUsers []*user
		actualBalancesT1          = make(map[int64]float64)
		actualBalancesT2          = make(map[int64]float64)
	)
	usrs, err := m.getUsers(ctx, users)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get CreatedAt information for users:%#v", usrs)
	}
	for _, usr := range users {
		if usr.UserID == "" {
			continue
		}
		if _, ok := usrs[usr.ID]; ok {
			if usrs[usr.ID].CreatedAt == nil || usrs[usr.ID].CreatedAt.After(*m.recalculationBalanceStartDate.Time) {
				continue
			}
		}
		needToBeRecalculatedUsers = append(needToBeRecalculatedUsers, usr)
		actualBalancesT1[usr.ID] = usr.BalanceT1
		actualBalancesT2[usr.ID] = usr.BalanceT2
	}
	t1Referrals, t2Referrals, t1ActiveCounts, t2ActiveCounts, err := m.collectTiers(ctx, needToBeRecalculatedUsers)
	if err != nil {
		return nil, errors.Wrap(err, "can't get active users for users")
	}
	if len(t1Referrals) == 0 && len(t2Referrals) == 0 {
		log.Debug("No t1/t2 referrals gathered")

		return nil, nil
	}

	/******************************************************************************************************************************************************
		1. Fetching users history time ranges & adoptions information.
	******************************************************************************************************************************************************/
	var (
		now               = time.Now()
		historyTimeRanges = make(map[int64][]*historyRangeTime)
		usrIDs            = make(map[int64]struct{}, len(t1Referrals)+len(t2Referrals)+len(needToBeRecalculatedUsers))
		updatedUsers      = make(map[int64]*user, len(users))
	)
	for _, values := range t1Referrals {
		for _, val := range values {
			usrIDs[val] = struct{}{}
		}
	}
	for _, values := range t2Referrals {
		for _, val := range values {
			usrIDs[val] = struct{}{}
		}
	}
	for _, usr := range needToBeRecalculatedUsers {
		usrIDs[usr.ID] = struct{}{}
	}
	if len(usrIDs) == 0 {
		log.Debug("no user ids to be recalculated")

		return nil, nil
	}
	adoptions, err := tokenomics.GetAllAdoptions[float64](ctx, m.db)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get adoptions for users:%#v", needToBeRecalculatedUsers)
	}
	offset := int64(0)
	for {
		historyInformation, err := m.dwhClient.GetAdjustUserInformation(ctx, usrIDs, maxLimit, offset)
		if err != nil {
			return nil, errors.Wrapf(err, "can't get adjust user information for ids:#%v", usrIDs)
		}
		if len(historyInformation) == 0 {
			break
		}
		offset += maxLimit
		for _, info := range historyInformation {
			historyTimeRanges[info.ID] = append(historyTimeRanges[info.ID], &historyRangeTime{
				MiningSessionSoloPreviouslyEndedAt: info.MiningSessionSoloPreviouslyEndedAt,
				MiningSessionSoloStartedAt:         info.MiningSessionSoloStartedAt,
				MiningSessionSoloEndedAt:           info.MiningSessionSoloEndedAt,
				ResurrectSoloUsedAt:                info.ResurrectSoloUsedAt,
				CreatedAt:                          info.CreatedAt,
				SlashingRateSolo:                   info.SlashingRateSolo,
				BalanceT1Pending:                   info.BalanceT1Pending,
				BalanceT1PendingApplied:            info.BalanceT1PendingApplied,
				BalanceT2Pending:                   info.BalanceT2Pending,
				BalanceT2PendingApplied:            info.BalanceT2PendingApplied,
			})
		}
	}
	if len(historyTimeRanges) == 0 {
		log.Debug("empty history time ranges")

		return nil, nil
	}
	for _, usr := range needToBeRecalculatedUsers {
		clonedUser1 := *usr
		updatedUser := &clonedUser1
		updatedUser.BalanceT1 = 0
		updatedUser.BalanceT2 = 0
		updatedUser.BalanceLastUpdatedAt = nil

		var (
			isResurrected                bool
			slashingLastEndedAt          *time.Time
			lastMiningSessionSoloEndedAt *time.Time
		)
		if _, ok := historyTimeRanges[usr.ID]; ok {
			var previousUserStartedAt, previousUserEndedAt *time.Time
			for _, usrRange := range historyTimeRanges[usr.ID] {
				if updatedUser == nil {
					updatedUser = initializeEmptyUser(updatedUser, usr)
				}
				lastMiningSessionSoloEndedAt = usrRange.MiningSessionSoloEndedAt

				updatedUser.BalanceT1Pending = usrRange.BalanceT1Pending
				updatedUser.BalanceT1PendingApplied = usrRange.BalanceT1PendingApplied
				updatedUser.BalanceT2Pending = usrRange.BalanceT2Pending
				updatedUser.BalanceT2PendingApplied = usrRange.BalanceT2PendingApplied
				/******************************************************************************************************************************************************
					2. Resurrection check & handling.
				******************************************************************************************************************************************************/
				if !usrRange.ResurrectSoloUsedAt.IsNil() && usrRange.ResurrectSoloUsedAt.Unix() > 0 && !isResurrected {
					var resurrectDelta float64
					if timeSpent := usrRange.MiningSessionSoloStartedAt.Sub(*usrRange.MiningSessionSoloPreviouslyEndedAt.Time); cfg.Development {
						resurrectDelta = timeSpent.Minutes()
					} else {
						resurrectDelta = timeSpent.Hours()
					}
					updatedUser.BalanceT1 += updatedUser.SlashingRateT1 * resurrectDelta
					updatedUser.BalanceT2 += updatedUser.SlashingRateT2 * resurrectDelta
					updatedUser.SlashingRateT1 = 0
					updatedUser.SlashingRateT2 = 0

					isResurrected = true
				}
				/******************************************************************************************************************************************************
					3. Slashing calculations.
				******************************************************************************************************************************************************/
				if usrRange.SlashingRateSolo > 0 {
					if slashingLastEndedAt.IsNil() {
						slashingLastEndedAt = usrRange.MiningSessionSoloEndedAt
					}
					updatedUser.BalanceLastUpdatedAt = slashingLastEndedAt
					updatedUser.ResurrectSoloUsedAt = nil
					updatedUser, _, _ = mine(0., usrRange.CreatedAt, updatedUser, nil, nil)
					slashingLastEndedAt = usrRange.CreatedAt

					continue
				}
				if !slashingLastEndedAt.IsNil() && usrRange.MiningSessionSoloStartedAt.Sub(*slashingLastEndedAt.Time).Nanoseconds() > 0 {
					updatedUser.BalanceLastUpdatedAt = slashingLastEndedAt
					updatedUser.ResurrectSoloUsedAt = nil
					now := usrRange.MiningSessionSoloStartedAt
					updatedUser, _, _ = mine(0., now, updatedUser, nil, nil)
					slashingLastEndedAt = nil
				}
				/******************************************************************************************************************************************************
					4. Saving time range state for the next range for streaks case.
				******************************************************************************************************************************************************/
				if previousUserStartedAt != nil && previousUserStartedAt.Equal(*usrRange.MiningSessionSoloStartedAt.Time) &&
					previousUserEndedAt != nil && (usrRange.MiningSessionSoloEndedAt.After(*previousUserEndedAt.Time) ||
					usrRange.MiningSessionSoloEndedAt.Equal(*previousUserEndedAt.Time)) {

					previousUserStartedAt = usrRange.MiningSessionSoloStartedAt

					usrRange.MiningSessionSoloStartedAt = previousUserEndedAt
					previousUserEndedAt = usrRange.MiningSessionSoloEndedAt
				} else {
					previousUserStartedAt = usrRange.MiningSessionSoloStartedAt
					previousUserEndedAt = usrRange.MiningSessionSoloEndedAt
				}
				/******************************************************************************************************************************************************
					5. T1 Balance calculation for the current user time range.
				******************************************************************************************************************************************************/
				if _, ok := t1Referrals[usr.ID]; ok {
					for _, refID := range t1Referrals[usr.ID] {
						if _, ok := historyTimeRanges[refID]; ok {
							var previousT1MiningSessionStartedAt, previousT1MiningSessionEndedAt *time.Time
							for _, timeRange := range historyTimeRanges[refID] {
								if timeRange.SlashingRateSolo > 0 {
									continue
								}
								if previousT1MiningSessionStartedAt != nil && previousT1MiningSessionStartedAt.Equal(*timeRange.MiningSessionSoloStartedAt.Time) &&
									previousT1MiningSessionEndedAt != nil && (timeRange.MiningSessionSoloEndedAt.After(*previousT1MiningSessionEndedAt.Time) ||
									timeRange.MiningSessionSoloEndedAt.Equal(*previousT1MiningSessionEndedAt.Time)) {

									previousT1MiningSessionStartedAt = timeRange.MiningSessionSoloStartedAt
									timeRange.MiningSessionSoloStartedAt = previousT1MiningSessionEndedAt
									previousT1MiningSessionEndedAt = timeRange.MiningSessionSoloEndedAt
								} else {
									previousT1MiningSessionStartedAt = timeRange.MiningSessionSoloStartedAt
									previousT1MiningSessionEndedAt = timeRange.MiningSessionSoloEndedAt
								}
								startedAt, endedAt := calculateTimeBounds(timeRange, usrRange)
								if startedAt == nil && endedAt == nil {
									continue
								}

								adoptionRanges := splitByAdoptionTimeRanges(adoptions, startedAt, endedAt)

								var previousTimePoint *time.Time
								for _, adoptionRange := range adoptionRanges {
									if previousTimePoint == nil {
										previousTimePoint = adoptionRange.TimePoint

										continue
									}
									if previousTimePoint.Equal(*adoptionRange.TimePoint.Time) {
										continue
									}
									updatedUser.ActiveT1Referrals = 1
									updatedUser.ActiveT2Referrals = 0
									updatedUser.MiningSessionSoloStartedAt = previousTimePoint
									updatedUser.MiningSessionSoloEndedAt = time.New(adoptionRange.TimePoint.Add(1 * stdlibtime.Nanosecond))
									updatedUser.BalanceLastUpdatedAt = nil
									updatedUser.ResurrectSoloUsedAt = nil
									now := adoptionRange.TimePoint

									updatedUser, _, _ = mine(adoptionRange.BaseMiningRate, now, updatedUser, nil, nil)

									previousTimePoint = adoptionRange.TimePoint
								}
							}
						}
					}
				}
				/******************************************************************************************************************************************************
					6. T2 Balance calculation for the current user time range.
				******************************************************************************************************************************************************/
				if _, ok := t2Referrals[usr.ID]; ok {
					for _, refID := range t2Referrals[usr.ID] {
						if _, ok := historyTimeRanges[refID]; ok {
							var previousT2MiningSessionStartedAt, previousT2MiningSessionEndedAt *time.Time
							for _, timeRange := range historyTimeRanges[refID] {
								if timeRange.SlashingRateSolo > 0 {
									continue
								}
								if previousT2MiningSessionStartedAt != nil && previousT2MiningSessionStartedAt.Equal(*timeRange.MiningSessionSoloStartedAt.Time) &&
									previousT2MiningSessionEndedAt != nil && (timeRange.MiningSessionSoloEndedAt.After(*previousT2MiningSessionEndedAt.Time) ||
									timeRange.MiningSessionSoloEndedAt.Equal(*previousT2MiningSessionEndedAt.Time)) {

									previousT2MiningSessionStartedAt = timeRange.MiningSessionSoloStartedAt
									timeRange.MiningSessionSoloStartedAt = previousT2MiningSessionEndedAt
									previousT2MiningSessionEndedAt = timeRange.MiningSessionSoloEndedAt
								} else {
									previousT2MiningSessionEndedAt = timeRange.MiningSessionSoloEndedAt
									previousT2MiningSessionStartedAt = timeRange.MiningSessionSoloStartedAt
								}
								startedAt, endedAt := calculateTimeBounds(timeRange, usrRange)
								if startedAt == nil && endedAt == nil {
									continue
								}

								adoptionRanges := splitByAdoptionTimeRanges(adoptions, startedAt, endedAt)

								var previousTimePoint *time.Time
								for _, adoptionRange := range adoptionRanges {
									if previousTimePoint == nil {
										previousTimePoint = adoptionRange.TimePoint

										continue
									}
									if previousTimePoint.Equal(*adoptionRange.TimePoint.Time) {
										continue
									}
									updatedUser.ActiveT1Referrals = 0
									updatedUser.ActiveT2Referrals = 1
									updatedUser.MiningSessionSoloPreviouslyEndedAt = usr.MiningSessionSoloPreviouslyEndedAt
									updatedUser.MiningSessionSoloStartedAt = previousTimePoint
									updatedUser.MiningSessionSoloEndedAt = time.New(adoptionRange.TimePoint.Add(1 * stdlibtime.Nanosecond))
									updatedUser.BalanceLastUpdatedAt = nil
									updatedUser.ResurrectSoloUsedAt = nil
									now := adoptionRange.TimePoint

									updatedUser, _, _ = mine(adoptionRange.BaseMiningRate, now, updatedUser, nil, nil)

									previousTimePoint = adoptionRange.TimePoint
								}
							}
						}
					}
				}
			}
			if !lastMiningSessionSoloEndedAt.IsNil() {
				if timeDiff := now.Sub(*lastMiningSessionSoloEndedAt.Time); cfg.Development {
					if timeDiff >= 60*stdlibtime.Minute {
						updatedUser = nil
					}
				} else {
					if timeDiff >= 60*stdlibtime.Hour*24 {
						updatedUser = nil
					}
				}
			}
		}
		if updatedUser == nil {
			updatedUser = initializeEmptyUser(updatedUser, usr)
		}
		updatedUser.ActiveT1Referrals = int32(t1ActiveCounts[usr.ID])
		updatedUser.ActiveT2Referrals = int32(t2ActiveCounts[usr.ID])
		updatedUsers[updatedUser.ID] = updatedUser
	}

	return updatedUsers, nil
}
