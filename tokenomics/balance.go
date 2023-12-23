// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"math"
	"sort"
	stdlibtime "time"

	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	dwh "github.com/ice-blockchain/freezer/bookkeeper/storage"
	"github.com/ice-blockchain/freezer/model"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

func (r *repository) GetTotalCoinsSummary(ctx context.Context, days uint64, utcOffset stdlibtime.Duration) (*TotalCoinsSummary, error) {
	var (
		dates    []stdlibtime.Time
		res      = &TotalCoinsSummary{TimeSeries: make([]*TotalCoinsTimeSeriesDataPoint, 0, days)}
		now      = time.Now()
		location = stdlibtime.FixedZone(utcOffset.String(), int(utcOffset.Seconds()))
	)

	dates, res.TimeSeries = r.totalCoinsDates(now, days)
	totalCoins, err := r.getCachedTotalCoins(ctx, dates)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to getCachedTotalCoins for createdAts:%#v", dates)
	}
	for _, child := range res.TimeSeries {
		for _, stats := range totalCoins {
			dayMatch := stats.CreatedAt.Truncate(r.cfg.GlobalAggregationInterval.Parent).Equal(child.Date.Truncate(r.cfg.GlobalAggregationInterval.Parent))
			if dayMatch {
				child.Standard = stats.BalanceTotalStandard
				child.PreStaking = stats.BalanceTotalPreStaking
				child.Blockchain = stats.BalanceTotalEthereum
				child.Total = child.Standard + child.PreStaking + child.Blockchain
				break
			}
		}
		child.Date = child.Date.In(location)
	}
	res.TotalCoins = res.TimeSeries[0].TotalCoins

	return res, nil
}

func (r *repository) GetBalanceSummary( //nolint:lll // .
	ctx context.Context, userID string,
) (*BalanceSummary, error) {
	id, err := GetOrInitInternalID(ctx, r.db, userID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", userID)
	}
	res, err := storage.Get[struct {
		model.UserIDField
		model.LatestDeviceField
		model.BalanceSoloField
		model.BalanceT0Field
		model.BalanceT1Field
		model.BalanceT2Field
		model.BalanceSoloEthereumField
		model.BalanceT0EthereumField
		model.BalanceT1EthereumField
		model.BalanceT2EthereumField
		model.PreStakingBonusField
		model.PreStakingAllocationField
	}](ctx, r.db, model.SerializedUsersKey(id))
	if err != nil || len(res) == 0 {
		if err == nil {
			err = errors.Wrapf(ErrRelationNotFound, "missing state for id:%v", id)
		}

		return nil, errors.Wrapf(err, "failed to get balanceSummary for id:%v", id)
	}
	if r.isAdvancedTeamDisabled(res[0].LatestDevice) {
		res[0].BalanceT2 = 0
	}
	t1Standard, t1PreStaking := ApplyPreStaking(res[0].BalanceT0+res[0].BalanceT1, res[0].PreStakingAllocation, res[0].PreStakingBonus)
	t2Standard, t2PreStaking := ApplyPreStaking(res[0].BalanceT2, res[0].PreStakingAllocation, res[0].PreStakingBonus)
	soloStandard, soloPreStaking := ApplyPreStaking(res[0].BalanceSolo, res[0].PreStakingAllocation, res[0].PreStakingBonus)

	return &BalanceSummary{
		Balances: Balances[string]{
			Total:                  fmt.Sprintf(floatToStringFormatter, soloStandard+soloPreStaking+t1Standard+t1PreStaking+t2Standard+t2PreStaking),
			TotalNoPreStakingBonus: fmt.Sprintf(floatToStringFormatter, res[0].BalanceSolo+res[0].BalanceT0+res[0].BalanceT1+res[0].BalanceT2),
			Standard:               fmt.Sprintf(floatToStringFormatter, soloStandard+t1Standard+t2Standard),
			PreStaking:             fmt.Sprintf(floatToStringFormatter, soloPreStaking+t1PreStaking+t2PreStaking),
			T1:                     fmt.Sprintf(floatToStringFormatter, t1Standard+t1PreStaking),
			T2:                     fmt.Sprintf(floatToStringFormatter, t2Standard+t2PreStaking),
			TotalReferrals:         fmt.Sprintf(floatToStringFormatter, t1Standard+t1PreStaking+t2Standard+t2PreStaking),
			TotalMiningBlockchain:  fmt.Sprintf(floatToStringFormatter, res[0].BalanceSoloEthereum+res[0].BalanceT0Ethereum+res[0].BalanceT1Ethereum+res[0].BalanceT2Ethereum), //nolint:lll // .
		},
	}, nil
}

func (r *repository) GetBalanceHistory( //nolint:funlen,gocognit,revive,gocyclo,cyclop,revive // Better to be grouped together.
	ctx context.Context, userID string, start, end *time.Time, _ stdlibtime.Duration, limit, offset uint64,
) ([]*BalanceHistoryEntry, error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	start, end = time.New(start.UTC()), time.New(end.UTC())
	var factor stdlibtime.Duration
	if start.After(*end.Time) {
		factor = -1
	} else {
		factor = 1
	}
	dates, notBeforeTime, notAfterTime := r.calculateDates(limit, offset, start, end, factor)
	id, gErr := GetOrInitInternalID(ctx, r.db, userID)
	if gErr != nil {
		return nil, errors.Wrapf(gErr, "failed to getOrInitInternalID for userID:%v", userID)
	}
	balanceHistory, gErr := r.dwh.SelectBalanceHistory(ctx, id, dates)
	if gErr != nil {
		return nil, errors.Wrapf(gErr, "failed to SelectBalanceHistory for id:%v,createdAts:%#v", id, dates)
	}

	return r.processBalanceHistory(balanceHistory, factor > 0, notBeforeTime, notAfterTime), nil
}

func (r *repository) calculateDates(limit, offset uint64, start, end *time.Time, factor stdlibtime.Duration) (dates []stdlibtime.Time, notBeforeTime, notAfterTime *time.Time) {
	const (
		hoursInADay = 24
	)
	calculatedLimit := (limit / hoursInADay) * uint64(r.cfg.GlobalAggregationInterval.Parent/r.cfg.GlobalAggregationInterval.Child)
	var afterStartPadding, beforeStartPadding uint64
	if factor > 0 {
		if r.cfg.GlobalAggregationInterval.Child == stdlibtime.Hour {
			afterStartPadding = 24 - uint64(start.Add(stdlibtime.Duration(calculatedLimit*uint64(stdlibtime.Hour))).Hour())
			beforeStartPadding = uint64(start.Add(stdlibtime.Duration(-calculatedLimit * uint64(stdlibtime.Hour))).Hour())
		} else {
			afterStartPadding = 60 - uint64(start.Add(stdlibtime.Duration(-calculatedLimit*uint64(stdlibtime.Minute))).Minute())
			beforeStartPadding = uint64(start.Add(stdlibtime.Duration(-calculatedLimit * uint64(stdlibtime.Minute))).Minute())
		}
	} else {
		if r.cfg.GlobalAggregationInterval.Child == stdlibtime.Hour {
			beforeStartPadding = uint64(start.Add(stdlibtime.Duration(-calculatedLimit*uint64(stdlibtime.Hour))).Hour()) + 1
		} else {
			beforeStartPadding = uint64(start.Add(stdlibtime.Duration(-calculatedLimit*uint64(stdlibtime.Minute))).Minute()) + 1
		}
	}
	mappedLimit := calculatedLimit + beforeStartPadding + afterStartPadding
	mappedOffset := (offset / hoursInADay) * uint64(r.cfg.GlobalAggregationInterval.Parent/r.cfg.GlobalAggregationInterval.Child)
	dates = make([]stdlibtime.Time, 0, mappedLimit)
	if factor > 0 {
		for ix := stdlibtime.Duration(mappedOffset); ix < stdlibtime.Duration(mappedLimit+mappedOffset); ix++ {
			dates = append(dates, start.Add(-stdlibtime.Duration(beforeStartPadding)*r.cfg.GlobalAggregationInterval.Child).Add(ix*factor*r.cfg.GlobalAggregationInterval.Child).Truncate(r.cfg.GlobalAggregationInterval.Child))
		}
		if r.cfg.GlobalAggregationInterval.Child == stdlibtime.Hour {
			notBeforeTime = time.New(start.Add(stdlibtime.Duration(mappedOffset * uint64(stdlibtime.Hour))))
			notAfterTime = time.New(start.Add(stdlibtime.Duration((calculatedLimit + mappedOffset) * uint64(stdlibtime.Hour))))
		} else {
			notBeforeTime = time.New(start.Add(stdlibtime.Duration(mappedOffset * uint64(stdlibtime.Minute))))
			notAfterTime = time.New(start.Add(stdlibtime.Duration((calculatedLimit + mappedOffset) * uint64(stdlibtime.Minute))))
		}
		if notAfterTime.UnixNano() > end.UnixNano() {
			notAfterTime = end
		}
	} else {
		for ix := stdlibtime.Duration(mappedOffset); ix < stdlibtime.Duration(mappedLimit+mappedOffset); ix++ {
			dates = append(dates, start.Add(ix*factor*r.cfg.GlobalAggregationInterval.Child).Truncate(r.cfg.GlobalAggregationInterval.Child))
		}
		if r.cfg.GlobalAggregationInterval.Child == stdlibtime.Hour {
			notBeforeTime = time.New(start.Add(stdlibtime.Duration((-calculatedLimit - mappedOffset) * uint64(stdlibtime.Hour))))
			notAfterTime = time.New(start.Add(stdlibtime.Duration(-mappedOffset * uint64(stdlibtime.Hour))))
		} else {
			notBeforeTime = time.New(start.Add(stdlibtime.Duration((-calculatedLimit - mappedOffset) * uint64(stdlibtime.Minute))))
			notAfterTime = time.New(start.Add(stdlibtime.Duration(-mappedOffset * uint64(stdlibtime.Minute))))
		}
	}

	return
}

func (r *repository) processBalanceHistory(
	res []*dwh.BalanceHistory,
	startDateIsBeforeEndDate bool,
	notBeforeTime, notAfterTime *time.Time,
) []*BalanceHistoryEntry { //nolint:funlen,gocognit,revive // .
	childDateLayout := r.cfg.globalAggregationIntervalChildDateFormat()
	parentDateLayout := r.cfg.globalAggregationIntervalParentDateFormat()
	type parentType struct {
		*BalanceHistoryEntry
		children map[string]*BalanceHistoryEntry
	}
	parents := make(map[string]*parentType, 1+1)
	parentKeys := make([]string, 0, len(parents))
	for _, bal := range res {
		childFormat, parentFormat := bal.CreatedAt.Format(childDateLayout), bal.CreatedAt.Format(parentDateLayout)
		if _, found := parents[parentFormat]; !found {
			parent, pErr := stdlibtime.ParseInLocation(parentDateLayout, parentFormat, stdlibtime.UTC)
			log.Panic(pErr) //nolint:revive // Intended.
			parents[parentFormat] = &parentType{
				BalanceHistoryEntry: &BalanceHistoryEntry{
					Time:    parent,
					Balance: new(BalanceHistoryBalanceDiff),
				},
				children: make(map[string]*BalanceHistoryEntry, int(r.cfg.GlobalAggregationInterval.Parent/r.cfg.GlobalAggregationInterval.Child)),
			}
			parentKeys = append(parentKeys, parentFormat)
		}
		if _, found := parents[parentFormat].children[childFormat]; !found {
			parents[parentFormat].children[childFormat] = &BalanceHistoryEntry{
				Time:    *bal.CreatedAt.Time,
				Balance: new(BalanceHistoryBalanceDiff),
			}
		}
		var total float64
		if bal.BalanceTotalSlashed >= 0 {
			total = bal.BalanceTotalMinted - bal.BalanceTotalSlashed
		} else {
			total = bal.BalanceTotalMinted - (bal.BalanceTotalSlashed * -1)
		}
		parents[parentFormat].children[childFormat].Balance.amount = total
		parents[parentFormat].children[childFormat].Balance.Negative = total < 0
		parents[parentFormat].children[childFormat].Balance.Amount = fmt.Sprintf(floatToStringFormatter, math.Abs(total))
	}
	sort.Strings(parentKeys)
	history := make([]*BalanceHistoryEntry, 0, len(parents))

	var prevParent *parentType
	var prevChild *BalanceHistoryEntry
	for _, pKey := range parentKeys {
		parents[pKey].BalanceHistoryEntry.TimeSeries = make([]*BalanceHistoryEntry, 0, len(parents[pKey].children))
		childrenKeys := make([]string, 0)
		for cKey := range parents[pKey].children {
			childrenKeys = append(childrenKeys, cKey)
		}
		sort.Strings(childrenKeys)
		for _, cKey := range childrenKeys {
			parents[pKey].children[cKey].TimeSeries = make([]*BalanceHistoryEntry, 0, 0)
			if prevChild == nil {
				parents[pKey].children[cKey].Balance.Bonus = 0
			} else {
				parents[pKey].children[cKey].setBalanceDiffBonus(prevChild.Balance.amount)
			}
			parents[pKey].Balance.amount += parents[pKey].children[cKey].Balance.amount
			if time.New(parents[pKey].children[cKey].Time).UnixNano() >= notBeforeTime.UnixNano() && time.New(parents[pKey].children[cKey].Time).UnixNano() <= notAfterTime.UnixNano() {
				parents[pKey].BalanceHistoryEntry.TimeSeries = append(parents[pKey].BalanceHistoryEntry.TimeSeries, parents[pKey].children[cKey])
				prevChild = parents[pKey].children[cKey]
			}
			childrenKeys = append(childrenKeys, cKey)
		}
		if prevParent == nil {
			parents[pKey].Balance.Bonus = 0
		} else {
			parents[pKey].setBalanceDiffBonus(prevParent.Balance.amount)
		}
		parents[pKey].Balance.Negative = parents[pKey].Balance.amount < 0
		parents[pKey].Balance.Amount = fmt.Sprintf(floatToStringFormatter, math.Abs(parents[pKey].Balance.amount))
		if !startDateIsBeforeEndDate {
			sort.SliceStable(parents[pKey].BalanceHistoryEntry.TimeSeries, func(i, j int) bool {
				return parents[pKey].BalanceHistoryEntry.TimeSeries[i].Time.After(parents[pKey].BalanceHistoryEntry.TimeSeries[j].Time)
			})
		}
		if len(parents[pKey].BalanceHistoryEntry.TimeSeries) > 0 {
			history = append(history, parents[pKey].BalanceHistoryEntry)
		}
		prevParent = parents[pKey]
	}
	if !startDateIsBeforeEndDate {
		sort.SliceStable(history, func(i, j int) bool {
			return history[i].Time.After(history[j].Time)
		})
	}

	return history
}

func (e *BalanceHistoryEntry) setBalanceDiffBonus(from float64) {
	to := e.Balance.amount
	if from < 0 && to > 0 {
		e.Balance.Bonus = roundFloat64(((from - to) / from) * 100)
	} else {
		e.Balance.Bonus = roundFloat64(-1 * ((from - to) / from) * 100)
	}
}

//nolint:funlen // .
func (s *completedTasksSource) Process(ctx context.Context, message *messagebroker.Message) (err error) {
	if ctx.Err() != nil || len(message.Value) == 0 {
		return errors.Wrap(ctx.Err(), "unexpected deadline while processing message")
	}
	const requiredCompletedTasks, adoptionMultiplicationFactor = 6, 150
	var val struct {
		UserID         string `json:"userId" example:"edfd8c02-75e0-4687-9ac2-1ce4723865c4"`
		CompletedTasks uint64 `json:"completedTasks,omitempty" example:"3"`
	}
	if err = json.UnmarshalContext(ctx, message.Value, &val); err != nil || val.UserID == "" || val.CompletedTasks != requiredCompletedTasks {
		return errors.Wrapf(err, "process: cannot unmarshall %v into %#v", string(message.Value), &val)
	}
	duplGuardKey := fmt.Sprintf("completed_tasks_ice_prize_dupl_guards:%v", val.UserID)
	if set, dErr := s.db.SetNX(ctx, duplGuardKey, "", s.cfg.MiningSessionDuration.Min).Result(); dErr != nil || !set {
		if dErr == nil {
			dErr = ErrDuplicate
		}

		return errors.Wrapf(dErr, "SetNX failed for completed_tasks_ice_prize_dupl_guard, userID: %v", val.UserID)
	}
	defer func() {
		if err != nil {
			undoCtx, cancelUndo := context.WithTimeout(context.Background(), requestDeadline)
			defer cancelUndo()
			err = multierror.Append( //nolint:wrapcheck // .
				err,
				errors.Wrapf(s.db.Del(undoCtx, duplGuardKey).Err(), "failed to del completed_tasks_ice_prize_dupl_guard key"),
			).ErrorOrNil()
		}
	}()
	adoption, err := GetCurrentAdoption(ctx, s.db)
	if err != nil {
		return errors.Wrap(err, "failed to getCurrentAdoption")
	}
	id, err := GetOrInitInternalID(ctx, s.db, val.UserID)
	if err != nil {
		return errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", val.UserID)
	}
	prize := adoption.BaseMiningRate * adoptionMultiplicationFactor

	return errors.Wrapf(s.db.HIncrByFloat(ctx, model.SerializedUsersKey(id), "balance_solo_pending", prize).Err(),
		"failed to incr balance_solo_pending for userID:%v by %v", val.UserID, prize)
}

//nolint:gomnd // .
func ApplyPreStaking(amount, preStakingAllocation, preStakingBonus float64) (float64, float64) {
	standardAmount := (amount * (100 - preStakingAllocation)) / 100
	preStakingAmount := (amount * (100 + preStakingBonus) * preStakingAllocation) / 10000

	return standardAmount, preStakingAmount
}

func (r *repository) totalCoinsDates(now *time.Time, days uint64) ([]stdlibtime.Time, []*TotalCoinsTimeSeriesDataPoint) {
	var (
		adjustForLatencyToProcessAllUsers = -(r.cfg.GlobalAggregationInterval.Child / 4)
		truncationInterval                = r.cfg.GlobalAggregationInterval.Child
		dayInterval                       = r.cfg.GlobalAggregationInterval.Parent
		dates                             = make([]stdlibtime.Time, 0, days)
		timeSeries                        = make([]*TotalCoinsTimeSeriesDataPoint, 0, days)
	)
	for day := uint64(0); day < days; day++ {
		date := now.Add(dayInterval * -1 * stdlibtime.Duration(day)).Add(adjustForLatencyToProcessAllUsers).Truncate(truncationInterval)
		dates = append(dates, date)
		timeSeries = append(timeSeries, &TotalCoinsTimeSeriesDataPoint{Date: date})
	}
	return dates, timeSeries
}

func (r *repository) cacheTotalCoins(ctx context.Context, coins []*dwh.TotalCoins) error {
	now := time.Now()
	if responses, err := r.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		var err error
		for _, tc := range coins {
			key := r.totalCoinsCacheKey(*tc.CreatedAt.Time)
			lastRecordCurrentlyUpdating := now.Truncate(r.cfg.GlobalAggregationInterval.Parent).Equal(tc.CreatedAt.Time.Truncate(r.cfg.GlobalAggregationInterval.Parent))
			if lastRecordCurrentlyUpdating {
				err = multierror.Append(err,
					r.db.HSet(ctx, key, "created_at", tc.CreatedAt,
						"blockchain", tc.BalanceTotalEthereum,
						"standard", tc.BalanceTotalStandard,
						"pre_staking", tc.BalanceTotalPreStaking).Err(),
				).ErrorOrNil()
			} else {
				err = multierror.Append(err,
					r.db.HSetNX(ctx, key, "created_at", tc.CreatedAt).Err(),
					r.db.HSetNX(ctx, key, "blockchain", tc.BalanceTotalEthereum).Err(),
					r.db.HSetNX(ctx, key, "standard", tc.BalanceTotalStandard).Err(),
					r.db.HSetNX(ctx, key, "pre_staking", tc.BalanceTotalPreStaking).Err(),
				).ErrorOrNil()
			}
		}

		return err
	}); err != nil {
		return errors.Wrapf(err, "failed to save to totalCoinsCache: %+v", coins)
	} else {
		errs := make([]error, 0, 1+1)
		for _, response := range responses {
			if err = response.Err(); err != nil {
				errs = append(errs, errors.Wrapf(err, "failed to `%v`", response.FullName()))
			}
		}
		return multierror.Append(nil, errs...).ErrorOrNil() //nolint:wrapcheck // .
	}
}

func (r *repository) getCachedTotalCoins(ctx context.Context, dates []stdlibtime.Time) ([]*dwh.TotalCoins, error) {
	keys := make([]string, 0, len(dates))
	for _, d := range dates {
		keys = append(keys, r.totalCoinsCacheKey(d))
	}
	cacheResult, err := storage.Get[struct {
		CreatedAt              *time.Time `redis:"created_at"`
		BalanceTotalStandard   float64    `redis:"standard"`
		BalanceTotalPreStaking float64    `redis:"pre_staking"`
		BalanceTotalEthereum   float64    `redis:"blockchain"`
	}](ctx, r.db, keys...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get cached coinStats for dates %v", dates)
	}
	cached := make([]*dwh.TotalCoins, 0, len(cacheResult))
	for _, c := range cacheResult {
		cached = append(cached, &dwh.TotalCoins{
			CreatedAt:              c.CreatedAt,
			BalanceTotalStandard:   c.BalanceTotalStandard,
			BalanceTotalPreStaking: c.BalanceTotalPreStaking,
			BalanceTotalEthereum:   c.BalanceTotalEthereum,
		})
	}

	return cached, nil
}

func (r *repository) totalCoinsCacheKey(date stdlibtime.Time) string {
	return fmt.Sprintf("totalCoinStats:%v", date.Truncate(r.cfg.GlobalAggregationInterval.Parent).Format(stdlibtime.RFC3339))
}

func (r *repository) keepTotalCoinsCacheUpdated(ctx context.Context) {
	for ctx.Err() == nil {
		now := time.Now()
		if now.Sub(now.Truncate(r.cfg.GlobalAggregationInterval.Child)) >= stdlibtime.Duration(float64(r.cfg.GlobalAggregationInterval.Child)*0.75) {
			dwhCtx, cancel := context.WithTimeout(ctx, stdlibtime.Duration(0.20*float64(r.cfg.GlobalAggregationInterval.Child)))
			if err := r.buildTotalCoinCache(dwhCtx, now.Truncate(r.cfg.GlobalAggregationInterval.Child)); err != nil {
				log.Error(errors.Wrapf(err, "failed to update total coin stats cache for date %v", *now.Time))
			}
			cancel()
		} else {
			stdlibtime.Sleep(stdlibtime.Second)
		}
	}
}

func (r *repository) buildTotalCoinCache(ctx context.Context, dates ...stdlibtime.Time) error {
	totalCoins, err := r.dwh.SelectTotalCoins(ctx, dates)
	if err != nil {
		return errors.Wrapf(err, "failed to read total coin stats cacheable values for dates %v", dates)
	}
	for _, d := range dates {
		found := false
		for _, stats := range totalCoins {
			if stats.CreatedAt.Equal(d) {
				found = true
				break
			}
		}
		if !found {
			totalCoins = append(totalCoins, &dwh.TotalCoins{
				CreatedAt:              time.New(d),
				BalanceTotalStandard:   0,
				BalanceTotalPreStaking: 0,
				BalanceTotalEthereum:   0,
			})
		}
	}
	return errors.Wrapf(
		r.cacheTotalCoins(ctx, totalCoins),
		"failed to save total coin stats cache for dates %v", dates)
}

func (r *repository) mustInitTotalCoinsCache(ctx context.Context) {
	now := time.Now()
	dates, _ := r.totalCoinsDates(now, daysCountToInitCoinsCacheOnStartup)
	alreadyCached, err := r.getCachedTotalCoins(ctx, dates)
	if err != nil {
		log.Panic(errors.Wrapf(err, "failed to init total coin stats cache"))
	}
	for _, cached := range alreadyCached {
		for dateIdx, date := range dates {
			daysMatch := cached.CreatedAt.Truncate(r.cfg.GlobalAggregationInterval.Parent).Equal(date.Truncate(r.cfg.GlobalAggregationInterval.Parent))
			if daysMatch {
				dates = append(dates[:dateIdx], dates[dateIdx+1:]...)

				break
			}
		}
	}
	for _, d := range dates {
		if err = backoff.RetryNotify(
			func() error {
				return errors.Wrapf(r.buildTotalCoinCache(ctx, d), "failed to build/init total coins cache for %v", d)
			},
			backoff.WithContext(&backoff.ExponentialBackOff{
				InitialInterval:     500 * stdlibtime.Millisecond,
				RandomizationFactor: 0.5,
				Multiplier:          2.5,
				MaxInterval:         2 * stdlibtime.Minute,
				MaxElapsedTime:      30 * stdlibtime.Second,
				Stop:                backoff.Stop,
				Clock:               backoff.SystemClock,
			}, ctx),
			func(e error, next stdlibtime.Duration) {
				log.Error(errors.Wrapf(e, "failed to init total coins cache retrying in %v... ", next))
			}); err != nil {
			log.Panic(errors.Wrapf(err,
				"failed to init total coin stats cache for %v days (%v)", daysCountToInitCoinsCacheOnStartup, dates))
		}

	}
}
