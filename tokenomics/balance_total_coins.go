// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	stdlibtime "time"

	"github.com/alitto/pond"
	"github.com/pkg/errors"

	dwh "github.com/ice-blockchain/freezer/bookkeeper/storage"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

func (r *repository) GetTotalCoinsSummary(ctx context.Context, days uint64, utcOffset stdlibtime.Duration) (*TotalCoinsSummary, error) {
	var (
		dates []stdlibtime.Time
		res   = &TotalCoinsSummary{TimeSeries: make([]*TotalCoinsTimeSeriesDataPoint, 0, days)}
		now   = time.Now()
	)

	dates, res.TimeSeries = r.totalCoinsDates(now, days)
	totalCoins, err := r.getCachedTotalCoins(ctx, dates)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to getCachedTotalCoins for createdAts:%#v", dates)
	}
	for _, child := range res.TimeSeries {
		for _, stats := range totalCoins {
			dayMatch := stats.CreatedAt.Equal(child.Date)
			if dayMatch {
				child.Standard = stats.BalanceTotalStandard
				child.PreStaking = stats.BalanceTotalPreStaking
				child.Blockchain = stats.BalanceTotalEthereum
				child.Total = stats.BalanceTotal
				break
			}
		}
		child.Date = child.Date.Add(-1 * stdlibtime.Nanosecond)

	}
	res.TotalCoins = res.TimeSeries[0].TotalCoins

	return res, nil
}

func (r *repository) totalCoinsDates(now *time.Time, days uint64) ([]stdlibtime.Time, []*TotalCoinsTimeSeriesDataPoint) {
	var (
		truncationInterval = r.cfg.GlobalAggregationInterval.Child
		dates              = make([]stdlibtime.Time, 0, days)
		timeSeries         = make([]*TotalCoinsTimeSeriesDataPoint, 0, days)
		dayInterval        = r.cfg.GlobalAggregationInterval.Parent
		start              = now.Add(-1 * truncationInterval).Truncate(truncationInterval)
	)
	dates = append(dates, start)
	timeSeries = append(timeSeries, &TotalCoinsTimeSeriesDataPoint{Date: start})
	for day := uint64(0); day < days-1; day++ {
		var date stdlibtime.Time
		date = now.Add(dayInterval * -1 * stdlibtime.Duration(day)).Truncate(dayInterval)
		dates = append(dates, date)
		timeSeries = append(timeSeries, &TotalCoinsTimeSeriesDataPoint{Date: date})
	}
	return dates, timeSeries
}

func (r *repository) cacheTotalCoins(ctx context.Context, coins []*dwh.TotalCoins) error {
	val := make([]interface{ Key() string }, 0, len(coins))
	for _, v := range coins {
		val = append(val, v)
	}
	return errors.Wrapf(storage.Set(ctx, r.db, val...), "failed to set cache value for total coins: %#v", coins)
}

func (r *repository) getCachedTotalCoins(ctx context.Context, dates []stdlibtime.Time) ([]*dwh.TotalCoins, error) {
	keys := make([]string, 0, len(dates))
	for _, d := range dates {
		keys = append(keys, r.totalCoinsCacheKey(d))
	}
	cached, err := storage.Get[dwh.TotalCoins](ctx, r.db, keys...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get cached coinStats for dates %v", dates)
	}

	return cached, nil
}

func (r *repository) totalCoinsCacheKey(date stdlibtime.Time) string {
	return fmt.Sprintf("totalCoinStats:%v", date.Truncate(r.cfg.GlobalAggregationInterval.Child).Format(stdlibtime.RFC3339))
}

func (r *repository) keepTotalCoinsCacheUpdated(ctx context.Context) {
	for ctx.Err() == nil {
		now := time.Now()
		if now.Sub(now.Truncate(r.cfg.GlobalAggregationInterval.Child)) >= stdlibtime.Duration(float64(r.cfg.GlobalAggregationInterval.Child)*0.75) {
			dwhCtx, cancel := context.WithTimeout(ctx, 1*stdlibtime.Minute)
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
			if cached.CreatedAt.Equal(date) {
				dates = append(dates[:dateIdx], dates[dateIdx+1:]...)

				break
			}
		}
	}
	workerPool := pond.New(routinesCountToInitCoinsCacheOnStartup, 0, pond.MinWorkers(routinesCountToInitCoinsCacheOnStartup))
	for _, date := range dates {
		fetchDate := date
		workerPool.Submit(func() {
			err = errors.New("first try")
			for err != nil {
				log.Info(fmt.Sprintf("Building total coins cache for %v", fetchDate))
				err = errors.Wrapf(r.buildTotalCoinCache(ctx, fetchDate), "failed to build/init total coins cache for %v", fetchDate)
				if err != nil {
					log.Error(err)
				}
			}
		})
	}
	workerPool.StopAndWait()
}
