// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"sync/atomic"
	"testing"
	stdlibtime "time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dwh "github.com/ice-blockchain/freezer/bookkeeper/storage"
	"github.com/ice-blockchain/wintr/time"
)

func TestCalculateDates_Factor1(t *testing.T) {
	t.Parallel()
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}

	limit := uint64(24)
	offset := uint64(0)
	start := time.New(stdlibtime.Date(2023, 6, 6, 5, 15, 10, 1, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 7, 5, 15, 10, 1, stdlibtime.UTC))
	factor := stdlibtime.Duration(1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 30)
	assert.Equal(t, start, notBeforeTime)
	assert.Equal(t, end, notAfterTime)

	expected := []stdlibtime.Time{
		stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 10, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 11, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 12, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 13, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 14, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 15, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 16, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 17, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 18, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 19, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 20, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 21, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 22, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 23, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 24, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 25, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 26, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 27, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 28, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 29, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 30, 0, 0, 0, 0, stdlibtime.UTC),
	}
	assert.EqualValues(t, expected, dates)
}

func TestCalculateDates_Limit24_Offset0_FactorMinus1(t *testing.T) {
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}
	limit := uint64(24)
	offset := uint64(0)
	start := time.New(stdlibtime.Date(2023, 6, 7, 5, 15, 10, 1, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 6, 5, 15, 10, 1, stdlibtime.UTC))
	factor := stdlibtime.Duration(-1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 30)
	assert.Equal(t, end, notBeforeTime)
	assert.Equal(t, start, notAfterTime)

	expected := []stdlibtime.Time{
		stdlibtime.Date(2023, 6, 30, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 29, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 28, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 27, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 26, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 25, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 24, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 23, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 22, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 21, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 20, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 19, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 18, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 17, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 16, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 15, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 14, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 13, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 12, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 11, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 10, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, stdlibtime.UTC),
		stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC),
	}
	assert.EqualValues(t, expected, dates)
}

func TestProcessBalanceHistory_ChildIsHour_AfterBeforeLimits(t *testing.T) {
	t.Parallel()
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}
	now := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	utcOffset := stdlibtime.Duration(0)
	location := stdlibtime.FixedZone(utcOffset.String(), int(utcOffset.Seconds()))

	/******************************************************************************************************************************************************
		1. History - data from clickhouse.
	******************************************************************************************************************************************************/
	history := []*dwh.BalanceHistory{
		{
			CreatedAt:           time.New(now.Add(-1 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(-2 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  28.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(-3 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  32.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(-4 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  31.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(-5 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(-6 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 17.,
		},
	}
	/******************************************************************************************************************************************************
		2. Not before time is -10 days. Not after time = now. startDateIsBeforeEndDate = true.
	******************************************************************************************************************************************************/

	notBeforeTime := time.New(now.Add(-10 * repo.cfg.GlobalAggregationInterval.Parent))
	notAfterTime := now
	startDateIsBeforeEndDate := true

	entries := repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime, utcOffset)
	expected := []*BalanceHistoryEntry{
		{
			Time: stdlibtime.Date(2023, 5, 1, 0, 0, 0, 0, location),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   8.,
				Amount:   "8.00",
				Bonus:    0.,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 30, 0, 0, 0, 0, location),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   -17.,
						Amount:   "17.00",
						Bonus:    0.,
						Negative: true,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, location),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25,
						Amount:   "25.00",
						Bonus:    247.06,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   116,
				Amount:   "116.00",
				Bonus:    1350.0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.,
						Amount:   "31.00",
						Bonus:    24,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.,
						Amount:   "32.00",
						Bonus:    3.23,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Bonus:    -12.5,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.,
						Amount:   "25.00",
						Bonus:    -10.71,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)

	/******************************************************************************************************************************************************
		3. Not before time is -5 hours. Not after time = now. startDateIsBeforeEndDate = true.
	******************************************************************************************************************************************************/
	notBeforeTime = time.New(now.Add(-5 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent))
	notAfterTime = time.New(now.Truncate(repo.cfg.GlobalAggregationInterval.Parent))
	startDateIsBeforeEndDate = true

	entries = repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime, utcOffset)
	expected = []*BalanceHistoryEntry{
		{
			Time: stdlibtime.Date(2023, 5, 1, 0, 0, 0, 0, location),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   8.,
				Amount:   "8.00",
				Bonus:    0.,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, location),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25,
						Amount:   "25.00",
						Bonus:    0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   116,
				Amount:   "116.00",
				Bonus:    1350.0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.,
						Amount:   "31.00",
						Bonus:    24,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.,
						Amount:   "32.00",
						Bonus:    3.23,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Bonus:    -12.5,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.,
						Amount:   "25.00",
						Bonus:    -10.71,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)

	/******************************************************************************************************************************************************
		3. Not before time is -5 hours. Not after time = now. startDateIsBeforeEndDate = false.
	******************************************************************************************************************************************************/
	startDateIsBeforeEndDate = false
	entries = repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime, utcOffset)
	expected = []*BalanceHistoryEntry{
		{
			Time: stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   116,
				Amount:   "116.00",
				Bonus:    1350.0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.,
						Amount:   "25.00",
						Bonus:    -10.71,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Bonus:    -12.5,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.,
						Amount:   "32.00",
						Bonus:    3.23,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.,
						Amount:   "31.00",
						Bonus:    24,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 5, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   8.,
				Amount:   "8.00",
				Bonus:    0.,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, location),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25,
						Amount:   "25.00",
						Bonus:    0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)
}

func TestProcessBalanceHistory_ChildIsHour_ProdLimits(t *testing.T) {
	t.Parallel()
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}
	now := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	utcOffset := stdlibtime.Duration(0)
	location := stdlibtime.FixedZone(utcOffset.String(), int(utcOffset.Seconds()))

	/******************************************************************************************************************************************************
		1. History - data from clickhouse.
	******************************************************************************************************************************************************/
	history := []*dwh.BalanceHistory{
		{
			CreatedAt:           time.New(now.Add(1 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(2 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  28.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(3 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  32.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(4 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  31.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(5 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(6 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  12.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(7 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  29.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(8 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 5.,
		},
		{
			CreatedAt:           time.New(now.Add(9 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 19.,
		},
		{
			CreatedAt:           time.New(now.Add(10 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 20.,
		},
		{
			CreatedAt:           time.New(now.Add(11 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  13.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(12 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(13 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(14 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(15 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  5.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(16 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(17 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(18 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  30.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(19 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  28.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(20 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  32.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(21 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  31.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(22 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(23 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 17.,
		},
		{
			CreatedAt:           time.New(now.Add(24 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(25 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(26 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(27 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  10.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(28 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(29 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  10.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(30 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
	}

	/******************************************************************************************************************************************************
		2. request = 1 day. startDateIsBeforeEndDate = false.
	******************************************************************************************************************************************************/
	notBeforeTime := time.New(now.Truncate(repo.cfg.GlobalAggregationInterval.Parent))
	notAfterTime := time.New(now.Add(1 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent))
	startDateIsBeforeEndDate := false

	entries := repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime, utcOffset)
	expected := []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   410,
				Amount:   "410.00",
				Bonus:    0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25,
						Amount:   "25.00",
						Bonus:    0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)

	/******************************************************************************************************************************************************
		3. request = week. startDateIsBeforeEndDate = false.
	******************************************************************************************************************************************************/
	notBeforeTime = now
	notAfterTime = time.New(now.Add(7 * repo.cfg.GlobalAggregationInterval.Parent))
	startDateIsBeforeEndDate = false

	entries = repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime, utcOffset)
	expected = []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   410,
				Amount:   "410.00",
				Bonus:    0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 12, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   29.0,
						Amount:   "29.00",
						Bonus:    141.67,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 11, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   12.0,
						Amount:   "12.00",
						Bonus:    -52,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 10, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25,
						Amount:   "25.00",
						Bonus:    -19.35,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.0,
						Amount:   "31.00",
						Bonus:    -3.13,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.0,
						Amount:   "32.00",
						Bonus:    14.29,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.0,
						Amount:   "28.00",
						Bonus:    12,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.0,
						Amount:   "25.00",
						Bonus:    0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)

	/******************************************************************************************************************************************************
		5. request = month. startDateIsBeforeEndDate = false.
	******************************************************************************************************************************************************/
	notBeforeTime = now
	notAfterTime = time.New(now.Add(30 * repo.cfg.GlobalAggregationInterval.Parent))
	startDateIsBeforeEndDate = false

	entries = repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime, utcOffset)
	expected = []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 7, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   75.0,
				Amount:   "75.00",
				Bonus:    -81.71,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 7, 5, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   20.0,
						Amount:   "20.00",
						Bonus:    100,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 7, 4, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   10.0,
						Amount:   "10.00",
						Bonus:    -50,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 7, 3, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   20.0,
						Amount:   "20.00",
						Bonus:    100,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 7, 2, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   10.0,
						Amount:   "10.00",
						Bonus:    -33.33,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 7, 1, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   15.0,
						Amount:   "15.00",
						Bonus:    -25.00,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   410,
				Amount:   "410.00",
				Bonus:    0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 30, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   20.0,
						Amount:   "20.00",
						Bonus:    33.33,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 29, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   15.0,
						Amount:   "15.00",
						Bonus:    188.24,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 28, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   -17.0,
						Amount:   "17.00",
						Bonus:    -168,
						Negative: true,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 27, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.0,
						Amount:   "25.00",
						Bonus:    -19.35,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 26, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.0,
						Amount:   "31.00",
						Bonus:    -3.13,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 25, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.0,
						Amount:   "32.00",
						Bonus:    14.29,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 24, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.0,
						Amount:   "28.00",
						Bonus:    -6.67,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 23, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   30.0,
						Amount:   "30.00",
						Bonus:    20,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 22, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.0,
						Amount:   "25.00",
						Bonus:    25,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 21, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   20.0,
						Amount:   "20.00",
						Bonus:    300,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 20, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   5.0,
						Amount:   "5.00",
						Bonus:    -66.67,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 19, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   15.0,
						Amount:   "15.00",
						Bonus:    -0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 18, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   15.0,
						Amount:   "15.00",
						Bonus:    -0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 17, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   15.0,
						Amount:   "15.00",
						Bonus:    15.38,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 16, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   13.0,
						Amount:   "13.00",
						Bonus:    165,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 15, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   -20.0,
						Amount:   "20.00",
						Bonus:    5.26,
						Negative: true,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 14, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   -19.0,
						Amount:   "19.00",
						Bonus:    280,
						Negative: true,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 13, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   -5.0,
						Amount:   "5.00",
						Bonus:    -117.24,
						Negative: true,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 12, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   29.0,
						Amount:   "29.00",
						Bonus:    141.67,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 11, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   12.0,
						Amount:   "12.00",
						Bonus:    -52,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 10, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25,
						Amount:   "25.00",
						Bonus:    -19.35,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.0,
						Amount:   "31.00",
						Bonus:    -3.13,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.0,
						Amount:   "32.00",
						Bonus:    14.29,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.0,
						Amount:   "28.00",
						Bonus:    12,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.0,
						Amount:   "25.00",
						Bonus:    0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)
}

func TestProcessBalanceHistory_ChildIsHour_Location(t *testing.T) {
	t.Parallel()
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}
	now := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	utcOffset := stdlibtime.Duration(3 * stdlibtime.Hour)
	location := stdlibtime.FixedZone(utcOffset.String(), int(utcOffset.Seconds()))

	/******************************************************************************************************************************************************
		1. History - data from clickhouse.
	******************************************************************************************************************************************************/
	history := []*dwh.BalanceHistory{
		{
			CreatedAt:           time.New(now.Add(1 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(2 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  28.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(3 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  32.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(4 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  31.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(5 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(6 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  12.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(7 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  29.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(8 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 5.,
		},
		{
			CreatedAt:           time.New(now.Add(9 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 19.,
		},
		{
			CreatedAt:           time.New(now.Add(10 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 20.,
		},
		{
			CreatedAt:           time.New(now.Add(11 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  13.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(12 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(13 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(14 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(15 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  5.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(16 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(17 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(18 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  30.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(19 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  28.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(20 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  32.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(21 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  31.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(22 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(23 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 17.,
		},
		{
			CreatedAt:           time.New(now.Add(24 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(25 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(26 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  15.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(27 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  10.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(28 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(29 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  10.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(30 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent)),
			BalanceTotalMinted:  20.,
			BalanceTotalSlashed: 0.,
		},
	}

	/******************************************************************************************************************************************************
		3. request = week. startDateIsBeforeEndDate = false.
	******************************************************************************************************************************************************/
	notBeforeTime := now
	notAfterTime := time.New(now.Add(7 * repo.cfg.GlobalAggregationInterval.Parent))
	startDateIsBeforeEndDate := false

	entries := repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime, utcOffset)
	expected := []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, location)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   410,
				Amount:   "410.00",
				Bonus:    0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 12, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   29.0,
						Amount:   "29.00",
						Bonus:    141.67,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 11, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   12.0,
						Amount:   "12.00",
						Bonus:    -52,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 10, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25,
						Amount:   "25.00",
						Bonus:    -19.35,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.0,
						Amount:   "31.00",
						Bonus:    -3.13,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.0,
						Amount:   "32.00",
						Bonus:    14.29,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.0,
						Amount:   "28.00",
						Bonus:    12,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, location)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.0,
						Amount:   "25.00",
						Bonus:    0,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)

}

//nolint:lll // .
func TestEnhanceWithBlockchainCoinStats(t *testing.T) {
	cfg := Config{GlobalAggregationInterval: struct {
		Parent stdlibtime.Duration `yaml:"parent"`
		Child  stdlibtime.Duration `yaml:"child"`
	}(struct {
		Parent stdlibtime.Duration
		Child  stdlibtime.Duration
	}{Parent: 24 * stdlibtime.Hour, Child: 1 * stdlibtime.Hour})}

	r := &repository{cfg: &cfg}
	r.cfg.blockchainCoinStatsJSON = new(atomic.Pointer[blockchainCoinStatsJSON])
	_, dates := r.totalCoinsDates(time.Now(), 5)
	totalBlockchainLastDay := float64(366270)
	sourceStats := &TotalCoinsSummary{
		TimeSeries: []*TotalCoinsTimeSeriesDataPoint{
			{
				Date: dates[0].Date,
				TotalCoins: TotalCoins{
					Total:      29830000,
					Blockchain: totalBlockchainLastDay,
					Standard:   29830000,
					PreStaking: 21820000,
				},
			},
			{
				Date: dates[1].Date,
				TotalCoins: TotalCoins{
					Total:      29770000,
					Blockchain: 355530,
					Standard:   29770000,
					PreStaking: 21770000,
				},
			},
			{
				Date: dates[2].Date,
				TotalCoins: TotalCoins{
					Total:      29600000,
					Blockchain: 344940,
					Standard:   29600000,
					PreStaking: 21610000,
				},
			},
			{
				Date: dates[3].Date,
				TotalCoins: TotalCoins{
					Total:      29410000,
					Blockchain: 334510,
					Standard:   29410000,
					PreStaking: 21100000,
				},
			},
			{
				Date: dates[4].Date,
				TotalCoins: TotalCoins{
					Total:      29110000,
					Blockchain: 324000,
					Standard:   29110000,
					PreStaking: 20890000,
				},
			},
		},
		TotalCoins: TotalCoins{
			Total:      29830000,
			Blockchain: totalBlockchainLastDay,
			Standard:   29830000,
			PreStaking: 21820000,
		},
	}
	t.Run("applied for only one day (first)", func(t *testing.T) {
		r.cfg.blockchainCoinStatsJSON.Store(&blockchainCoinStatsJSON{
			CoinsAddedHistory: []*struct {
				Date       *time.Time `json:"date"`
				CoinsAdded float64    `json:"coinsAdded"`
			}{
				{CoinsAdded: 100, Date: time.New(dates[0].Date.Add(-1 * stdlibtime.Second))},
			},
		})
		resultStats := r.enhanceWithBlockchainCoinStats(sourceStats)
		expectedStats := expectedEnhancedBlockchainStats(sourceStats, totalBlockchainLastDay+(100), []float64{
			totalBlockchainLastDay + 100, 355730, 345340, 334410, 329100,
		})
		require.EqualValues(t, expectedStats, resultStats)
	})
	t.Run("applied for all days, nothing before most recent", func(t *testing.T) {
		r.cfg.blockchainCoinStatsJSON.Store(&blockchainCoinStatsJSON{
			CoinsAddedHistory: []*struct {
				Date       *time.Time `json:"date"`
				CoinsAdded float64    `json:"coinsAdded"`
			}{
				{CoinsAdded: 10740, Date: time.New(dates[0].Date.Add(-1 * stdlibtime.Second))},
				{CoinsAdded: 10590, Date: time.New(dates[1].Date.Add(-1 * stdlibtime.Second))},
				{CoinsAdded: 10430, Date: time.New(dates[2].Date.Add(-1 * stdlibtime.Second))},
				{CoinsAdded: 10510, Date: time.New(dates[3].Date.Add(-1 * stdlibtime.Second))},
			},
		})
		resultStats := r.enhanceWithBlockchainCoinStats(sourceStats)
		expectedStats := expectedEnhancedBlockchainStats(sourceStats, totalBlockchainLastDay+10510+10430+10590+10740, []float64{
			totalBlockchainLastDay + 10510 + 10430 + 10590 + 10740,
			355530 + 10510 + 10430 + 10590,
			344940 + 10510 + 10430,
			334510 + 10510,
			324000,
		})
		require.EqualValues(t, expectedStats, resultStats)
	})
	t.Run("applied for all days, and before most recent entry => affects total", func(t *testing.T) {
		mostRecentAdditionalCoins := float64(100)
		r.cfg.blockchainCoinStatsJSON.Store(&blockchainCoinStatsJSON{
			CoinsAddedHistory: []*struct {
				Date       *time.Time `json:"date"`
				CoinsAdded float64    `json:"coinsAdded"`
			}{
				{CoinsAdded: mostRecentAdditionalCoins, Date: time.New(dates[0].Date.Add(-10 * stdlibtime.Second))},
				{CoinsAdded: 10740, Date: time.New(dates[0].Date.Add(-1 * stdlibtime.Second))},
				{CoinsAdded: 10590, Date: time.New(dates[1].Date.Add(-1 * stdlibtime.Second))},
				{CoinsAdded: 10430, Date: time.New(dates[2].Date.Add(-1 * stdlibtime.Second))},
				{CoinsAdded: 10510, Date: time.New(dates[3].Date.Add(-1 * stdlibtime.Second))},
			},
		})
		resultStats := r.enhanceWithBlockchainCoinStats(sourceStats)
		expectedStats := expectedEnhancedBlockchainStats(sourceStats, totalBlockchainLastDay+10510+10430+10590+10740+mostRecentAdditionalCoins, []float64{
			totalBlockchainLastDay + 10510 + 10430 + 10590 + 10740 + mostRecentAdditionalCoins,
			355530 + 10510 + 10430 + 10590,
			344940 + 10510 + 10430,
			334510 + 10510,
			324000,
		})
		require.EqualValues(t, expectedStats, resultStats)
	})
}

func expectedEnhancedBlockchainStats(sourceStats *TotalCoinsSummary, totals float64, blockchainCoins []float64) *TotalCoinsSummary {
	expected := *sourceStats
	for i, c := range blockchainCoins {
		expected.TimeSeries[i].Blockchain = c
	}
	expected.Blockchain = totals

	return &expected
}
