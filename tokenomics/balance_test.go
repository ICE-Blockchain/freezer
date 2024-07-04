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

func TestCalculateDates_Limit24_Offset0_Factor1(t *testing.T) {
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
	start := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 7, 5, 15, 10, 1, stdlibtime.UTC))
	factor := stdlibtime.Duration(1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 1)

	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime)

	expectedStart := time.New(stdlibtime.Date(2023, 6, 5, 5, 0, 0, 0, stdlibtime.UTC))
	expected := []stdlibtime.Time{
		expectedStart.Time.Truncate(24 * stdlibtime.Hour),
	}
	assert.EqualValues(t, expected, dates)

}

func TestCalculateDates_Limit12_Offset0_Factor1(t *testing.T) {
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
	limit := uint64(12)
	offset := uint64(0)
	start := time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC))
	factor := stdlibtime.Duration(1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 0)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime) // Cuz calculated limit is 0.

	assert.Empty(t, dates)
}

func TestCalculateDates_Limit36_Offset0_Factor1(t *testing.T) {
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}

	limit := uint64(36)
	offset := uint64(0)
	start := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 7, 5, 15, 10, 1, stdlibtime.UTC))
	factor := stdlibtime.Duration(1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 1)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime)

	expectedStart := time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC))
	expected := []stdlibtime.Time{
		*expectedStart.Time,
	}
	assert.EqualValues(t, expected, dates)
}

func TestCalculateDates_Limit48_Offset0_Factor1(t *testing.T) {
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}
	limit := uint64(48)
	offset := uint64(0)
	start := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 6, 5, 15, 10, 1, stdlibtime.UTC))
	factor := stdlibtime.Duration(1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 2)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime)

	expectedStart := time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC))
	expected := []stdlibtime.Time{
		*expectedStart.Time,
		expectedStart.Add(1 * repo.cfg.GlobalAggregationInterval.Parent),
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
	start := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	var end *time.Time
	factor := stdlibtime.Duration(-1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 1)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime)

	expectedStart := time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC))
	expected := []stdlibtime.Time{
		*expectedStart.Time,
	}
	assert.EqualValues(t, expected, dates)
}

func TestCalculateDates_Limit24_Offset24_FactorMinus1(t *testing.T) {
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
	offset := uint64(24)
	start := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	var end *time.Time
	factor := stdlibtime.Duration(-1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 1)

	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime)

	expectedStart := time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC))
	expected := []stdlibtime.Time{
		expectedStart.Add(-1 * repo.cfg.GlobalAggregationInterval.Parent),
	}
	assert.EqualValues(t, expected, dates)
}

func TestCalculateDates_Limit24_Offset24_Factor1(t *testing.T) {
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
	offset := uint64(24)
	start := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 7, 5, 15, 10, 1, stdlibtime.UTC))
	factor := stdlibtime.Duration(1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 1)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime)
	expectedStart := time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC))
	expected := []stdlibtime.Time{
		expectedStart.Add(1 * repo.cfg.GlobalAggregationInterval.Parent),
	}
	assert.EqualValues(t, expected, dates)
}

func TestCalculateDates_Limit48_Offset48_FactorMinus1(t *testing.T) {
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  stdlibtime.Hour,
		},
	}}
	limit := uint64(48)
	offset := uint64(48)
	start := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	end := time.New(stdlibtime.Date(2023, 6, 5, 5, 15, 10, 1, stdlibtime.UTC))
	factor := stdlibtime.Duration(-1)

	dates, notBeforeTime, notAfterTime := repo.calculateDates(limit, offset, start, end, factor)
	assert.Len(t, dates, 2)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC)), notBeforeTime)
	assert.Equal(t, time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC)), notAfterTime)
	expectedStart := time.New(stdlibtime.Date(2023, 6, 5, 0, 0, 0, 0, stdlibtime.UTC))
	expected := []stdlibtime.Time{
		expectedStart.Add(-2 * repo.cfg.GlobalAggregationInterval.Parent),
		expectedStart.Add(-3 * repo.cfg.GlobalAggregationInterval.Parent),
	}
	assert.EqualValues(t, expected, dates)
}

func TestProcessBalanceHistory_ChildIsHour(t *testing.T) {
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

	entries := repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime)
	expected := []*BalanceHistoryEntry{
		{
			Time: stdlibtime.Date(2023, 5, 30, 0, 0, 0, 0, stdlibtime.UTC),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   -17.,
				Amount:   "17.00",
				Bonus:    0.,
				Negative: true,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 30, 0, 0, 0, 0, stdlibtime.UTC),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   -17.,
						Amount:   "17.00",
						Bonus:    0.,
						Negative: true,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25,
				Amount:   "25.00",
				Bonus:    247.06,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   31.,
				Amount:   "31.00",
				Bonus:    24,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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
			Time: *time.New(stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   32.,
				Amount:   "32.00",
				Bonus:    3.23,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.,
						Amount:   "32.00",
						Bonus:    3.23,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    -12.5,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Bonus:    -12.5,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25.,
				Amount:   "25.00",
				Bonus:    -10.71,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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

	entries = repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime)
	expected = []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25,
				Amount:   "25.00",
				Bonus:    247.06,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   31.,
				Amount:   "31.00",
				Bonus:    24,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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
			Time: *time.New(stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   32.,
				Amount:   "32.00",
				Bonus:    3.23,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 2, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.,
						Amount:   "32.00",
						Bonus:    3.23,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    -12.5,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 3, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Bonus:    -12.5,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25.,
				Amount:   "25.00",
				Bonus:    -10.71,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 4, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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
}

func TestProcessBalanceHistory_ChildIsHour_TimeGrow(t *testing.T) {
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
			BalanceTotalMinted:  0.,
			BalanceTotalSlashed: 17.,
		},
	}

	/******************************************************************************************************************************************************
		2. Not before time is now. Not after time = +4 hours. startDateIsBeforeEndDate = false.
	******************************************************************************************************************************************************/
	notBeforeTime := time.New(now.Truncate(repo.cfg.GlobalAggregationInterval.Parent))
	notAfterTime := time.New(now.Add(4 * repo.cfg.GlobalAggregationInterval.Parent).Truncate(repo.cfg.GlobalAggregationInterval.Parent))
	startDateIsBeforeEndDate := true

	entries := repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime)
	expected := []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25,
				Amount:   "25.00",
				Bonus:    0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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
			Time: *time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    12,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Bonus:    12,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   32.,
				Amount:   "32.00",
				Bonus:    14.29,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.,
						Amount:   "32.00",
						Bonus:    14.29,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   31.,
				Amount:   "31.00",
				Bonus:    -3.13,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.,
						Amount:   "31.00",
						Bonus:    -3.13,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)

	startDateIsBeforeEndDate = false
	entries = repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime)
	expected = []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   31.,
				Amount:   "31.00",
				Bonus:    -3.13,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 9, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   31.,
						Amount:   "31.00",
						Bonus:    -3.13,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   32.,
				Amount:   "32.00",
				Bonus:    14.29,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 8, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   32.,
						Amount:   "32.00",
						Bonus:    14.29,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    12,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 7, 0, 0, 0, 0, stdlibtime.UTC)).Time,
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Bonus:    12,
						Negative: false,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25,
				Amount:   "25.00",
				Bonus:    0,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: *time.New(stdlibtime.Date(2023, 6, 6, 0, 0, 0, 0, stdlibtime.UTC)).Time,
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

func TestProcessBalanceHistory_ChildIsEqualToParent24H(t *testing.T) {
	t.Parallel()
	repo := &repository{cfg: &Config{
		GlobalAggregationInterval: struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		}{
			Parent: 24 * stdlibtime.Hour,
			Child:  24 * stdlibtime.Hour,
		},
	}}
	now := time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC))
	history := []*dwh.BalanceHistory{
		{
			CreatedAt:           now,
			BalanceTotalMinted:  25.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(-1 * repo.cfg.GlobalAggregationInterval.Child)),
			BalanceTotalMinted:  28.,
			BalanceTotalSlashed: 0.,
		},
		{
			CreatedAt:           time.New(now.Add(-2 * repo.cfg.GlobalAggregationInterval.Child)),
			BalanceTotalMinted:  28.,
			BalanceTotalSlashed: 0.,
		},
	}

	notBeforeTime := time.New(now.Add(-24 * repo.cfg.GlobalAggregationInterval.Child))
	notAfterTime := now
	startDateIsBeforeEndDate := true

	entries := repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime)

	expected := []*BalanceHistoryEntry{
		{
			Time: stdlibtime.Date(2023, 5, 30, 0, 0, 0, 0, stdlibtime.UTC),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    0.,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 30, 0, 0, 0, 0, stdlibtime.UTC),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Negative: false,
						Bonus:    0,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    0.,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Negative: false,
						Bonus:    0,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25.,
				Amount:   "25.00",
				Bonus:    -10.71,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.,
						Amount:   "25.00",
						Negative: false,
						Bonus:    -10.71,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}
	assert.EqualValues(t, expected, entries)

	startDateIsBeforeEndDate = false
	entries = repo.processBalanceHistory(history, startDateIsBeforeEndDate, notBeforeTime, notAfterTime)

	expected = []*BalanceHistoryEntry{
		{
			Time: *time.New(stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC)).Time,
			Balance: &BalanceHistoryBalanceDiff{
				amount:   25.,
				Amount:   "25.00",
				Bonus:    -10.71,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 6, 1, 0, 0, 0, 0, stdlibtime.UTC),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   25.,
						Amount:   "25.00",
						Negative: false,
						Bonus:    -10.71,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    0.,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 31, 0, 0, 0, 0, stdlibtime.UTC),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Negative: false,
						Bonus:    0,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
		{
			Time: stdlibtime.Date(2023, 5, 30, 0, 0, 0, 0, stdlibtime.UTC),
			Balance: &BalanceHistoryBalanceDiff{
				amount:   28.,
				Amount:   "28.00",
				Bonus:    0.,
				Negative: false,
			},
			TimeSeries: []*BalanceHistoryEntry{
				{
					Time: stdlibtime.Date(2023, 5, 30, 0, 0, 0, 0, stdlibtime.UTC),
					Balance: &BalanceHistoryBalanceDiff{
						amount:   28.,
						Amount:   "28.00",
						Negative: false,
						Bonus:    0,
					},
					TimeSeries: []*BalanceHistoryEntry{},
				},
			},
		},
	}

	assert.EqualValues(t, expected, entries)
}
