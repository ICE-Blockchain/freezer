// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"testing"
	stdlibtime "time"

	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appCfg "github.com/ice-blockchain/wintr/config"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/time"
)

func helperCreateRepoWithRedisOnly(t *testing.T) *repository {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			t.Skip("skipping test; redis is not available")
		}
	}()

	var cfg Config
	appCfg.MustLoadFromKey(applicationYamlKey, &cfg)

	db := storage.MustConnect(context.TODO(), applicationYamlKey)
	repo := &repository{
		cfg: &cfg,
		shutdown: func() error {
			return multierror.Append(db.Close()).ErrorOrNil()
		},
		db: db,
	}

	return repo
}

func TestGetCoinStatsBlockchainDetails(t *testing.T) {
	t.Parallel()

	repo := helperCreateRepoWithRedisOnly(t)

	t.Run("InvalidConfig", func(t *testing.T) {
		repo.cfg.DetailedCoinMetrics.RefreshInterval = 0
		require.Panics(t, func() {
			repo.keepBlockchainDetailsCacheUpdated(context.Background())
		})
	})

	t.Run("ReadFromEmptyCache", func(t *testing.T) {
		_, err := repo.db.Del(context.TODO(), totalCoinStatsDetailsKey).Result()
		require.NoError(t, err)

		data, err := repo.loadCachedBlockchainDetails(context.TODO())
		require.NoError(t, err)
		require.Nil(t, data)
	})

	t.Run("FillFromKeeper", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), stdlibtime.Second*2)
		defer cancel()

		repo.cfg.DetailedCoinMetrics.RefreshInterval = stdlibtime.Minute
		repo.keepBlockchainDetailsCacheUpdated(ctx)
	})

	t.Run("CheckTimestampNoUpdate", func(t *testing.T) {
		err := repo.updateCachedBlockchainDetails(context.TODO())
		require.NoError(t, err)
	})

	t.Run("ReadCache", func(t *testing.T) {
		data, err := repo.loadCachedBlockchainDetails(context.TODO())
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Greater(t, data.CurrentPrice, 0.0)
		require.Greater(t, data.Volume24h, 0.0)
		require.NotNil(t, data.Timestamp)
	})

	require.NoError(t, repo.Close())
}

func TestTotalCoinsDates_HistoryGenerationDeltaPassed(t *testing.T) {
	t.Parallel()

	var cfg Config
	appCfg.MustLoadFromKey(applicationYamlKey, &cfg)
	cfg.GlobalAggregationInterval.Parent = 24 * stdlibtime.Hour
	cfg.GlobalAggregationInterval.Child = 1 * stdlibtime.Hour
	repo := &repository{cfg: &cfg}

	now := time.New(stdlibtime.Date(2023, 7, 9, 5, 15, 10, 1, stdlibtime.UTC))
	dates, timeSeries := repo.totalCoinsDates(now, 7)
	assert.Equal(t, []stdlibtime.Time{
		now.Truncate(cfg.GlobalAggregationInterval.Parent),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-1 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-2 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-3 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-4 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-5 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-6 * 24 * stdlibtime.Hour),
	}, dates)
	assert.Equal(t, []*TotalCoinsTimeSeriesDataPoint{
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-1 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-2 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-3 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-4 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-5 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-6 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
	}, timeSeries)
}

func TestTotalCoinsDates_HistoryGenerationDeltaNotPassed(t *testing.T) {
	t.Parallel()

	var cfg Config
	appCfg.MustLoadFromKey(applicationYamlKey, &cfg)
	cfg.GlobalAggregationInterval.Parent = 24 * stdlibtime.Hour
	cfg.GlobalAggregationInterval.Child = 1 * stdlibtime.Hour
	repo := &repository{cfg: &cfg}

	now := time.New(stdlibtime.Date(2023, 7, 9, 0, 15, 10, 1, stdlibtime.UTC))
	dates, timeSeries := repo.totalCoinsDates(now, 7)
	assert.Equal(t, []stdlibtime.Time{
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-1 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-2 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-3 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-4 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-5 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-6 * 24 * stdlibtime.Hour),
		now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-7 * 24 * stdlibtime.Hour),
	}, dates)
	assert.Equal(t, []*TotalCoinsTimeSeriesDataPoint{
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-1 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-2 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-3 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-4 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-5 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-6 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
		{
			Date:       now.Truncate(cfg.GlobalAggregationInterval.Parent).Add(-7 * 24 * stdlibtime.Hour),
			TotalCoins: TotalCoins{Total: 0., Blockchain: 0., Standard: 0., PreStaking: 0.},
		},
	}, timeSeries)
}
