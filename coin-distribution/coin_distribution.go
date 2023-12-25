// SPDX-License-Identifier: ice License 1.0

package coindistribution

import (
	"context"
	"fmt"
	stdlibtime "time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"

	appCfg "github.com/ice-blockchain/wintr/config"
	"github.com/ice-blockchain/wintr/connectors/storage/v2"
	"github.com/ice-blockchain/wintr/log"
)

func (d *databaseConfig) MustDisable(ctx context.Context, reason string) {
	for err := d.Disable(ctx); err != nil; err = d.Disable(ctx) {
		log.Error(errors.Wrap(err, "failed to disable coinDistributer"))
		stdlibtime.Sleep(stdlibtime.Millisecond)
	}

	log.Error(sendCoinDistributionsProcessingStoppedDueToUnrecoverableFailureSlackMessage(ctx, reason),
		"failed to sendCoinDistributionsProcessingStoppedDueToUnrecoverableFailureSlackMessage")
}

func databaseSetValue[T bool | constraints.Integer](ctx context.Context, db storage.Execer, key string, value T) error {
	reqCtx, cancel := context.WithTimeout(ctx, requestDeadline)
	defer cancel()

	textValue := fmt.Sprintf("%v", value)
	rows, err := storage.Exec(reqCtx, db, `UPDATE global SET value = $2 WHERE key = $1`, key, textValue)
	if err == nil && rows == 0 {
		err = storage.ErrNotFound
	}

	return errors.Wrapf(err, "failed to set %v to %q", key, textValue)
}

func databaseGetValue[T bool | constraints.Integer](ctx context.Context, db storage.Querier, key string, value *T) error {
	var hint string

	if value == nil {
		log.Panic(key + ": value is nil")
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestDeadline)
	defer cancel()

	var x any = value
	switch x.(type) {
	case *bool:
		hint = "boolean"
	case *int, *int8, *int16, *int32, *int64:
		hint = "bigint"
	case *uint, *uint8, *uint16, *uint32, *uint64:
		hint = "bigint"
	default:
		log.Panic(fmt.Sprintf("%s: unsupported type %T: %v", key, x, *value))
	}

	v, err := storage.ExecOne[T](reqCtx, db, "SELECT value::"+hint+" FROM global WHERE key = $1", key)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v", key)
	}
	*value = *v

	return nil
}

func (d *databaseConfig) GetGasLimit(ctx context.Context) (val uint64, err error) {
	err = databaseGetValue(ctx, d.DB, configKeyoinDistributerGasLimit, &val)

	return val, err
}

func (d *databaseConfig) IsEnabled(ctx context.Context) (val bool) {
	log.Error(errors.Wrap(databaseGetValue(ctx, d.DB, configKeyCoinDistributerEnabled, &val), "failed to databaseGetValue"))

	return val
}

func (d *databaseConfig) IsOnDemandMode(ctx context.Context) (val bool) {
	log.Error(databaseGetValue(ctx, d.DB, configKeyCoinDistributerOnDemand, &val), "failed to databaseGetValue")

	return val
}

func (d *databaseConfig) DisableOnDemand(ctx context.Context) error {
	return databaseSetValue(ctx, d.DB, configKeyCoinDistributerOnDemand, false)
}

func (d *databaseConfig) Disable(ctx context.Context) error {
	return databaseSetValue(ctx, d.DB, configKeyCoinDistributerEnabled, false)
}

func (d *databaseConfig) HasPendingTransactions(ctx context.Context, status ethApiStatus) bool {
	reqCtx, cancel := context.WithTimeout(ctx, requestDeadline)
	defer cancel()

	val, err := storage.ExecOne[bool](reqCtx, d.DB, `SELECT true FROM pending_coin_distributions where eth_status = $1 limit 1`, status)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			err = nil
		}
		log.Error(errors.Wrap(err, "failed to check for pending transactions"))

		return false
	}

	return *val
}

func init() {
	appCfg.MustLoadFromKey(applicationYamlKey, &cfg)
}

func MustStartCoinDistribution(ctx context.Context, _ context.CancelFunc) Client {
	cfg.EnsureValid()
	eth := mustNewEthClient(ctx, cfg.Ethereum.RPC, cfg.Ethereum.PrivateKey, cfg.Ethereum.ContractAddress)

	cd := mustCreateCoinDistributionFromConfig(ctx, &cfg, eth)
	cd.MustStart(ctx, nil, nil)

	go startPrepareCoinDistributionsForReviewMonitor(ctx, cd.DB)

	return cd
}

func mustCreateCoinDistributionFromConfig(ctx context.Context, conf *config, ethClient ethClient) *coinDistributer {
	db := storage.MustConnect(ctx, ddl, applicationYamlKey)
	cd := &coinDistributer{
		Client:    ethClient,
		Processor: newCoinProcessor(ethClient, db, conf),
		Tracker:   newCoinTracker(ethClient, db, conf),
		DB:        db,
	}

	return cd
}

func (cd *coinDistributer) MustStart(ctx context.Context, notifyProcessed chan<- *batch, notifyTracked chan<- []*string) {
	cd.Tracker.Start(ctx, notifyTracked)
	cd.Processor.Start(ctx, notifyProcessed)
}

func (cd *coinDistributer) Close() error {
	return multierror.Append( //nolint:wrapcheck //.
		errors.Wrap(cd.Processor.Close(), "failed to close processor"),
		errors.Wrap(cd.Tracker.Close(), "failed to close tracker"),
		errors.Wrap(cd.Client.Close(), "failed to close eth client"),
		errors.Wrap(cd.DB.Close(), "failed to close db"),
	).ErrorOrNil()
}

func (cd *coinDistributer) CheckHealth(ctx context.Context) error {
	return errors.Wrap(cd.DB.Ping(ctx), "[health-check] failed to ping DB")
}
