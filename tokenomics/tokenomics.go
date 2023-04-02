// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	stdlibtime "time"

	"github.com/cenkalti/backoff/v4"
	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/ice-blockchain/eskimo/users"
	appCfg "github.com/ice-blockchain/wintr/config"
	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v2"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/multimedia/picture"
	"github.com/ice-blockchain/wintr/time"
)

func New(ctx context.Context, _ context.CancelFunc) Repository {
	var cfg config
	appCfg.MustLoadFromKey(applicationYamlKey, &cfg)

	db := storage.MustConnect(ctx, "", applicationYamlKey)

	return &repository{
		cfg:           &cfg,
		shutdown:      db.Close,
		db:            db,
		pictureClient: picture.New(applicationYamlKey),
	}
}

func StartProcessor(ctx context.Context, cancel context.CancelFunc) Processor {
	var cfg config
	appCfg.MustLoadFromKey(applicationYamlKey, &cfg)
	prc := &processor{repository: &repository{
		cfg:           &cfg,
		db:            storage.MustConnect(ctx, getDDL(ddl, &cfg), applicationYamlKey),
		mb:            messagebroker.MustConnect(ctx, applicationYamlKey),
		pictureClient: picture.New(applicationYamlKey),
	}}
	//nolint:contextcheck // It's intended. Cuz we want to close everything gracefully.
	mbConsumer := messagebroker.MustConnectAndStartConsuming(context.Background(), cancel, applicationYamlKey,
		&usersTableSource{processor: prc},
		&globalTableSource{processor: prc},
		&miningSessionsTableSource{processor: prc},
		&addBalanceCommandsSource{processor: prc},
		&viewedNewsSource{processor: prc},
		&deviceMetadataTableSource{processor: prc},
	)
	prc.shutdown = closeAll(mbConsumer, prc.mb, prc.db)

	prc.initializeExtraBonusWorkers(ctx)
	prc.mustNotifyCurrentAdoption(ctx)
	go prc.startStreams(ctx)
	go prc.startCleaners(ctx)

	return prc
}

func getDDL(ddl string, cfg *config) string {
	extraBonusesValues := make([]string, 0, len(cfg.ExtraBonuses.FlatValues))
	for ix, value := range cfg.ExtraBonuses.FlatValues {
		extraBonusesValues = append(extraBonusesValues, fmt.Sprintf("(%v,%v)", ix, value))
	}
	now := time.Now()
	adoptionValues := make([]string, 0, len(cfg.AdoptionMilestoneSwitch.ActiveUserMilestones))
	for ix, milestone := range cfg.AdoptionMilestoneSwitch.ActiveUserMilestones {
		achievedAtDate := "null"
		if ix == 0 {
			achievedAtDate = fmt.Sprintf("'%v'", now.Add(-24*stdlibtime.Hour).UTC().Format("2006-01-02 15:04:05"))
		}
		adoptionValues = append(adoptionValues, fmt.Sprintf("(%v,%v,'%v',%v)", ix+1, milestone.Users, milestone.BaseMiningRate, achievedAtDate))
	}

	return fmt.Sprintf(ddl,
		cfg.WorkerCount-1,
		strings.Join(extraBonusesValues, ","),
		now.Add(-1*users.NanosSinceMidnight(now)).UnixNano(),
		strings.Join(adoptionValues, ","))
}

func (r *repository) Close() error {
	return errors.Wrap(r.shutdown(), "closing repository failed")
}

func closeAll(mbConsumer, mbProducer messagebroker.Client, db *storage.DB, otherClosers ...func() error) func() error {
	return func() error {
		err1 := errors.Wrap(mbConsumer.Close(), "closing mbConsumer connection failed")
		err2 := errors.Wrap(db.Close(), "closing db connection failed")
		err3 := errors.Wrap(mbProducer.Close(), "closing message broker producer connection failed")
		errs := make([]error, 0, 1+1+1+len(otherClosers))
		errs = append(errs, err1, err2, err3)
		for _, closeOther := range otherClosers {
			if err := closeOther(); err != nil {
				errs = append(errs, err)
			}
		}

		return errors.Wrap(multierror.Append(nil, errs...).ErrorOrNil(), "failed to close resources")
	}
}

func (p *processor) Close() error {
	if p.cancelStreams != nil {
		p.cancelStreams()
	}
	if p.streamsDoneWg != nil {
		p.streamsDoneWg.Wait()
	}

	return errors.Wrap(p.repository.Close(), "closing repository failed")
}

func (p *processor) startStreams(ctx context.Context) { //nolint:funlen // .
	if ctx.Err() != nil {
		return
	}
	log.Info("trying to start streams")
	const key = "streams-processing-exclusive-lock"
	tuple := &users.GlobalUnsigned{Key: key, Value: uint64(time.Now().UnixNano())}
	if err := p.insertGlobalUnsignedValue(ctx, tuple, false); err != nil {
		log.Error(errors.Wrapf(err, "failed to start streams, because failed to insert into global: %#v", tuple))
		const waitDuration = 5 * stdlibtime.Second
		stdlibtime.Sleep(waitDuration)
		p.startStreams(ctx)

		return
	}
	log.Info("streams started")
	defer func() {
		log.Error(errors.Wrapf(p.deleteGlobalUnsignedValue(context.Background(), key), "failed to deleteGlobalUnsignedValue(%v)", key))
	}()
	streamsCtx, cancelStreams := context.WithCancel(ctx)
	p.streamsDoneWg = new(sync.WaitGroup)
	p.streamsDoneWg.Add(1 + 1 + 1 + 1)
	p.cancelStreams = cancelStreams
	go func() {
		defer p.streamsDoneWg.Done()
		(&balanceRecalculationTriggerStreamSource{processor: p}).start(streamsCtx)
	}()
	go func() {
		defer p.streamsDoneWg.Done()
		(&miningRatesRecalculationTriggerStreamSource{processor: p}).start(streamsCtx)
	}()
	go func() {
		defer p.streamsDoneWg.Done()
		(&blockchainBalanceSynchronizationTriggerStreamSource{processor: p}).start(streamsCtx)
	}()
	go func() {
		defer p.streamsDoneWg.Done()
		(&extraBonusProcessingTriggerStreamSource{processor: p}).start(streamsCtx)
	}()
	p.streamsDoneWg.Wait()
	log.Info("streams stopped")
}

func (p *processor) startCleaners(ctx context.Context) {
	ticker := stdlibtime.NewTicker(stdlibtime.Duration(10+rand.Intn(30)) * stdlibtime.Second) //nolint:gosec,gomnd // Not an  issue.
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			const deadline = 30 * stdlibtime.Second
			reqCtx, cancel := context.WithTimeout(ctx, deadline)
			log.Error(errors.Wrap(p.deleteOldProcessedMiningSessions(reqCtx), "failed to deleteOldProcessedMiningSessions"))
			cancel()
		case <-ctx.Done():
			return
		}
	}
}

func (p *processor) deleteOldProcessedMiningSessions(ctx context.Context) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	sql := `DELETE FROM processed_mining_sessions WHERE session_number < $1`
	if _, err := storage.Exec(ctx, p.db, sql, p.sessionNumber(time.New(time.Now().Add(-24*stdlibtime.Hour)))); err != nil {
		return errors.Wrap(err, "failed to delete old data from processed_mining_sessions")
	}

	return nil
}

func (p *processor) CheckHealth(ctx context.Context) error {
	if err := p.db.Ping(ctx); err != nil {
		return errors.Wrap(err, "[health-check] failed to ping DB")
	}
	type ts struct {
		TS *time.Time `json:"ts"`
	}
	now := ts{TS: time.Now()}
	bytes, err := json.MarshalContext(ctx, now)
	if err != nil {
		return errors.Wrapf(err, "[health-check] failed to marshal %#v", now)
	}
	responder := make(chan error, 1)
	p.mb.SendMessage(ctx, &messagebroker.Message{
		Headers: map[string]string{"producer": "freezer"},
		Key:     p.cfg.MessageBroker.Topics[0].Name,
		Topic:   p.cfg.MessageBroker.Topics[0].Name,
		Value:   bytes,
	}, responder)

	return errors.Wrapf(<-responder, "[health-check] failed to send health check message to broker")
}

func retry(ctx context.Context, op func() error) error {
	//nolint:wrapcheck // No need, its just a proxy.
	return backoff.RetryNotify(
		op,
		//nolint:gomnd // Because those are static configs.
		backoff.WithContext(&backoff.ExponentialBackOff{
			InitialInterval:     100 * stdlibtime.Millisecond,
			RandomizationFactor: 0.5,
			Multiplier:          2.5,
			MaxInterval:         stdlibtime.Second,
			MaxElapsedTime:      25 * stdlibtime.Second,
			Stop:                backoff.Stop,
			Clock:               backoff.SystemClock,
		}, ctx),
		func(e error, next stdlibtime.Duration) {
			log.Error(errors.Wrapf(e, "call failed. retrying in %v... ", next))
		})
}

func ContextWithHashCode(ctx context.Context, hashCode uint64) context.Context {
	if hashCode == 0 {
		return ctx
	}

	return context.WithValue(ctx, userHashCodeCtxValueKey, hashCode) //nolint:revive,staticcheck // Not an issue.
}

func requestingUserID(ctx context.Context) (requestingUserID string) {
	requestingUserID, _ = ctx.Value(requestingUserIDCtxValueKey).(string) //nolint:errcheck // Not needed.

	return
}

func (r *repository) workerIndex(ctx context.Context) (workerIndex int16) {
	return int16(uint64(r.hashCode(ctx)) % uint64(r.cfg.WorkerCount))
}

func (*repository) hashCode(ctx context.Context) (hashCode int64) {
	userHashCode, _ := ctx.Value(userHashCodeCtxValueKey).(uint64) //nolint:errcheck // Not needed.

	return int64(userHashCode)
}

func executeBatchConcurrently[ARG any](ctx context.Context, process func(context.Context, ARG) error, args []ARG) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "unexpected deadline")
	}
	if len(args) == 0 {
		return nil
	}
	wg := new(sync.WaitGroup)
	wg.Add(len(args))
	errChan := make(chan error, len(args))
	for i := range args {
		go func(ix int) {
			defer wg.Done()
			errChan <- errors.Wrapf(process(ctx, args[ix]), "failed to process:%#v", args[ix])
		}(i)
	}
	wg.Wait()
	close(errChan)
	errs := make([]error, 0, len(args))
	for err := range errChan {
		errs = append(errs, err)
	}

	return errors.Wrap(multierror.Append(nil, errs...).ErrorOrNil(), "at least one arg processing failed")
}

func (c *config) globalAggregationIntervalChildDateFormat() string {
	const hoursInADay = 24
	switch c.GlobalAggregationInterval.Child { //nolint:exhaustive // We don't care about the others.
	case stdlibtime.Minute:
		return minuteFormat
	case stdlibtime.Hour:
		return hourFormat
	case hoursInADay * stdlibtime.Hour:
		return dayFormat
	default:
		log.Panic(fmt.Sprintf("invalid interval: %v", c.GlobalAggregationInterval.Child))

		return ""
	}
}

func (c *config) globalAggregationIntervalParentDateFormat() string {
	const hoursInADay = 24
	switch c.GlobalAggregationInterval.Parent { //nolint:exhaustive // We don't care about the others.
	case stdlibtime.Minute:
		return minuteFormat
	case stdlibtime.Hour:
		return hourFormat
	case hoursInADay * stdlibtime.Hour:
		return dayFormat
	default:
		log.Panic(fmt.Sprintf("invalid interval: %v", c.GlobalAggregationInterval.Parent))

		return ""
	}
}

func (c *config) lastXMiningSessionsCollectingIntervalDateFormat() string {
	const hoursInADay = 24
	switch c.RollbackNegativeMining.LastXMiningSessionsCollectingInterval { //nolint:exhaustive // We don't care about the others.
	case stdlibtime.Minute:
		return minuteFormat
	case stdlibtime.Hour:
		return hourFormat
	case hoursInADay * stdlibtime.Hour:
		return dayFormat
	default:
		log.Panic(fmt.Sprintf("invalid interval: %v", c.RollbackNegativeMining.LastXMiningSessionsCollectingInterval))

		return ""
	}
}

func (r *repository) lastXMiningSessionsCollectingIntervalDateFormat(now *time.Time) string {
	return now.Format(r.cfg.lastXMiningSessionsCollectingIntervalDateFormat())
}
