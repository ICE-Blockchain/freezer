// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	stdlibtime "time"

	"github.com/ethereum/go-ethereum"
	ethabi "github.com/ethereum/go-ethereum/accounts/abi"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	"github.com/ice-blockchain/freezer/model"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

func (r *repository) GetMiningBoostSummary(ctx context.Context, userID string) (*MiningBoostSummary, error) {
	id, err := GetOrInitInternalID(ctx, r.db, userID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", userID)
	}
	res, err := storage.Get[struct {
		model.MiningBoostLevelIndexField
		model.UserIDField
	}](ctx, r.db, model.SerializedUsersKey(id))
	if err != nil || len(res) == 0 {
		if err == nil {
			err = errors.Wrapf(ErrRelationNotFound, "missing state for id:%v", id)
		}

		return nil, errors.Wrapf(err, "failed to get mining boost info for id:%v", id)
	}

	var currentLevelIndex *uint8
	if res[0].MiningBoostLevelIndex != nil {
		val := uint8(*res[0].MiningBoostLevelIndex)
		currentLevelIndex = &val
	}

	return &MiningBoostSummary{
		Levels:            *r.cfg.MiningBoost.levels.Load(),
		CurrentLevelIndex: currentLevelIndex,
	}, nil
}

func (r *repository) InitializeMiningBoostUpgrade(ctx context.Context, miningBoostLevelIndex uint8, userID string) (*PendingMiningBoostUpgrade, error) {
	if miningBoostLevelIndex > uint8(len(r.cfg.MiningBoost.Levels)-1) {
		return nil, errors.New("mining boost already at max level")
	}
	id, err := GetOrInitInternalID(ctx, r.db, userID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", userID)
	}
	res, err := storage.Get[struct {
		model.MiningBoostLevelIndexField
		model.MiningBoostAmountBurntField
		model.UserIDField
	}](ctx, r.db, model.SerializedUsersKey(id))
	if err != nil || len(res) == 0 {
		if err == nil {
			err = errors.Wrapf(ErrRelationNotFound, "missing state for id:%v", id)
		}

		return nil, errors.Wrapf(err, "failed to get mining boost info for id:%v", id)
	}

	if res[0].MiningBoostLevelIndex != nil && uint8(*res[0].MiningBoostLevelIndex) >= miningBoostLevelIndex {
		return nil, errors.Errorf("current mining boost level `%v` is greater or equal than provided one `%v`", *res[0].MiningBoostLevelIndex, miningBoostLevelIndex)
	}

	var previousLevelPrices float64
	if res[0].MiningBoostLevelIndex != nil {
		for ix, level := range *r.cfg.MiningBoost.levels.Load() {
			if ix <= int(*res[0].MiningBoostLevelIndex) {
				previousLevelPrices += level.icePrice
			}
		}
	}
	icePrice := strconv.FormatFloat((*r.cfg.MiningBoost.levels.Load())[miningBoostLevelIndex].icePrice-previousLevelPrices, 'f', 15, 64)
	key := fmt.Sprintf("mining_boost_upgrades:%v", id)
	val := fmt.Sprintf("%v:%v", miningBoostLevelIndex, icePrice)
	result, err := r.db.Set(ctx, key, val, 15*stdlibtime.Minute).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to set new mining_boost_upgrade for userID:%v", userID)
	}
	if result != "OK" {
		return nil, errors.Errorf("unexpected db response while trying to set new mining_boost_upgrade for userID:%v, %v", userID, result)
	}

	return &PendingMiningBoostUpgrade{
		ExpiresAt:      time.New(stdlibtime.Now().Add(15 * stdlibtime.Minute)),
		ICEPrice:       icePrice,
		PaymentAddress: r.cfg.MiningBoost.PaymentAddress,
	}, nil
}

func (r *repository) FinalizeMiningBoostUpgrade(ctx context.Context, network BlockchainNetworkType, txHash, userID string) (*PendingMiningBoostUpgrade, error) {
	if network != BNBBlockchainNetworkType && network != EthereumBlockchainNetworkType && network != ArbitrumBlockchainNetworkType {
		return nil, errors.Errorf("invalid network %v", network)
	}
	id, err := GetOrInitInternalID(ctx, r.db, userID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to getOrInitInternalID for userID:%v", userID)
	}
	key := fmt.Sprintf("mining_boost_upgrades:%v", id)
	result, err := r.db.Get(ctx, key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, errors.Wrapf(err, "failed to get mining_boost_upgrades for user_id %v", userID)
	}
	parts := strings.Split(result, ":")
	if len(parts) != 2 {
		return nil, ErrNotFound
	}
	ttl, err := r.db.TTL(ctx, key).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get TTL for mining_boost_upgrades for user_id %v", userID)
	}
	expireAt := time.Now().Add(ttl.Abs())

	rawMiningBoostLevelIndex, rawICEPrice := parts[0], parts[1]
	miningBoostLevelIndex, err := strconv.ParseUint(rawMiningBoostLevelIndex, 10, 64)
	log.Panic(err)
	icePrice, err := strconv.ParseFloat(rawICEPrice, 64)
	log.Panic(err)

	res, err := storage.Get[struct {
		model.MiningBoostLevelIndexField
		model.MiningBoostAmountBurntField
		model.UserIDField
	}](ctx, r.db, model.SerializedUsersKey(id))
	if err != nil || len(res) == 0 {
		if err == nil {
			err = errors.Wrapf(ErrRelationNotFound, "missing state for id:%v", id)
		}

		return nil, errors.Wrapf(err, "failed to get mining boost info for id:%v", id)
	}

	if res[0].MiningBoostLevelIndex != nil && uint64(*res[0].MiningBoostLevelIndex) >= miningBoostLevelIndex {
		return nil, errors.Errorf("current mining boost level `%v` is greater or equal than provided one `%v`", *res[0].MiningBoostLevelIndex, miningBoostLevelIndex)
	}
	txHash = strings.ToLower(txHash)
	burntAmount, err := r.getBurntAmountForMiningBoostUpgrade(ctx, network, txHash)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, errors.Wrap(err, "failed to getBurntAmountForMiningBoostUpgrade")
	}
	if burntAmount <= 0 {
		if err != nil {
			log.Error(errors.Wrapf(err, "tx for upgrading mining boost tier is invalid: failed to getBurntAmountForMiningBoostUpgrade for tx %v userID %v", txHash, userID))
		}
		return nil, ErrInvalidMiningBoostUpgradeTX
	}

	sKey := fmt.Sprintf("mining_boost_accepted_transactions:%v", id)
	if _, zErr := r.db.ZRank(ctx, sKey, txHash).Result(); zErr != nil && !errors.Is(zErr, redis.Nil) {
		return nil, errors.Errorf("failed to check uniqueness of tx hash for userID: `%v`", userID)
	} else if zErr == nil {
		return nil, errors.Errorf("tx hash was used before: `%v`", txHash)
	}

	amount := model.FlexibleFloat64(burntAmount)
	if res[0].MiningBoostAmountBurnt != nil {
		amount += *res[0].MiningBoostAmountBurnt
	}
	newMiningBoostLevelIndex := model.FlexibleUint64(miningBoostLevelIndex)
	if extraBurntAmount := burntAmount - icePrice; extraBurntAmount > 0 {
		for ix, level := range *r.cfg.MiningBoost.levels.Load() {
			if ix > int(miningBoostLevelIndex) {
				extraBurntAmount -= level.icePrice - (*r.cfg.MiningBoost.levels.Load())[ix-1].icePrice
			}
			if extraBurntAmount >= 0 {
				newMiningBoostLevelIndex = model.FlexibleUint64(ix)
			}
		}
	}
	updatedState := struct {
		model.MiningBoostLevelIndexField
		model.MiningBoostAmountBurntField
		model.PreStakingAllocationField
		model.PreStakingBonusField
		model.DeserializedUsersKey
	}{
		MiningBoostLevelIndexField:  model.MiningBoostLevelIndexField{MiningBoostLevelIndex: &newMiningBoostLevelIndex},
		MiningBoostAmountBurntField: model.MiningBoostAmountBurntField{MiningBoostAmountBurnt: &amount},
		PreStakingAllocationField:   model.PreStakingAllocationField{PreStakingAllocation: 100},
		PreStakingBonusField:        model.PreStakingBonusField{PreStakingBonus: float64((*r.cfg.MiningBoost.levels.Load())[int(newMiningBoostLevelIndex)].MiningRateBonus)},
		DeserializedUsersKey:        model.DeserializedUsersKey{ID: id},
	}
	if responses, txErr := r.db.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		if pErr := pipeliner.HSet(ctx, updatedState.Key(), storage.SerializeValue(updatedState)...).Err(); pErr != nil {
			return pErr
		}

		member := redis.Z{
			Score:  float64(time.Now().UnixNano()),
			Member: txHash,
		}

		return pipeliner.ZAddNX(ctx, sKey, member).Err()
	}); txErr != nil {
		return nil, errors.Wrapf(err, "[1]failed to send mining boost upgrade tx pipeline for userID:%v", userID)

	} else {
		errs := make([]error, 0, 2)
		for _, response := range responses {
			if err = response.Err(); err != nil {
				errs = append(errs, errors.Wrapf(err, "failed to `%v`", response.FullName()))
			}
		}
		if err = multierror.Append(nil, errs...).ErrorOrNil(); err != nil {
			return nil, errors.Wrapf(err, "[2]failed to send mining boost upgrade tx pipeline for userID:%v", userID)
		}
	}

	if icePrice-burntAmount <= 0 {
		return nil, nil
	}

	return &PendingMiningBoostUpgrade{
		ExpiresAt:      time.New(stdlibtime.Unix(0, expireAt.UnixNano())),
		ICEPrice:       strconv.FormatFloat(icePrice-burntAmount, 'f', 15, 64),
		PaymentAddress: r.cfg.MiningBoost.PaymentAddress,
	}, nil
}

const (
	erc20ABI = `[{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`
)

func (r *repository) getBurntAmountForMiningBoostUpgrade(ctx context.Context, network BlockchainNetworkType, txHash string) (float64, error) {
	networkClient := r.cfg.MiningBoost.networkClients[network][r.cfg.MiningBoost.networkEndpointCurrentLBIndex[network].Add(1)%uint64(len(r.cfg.MiningBoost.networkClients[network]))] //nolint:lll // .

	receipt, err := networkClient.TransactionReceipt(ctx, ethcommon.HexToHash(txHash))
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return 0, ErrNotFound
		}
		return 0, errors.Wrapf(err, "failed to get TransactionReceipt for tx: %v", txHash)
	}

	parsedABI, err := ethabi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse erc 20 ABI for tx: %v", txHash)
	}

	for _, vLog := range receipt.Logs {
		if event, evErr := parsedABI.EventByID(vLog.Topics[0]); evErr != nil {
			return 0, errors.Wrapf(evErr, "failed to get EventByID: %#v", vLog)
		} else if event.Name != "Transfer" {
			continue
		}

		if vLog.Address != ethcommon.HexToAddress(r.cfg.MiningBoost.ContractAddresses[network]) {
			return 0, nil
		}

		var transferEvent struct{ Value *big.Int }
		if evErr := parsedABI.UnpackIntoInterface(&transferEvent, "Transfer", vLog.Data); evErr != nil {
			return 0, errors.Wrapf(evErr, "failed to get UnpackIntoInterface[%#v]: %#v", &transferEvent, vLog)
		}

		if ethcommon.HexToAddress(vLog.Topics[2].Hex()) == r.cfg.MiningBoost.paymentAddress && transferEvent.Value.Cmp(new(big.Int).SetUint64(0)) > 0 {
			amount, _ := transferEvent.Value.Float64()

			return amount / iceFlakesDenomination, nil
		}
	}

	return 0, nil
}

const iceFlakesDenomination = 1_000_000_000_000_000_000

func (r *repository) startICEPriceSyncer(ctx context.Context) {
	ticker := stdlibtime.NewTicker(10 * stdlibtime.Minute) //nolint:gosec,gomnd // Not an  issue.
	defer ticker.Stop()
	r.cfg.MiningBoost.icePrice = new(atomic.Pointer[float64])
	r.cfg.MiningBoost.levels = new(atomic.Pointer[[]*MiningBoostLevel])
	r.cfg.MiningBoost.networkEndpointCurrentLBIndex = make(map[BlockchainNetworkType]*atomic.Uint64, len(r.cfg.MiningBoost.NetworkEndpoints))
	r.cfg.MiningBoost.networkClients = make(map[BlockchainNetworkType][]*ethclient.Client, len(r.cfg.MiningBoost.NetworkEndpoints))
	r.cfg.MiningBoost.paymentAddress = ethcommon.HexToAddress(r.cfg.MiningBoost.PaymentAddress)
	for network, endpoints := range r.cfg.MiningBoost.NetworkEndpoints {
		clients := make([]*ethclient.Client, 0, len(endpoints))
		for ix, endpoint := range endpoints {
			rpcClient, err := ethclient.DialContext(ctx, endpoint)
			log.Panic(errors.Wrapf(err, "failed to connect to ethereum RPC[%v][%v]", network, ix)) //nolint:revive,nolintlint //.
			clients = append(clients, rpcClient)
		}
		r.cfg.MiningBoost.networkClients[network] = clients
		r.cfg.MiningBoost.networkEndpointCurrentLBIndex[network] = new(atomic.Uint64)
	}
	log.Panic(errors.Wrap(r.syncICEPrice(ctx), "failed to syncICEPrice"))

	for {
		select {
		case <-ticker.C:
			reqCtx, cancel := context.WithTimeout(ctx, requestDeadline)
			log.Error(errors.Wrap(r.syncICEPrice(reqCtx), "failed to syncICEPrice"))
			cancel()
		case <-ctx.Done():
			return
		}
	}
}

func (r *repository) syncICEPrice(ctx context.Context) error {
	detailedCoinMetrics, err := r.detailedMetricsRepo.ReadDetails(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to read detailedCoinMetrics")
	}
	r.cfg.MiningBoost.icePrice.Store(&detailedCoinMetrics.CurrentPrice)
	r.cfg.MiningBoost.levels.Store(r.buildMiningBoostLevels())

	return nil
}

func (r *repository) buildMiningBoostLevels() *[]*MiningBoostLevel {
	levels := make([]*MiningBoostLevel, 0, len(r.cfg.MiningBoost.Levels))
	for dollars, level := range r.cfg.MiningBoost.Levels {
		clone := *level
		clone.icePrice = dollars / *r.cfg.MiningBoost.icePrice.Load()
		clone.ICEPrice = strconv.FormatFloat(clone.icePrice, 'f', 15, 64)
		levels = append(levels, &clone)
	}
	sort.SliceStable(levels, func(ii, jj int) bool { return levels[ii].icePrice < levels[jj].icePrice })

	return &levels
}
