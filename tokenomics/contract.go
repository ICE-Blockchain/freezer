// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"context"
	_ "embed"
	"io"
	stdlibtime "time"

	"github.com/pkg/errors"

	messagebroker "github.com/ice-blockchain/wintr/connectors/message_broker"
	"github.com/ice-blockchain/wintr/connectors/storage/v3"
	"github.com/ice-blockchain/wintr/multimedia/picture"
	"github.com/ice-blockchain/wintr/time"
)

// Public API.

const (
	MaxPreStakingYears = 5
)

const (
	PositiveMiningRateType MiningRateType = "positive"
	NegativeMiningRateType MiningRateType = "negative"
	NoneMiningRateType     MiningRateType = "none"
)

var (
	ErrNotFound                                        = errors.New("not found")
	ErrRelationNotFound                                = errors.New("relationship not found")
	ErrDuplicate                                       = errors.New("duplicate")
	ErrNegativeMiningProgressDecisionRequired          = errors.New("you have negative mining progress, please decide what to do with it")
	ErrRaceCondition                                   = errors.New("race condition")
	ErrGlobalRankHidden                                = errors.New("global rank is hidden")
	ErrDecreasingPreStakingAllocationOrYearsNotAllowed = errors.New("decreasing pre-staking allocation or years not allowed")
	PreStakingBonusesPerYear                           = map[uint8]uint16{
		1: 35,
		2: 70,
		3: 115,
		4: 170,
		5: 250,
	}
	PreStakingYearsByPreStakingBonuses = map[uint16]uint8{
		35:  1,
		70:  2,
		115: 3,
		170: 4,
		250: 5,
	}
)

type (
	MiningRateType string
	Miner          struct {
		Balance           string `json:"balance,omitempty" redis:"balance_total" example:"12345.6334"`
		UserID            string `json:"userId,omitempty" redis:"user_id"  example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
		Username          string `json:"username,omitempty" redis:"username" example:"jdoe"`
		ProfilePictureURL string `json:"profilePictureUrl,omitempty" redis:"profile_picture_name" example:"https://somecdn.com/p1.jpg"`
	}
	BalanceSummary struct {
		Balances[string]
	}
	Balances[DENOM ~float64 | ~string] struct {
		Total                          DENOM  `json:"total,omitempty" swaggertype:"string" example:"1,243.02"`
		BaseFactor                     DENOM  `json:"baseFactor,omitempty" swaggerignore:"true" swaggertype:"string" example:"1,243.02"`
		Standard                       DENOM  `json:"standard,omitempty" swaggertype:"string" example:"1,243.02"`
		PreStaking                     DENOM  `json:"preStaking,omitempty" swaggertype:"string" example:"1,243.02"`
		TotalNoPreStakingBonus         DENOM  `json:"totalNoPreStakingBonus,omitempty" swaggertype:"string" example:"1,243.02"`
		T1                             DENOM  `json:"t1,omitempty" swaggertype:"string" example:"1,243.02"`
		T2                             DENOM  `json:"t2,omitempty" swaggertype:"string" example:"1,243.02"`
		TotalReferrals                 DENOM  `json:"totalReferrals,omitempty" swaggertype:"string" example:"1,243.02"`
		UserID                         string `json:"userId,omitempty" swaggerignore:"true" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
		miningBlockchainAccountAddress string
	}
	BalanceHistoryBalanceDiff struct {
		Amount   string  `json:"amount" example:"1,243.02"`
		amount   float64 //nolint:revive // That's intended.
		Bonus    int64   `json:"bonus" example:"120"`
		Negative bool    `json:"negative" example:"true"`
	}
	BalanceHistoryEntry struct {
		Time       stdlibtime.Time            `json:"time" swaggertype:"string" example:"2022-01-03T16:20:52.156534Z"`
		Balance    *BalanceHistoryBalanceDiff `json:"balance"`
		TimeSeries []*BalanceHistoryEntry     `json:"timeSeries"`
	}
	AdoptionSummary struct {
		Milestones       []*Adoption[string] `json:"milestones"`
		TotalActiveUsers uint64              `json:"totalActiveUsers" example:"11"`
	}
	AdoptionSnapshot struct {
		*Adoption[float64]
		Before *Adoption[float64] `json:"before,omitempty"`
	}
	Adoption[DENOM ~string | ~float64] struct {
		AchievedAt       *time.Time `json:"achievedAt,omitempty" redis:"achieved_at" example:"2022-01-03T16:20:52.156534Z"`
		BaseMiningRate   DENOM      `json:"baseMiningRate,omitempty" redis:"base_mining_rate" swaggertype:"string" example:"1,243.02"`
		Milestone        uint64     `json:"milestone,omitempty" redis:"milestone" example:"1"`
		TotalActiveUsers uint64     `json:"totalActiveUsers,omitempty" redis:"total_active_users" example:"1"`
	}
	PreStakingSummary struct {
		*PreStaking
		Bonus uint64 `json:"bonus,omitempty" example:"100"`
	}
	PreStaking struct {
		UserID     string `json:"userId,omitempty" swaggerignore:"true" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
		Years      uint64 `json:"years,omitempty" example:"1"`
		Allocation uint64 `json:"allocation,omitempty" example:"100"`
	}
	MiningRateBonuses struct {
		T1         uint64 `json:"t1,omitempty" example:"100"`
		T2         uint64 `json:"t2,omitempty" example:"200"`
		PreStaking uint64 `json:"preStaking,omitempty" example:"300"`
		Extra      uint64 `json:"extra,omitempty" example:"300"`
		Total      uint64 `json:"total,omitempty" example:"300"`
	}
	MiningRateSummary[DENOM ~string | ~float64] struct {
		Bonuses *MiningRateBonuses `json:"bonuses,omitempty"`
		Amount  DENOM              `json:"amount,omitempty" example:"1,234,232.001" swaggertype:"string"`
	}
	MiningRates[T float64 | *MiningRateSummary[string]] struct {
		Total                          T              `json:"total,omitempty"`
		TotalNoPreStakingBonus         T              `json:"totalNoPreStakingBonus,omitempty"`
		PositiveTotalNoPreStakingBonus T              `json:"positiveTotalNoPreStakingBonus,omitempty"`
		Standard                       T              `json:"standard,omitempty"`
		PreStaking                     T              `json:"preStaking,omitempty"`
		Base                           T              `json:"base,omitempty"`
		Type                           MiningRateType `json:"type,omitempty"`
		UserID                         string         `json:"userId,omitempty" swaggerignore:"true" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
	}
	MiningSummary struct {
		MiningRates   *MiningRates[*MiningRateSummary[string]] `json:"miningRates,omitempty"`
		MiningSession *MiningSession                           `json:"miningSession,omitempty"`
		ExtraBonusSummary
		MiningStreak                uint64 `json:"miningStreak,omitempty"  example:"2"`
		RemainingFreeMiningSessions uint64 `json:"remainingFreeMiningSessions,omitempty" example:"1"`
	}
	MiningSession struct {
		LastNaturalMiningStartedAt    *time.Time          `json:"lastNaturalMiningStartedAt,omitempty" example:"2022-01-03T16:20:52.156534Z" swaggerignore:"true"`
		StartedAt                     *time.Time          `json:"startedAt,omitempty" example:"2022-01-03T16:20:52.156534Z"`
		EndedAt                       *time.Time          `json:"endedAt,omitempty" example:"2022-01-03T16:20:52.156534Z"`
		PreviouslyEndedAt             *time.Time          `json:"previouslyEndedAt,omitempty" swaggerignore:"true" example:"2022-01-03T16:20:52.156534Z"`
		ResettableStartingAt          *time.Time          `json:"resettableStartingAt,omitempty" example:"2022-01-03T16:20:52.156534Z" `
		WarnAboutExpirationStartingAt *time.Time          `json:"warnAboutExpirationStartingAt,omitempty" example:"2022-01-03T16:20:52.156534Z" `
		Free                          *bool               `json:"free,omitempty" example:"true"`
		UserID                        *string             `json:"userId,omitempty" swaggerignore:"true" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
		Extension                     stdlibtime.Duration `json:"extension,omitempty" swaggerignore:"true" example:"24h"`
		MiningStreak                  uint64              `json:"miningStreak,omitempty" swaggerignore:"true" example:"11"`
	}
	ExtraBonusSummary struct {
		UserID              string `json:"userId,omitempty" swaggerignore:"true" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
		AvailableExtraBonus uint16 `json:"availableExtraBonus,omitempty" example:"2"`
		ExtraBonusIndex     uint16 `json:"extraBonusIndex,omitempty" swaggerignore:"true" example:"1"`
	}
	RankingSummary struct {
		GlobalRank uint64 `json:"globalRank,omitempty" example:"12333"`
	}
	FreeMiningSessionStarted struct {
		StartedAt                   *time.Time `json:"startedAt,omitempty"`
		EndedAt                     *time.Time `json:"endedAt,omitempty"`
		UserID                      string     `json:"userId,omitempty" `
		ID                          string     `json:"id,omitempty"`
		RemainingFreeMiningSessions uint64     `json:"remainingFreeMiningSessions,omitempty"`
		MiningStreak                uint64     `json:"miningStreak,omitempty" example:"11"`
	}
	ReadRepository interface {
		GetBalanceSummary(ctx context.Context, userID string) (*BalanceSummary, error)
		GetRankingSummary(ctx context.Context, userID string) (*RankingSummary, error)
		GetTopMiners(ctx context.Context, keyword string, limit, offset uint64) ([]*Miner, error)
		GetMiningSummary(ctx context.Context, userID string) (*MiningSummary, error)
		GetPreStakingSummary(ctx context.Context, userID string) (*PreStakingSummary, error)
		GetBalanceHistory(ctx context.Context, userID string, start, end *time.Time, utcOffset stdlibtime.Duration, limit, offset uint64) ([]*BalanceHistoryEntry, error) //nolint:lll // .
		GetAdoptionSummary(context.Context) (*AdoptionSummary, error)
	}
	WriteRepository interface {
		StartNewMiningSession(ctx context.Context, ms *MiningSummary, rollbackNegativeMiningProgress *bool) error
		ClaimExtraBonus(ctx context.Context, ebs *ExtraBonusSummary) error
		StartOrUpdatePreStaking(context.Context, *PreStakingSummary) error
	}
	Repository interface {
		io.Closer

		ReadRepository
		WriteRepository
	}
	Processor interface {
		Repository
		CheckHealth(context.Context) error
	}
)

// Private API.

const (
	applicationYamlKey                  = "tokenomics"
	dayFormat, hourFormat, minuteFormat = "2006-01-02", "2006-01-02T15", "2006-01-02T15:04"
	totalActiveUsersGlobalKey           = "TOTAL_ACTIVE_USERS"
	requestingUserIDCtxValueKey         = "requestingUserIDCtxValueKey"
	userHashCodeCtxValueKey             = "userHashCodeCtxValueKey"
	requestDeadline                     = 25 * stdlibtime.Second
)

type (
	balance struct {
		UpdatedAt   *time.Time `json:"updatedAt,omitempty" example:"2022-01-03T16:20:52.156534Z"`
		Amount      float64    `json:"amount,omitempty" example:"1,235.777777777"`
		UserID      string     `json:"userId,omitempty" example:"did:ethr:0x4B73C58370AEfcEf86A6021afCDe5673511376B2"`
		TypeDetail  string     `json:"typeDetail,omitempty" example:"/2022-01-03"`
		HashCode    int64      `json:"hashCode,omitempty" example:"11"`
		WorkerIndex int16      `json:"workerIndex,omitempty" example:"11"`
		Type        string     `json:"type,omitempty" example:"1"`
		Negative    bool       `json:"negative,omitempty" example:"false"`
	}
	miner struct {
		BalanceLastUpdatedAt          *time.Time `redis:"balance_last_updated_at"`
		LastStartMiningTappedAt       *time.Time `redis:"last_start_mining_tapped_at"`
		MiningSessionSoloStartedAt    *time.Time `redis:"mining_session_solo_started_at"`
		MiningSessionT0StartedAt      *time.Time `redis:"mining_session_t0_started_at"`
		MiningSessionTMinus1StartedAt *time.Time `redis:"mining_session_tminus1_started_at"`
		MiningSessionSoloEndedAt      *time.Time `redis:"mining_session_solo_ended_at"`
		MiningSessionT0EndedAt        *time.Time `redis:"mining_session_t0_ended_at"`
		MiningSessionTMinus1EndedAt   *time.Time `redis:"mining_session_tminus1_ended_at"`
		ExtraBonusStartedAt           *time.Time `redis:"extra_bonus_started_at"`
		ExtraBonusEndedAt             *time.Time `redis:"extra_bonus_ended_at"`
		ResurrectSoloUsedAt           *time.Time `redis:"resurrect_solo_used_at"`
		ResurrectT0UsedAt             *time.Time `redis:"resurrect_t0_used_at"`
		ResurrectTMinus1UsedAt        *time.Time `redis:"resurrect_tminus1_used_at"`
		UserID                        int64      `redis:"-"`
		BalanceTotal                  int64      `redis:"balance_total"`
		BalanceTotalMinted            int64      `redis:"balance_total_minted"`
		BalanceTotalSlashed           int64      `redis:"balance_total_slashed"`
		BalanceSolo                   int64      `redis:"balance_solo"`
		BalanceT0                     int64      `redis:"balance_t0"`
		BalanceT1                     int64      `redis:"balance_t1"`
		BalanceT2                     int64      `redis:"balance_t2"`
		BalanceForT0                  int64      `redis:"balance_for_t0"`
		BalanceForTMinus1             int64      `redis:"balance_for_tminus1"`
		SlashingRateSolo              float64    `redis:"slashing_rate_solo"`
		SlashingRateT0                float64    `redis:"slashing_rate_t0"`
		SlashingRateT1                float64    `redis:"slashing_rate_t1"`
		SlashingRateT2                float64    `redis:"slashing_rate_t2"`
		SlashingRateForT0             float64    `redis:"slashing_rate_for_t0"`
		SlashingRateForTMinus1        float64    `redis:"slashing_rate_for_tminus1"`
		ActiveT1Referrals             uint32     `redis:"active_t1_referrals"`
		ActiveT2Referrals             uint32     `redis:"active_t2_referrals"`
		ExtraBonus                    uint16     `redis:"extra_bonus"`
		PreStakingBonus               uint16     `redis:"pre_staking_bonus"`
		PreStakingAllocation          uint16     `redis:"pre_staking_allocation"`
		LastExtraBonusIndexNotified   uint16     `redis:"extra_bonus_last_notified_index"`
		NewsSeen                      uint16     `redis:"news_seen"`
		UTCOffset                     int16      `redis:"utc_offset"`
	}
	user struct {
		FreeMiningSessionLastAwardedAt *time.Time `redis:"day_off_last_awarded_at"`
		LastNaturalMiningStartedAt     *time.Time `redis:"mining_session_solo_last_start_mining_tapped_at"`
		UserID                         string     `redis:"user_id"`
		ProfilePictureName             string     `redis:"profile_picture_name"`
		Username                       string     `redis:"username"`
		MiningBlockchainAccountAddress string     `redis:"mining_blockchain_account_address"`
		BlockchainAccountAddress       string     `redis:"blockchain_account_address"`
		ID                             int64      `redis:"-"`
		IDT0                           int64      `redis:"id_t0"`
		IDTMinus1                      int64      `redis:"id_tminus1"`
		UTCOffset                      int16      `redis:"utc_offset"`
		NewsSeen                       int16      `redis:"news_seen"`
		LastExtraBonusIndexNotified    int16      `redis:"extra_bonus_last_notified_index"`
		HideRanking                    bool       `redis:"hide_ranking"`
		KYCPassed                      bool       `redis:"kyc_passed"`
	}
	deserializedUsersKey struct {
		ID int64 `redis:"-"`
	}

	usersTableSource struct {
		*processor
	}

	miningSessionsTableSource struct {
		*processor
	}

	completedTasksSource struct {
		*processor
	}

	viewedNewsSource struct {
		*processor
	}

	deviceMetadataTableSource struct {
		*processor
	}

	repository struct {
		cfg           *config
		shutdown      func() error
		db            storage.DB
		mb            messagebroker.Client
		pictureClient picture.Client
	}

	processor struct {
		*repository
	}

	config struct {
		messagebroker.Config    `mapstructure:",squash"` //nolint:tagliatelle // Nope.
		AdoptionMilestoneSwitch struct {
			ActiveUserMilestones []struct {
				Users          uint64  `yaml:"users"`
				BaseMiningRate float64 `yaml:"baseMiningRate"`
			} `yaml:"activeUserMilestones"`
			ConsecutiveDurationsRequired uint64              `yaml:"consecutiveDurationsRequired"`
			Duration                     stdlibtime.Duration `yaml:"duration"`
		} `yaml:"adoptionMilestoneSwitch"`
		ExtraBonuses struct {
			FlatValues                []uint64            `yaml:"flatValues"`
			NewsSeenValues            []uint64            `yaml:"newsSeenValues"`
			MiningStreakValues        []uint64            `yaml:"miningStreakValues"`
			Duration                  stdlibtime.Duration `yaml:"duration"`
			UTCOffsetDuration         stdlibtime.Duration `yaml:"utcOffsetDuration" mapstructure:"utcOffsetDuration"`
			ClaimWindow               stdlibtime.Duration `yaml:"claimWindow"`
			DelayedClaimPenaltyWindow stdlibtime.Duration `yaml:"delayedClaimPenaltyWindow"`
			AvailabilityWindow        stdlibtime.Duration `yaml:"availabilityWindow"`
			TimeToAvailabilityWindow  stdlibtime.Duration `yaml:"timeToAvailabilityWindow"`
			Chunks                    uint64              `yaml:"chunks"`
		} `yaml:"extraBonuses"`
		RollbackNegativeMining struct {
			Available struct {
				After stdlibtime.Duration `yaml:"after"`
				Until stdlibtime.Duration `yaml:"until"`
			} `yaml:"available"`
		} `yaml:"rollbackNegativeMining"`
		MiningSessionDuration struct {
			Min                      stdlibtime.Duration `yaml:"min"`
			Max                      stdlibtime.Duration `yaml:"max"`
			WarnAboutExpirationAfter stdlibtime.Duration `yaml:"warnAboutExpirationAfter"`
		} `yaml:"miningSessionDuration"`
		ReferralBonusMiningRates struct {
			T0 uint16 `yaml:"t0"`
			T1 uint32 `yaml:"t1"`
			T2 uint32 `yaml:"t2"`
		} `yaml:"referralBonusMiningRates"`
		ConsecutiveNaturalMiningSessionsRequiredFor1ExtraFreeArtificialMiningSession struct {
			Min uint64 `yaml:"min"`
			Max uint64 `yaml:"max"`
		} `yaml:"consecutiveNaturalMiningSessionsRequiredFor1ExtraFreeArtificialMiningSession"`
		GlobalAggregationInterval struct {
			Parent stdlibtime.Duration `yaml:"parent"`
			Child  stdlibtime.Duration `yaml:"child"`
		} `yaml:"globalAggregationInterval"`
	}
)
