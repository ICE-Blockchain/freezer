// SPDX-License-Identifier: ice License 1.0

package extrabonusnotifier

import (
	"github.com/ice-blockchain/wintr/time"
)

func IsExtraBonusAvailable(currentTime, extraBonusStartedAt *time.Time, id int64) (available bool) {
	return extraBonusStartedAt.IsNil() || (!extraBonusStartedAt.IsNil() && currentTime.After(extraBonusStartedAt.Add(cfg.ExtraBonuses.Duration)))
}
