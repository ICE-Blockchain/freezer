// SPDX-License-Identifier: ice License 1.0

package extrabonusnotifier

import (
	"testing"
	stdlibtime "time"

	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/wintr/time"
)

var (
	testTime = time.New(stdlibtime.Date(2023, 1, 2, 3, 4, 5, 6, stdlibtime.UTC))
)

func newUser() *User {
	u := new(User)
	u.UserID = "test_user_id"
	u.ID = 111_111

	return u
}

func Test_isExtraBonusAvailable_BonusValue(t *testing.T) {
	t.Parallel()

	t.Run("current time before extraBonusStartedAt + duration", func(t *testing.T) {
		now := time.New(stdlibtime.Date(testTime.Year(), testTime.Month(), testTime.Day(), 6, 00, 00, 00, testTime.Location()))

		m := newUser()
		m.UTCOffset = 180
		m.ExtraBonusLastClaimAvailableAt = time.New(now.Add(-stdlibtime.Hour * 24))
		extraBonusStartedAt := time.Now()

		b := IsExtraBonusAvailable(now, extraBonusStartedAt, m.ID)
		require.False(t, b)
		require.EqualValues(t, 0, m.ExtraBonusIndex)
	})

	t.Run("current time after extraBonusStartedAt + duration", func(t *testing.T) {
		now := time.New(stdlibtime.Date(testTime.Year(), testTime.Month(), testTime.Day(), 6, 00, 00, 00, testTime.Location()))

		m := newUser()
		extraBonusStartedAt := time.New(now.Add(-48 * stdlibtime.Hour))

		b := IsExtraBonusAvailable(now, extraBonusStartedAt, m.ID)
		require.True(t, b)
		require.EqualValues(t, 0, m.ExtraBonusIndex)
	})

	t.Run("extraBonusStartedAt is nil", func(t *testing.T) {
		now := time.New(stdlibtime.Date(testTime.Year(), testTime.Month(), testTime.Day(), 6, 00, 00, 00, testTime.Location()))

		m := newUser()

		b := IsExtraBonusAvailable(now, nil, m.ID)
		require.True(t, b)
		require.EqualValues(t, 0, m.ExtraBonusIndex)
	})
}
