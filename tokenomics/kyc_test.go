// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"testing"
	stdlibtime "time"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/wintr/time"
)

func TestLoadBalanceKYCUsers(t *testing.T) {
	now := time.Now()
	startDate := time.New(now.Add(-1 * stdlibtime.Minute))
	duration := 10 * stdlibtime.Minute
	miningDuration := 1 * stdlibtime.Minute
	assert.True(t, loadBalanceKYC(now, startDate, duration, miningDuration, 0))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 1))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 2))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 3))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 4))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 5))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 6))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 7))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 8))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 9))
	assert.True(t, loadBalanceKYC(now, startDate, duration, miningDuration, 10))
	assert.False(t, loadBalanceKYC(now, startDate, duration, miningDuration, 11))
	assert.True(t, loadBalanceKYC(time.New(now.Add(2*stdlibtime.Second)), startDate, duration, miningDuration, 12))
	assert.True(t, loadBalanceKYC(time.New(now.Add(3*stdlibtime.Second)), startDate, duration, miningDuration, 13))
	assert.True(t, loadBalanceKYC(time.New(now.Add(4*stdlibtime.Second)), startDate, duration, miningDuration, 14))
	assert.True(t, loadBalanceKYC(time.New(now.Add(4*stdlibtime.Second)), startDate, duration, miningDuration, 24))
}
