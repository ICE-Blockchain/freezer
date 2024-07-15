// SPDX-License-Identifier: ice License 1.0

package tokenomics

import (
	"fmt"
	"testing"
	stdlibtime "time"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/wintr/log"
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
	assert.True(t, loadBalanceKYC(time.New(now.Add(3*stdlibtime.Second)), startDate, duration, miningDuration, 12))
	assert.True(t, loadBalanceKYC(time.New(now.Add(3*stdlibtime.Second)), startDate, duration, miningDuration, 13))
	assert.False(t, loadBalanceKYC(time.New(now.Add(2*stdlibtime.Second)), startDate, duration, miningDuration, 13)) // for 13th not reached yet
	assert.True(t, loadBalanceKYC(time.New(now.Add(4*stdlibtime.Second)), startDate, duration, miningDuration, 14))
	assert.True(t, loadBalanceKYC(time.New(now.Add(4*stdlibtime.Second)), startDate, duration, miningDuration, 24))
}

func TestLoadBalanceKYCUsersALotOfUsers(t *testing.T) {
	now := time.Now()
	startDate := time.New(stdlibtime.Date(2024, 7, 15, 15, 00, 00, 0, stdlibtime.UTC))
	duration := 120 * stdlibtime.Hour
	miningDuration := 1 * stdlibtime.Minute
	breaked := false
	for i := range 1000000 {
		if !loadBalanceKYC(now, startDate, duration, miningDuration, int64(i)) {
			breaked = true
			log.Info(fmt.Sprintf("Stopped at %v at %v", i, now))
			break
		}
	}
	assert.True(t, breaked)

}
