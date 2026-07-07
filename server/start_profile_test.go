package server

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/log"
)

func TestStartCmdProfileFlags(t *testing.T) {
	cmd := StartCmd(nil, t.TempDir())

	blockFlag := cmd.Flags().Lookup(flagBlockProfileRate)
	require.NotNil(t, blockFlag)
	require.Equal(t, "0", blockFlag.DefValue)

	mutexFlag := cmd.Flags().Lookup(flagMutexProfileFrac)
	require.NotNil(t, mutexFlag)
	require.Equal(t, "0", mutexFlag.DefValue)

	require.NoError(t, cmd.Flags().Set(flagBlockProfileRate, "123"))
	require.NoError(t, cmd.Flags().Set(flagMutexProfileFrac, "7"))

	blockProfileRate, err := cmd.Flags().GetInt(flagBlockProfileRate)
	require.NoError(t, err)
	require.Equal(t, 123, blockProfileRate)

	mutexProfileFraction, err := cmd.Flags().GetInt(flagMutexProfileFrac)
	require.NoError(t, err)
	require.Equal(t, 7, mutexProfileFraction)
}

func TestWrapCPUProfileAppliesRuntimeProfileFlags(t *testing.T) {
	origSetBlockProfileRate := setBlockProfileRate
	origSetMutexProfileFraction := setMutexProfileFraction
	t.Cleanup(func() {
		setBlockProfileRate = origSetBlockProfileRate
		setMutexProfileFraction = origSetMutexProfileFraction
	})

	var blockRates []int
	var mutexFractions []int
	setBlockProfileRate = func(rate int) {
		blockRates = append(blockRates, rate)
	}
	setMutexProfileFraction = func(fraction int) int {
		mutexFractions = append(mutexFractions, fraction)
		return 3
	}

	v := viper.New()
	v.Set(flagBlockProfileRate, 123)
	v.Set(flagMutexProfileFrac, 7)

	svrCtx := &Context{
		Viper:  v,
		Logger: log.NewNopLogger(),
	}

	err := wrapCPUProfile(svrCtx, func() error {
		require.Equal(t, []int{123}, blockRates)
		require.Equal(t, []int{7}, mutexFractions)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []int{123, 0}, blockRates)
	require.Equal(t, []int{7, 3}, mutexFractions)
}

func TestWrapCPUProfileSkipsRuntimeProfileDefaults(t *testing.T) {
	origSetBlockProfileRate := setBlockProfileRate
	origSetMutexProfileFraction := setMutexProfileFraction
	t.Cleanup(func() {
		setBlockProfileRate = origSetBlockProfileRate
		setMutexProfileFraction = origSetMutexProfileFraction
	})

	setBlockProfileRate = func(rate int) {
		t.Fatalf("unexpected block profile rate %d", rate)
	}
	setMutexProfileFraction = func(fraction int) int {
		t.Fatalf("unexpected mutex profile fraction %d", fraction)
		return 0
	}

	svrCtx := &Context{
		Viper:  viper.New(),
		Logger: log.NewNopLogger(),
	}

	called := false
	err := wrapCPUProfile(svrCtx, func() error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}
