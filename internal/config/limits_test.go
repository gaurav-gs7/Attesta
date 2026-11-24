package config

import "testing"

func TestSLOThresholds(t *testing.T) {
	t.Setenv("ATTESTA_SLO_MIN_ERROR_BUDGET_PCT", "25")
	t.Setenv("ATTESTA_SLO_FAST_BURN_RATE", "10")
	got, err := SLOThresholds()
	if err != nil || got.MinBudgetRemainingPct != 25 || got.FastBurnRate != 10 || got.SlowBurnRate != 6 {
		t.Fatalf("thresholds %+v, %v", got, err)
	}
	t.Setenv("ATTESTA_SLO_SLOW_BURN_RATE", "0")
	if _, err := SLOThresholds(); err == nil {
		t.Fatal("a zero burn rate was accepted")
	}
	t.Setenv("ATTESTA_SLO_SLOW_BURN_RATE", "fast")
	if _, err := SLOThresholds(); err == nil {
		t.Fatal("a non-number was accepted")
	}
}
