package ingest

import (
	"testing"
	"time"
)

func TestCalcBackoff_ZeroErrors(t *testing.T) {
	d := calcBackoff(0, time.Second, 60*time.Second)
	if d != 0 {
		t.Errorf("calcBackoff(0) = %v, want 0", d)
	}
}

func TestCalcBackoff_NegativeErrors(t *testing.T) {
	d := calcBackoff(-1, time.Second, 60*time.Second)
	if d != 0 {
		t.Errorf("calcBackoff(-1) = %v, want 0", d)
	}
}

func TestCalcBackoff_ZeroInitial(t *testing.T) {
	d := calcBackoff(5, 0, 60*time.Second)
	if d != 0 {
		t.Errorf("calcBackoff with zero initial = %v, want 0", d)
	}
}

func TestCalcBackoff_Doubling(t *testing.T) {
	initial := time.Second
	max := 60 * time.Second

	// Without jitter, delay should be initial * 2^(n-1), capped at max.
	// With up to 25% jitter added, the upper bound is delay*1.25.
	cases := []struct {
		n    int
		base time.Duration // expected base before jitter
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 32 * time.Second},
		{7, 60 * time.Second}, // capped at max
		{10, 60 * time.Second}, // still capped
	}

	for _, tc := range cases {
		d := calcBackoff(tc.n, initial, max)
		if d < tc.base {
			t.Errorf("calcBackoff(%d): got %v < base %v", tc.n, d, tc.base)
		}
		upper := tc.base + tc.base/4
		if upper > max {
			upper = max
		}
		if d > upper {
			t.Errorf("calcBackoff(%d): got %v > upper bound %v", tc.n, d, upper)
		}
	}
}

func TestCalcBackoff_NeverExceedsMax(t *testing.T) {
	max := 30 * time.Second
	for n := 1; n <= 20; n++ {
		d := calcBackoff(n, time.Second, max)
		if d > max {
			t.Errorf("calcBackoff(%d) = %v exceeds max %v", n, d, max)
		}
	}
}
