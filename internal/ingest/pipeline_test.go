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

	// Jitter subtracts up to 25%, so delay is in [base*0.75, base].
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
		lower := tc.base - tc.base/4
		if d < lower {
			t.Errorf("calcBackoff(%d): got %v < lower bound %v", tc.n, d, lower)
		}
		if d > tc.base {
			t.Errorf("calcBackoff(%d): got %v > base %v", tc.n, d, tc.base)
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

func TestCalcBackoff_JitterAtMaxCap(t *testing.T) {
	// When capped at max, jitter should still produce variation (not always max).
	max := 60 * time.Second
	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := calcBackoff(10, time.Second, max)
		seen[d] = true
		if d > max {
			t.Fatalf("calcBackoff(10) = %v exceeds max %v", d, max)
		}
		lower := max - max/4 // 45s
		if d < lower {
			t.Fatalf("calcBackoff(10) = %v below lower bound %v", d, lower)
		}
	}
	if len(seen) < 2 {
		t.Errorf("expected jitter variation at max cap, got %d unique values", len(seen))
	}
}
