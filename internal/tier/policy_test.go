package tier

import (
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
)

func TestPolicyAgeBasedDemotion(t *testing.T) {
	cfg := config.TiersConfig{
		Memory: config.MemoryTierConfig{
			Enabled: true,
			MaxAge:  config.Duration(5 * time.Minute),
		},
	}
	pe := NewPolicyEngine(cfg)

	now := time.Now()
	blocks := []meta.BlockEntry{
		{BlockID: 1, LastTS: now.Add(-10 * time.Minute)}, // older than 5m -> demote
		{BlockID: 2, LastTS: now.Add(-3 * time.Minute)},  // newer than 5m -> keep
		{BlockID: 3, LastTS: now.Add(-6 * time.Minute)},  // older than 5m -> demote
	}

	candidates := pe.EvaluateDemotion(blocks, 5*time.Minute, 0, 0, now)
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}
}

func TestPolicySizeBasedDemotion(t *testing.T) {
	cfg := config.TiersConfig{}
	pe := NewPolicyEngine(cfg)

	now := time.Now()
	blocks := []meta.BlockEntry{
		{BlockID: 1, LastTS: now.Add(-3 * time.Minute), SizeBytes: 4 * 1024 * 1024},
		{BlockID: 2, LastTS: now.Add(-2 * time.Minute), SizeBytes: 4 * 1024 * 1024},
		{BlockID: 3, LastTS: now.Add(-1 * time.Minute), SizeBytes: 4 * 1024 * 1024},
	}

	// Total = 12MB, limit = 8MB -> should demote 1 block (oldest)
	candidates := pe.EvaluateDemotion(blocks, 0, 8*1024*1024, 0, now)
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].BlockID != 1 {
		t.Errorf("expected oldest block (1) to be demoted, got %d", candidates[0].BlockID)
	}
}

func TestPolicyCountBasedDemotion(t *testing.T) {
	cfg := config.TiersConfig{}
	pe := NewPolicyEngine(cfg)

	now := time.Now()
	blocks := []meta.BlockEntry{
		{BlockID: 1, LastTS: now.Add(-3 * time.Minute)},
		{BlockID: 2, LastTS: now.Add(-2 * time.Minute)},
		{BlockID: 3, LastTS: now.Add(-1 * time.Minute)},
	}

	// Max 2 blocks -> demote 1
	candidates := pe.EvaluateDemotion(blocks, 0, 0, 2, now)
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].BlockID != 1 {
		t.Errorf("expected oldest block to be demoted")
	}
}

func TestPolicyEmptyBlocks(t *testing.T) {
	cfg := config.TiersConfig{}
	pe := NewPolicyEngine(cfg)
	candidates := pe.EvaluateDemotion(nil, time.Hour, 0, 0, time.Now())
	if len(candidates) != 0 {
		t.Errorf("expected 0 candidates for empty blocks, got %d", len(candidates))
	}
}
