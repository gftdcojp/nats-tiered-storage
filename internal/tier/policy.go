package tier

import (
	"sort"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
)

// PolicyEngine evaluates tier demotion policies.
type PolicyEngine struct {
	cfg config.TiersConfig
}

// NewPolicyEngine creates a new policy engine.
func NewPolicyEngine(cfg config.TiersConfig) *PolicyEngine {
	return &PolicyEngine{cfg: cfg}
}

// EvaluateDemotion returns blocks that should be demoted from the current tier.
// Blocks are returned in order of priority (oldest first).
func (p *PolicyEngine) EvaluateDemotion(
	blocks []meta.BlockEntry,
	maxAge time.Duration,
	maxBytes int64,
	maxBlocks int,
	now time.Time,
) []meta.BlockEntry {
	if len(blocks) == 0 {
		return nil
	}

	// Sort blocks by LastTS ascending (oldest first)
	sorted := make([]meta.BlockEntry, len(blocks))
	copy(sorted, blocks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LastTS.Before(sorted[j].LastTS)
	})

	var candidates []meta.BlockEntry
	seen := make(map[uint64]bool)

	// Age-based demotion
	if maxAge > 0 {
		cutoff := now.Add(-maxAge)
		for _, blk := range sorted {
			if blk.LastTS.Before(cutoff) && !seen[blk.BlockID] {
				candidates = append(candidates, blk)
				seen[blk.BlockID] = true
			}
		}
	}

	// Size-based demotion: demote oldest blocks until under limit
	if maxBytes > 0 {
		var totalBytes int64
		for _, blk := range sorted {
			totalBytes += blk.SizeBytes
		}
		for _, blk := range sorted {
			if totalBytes <= maxBytes {
				break
			}
			if !seen[blk.BlockID] {
				candidates = append(candidates, blk)
				seen[blk.BlockID] = true
			}
			totalBytes -= blk.SizeBytes
		}
	}

	// Count-based demotion: demote oldest blocks until under limit
	if maxBlocks > 0 {
		remaining := len(sorted)
		for _, blk := range sorted {
			if remaining <= maxBlocks {
				break
			}
			if !seen[blk.BlockID] {
				candidates = append(candidates, blk)
				seen[blk.BlockID] = true
			}
			remaining--
		}
	}

	return candidates
}
