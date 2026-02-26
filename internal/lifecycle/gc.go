package lifecycle

// Garbage collection helpers for expired blocks.
// The main GC logic is integrated into Manager.gcCycle() in manager.go.
// This file provides additional utility functions for manual GC operations.

import (
	"context"

	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

// CollectOrphans finds blocks that exist in metadata but not in any of their recorded tiers.
// This can happen if a crash occurs during write-through ingest or eviction.
func CollectOrphans(ctx context.Context, metaStore meta.Store, ctrl *tier.Controller, stream string, logger *zap.Logger) (int, error) {
	blocks, err := metaStore.ListBlocks(ctx, stream, nil)
	if err != nil {
		return 0, err
	}

	collected := 0
	for _, blk := range blocks {
		ref := blk.Ref()
		foundInAny := false
		for _, t := range blk.EffectiveTiers() {
			store := ctrl.StoreForTier(t)
			if store == nil {
				continue
			}
			exists, err := store.Exists(ctx, ref)
			if err != nil {
				logger.Warn("error checking block existence",
					zap.Uint64("block_id", blk.BlockID), zap.String("tier", t.String()), zap.Error(err))
				continue
			}
			if exists {
				foundInAny = true
				break
			}
		}
		if !foundInAny {
			logger.Warn("orphaned block metadata found, cleaning up",
				zap.Uint64("block_id", blk.BlockID))
			if err := metaStore.DeleteBlock(ctx, stream, blk.BlockID); err != nil {
				logger.Error("failed to delete orphan metadata",
					zap.Uint64("block_id", blk.BlockID), zap.Error(err))
				continue
			}
			collected++
		}
	}

	return collected, nil
}
