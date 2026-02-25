package lifecycle

import (
	"context"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

// Manager handles retention enforcement and garbage collection across tiers.
type Manager struct {
	ctrl      *tier.Controller
	meta      meta.Store
	streamCfg config.StreamConfig
	logger    *zap.Logger
}

// NewManager creates a new lifecycle manager.
func NewManager(ctrl *tier.Controller, metaStore meta.Store, streamCfg config.StreamConfig, logger *zap.Logger) *Manager {
	return &Manager{
		ctrl:      ctrl,
		meta:      metaStore,
		streamCfg: streamCfg,
		logger:    logger,
	}
}

// Run starts the periodic retention/GC loop.
func (m *Manager) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.gcCycle(ctx); err != nil {
				m.logger.Error("gc cycle error", zap.Error(err))
			}
		}
	}
}

func (m *Manager) gcCycle(ctx context.Context) error {
	// Enforce blob tier max_age (permanent deletion)
	if m.streamCfg.Tiers.Blob.Enabled && m.streamCfg.Tiers.Blob.MaxAge.Duration() > 0 {
		blobTier := tier.TierBlob
		blocks, err := m.meta.ListBlocks(ctx, m.streamCfg.Name, &blobTier)
		if err != nil {
			return err
		}

		cutoff := time.Now().Add(-m.streamCfg.Tiers.Blob.MaxAge.Duration())
		for _, blk := range blocks {
			if blk.LastTS.Before(cutoff) {
				m.logger.Info("deleting expired block from blob tier",
					zap.Uint64("block_id", blk.BlockID),
					zap.Time("last_ts", blk.LastTS),
					zap.Time("cutoff", cutoff),
				)
				ref := blk.Ref()
				if err := m.ctrl.DeleteFromTier(ctx, ref, tier.TierBlob); err != nil {
					m.logger.Error("failed to delete expired block",
						zap.Error(err), zap.Uint64("block_id", blk.BlockID))
					continue
				}
				if err := m.meta.DeleteBlock(ctx, m.streamCfg.Name, blk.BlockID); err != nil {
					m.logger.Error("failed to delete block metadata",
						zap.Error(err), zap.Uint64("block_id", blk.BlockID))
				}
			}
		}
	}

	return nil
}
