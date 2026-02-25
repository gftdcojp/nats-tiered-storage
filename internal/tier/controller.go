package tier

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/metrics"
	"go.uber.org/zap"
)

// ControllerConfig holds dependencies for the tier controller.
type ControllerConfig struct {
	Stream string
	Memory TierStore
	File   TierStore
	Blob   TierStore
	Meta   meta.Store
	Policy config.TiersConfig
	Logger *zap.Logger
}

// Controller orchestrates data flow across the three tiers.
type Controller struct {
	stream string
	memory TierStore
	file   TierStore
	blob   TierStore
	meta   meta.Store
	policy *PolicyEngine
	logger *zap.Logger
	mu     sync.Mutex
}

// NewController creates a new tier controller.
func NewController(cfg ControllerConfig) *Controller {
	return &Controller{
		stream: cfg.Stream,
		memory: cfg.Memory,
		file:   cfg.File,
		blob:   cfg.Blob,
		meta:   cfg.Meta,
		policy: NewPolicyEngine(cfg.Policy),
		logger: cfg.Logger,
	}
}

// Ingest receives a sealed block and places it in the initial tier.
func (c *Controller) Ingest(ctx context.Context, blk *block.Block) error {
	ref := BlockRef{
		Stream:   c.stream,
		BlockID:  blk.ID,
		FirstSeq: blk.FirstSeq,
		LastSeq:  blk.LastSeq,
	}

	// Determine initial tier
	var initialTier Tier
	var store TierStore
	if c.policy.cfg.Memory.Enabled && c.memory != nil {
		initialTier = TierMemory
		store = c.memory
	} else if c.policy.cfg.File.Enabled && c.file != nil {
		initialTier = TierFile
		store = c.file
	} else if c.policy.cfg.Blob.Enabled && c.blob != nil {
		initialTier = TierBlob
		store = c.blob
	} else {
		return fmt.Errorf("no enabled tier for stream %s", c.stream)
	}

	if err := store.Put(ctx, ref, blk); err != nil {
		return fmt.Errorf("storing block in %s tier: %w", initialTier, err)
	}

	// Collect subjects
	subjects := make(map[string]bool)
	for _, msg := range blk.Messages {
		subjects[msg.Subject] = true
	}
	subjectList := make([]string, 0, len(subjects))
	for s := range subjects {
		subjectList = append(subjectList, s)
	}

	entry := meta.BlockEntry{
		Stream:      c.stream,
		BlockID:     blk.ID,
		FirstSeq:    blk.FirstSeq,
		LastSeq:     blk.LastSeq,
		FirstTS:     blk.FirstTS,
		LastTS:      blk.LastTS,
		MsgCount:    blk.MsgCount,
		SizeBytes:   blk.SizeBytes,
		CurrentTier: initialTier,
		CreatedAt:   time.Now(),
		Subjects:    subjectList,
	}

	if err := c.meta.RecordBlock(ctx, entry); err != nil {
		return fmt.Errorf("recording block metadata: %w", err)
	}

	// Update tier metrics
	metrics.TierBlockCount.WithLabelValues(c.stream, initialTier.String()).Inc()
	metrics.TierBytes.WithLabelValues(c.stream, initialTier.String()).Add(float64(blk.SizeBytes))

	c.logger.Info("block ingested",
		zap.Uint64("block_id", blk.ID),
		zap.Uint64("first_seq", blk.FirstSeq),
		zap.Uint64("last_seq", blk.LastSeq),
		zap.Uint64("msg_count", blk.MsgCount),
		zap.String("tier", initialTier.String()),
	)

	return nil
}

// Demote moves a block from a hotter tier to a colder tier.
func (c *Controller) Demote(ctx context.Context, blockID uint64, from, to Tier) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ref, err := c.blockRefFromMeta(ctx, blockID)
	if err != nil {
		return err
	}

	fromStore := c.storeForTier(from)
	toStore := c.storeForTier(to)
	if fromStore == nil || toStore == nil {
		return fmt.Errorf("tier store not available: from=%s, to=%s", from, to)
	}

	// Read from source
	blk, err := fromStore.Get(ctx, ref)
	if err != nil {
		return fmt.Errorf("reading from %s tier: %w", from, err)
	}

	// Write to destination
	if err := toStore.Put(ctx, ref, blk); err != nil {
		return fmt.Errorf("writing to %s tier: %w", to, err)
	}

	// Update S3 key if moving to blob
	if to == TierBlob {
		// S3 key is determined by blob store internally
	}

	// Update metadata
	if err := c.meta.UpdateTier(ctx, c.stream, blockID, from, to); err != nil {
		return fmt.Errorf("updating tier metadata: %w", err)
	}

	// Delete from source
	if err := fromStore.Delete(ctx, ref); err != nil {
		c.logger.Warn("failed to delete from source tier after demotion",
			zap.Error(err), zap.Uint64("block_id", blockID), zap.String("from", from.String()))
	}

	// Update metrics
	metrics.DemotionOps.WithLabelValues(c.stream, from.String(), to.String()).Inc()
	metrics.TierBlockCount.WithLabelValues(c.stream, from.String()).Dec()
	metrics.TierBlockCount.WithLabelValues(c.stream, to.String()).Inc()

	c.logger.Info("block demoted",
		zap.Uint64("block_id", blockID),
		zap.String("from", from.String()),
		zap.String("to", to.String()),
	)

	return nil
}

// Promote copies a block from a colder to a hotter tier (cache warming).
func (c *Controller) Promote(ctx context.Context, blockID uint64, from, to Tier) error {
	ref, err := c.blockRefFromMeta(ctx, blockID)
	if err != nil {
		return err
	}

	fromStore := c.storeForTier(from)
	toStore := c.storeForTier(to)
	if fromStore == nil || toStore == nil {
		return fmt.Errorf("tier store not available")
	}

	blk, err := fromStore.Get(ctx, ref)
	if err != nil {
		return err
	}

	if err := toStore.Put(ctx, ref, blk); err != nil {
		return err
	}

	metrics.PromotionOps.WithLabelValues(c.stream, from.String(), to.String()).Inc()

	c.logger.Debug("block promoted (cached)",
		zap.Uint64("block_id", blockID),
		zap.String("from", from.String()),
		zap.String("to", to.String()),
	)

	return nil
}

// Retrieve finds and returns a message from whatever tier it resides in.
func (c *Controller) Retrieve(ctx context.Context, seq uint64) (*StoredMessage, error) {
	entry, err := c.meta.LookupBySequence(ctx, c.stream, seq)
	if err != nil {
		return nil, fmt.Errorf("looking up sequence %d: %w", seq, err)
	}

	ref := entry.Ref()
	tierName := entry.CurrentTier.String()
	start := time.Now()

	var msg *StoredMessage
	switch entry.CurrentTier {
	case TierMemory:
		msg, err = c.memory.GetMessage(ctx, ref, seq)
	case TierFile:
		msg, err = c.file.GetMessage(ctx, ref, seq)
		if err == nil && c.memory != nil && c.policy.cfg.Memory.Enabled {
			go c.Promote(context.Background(), entry.BlockID, TierFile, TierMemory)
		}
	case TierBlob:
		msg, err = c.blob.GetMessage(ctx, ref, seq)
		if err == nil && c.file != nil && c.policy.cfg.File.Enabled {
			go c.Promote(context.Background(), entry.BlockID, TierBlob, TierFile)
		}
	default:
		return nil, fmt.Errorf("message not found: seq=%d", seq)
	}

	metrics.ReadRequests.WithLabelValues(c.stream, tierName).Inc()
	metrics.ReadLatency.WithLabelValues(c.stream, tierName).Observe(time.Since(start).Seconds())

	return msg, err
}

// RetrieveRange returns messages in a sequence range.
func (c *Controller) RetrieveRange(ctx context.Context, startSeq, endSeq uint64) ([]*StoredMessage, error) {
	entries, err := c.meta.LookupBySequenceRange(ctx, c.stream, startSeq, endSeq)
	if err != nil {
		return nil, err
	}

	var result []*StoredMessage
	for _, entry := range entries {
		ref := entry.Ref()
		store := c.storeForTier(entry.CurrentTier)
		if store == nil {
			continue
		}

		blockStart := startSeq
		if entry.FirstSeq > blockStart {
			blockStart = entry.FirstSeq
		}
		blockEnd := endSeq
		if entry.LastSeq < blockEnd {
			blockEnd = entry.LastSeq
		}

		for seq := blockStart; seq <= blockEnd; seq++ {
			msg, err := store.GetMessage(ctx, ref, seq)
			if err != nil {
				continue
			}
			result = append(result, msg)
		}
	}
	return result, nil
}

// DeleteFromTier deletes a block from a specific tier.
func (c *Controller) DeleteFromTier(ctx context.Context, ref BlockRef, t Tier) error {
	store := c.storeForTier(t)
	if store == nil {
		return fmt.Errorf("tier store not available: %s", t)
	}
	return store.Delete(ctx, ref)
}

// RunDemotionLoop periodically evaluates policies and demotes eligible blocks.
func (c *Controller) RunDemotionLoop(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.demotionCycle(ctx); err != nil {
				c.logger.Error("demotion cycle error", zap.Error(err))
			}
		}
	}
}

func (c *Controller) demotionCycle(ctx context.Context) error {
	blocks, err := c.meta.ListBlocks(ctx, c.stream, nil)
	if err != nil {
		return err
	}

	now := time.Now()

	// Memory -> File
	if c.policy.cfg.Memory.Enabled && c.policy.cfg.File.Enabled {
		memTier := TierMemory
		memBlocks := filterByTier(blocks, memTier)
		candidates := c.policy.EvaluateDemotion(memBlocks, c.policy.cfg.Memory.MaxAge.Duration(), int64(c.policy.cfg.Memory.MaxBytes), c.policy.cfg.Memory.MaxBlocks, now)
		for _, blk := range candidates {
			if err := c.Demote(ctx, blk.BlockID, TierMemory, TierFile); err != nil {
				c.logger.Error("failed to demote from memory to file",
					zap.Error(err), zap.Uint64("block_id", blk.BlockID))
			}
		}
	}

	// File -> Blob
	if c.policy.cfg.File.Enabled && c.policy.cfg.Blob.Enabled {
		fileTier := TierFile
		fileBlocks := filterByTier(blocks, fileTier)
		candidates := c.policy.EvaluateDemotion(fileBlocks, c.policy.cfg.File.MaxAge.Duration(), int64(c.policy.cfg.File.MaxBytes), c.policy.cfg.File.MaxBlocks, now)
		for _, blk := range candidates {
			if err := c.Demote(ctx, blk.BlockID, TierFile, TierBlob); err != nil {
				c.logger.Error("failed to demote from file to blob",
					zap.Error(err), zap.Uint64("block_id", blk.BlockID))
			}
		}
	}

	return nil
}

// StoreForTier returns the TierStore for a given tier.
func (c *Controller) StoreForTier(t Tier) TierStore {
	return c.storeForTier(t)
}

func (c *Controller) storeForTier(t Tier) TierStore {
	switch t {
	case TierMemory:
		return c.memory
	case TierFile:
		return c.file
	case TierBlob:
		return c.blob
	}
	return nil
}

func (c *Controller) blockRefFromMeta(ctx context.Context, blockID uint64) (BlockRef, error) {
	entry, err := c.meta.GetBlock(ctx, c.stream, blockID)
	if err != nil {
		return BlockRef{}, err
	}
	return entry.Ref(), nil
}

func filterByTier(blocks []meta.BlockEntry, t Tier) []meta.BlockEntry {
	var result []meta.BlockEntry
	for _, b := range blocks {
		if b.CurrentTier == t {
			result = append(result, b)
		}
	}
	return result
}
