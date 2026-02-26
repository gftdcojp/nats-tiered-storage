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

// Ingest receives a sealed block and writes it to all enabled tiers (write-through).
func (c *Controller) Ingest(ctx context.Context, blk *block.Block) error {
	ref := BlockRef{
		Stream:   c.stream,
		BlockID:  blk.ID,
		FirstSeq: blk.FirstSeq,
		LastSeq:  blk.LastSeq,
	}

	// Write-through: put block in every enabled tier.
	type tierEntry struct {
		tier  Tier
		store TierStore
	}
	var targets []tierEntry
	if c.policy.cfg.Memory.Enabled && c.memory != nil {
		targets = append(targets, tierEntry{TierMemory, c.memory})
	}
	if c.policy.cfg.File.Enabled && c.file != nil {
		targets = append(targets, tierEntry{TierFile, c.file})
	}
	if c.policy.cfg.Blob.Enabled && c.blob != nil {
		targets = append(targets, tierEntry{TierBlob, c.blob})
	}
	if len(targets) == 0 {
		return fmt.Errorf("no enabled tier for stream %s", c.stream)
	}

	var tiers []Tier
	for _, t := range targets {
		if err := t.store.Put(ctx, ref, blk); err != nil {
			return fmt.Errorf("storing block in %s tier: %w", t.tier, err)
		}
		tiers = append(tiers, t.tier)
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
		CurrentTier: tiers[0],
		Tiers:       tiers,
		CreatedAt:   time.Now(),
		Subjects:    subjectList,
	}

	if err := c.meta.RecordBlock(ctx, entry); err != nil {
		return fmt.Errorf("recording block metadata: %w", err)
	}

	// Update tier metrics for all tiers
	for _, t := range tiers {
		metrics.TierBlockCount.WithLabelValues(c.stream, t.String()).Inc()
		metrics.TierBytes.WithLabelValues(c.stream, t.String()).Add(float64(blk.SizeBytes))
	}

	c.logger.Info("block ingested",
		zap.Uint64("block_id", blk.ID),
		zap.Uint64("first_seq", blk.FirstSeq),
		zap.Uint64("last_seq", blk.LastSeq),
		zap.Uint64("msg_count", blk.MsgCount),
		zap.Int("tiers", len(tiers)),
	)

	return nil
}

// Demote evicts a block from a hotter tier. With write-through, the block
// already exists in colder tiers, so only deletion from the source is needed.
func (c *Controller) Demote(ctx context.Context, blockID uint64, from, to Tier) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ref, err := c.blockRefFromMeta(ctx, blockID)
	if err != nil {
		return err
	}

	fromStore := c.storeForTier(from)
	if fromStore == nil {
		return fmt.Errorf("tier store not available: from=%s", from)
	}

	// Delete from the hot tier (data already exists in colder tiers via write-through).
	if err := fromStore.Delete(ctx, ref); err != nil {
		c.logger.Warn("failed to delete from source tier during eviction",
			zap.Error(err), zap.Uint64("block_id", blockID), zap.String("from", from.String()))
	}

	// Update metadata: remove 'from' from the Tiers list.
	if err := c.meta.UpdateTier(ctx, c.stream, blockID, from, to); err != nil {
		return fmt.Errorf("updating tier metadata: %w", err)
	}

	// Update metrics
	metrics.DemotionOps.WithLabelValues(c.stream, from.String(), to.String()).Inc()
	metrics.TierBlockCount.WithLabelValues(c.stream, from.String()).Dec()

	c.logger.Info("block evicted from tier",
		zap.Uint64("block_id", blockID),
		zap.String("from", from.String()),
	)

	return nil
}

// Promote copies a block from a colder to a hotter tier and updates metadata.
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

	// Record the new tier presence in metadata.
	if err := c.meta.AddTierPresence(ctx, c.stream, blockID, to); err != nil {
		c.logger.Warn("failed to update tier presence after promotion",
			zap.Error(err), zap.Uint64("block_id", blockID))
	}

	metrics.PromotionOps.WithLabelValues(c.stream, from.String(), to.String()).Inc()

	c.logger.Debug("block promoted",
		zap.Uint64("block_id", blockID),
		zap.String("from", from.String()),
		zap.String("to", to.String()),
	)

	return nil
}

// Retrieve finds and returns a message by falling through tiers from hottest to coldest.
func (c *Controller) Retrieve(ctx context.Context, seq uint64) (*StoredMessage, error) {
	entry, err := c.meta.LookupBySequence(ctx, c.stream, seq)
	if err != nil {
		return nil, fmt.Errorf("looking up sequence %d: %w", seq, err)
	}

	ref := entry.Ref()
	start := time.Now()

	// Try each tier from hottest to coldest until we find the message.
	for _, t := range entry.EffectiveTiers() {
		store := c.storeForTier(t)
		if store == nil {
			continue
		}
		msg, err := store.GetMessage(ctx, ref, seq)
		if err == nil {
			metrics.ReadRequests.WithLabelValues(c.stream, t.String()).Inc()
			metrics.ReadLatency.WithLabelValues(c.stream, t.String()).Observe(time.Since(start).Seconds())
			return msg, nil
		}
		// Tier miss (e.g. LRU eviction); fall through to next tier.
	}

	return nil, fmt.Errorf("message not found in any tier: seq=%d", seq)
}

// RetrieveRange returns messages in a sequence range, falling through tiers.
func (c *Controller) RetrieveRange(ctx context.Context, startSeq, endSeq uint64) ([]*StoredMessage, error) {
	entries, err := c.meta.LookupBySequenceRange(ctx, c.stream, startSeq, endSeq)
	if err != nil {
		return nil, err
	}

	var result []*StoredMessage
	for _, entry := range entries {
		ref := entry.Ref()

		blockStart := startSeq
		if entry.FirstSeq > blockStart {
			blockStart = entry.FirstSeq
		}
		blockEnd := endSeq
		if entry.LastSeq < blockEnd {
			blockEnd = entry.LastSeq
		}

		for seq := blockStart; seq <= blockEnd; seq++ {
			var found bool
			for _, t := range entry.EffectiveTiers() {
				store := c.storeForTier(t)
				if store == nil {
					continue
				}
				msg, err := store.GetMessage(ctx, ref, seq)
				if err == nil {
					result = append(result, msg)
					found = true
					break
				}
			}
			if !found {
				continue
			}
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

	// Evict from memory tier based on policy (data already in colder tiers via write-through).
	if c.policy.cfg.Memory.Enabled {
		memTier := TierMemory
		memBlocks := filterByTier(blocks, memTier)
		candidates := c.policy.EvaluateDemotion(memBlocks, c.policy.cfg.Memory.MaxAge.Duration(), int64(c.policy.cfg.Memory.MaxBytes), c.policy.cfg.Memory.MaxBlocks, now)
		for _, blk := range candidates {
			if err := c.Demote(ctx, blk.BlockID, TierMemory, TierFile); err != nil {
				c.logger.Error("failed to evict from memory",
					zap.Error(err), zap.Uint64("block_id", blk.BlockID))
			}
		}
	}

	// Evict from file tier based on policy (data already in blob tier via write-through).
	if c.policy.cfg.File.Enabled {
		fileTier := TierFile
		fileBlocks := filterByTier(blocks, fileTier)
		candidates := c.policy.EvaluateDemotion(fileBlocks, c.policy.cfg.File.MaxAge.Duration(), int64(c.policy.cfg.File.MaxBytes), c.policy.cfg.File.MaxBlocks, now)
		for _, blk := range candidates {
			if err := c.Demote(ctx, blk.BlockID, TierFile, TierBlob); err != nil {
				c.logger.Error("failed to evict from file",
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
		for _, bt := range b.EffectiveTiers() {
			if bt == t {
				result = append(result, b)
				break
			}
		}
	}
	return result
}
