package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

// Store implements tier.TierStore as an in-process LRU cache.
type Store struct {
	mu         sync.RWMutex
	cfg        config.MemoryTierConfig
	blocks     map[uint64]*block.Block // blockID -> Block
	order      []uint64                // LRU order, oldest first
	totalBytes int64
	logger     *zap.Logger
}

func NewStore(cfg config.MemoryTierConfig, logger *zap.Logger) *Store {
	return &Store{
		cfg:    cfg,
		blocks: make(map[uint64]*block.Block),
		logger: logger,
	}
}

func (s *Store) Put(_ context.Context, ref tier.BlockRef, data *block.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.blocks[ref.BlockID]; exists {
		return nil // idempotent
	}

	// Evict if needed
	for s.shouldEvict(data.SizeBytes) {
		s.evictOldest()
	}

	s.blocks[ref.BlockID] = data
	s.order = append(s.order, ref.BlockID)
	s.totalBytes += data.SizeBytes

	s.logger.Debug("block stored in memory",
		zap.Uint64("block_id", ref.BlockID),
		zap.Int64("size", data.SizeBytes),
		zap.Int64("total_bytes", s.totalBytes),
	)

	return nil
}

func (s *Store) Get(_ context.Context, ref tier.BlockRef) (*block.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blk, ok := s.blocks[ref.BlockID]
	if !ok {
		return nil, fmt.Errorf("block %d not found in memory tier", ref.BlockID)
	}
	return blk, nil
}

func (s *Store) GetMessage(_ context.Context, ref tier.BlockRef, seq uint64) (*tier.StoredMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blk, ok := s.blocks[ref.BlockID]
	if !ok {
		return nil, fmt.Errorf("block %d not found in memory tier", ref.BlockID)
	}

	if blk.Index == nil {
		return nil, fmt.Errorf("block %d has no index", ref.BlockID)
	}

	entry, found := blk.Index.Lookup(seq)
	if !found {
		return nil, fmt.Errorf("sequence %d not found in block %d", seq, ref.BlockID)
	}

	// Find the message directly
	for _, msg := range blk.Messages {
		if msg.Sequence == entry.Sequence {
			return &tier.StoredMessage{
				Stream:    blk.Stream,
				Subject:   msg.Subject,
				Sequence:  msg.Sequence,
				Data:      msg.Data,
				Headers:   parseHeaders(msg.Headers),
				Timestamp: msg.Timestamp,
			}, nil
		}
	}

	return nil, fmt.Errorf("sequence %d not found in block %d messages", seq, ref.BlockID)
}

func (s *Store) Delete(_ context.Context, ref tier.BlockRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	blk, ok := s.blocks[ref.BlockID]
	if !ok {
		return nil
	}

	s.totalBytes -= blk.SizeBytes
	delete(s.blocks, ref.BlockID)
	s.removeFromOrder(ref.BlockID)
	return nil
}

func (s *Store) Exists(_ context.Context, ref tier.BlockRef) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.blocks[ref.BlockID]
	return ok, nil
}

func (s *Store) Stats(_ context.Context) (tier.TierStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return tier.TierStats{
		Tier:        tier.TierMemory,
		BlockCount:  int64(len(s.blocks)),
		TotalBytes:  s.totalBytes,
		CapacityMax: int64(s.cfg.MaxBytes),
	}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks = nil
	s.order = nil
	s.totalBytes = 0
	return nil
}

func (s *Store) shouldEvict(incoming int64) bool {
	if len(s.blocks) == 0 {
		return false
	}
	if s.cfg.MaxBlocks > 0 && len(s.blocks) >= s.cfg.MaxBlocks {
		return true
	}
	if int64(s.cfg.MaxBytes) > 0 && s.totalBytes+incoming > int64(s.cfg.MaxBytes) {
		return true
	}
	return false
}

func (s *Store) evictOldest() {
	if len(s.order) == 0 {
		return
	}
	oldestID := s.order[0]
	s.order = s.order[1:]
	if blk, ok := s.blocks[oldestID]; ok {
		s.totalBytes -= blk.SizeBytes
		delete(s.blocks, oldestID)
		s.logger.Debug("evicted block from memory", zap.Uint64("block_id", oldestID))
	}
}

func (s *Store) removeFromOrder(blockID uint64) {
	for i, id := range s.order {
		if id == blockID {
			s.order = append(s.order[:i], s.order[i+1:]...)
			return
		}
	}
}

func parseHeaders(raw []byte) map[string][]string {
	if len(raw) == 0 {
		return nil
	}
	// Simple header parsing: each header is "Key: Value\r\n"
	headers := make(map[string][]string)
	lines := splitHeaderLines(raw)
	for _, line := range lines {
		for i, b := range line {
			if b == ':' {
				key := string(line[:i])
				val := string(line[i+1:])
				if len(val) > 0 && val[0] == ' ' {
					val = val[1:]
				}
				headers[key] = append(headers[key], val)
				break
			}
		}
	}
	return headers
}

func splitHeaderLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			line := data[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			if len(line) > 0 {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}
