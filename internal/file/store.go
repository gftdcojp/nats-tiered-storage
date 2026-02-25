package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

// Store implements tier.TierStore using local filesystem.
type Store struct {
	mu         sync.RWMutex
	cfg        config.FileTierConfig
	dataDir    string
	totalBytes int64
	blockCount int64
	logger     *zap.Logger
}

func NewStore(cfg config.FileTierConfig, logger *zap.Logger) (*Store, error) {
	if cfg.DataDir == "" {
		return &Store{cfg: cfg, logger: logger}, nil
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data dir %s: %w", cfg.DataDir, err)
	}
	return &Store{
		cfg:     cfg,
		dataDir: cfg.DataDir,
		logger:  logger,
	}, nil
}

func (s *Store) blockPath(ref tier.BlockRef) string {
	return filepath.Join(s.dataDir, ref.Stream, fmt.Sprintf("%010d.blk", ref.BlockID))
}

func (s *Store) indexPath(ref tier.BlockRef) string {
	return filepath.Join(s.dataDir, ref.Stream, fmt.Sprintf("%010d.idx", ref.BlockID))
}

func (s *Store) Put(_ context.Context, ref tier.BlockRef, data *block.Block) error {
	blkPath := s.blockPath(ref)
	if err := os.MkdirAll(filepath.Dir(blkPath), 0755); err != nil {
		return err
	}

	// Write block file
	if err := os.WriteFile(blkPath, data.Raw, 0644); err != nil {
		return fmt.Errorf("writing block file: %w", err)
	}

	// Write index file
	if data.Index != nil {
		idxPath := s.indexPath(ref)
		if err := os.WriteFile(idxPath, data.Index.Encode(), 0644); err != nil {
			os.Remove(blkPath) // cleanup on failure
			return fmt.Errorf("writing index file: %w", err)
		}
	}

	s.mu.Lock()
	s.totalBytes += data.SizeBytes
	s.blockCount++
	s.mu.Unlock()

	s.logger.Debug("block stored on disk",
		zap.Uint64("block_id", ref.BlockID),
		zap.String("path", blkPath),
		zap.Int64("size", data.SizeBytes),
	)

	return nil
}

func (s *Store) Get(_ context.Context, ref tier.BlockRef) (*block.Block, error) {
	blkPath := s.blockPath(ref)
	raw, err := os.ReadFile(blkPath)
	if err != nil {
		return nil, fmt.Errorf("reading block file: %w", err)
	}
	return block.Decode(raw)
}

func (s *Store) GetMessage(_ context.Context, ref tier.BlockRef, seq uint64) (*tier.StoredMessage, error) {
	// Try to use index for efficient lookup
	idxPath := s.indexPath(ref)
	idxData, err := os.ReadFile(idxPath)
	if err == nil {
		idx, err := block.DecodeIndex(idxData)
		if err == nil {
			entry, found := idx.Lookup(seq)
			if found {
				return s.readMessageAtOffset(ref, entry.Offset, entry.Size)
			}
		}
	}

	// Fallback: decode full block
	blk, err := s.Get(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	for _, msg := range blk.Messages {
		if msg.Sequence == seq {
			return &tier.StoredMessage{
				Stream:    blk.Stream,
				Subject:   msg.Subject,
				Sequence:  msg.Sequence,
				Data:      msg.Data,
				Timestamp: msg.Timestamp,
			}, nil
		}
	}
	return nil, fmt.Errorf("sequence %d not found in block %d", seq, ref.BlockID)
}

func (s *Store) readMessageAtOffset(ref tier.BlockRef, offset int64, size int32) (*tier.StoredMessage, error) {
	blkPath := s.blockPath(ref)
	f, err := os.Open(blkPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	msg, err := block.DecodeMessage(buf)
	if err != nil {
		return nil, err
	}
	return &tier.StoredMessage{
		Stream:    ref.Stream,
		Subject:   msg.Subject,
		Sequence:  msg.Sequence,
		Data:      msg.Data,
		Timestamp: msg.Timestamp,
	}, nil
}

func (s *Store) Delete(_ context.Context, ref tier.BlockRef) error {
	blkPath := s.blockPath(ref)
	info, err := os.Stat(blkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	size := info.Size()
	os.Remove(blkPath)
	os.Remove(s.indexPath(ref))

	s.mu.Lock()
	s.totalBytes -= size
	s.blockCount--
	s.mu.Unlock()

	return nil
}

func (s *Store) Exists(_ context.Context, ref tier.BlockRef) (bool, error) {
	_, err := os.Stat(s.blockPath(ref))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *Store) Stats(_ context.Context) (tier.TierStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return tier.TierStats{
		Tier:        tier.TierFile,
		BlockCount:  s.blockCount,
		TotalBytes:  s.totalBytes,
		CapacityMax: int64(s.cfg.MaxBytes),
	}, nil
}

func (s *Store) Close() error {
	return nil
}
