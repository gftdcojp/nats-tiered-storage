package memory

import (
	"context"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/types"
	"go.uber.org/zap"
)

func makeBlock(id uint64, firstSeq uint64, msgCount int) *block.Block {
	msgs := make([]block.Message, msgCount)
	now := time.Now()
	for i := 0; i < msgCount; i++ {
		msgs[i] = block.Message{
			Sequence:  firstSeq + uint64(i),
			Subject:   "test.subject",
			Data:      []byte("hello"),
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
		}
	}
	blk := &block.Block{
		ID:       id,
		Stream:   "TEST",
		FirstSeq: firstSeq,
		LastSeq:  firstSeq + uint64(msgCount-1),
		FirstTS:  now,
		LastTS:   now.Add(time.Duration(msgCount) * time.Millisecond),
		MsgCount: uint64(msgCount),
		Messages: msgs,
	}
	blk.Encode()
	return blk
}

func TestMemoryStorePutGet(t *testing.T) {
	store := NewStore(config.MemoryTierConfig{
		Enabled:  true,
		MaxBytes: config.ByteSize(100 * 1024 * 1024),
	}, zap.NewNop())
	defer store.Close()

	ctx := context.Background()
	blk := makeBlock(1, 100, 10)
	ref := types.BlockRef{Stream: "TEST", BlockID: 1, FirstSeq: 100, LastSeq: 109}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	exists, _ := store.Exists(ctx, ref)
	if !exists {
		t.Fatal("block should exist")
	}

	got, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.ID != 1 {
		t.Errorf("expected block ID 1, got %d", got.ID)
	}
}

func TestMemoryStoreGetMessage(t *testing.T) {
	store := NewStore(config.MemoryTierConfig{
		Enabled:  true,
		MaxBytes: config.ByteSize(100 * 1024 * 1024),
	}, zap.NewNop())
	defer store.Close()

	ctx := context.Background()
	blk := makeBlock(1, 100, 5)
	ref := types.BlockRef{Stream: "TEST", BlockID: 1, FirstSeq: 100, LastSeq: 104}
	store.Put(ctx, ref, blk)

	msg, err := store.GetMessage(ctx, ref, 102)
	if err != nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if msg.Sequence != 102 {
		t.Errorf("expected seq 102, got %d", msg.Sequence)
	}
}

func TestMemoryStoreEviction(t *testing.T) {
	store := NewStore(config.MemoryTierConfig{
		Enabled:   true,
		MaxBlocks: 2,
	}, zap.NewNop())
	defer store.Close()

	ctx := context.Background()

	// Add 3 blocks, should evict the first
	for i := uint64(1); i <= 3; i++ {
		blk := makeBlock(i, i*100, 5)
		ref := types.BlockRef{Stream: "TEST", BlockID: i}
		store.Put(ctx, ref, blk)
	}

	stats, _ := store.Stats(ctx)
	if stats.BlockCount != 2 {
		t.Errorf("expected 2 blocks after eviction, got %d", stats.BlockCount)
	}

	// Block 1 should be evicted
	exists, _ := store.Exists(ctx, types.BlockRef{BlockID: 1})
	if exists {
		t.Error("block 1 should have been evicted")
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	store := NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())
	defer store.Close()

	ctx := context.Background()
	blk := makeBlock(1, 100, 5)
	ref := types.BlockRef{Stream: "TEST", BlockID: 1}
	store.Put(ctx, ref, blk)

	store.Delete(ctx, ref)
	exists, _ := store.Exists(ctx, ref)
	if exists {
		t.Error("block should not exist after delete")
	}
}
