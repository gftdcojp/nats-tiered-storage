package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

func makeTestBlock(t *testing.T, id uint64, stream string, firstSeq uint64, msgCount int) *block.Block {
	t.Helper()
	b := block.NewBuilder(stream, id, 1<<20)
	for i := 0; i < msgCount; i++ {
		b.Add(block.Message{
			Sequence:  firstSeq + uint64(i),
			Subject:   fmt.Sprintf("%s.test", stream),
			Data:      []byte(fmt.Sprintf("msg-%d", i)),
			Timestamp: time.Now(),
		})
	}
	blk, err := b.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return blk
}

func newTestFileStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(config.FileTierConfig{
		Enabled: true,
		DataDir: dir,
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestFileStore_PutGet(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 10)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 10}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	got, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(got.Messages))
	}
	for i, msg := range got.Messages {
		want := fmt.Sprintf("msg-%d", i)
		if string(msg.Data) != want {
			t.Errorf("msg[%d] data = %q, want %q", i, msg.Data, want)
		}
	}
}

func TestFileStore_PutCreatesDirectory(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "NEWSTREAM", 1, 3)
	ref := tier.BlockRef{Stream: "NEWSTREAM", BlockID: 1, FirstSeq: 1, LastSeq: 3}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(store.dataDir, "NEWSTREAM")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("expected directory %s to be created", dir)
	}
}

func TestFileStore_GetMessage_WithIndex(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	// Verify index file exists
	idxPath := store.indexPath(ref)
	if _, err := os.Stat(idxPath); err != nil {
		t.Fatalf("index file not created: %v", err)
	}

	msg, err := store.GetMessage(ctx, ref, 3)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 3 {
		t.Fatalf("expected seq 3, got %d", msg.Sequence)
	}
	if string(msg.Data) != "msg-2" {
		t.Fatalf("expected data msg-2, got %s", msg.Data)
	}
}

func TestFileStore_GetMessage_Fallback(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	// Remove index file to force fallback
	os.Remove(store.indexPath(ref))

	msg, err := store.GetMessage(ctx, ref, 3)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 3 {
		t.Fatalf("expected seq 3, got %d", msg.Sequence)
	}
}

func TestFileStore_GetMessage_NotFound(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	_, err := store.GetMessage(ctx, ref, 999)
	if err == nil {
		t.Fatal("expected error for non-existent sequence")
	}
}

func TestFileStore_Delete(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(store.blockPath(ref)); !os.IsNotExist(err) {
		t.Error("block file still exists after delete")
	}
	if _, err := os.Stat(store.indexPath(ref)); !os.IsNotExist(err) {
		t.Error("index file still exists after delete")
	}
}

func TestFileStore_Delete_NonExistent(t *testing.T) {
	store := newTestFileStore(t)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 999}

	// Should be idempotent
	if err := store.Delete(context.Background(), ref); err != nil {
		t.Fatalf("expected no error deleting non-existent block, got: %v", err)
	}
}

func TestFileStore_Exists(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	exists, _ := store.Exists(ctx, ref)
	if exists {
		t.Error("expected not exists before put")
	}

	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	store.Put(ctx, ref, blk)

	exists, _ = store.Exists(ctx, ref)
	if !exists {
		t.Error("expected exists after put")
	}

	store.Delete(ctx, ref)
	exists, _ = store.Exists(ctx, ref)
	if exists {
		t.Error("expected not exists after delete")
	}
}

func TestFileStore_Stats(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()

	for i := uint64(1); i <= 3; i++ {
		blk := makeTestBlock(t, i, "ORDERS", i*10, 5)
		ref := tier.BlockRef{Stream: "ORDERS", BlockID: i, FirstSeq: i * 10, LastSeq: i*10 + 4}
		if err := store.Put(ctx, ref, blk); err != nil {
			t.Fatal(err)
		}
	}

	stats, _ := store.Stats(ctx)
	if stats.BlockCount != 3 {
		t.Fatalf("expected 3 blocks, got %d", stats.BlockCount)
	}
	if stats.TotalBytes <= 0 {
		t.Fatal("expected positive total bytes")
	}
	if stats.Tier != tier.TierFile {
		t.Fatalf("expected TierFile, got %v", stats.Tier)
	}
}

func TestFileStore_Durability_Reopen(t *testing.T) {
	dir := t.TempDir()
	cfg := config.FileTierConfig{Enabled: true, DataDir: dir}
	ctx := context.Background()

	// Store 1: write blocks
	s1, _ := NewStore(cfg, zap.NewNop())
	for i := uint64(1); i <= 5; i++ {
		blk := makeTestBlock(t, i, "ORDERS", i*10, 5)
		ref := tier.BlockRef{Stream: "ORDERS", BlockID: i, FirstSeq: i * 10, LastSeq: i*10 + 4}
		if err := s1.Put(ctx, ref, blk); err != nil {
			t.Fatal(err)
		}
	}
	s1.Close()

	// Store 2: new instance, same directory — should read back
	s2, _ := NewStore(cfg, zap.NewNop())
	for i := uint64(1); i <= 5; i++ {
		ref := tier.BlockRef{Stream: "ORDERS", BlockID: i, FirstSeq: i * 10, LastSeq: i*10 + 4}
		blk, err := s2.Get(ctx, ref)
		if err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
		if len(blk.Messages) != 5 {
			t.Fatalf("block %d: expected 5 messages, got %d", i, len(blk.Messages))
		}
	}
	s2.Close()
}

func TestFileStore_Durability_CorruptBlock(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	// Corrupt the block file
	blkPath := store.blockPath(ref)
	data, _ := os.ReadFile(blkPath)
	if len(data) > 100 {
		data[100] ^= 0xFF // flip bits
	}
	os.WriteFile(blkPath, data, 0644)

	_, err := store.Get(ctx, ref)
	if err == nil {
		t.Fatal("expected error decoding corrupted block")
	}
}

func TestFileStore_Durability_CorruptIndex(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	// Corrupt the index file
	idxPath := store.indexPath(ref)
	os.WriteFile(idxPath, []byte("garbage"), 0644)

	// GetMessage should fall back to full block decode
	msg, err := store.GetMessage(ctx, ref, 3)
	if err != nil {
		t.Fatalf("expected fallback to succeed, got: %v", err)
	}
	if msg.Sequence != 3 {
		t.Fatalf("expected seq 3, got %d", msg.Sequence)
	}
}

func TestFileStore_Durability_MissingIndex(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	os.Remove(store.indexPath(ref))

	msg, err := store.GetMessage(ctx, ref, 5)
	if err != nil {
		t.Fatalf("expected fallback to succeed, got: %v", err)
	}
	if msg.Sequence != 5 {
		t.Fatalf("expected seq 5, got %d", msg.Sequence)
	}
}

func TestFileStore_Concurrent_PutGet(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()

	var wg sync.WaitGroup

	// Writer goroutines
	for i := uint64(1); i <= 20; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			blk := makeTestBlock(t, id, "ORDERS", id*100, 5)
			ref := tier.BlockRef{Stream: "ORDERS", BlockID: id, FirstSeq: id * 100, LastSeq: id*100 + 4}
			if err := store.Put(ctx, ref, blk); err != nil {
				t.Errorf("put block %d: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	// Reader goroutines
	for i := uint64(1); i <= 20; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			ref := tier.BlockRef{Stream: "ORDERS", BlockID: id, FirstSeq: id * 100, LastSeq: id*100 + 4}
			blk, err := store.Get(ctx, ref)
			if err != nil {
				t.Errorf("get block %d: %v", id, err)
				return
			}
			if len(blk.Messages) != 5 {
				t.Errorf("block %d: expected 5 messages, got %d", id, len(blk.Messages))
			}
		}(i)
	}
	wg.Wait()
}

func TestFileStore_LargeBlock(t *testing.T) {
	store := newTestFileStore(t)
	ctx := context.Background()

	blk := makeTestBlock(t, 1, "ORDERS", 1, 10000)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 10000}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	got, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 10000 {
		t.Fatalf("expected 10000 messages, got %d", len(got.Messages))
	}

	// Verify random message via index lookup
	msg, err := store.GetMessage(ctx, ref, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 5000 {
		t.Fatalf("expected seq 5000, got %d", msg.Sequence)
	}
}
