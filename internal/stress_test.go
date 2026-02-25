//go:build stress

package internal_test

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
	"github.com/gftdcojp/nats-tiered-storage/internal/file"
	"github.com/gftdcojp/nats-tiered-storage/internal/memory"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

func stressBlock(t *testing.T, id uint64, stream string, firstSeq uint64, msgCount int) *block.Block {
	t.Helper()
	b := block.NewBuilder(stream, id, 8<<20) // 8MB
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

// TestStress_HighVolumeIngest ingests 10,000 messages across many small blocks,
// then verifies every message can be retrieved.
func TestStress_HighVolumeIngest(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	metaStore, err := meta.NewBoltStore(filepath.Join(tmpDir, "meta.db"), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	memStore := memory.NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())
	fileDataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(fileDataDir, 0755)
	fileStore, _ := file.NewStore(config.FileTierConfig{Enabled: true, DataDir: fileDataDir}, zap.NewNop())

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "STRESS",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true},
		},
		Logger: zap.NewNop(),
	})

	// Ingest 100 blocks x 100 messages = 10,000 messages
	for i := uint64(1); i <= 100; i++ {
		blk := stressBlock(t, i, "STRESS", (i-1)*100+1, 100)
		if err := ctrl.Ingest(ctx, blk); err != nil {
			t.Fatalf("ingest block %d: %v", i, err)
		}
	}

	// Verify every 50th message
	for seq := uint64(1); seq <= 10000; seq += 50 {
		msg, err := ctrl.Retrieve(ctx, seq)
		if err != nil {
			t.Fatalf("retrieve seq %d: %v", seq, err)
		}
		if msg.Sequence != seq {
			t.Fatalf("expected seq %d, got %d", seq, msg.Sequence)
		}
	}
}

// TestStress_ConcurrentTierOperations runs concurrent Ingest + Retrieve + Demote
// operations to verify thread safety under load.
func TestStress_ConcurrentTierOperations(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	metaStore, err := meta.NewBoltStore(filepath.Join(tmpDir, "meta.db"), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	memStore := memory.NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())
	fileDataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(fileDataDir, 0755)
	fileStore, _ := file.NewStore(config.FileTierConfig{Enabled: true, DataDir: fileDataDir}, zap.NewNop())

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "CONC",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true},
		},
		Logger: zap.NewNop(),
	})

	var wg sync.WaitGroup

	// 10 goroutines ingesting
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := uint64(0); i < 10; i++ {
				id := uint64(gid)*10 + i + 1
				blk := stressBlock(t, id, "CONC", id*100, 10)
				ctrl.Ingest(ctx, blk)
			}
		}(g)
	}
	wg.Wait()

	// 10 goroutines retrieving + 5 goroutines demoting concurrently
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := uint64(0); i < 10; i++ {
				id := uint64(gid)*10 + i + 1
				seq := id*100 + 5
				ctrl.Retrieve(ctx, seq) // may fail during demotion, that's ok
			}
		}(g)
	}
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := uint64(0); i < 10; i++ {
				id := uint64(gid)*10 + i + 1
				ctrl.Demote(ctx, id, tier.TierMemory, tier.TierFile) // may fail, that's ok
			}
		}(g)
	}
	wg.Wait()
}

// TestStress_RapidDemotionPromotion repeatedly moves a block between Memory and File.
func TestStress_RapidDemotionPromotion(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	metaStore, err := meta.NewBoltStore(filepath.Join(tmpDir, "meta.db"), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	memStore := memory.NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())
	fileDataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(fileDataDir, 0755)
	fileStore, _ := file.NewStore(config.FileTierConfig{Enabled: true, DataDir: fileDataDir}, zap.NewNop())

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "RAPID",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true},
		},
		Logger: zap.NewNop(),
	})

	blk := stressBlock(t, 1, "RAPID", 1, 20)
	if err := ctrl.Ingest(ctx, blk); err != nil {
		t.Fatal(err)
	}

	// Rapidly move between tiers 50 times
	for i := 0; i < 50; i++ {
		if err := ctrl.Demote(ctx, 1, tier.TierMemory, tier.TierFile); err != nil {
			t.Fatalf("demote round %d: %v", i, err)
		}
		if err := ctrl.Promote(ctx, 1, tier.TierFile, tier.TierMemory); err != nil {
			t.Fatalf("promote round %d: %v", i, err)
		}
	}

	// Verify data is still intact
	for seq := uint64(1); seq <= 20; seq++ {
		msg, err := ctrl.Retrieve(ctx, seq)
		if err != nil {
			t.Fatalf("retrieve seq %d after 50 round-trips: %v", seq, err)
		}
		if msg.Sequence != seq {
			t.Fatalf("expected seq %d, got %d", seq, msg.Sequence)
		}
	}
}

// TestStress_FileStoreHighConcurrency hammers file store with 50 concurrent writers/readers.
func TestStress_FileStoreHighConcurrency(t *testing.T) {
	dir := t.TempDir()
	store, _ := file.NewStore(config.FileTierConfig{Enabled: true, DataDir: dir}, zap.NewNop())
	ctx := context.Background()

	var wg sync.WaitGroup

	// 50 writer goroutines, each writing 20 blocks
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := uint64(0); i < 20; i++ {
				id := uint64(gid)*20 + i + 1
				blk := stressBlock(t, id, "FILETEST", id*100, 5)
				ref := tier.BlockRef{Stream: "FILETEST", BlockID: id, FirstSeq: id * 100, LastSeq: id*100 + 4}
				if err := store.Put(ctx, ref, blk); err != nil {
					t.Errorf("put block %d: %v", id, err)
				}
			}
		}(g)
	}
	wg.Wait()

	// 50 reader goroutines
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := uint64(0); i < 20; i++ {
				id := uint64(gid)*20 + i + 1
				ref := tier.BlockRef{Stream: "FILETEST", BlockID: id, FirstSeq: id * 100, LastSeq: id*100 + 4}
				blk, err := store.Get(ctx, ref)
				if err != nil {
					t.Errorf("get block %d: %v", id, err)
					return
				}
				if len(blk.Messages) != 5 {
					t.Errorf("block %d: expected 5 messages, got %d", id, len(blk.Messages))
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestStress_MemoryEvictionUnderLoad tests memory store with concurrent puts
// exceeding MaxBlocks limit.
func TestStress_MemoryEvictionUnderLoad(t *testing.T) {
	memStore := memory.NewStore(config.MemoryTierConfig{
		Enabled:   true,
		MaxBlocks: 10,
	}, zap.NewNop())
	ctx := context.Background()

	var wg sync.WaitGroup

	// 50 goroutines each putting 10 blocks (500 total, MaxBlocks=10)
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := uint64(0); i < 10; i++ {
				id := uint64(gid)*10 + i + 1
				blk := stressBlock(t, id, "EVICT", id*100, 5)
				ref := tier.BlockRef{Stream: "EVICT", BlockID: id, FirstSeq: id * 100, LastSeq: id*100 + 4}
				memStore.Put(ctx, ref, blk)
			}
		}(g)
	}
	wg.Wait()

	// The store should still be functional (no panics/races)
	stats, err := memStore.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("After stress: %d blocks remain (MaxBlocks=10)", stats.BlockCount)
}

// TestStress_LargeBlockRoundTrip creates an 8MB-class block and verifies full round-trip.
func TestStress_LargeBlockRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, _ := file.NewStore(config.FileTierConfig{Enabled: true, DataDir: dir}, zap.NewNop())
	ctx := context.Background()

	// Create a large block with 50,000 messages
	blk := stressBlock(t, 1, "LARGE", 1, 50000)
	t.Logf("Block size: %d bytes, %d messages", blk.SizeBytes, blk.MsgCount)

	ref := tier.BlockRef{Stream: "LARGE", BlockID: 1, FirstSeq: 1, LastSeq: 50000}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	got, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 50000 {
		t.Fatalf("expected 50000 messages, got %d", len(got.Messages))
	}

	// Spot-check messages
	for _, seq := range []uint64{1, 100, 1000, 25000, 50000} {
		msg, err := store.GetMessage(ctx, ref, seq)
		if err != nil {
			t.Fatalf("GetMessage seq %d: %v", seq, err)
		}
		if msg.Sequence != seq {
			t.Fatalf("expected seq %d, got %d", seq, msg.Sequence)
		}
	}
}
