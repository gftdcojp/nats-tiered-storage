package tier

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
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"go.uber.org/zap"
)

func makeBlock(t *testing.T, id uint64, stream string, firstSeq uint64, msgCount int) *block.Block {
	t.Helper()
	b := block.NewBuilder(stream, id, 1<<20)
	for i := 0; i < msgCount; i++ {
		b.Add(block.Message{
			Sequence:  firstSeq + uint64(i),
			Subject:   fmt.Sprintf("%s.test", stream),
			Data:      []byte(fmt.Sprintf("msg-%d", i)),
			Timestamp: time.Now().Add(-time.Hour), // slightly old
		})
	}
	blk, err := b.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return blk
}

func newTestMeta(t *testing.T) meta.Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	store, err := meta.NewBoltStore(path, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestController(t *testing.T, memEnabled, fileEnabled, blobEnabled bool) (*Controller, *mockTierStore, *mockTierStore, *mockTierStore, meta.Store) {
	t.Helper()
	metaStore := newTestMeta(t)
	var memStore, fileStore, blobStore *mockTierStore

	policyCfg := config.TiersConfig{}

	if memEnabled {
		memStore = newMockStore(TierMemory)
		policyCfg.Memory.Enabled = true
	}
	if fileEnabled {
		fileStore = newMockStore(TierFile)
		policyCfg.File.Enabled = true
	}
	if blobEnabled {
		blobStore = newMockStore(TierBlob)
		policyCfg.Blob.Enabled = true
	}

	var mem, file, blob TierStore
	if memStore != nil {
		mem = memStore
	}
	if fileStore != nil {
		file = fileStore
	}
	if blobStore != nil {
		blob = blobStore
	}

	ctrl := NewController(ControllerConfig{
		Stream: "TEST",
		Memory: mem,
		File:   file,
		Blob:   blob,
		Meta:   metaStore,
		Policy: policyCfg,
		Logger: zap.NewNop(),
	})
	return ctrl, memStore, fileStore, blobStore, metaStore
}

func TestController_Ingest_MemoryTier(t *testing.T) {
	ctrl, memStore, _, _, _ := newTestController(t, true, true, false)
	blk := makeBlock(t, 1, "TEST", 1, 5)

	if err := ctrl.Ingest(context.Background(), blk); err != nil {
		t.Fatal(err)
	}

	ref := BlockRef{Stream: "TEST", BlockID: 1}
	if !memStore.hasBlock(ref) {
		t.Error("block not in memory store")
	}
}

func TestController_Ingest_FileTier(t *testing.T) {
	ctrl, _, fileStore, _, _ := newTestController(t, false, true, false)
	blk := makeBlock(t, 1, "TEST", 1, 5)

	if err := ctrl.Ingest(context.Background(), blk); err != nil {
		t.Fatal(err)
	}

	ref := BlockRef{Stream: "TEST", BlockID: 1}
	if !fileStore.hasBlock(ref) {
		t.Error("block not in file store")
	}
}

func TestController_Ingest_NoTier(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, false, false, false)
	blk := makeBlock(t, 1, "TEST", 1, 5)

	err := ctrl.Ingest(context.Background(), blk)
	if err == nil {
		t.Fatal("expected error when no tier is enabled")
	}
}

func TestController_Ingest_MetaRecorded(t *testing.T) {
	ctrl, _, _, _, metaStore := newTestController(t, true, true, false)
	blk := makeBlock(t, 1, "TEST", 1, 5)

	if err := ctrl.Ingest(context.Background(), blk); err != nil {
		t.Fatal(err)
	}

	entry, err := metaStore.GetBlock(context.Background(), "TEST", 1)
	if err != nil {
		t.Fatal(err)
	}
	if entry.BlockID != 1 {
		t.Fatalf("expected block ID 1, got %d", entry.BlockID)
	}
	if entry.CurrentTier != TierMemory {
		t.Fatalf("expected memory tier, got %v", entry.CurrentTier)
	}
	if entry.MsgCount != 5 {
		t.Fatalf("expected 5 messages, got %d", entry.MsgCount)
	}
}

func TestController_Demote_MemoryToFile(t *testing.T) {
	ctrl, memStore, fileStore, _, metaStore := newTestController(t, true, true, false)
	ctx := context.Background()
	blk := makeBlock(t, 1, "TEST", 1, 5)

	if err := ctrl.Ingest(ctx, blk); err != nil {
		t.Fatal(err)
	}

	if err := ctrl.Demote(ctx, 1, TierMemory, TierFile); err != nil {
		t.Fatal(err)
	}

	ref := BlockRef{Stream: "TEST", BlockID: 1}
	if memStore.hasBlock(ref) {
		t.Error("block should be removed from memory after demotion")
	}
	if !fileStore.hasBlock(ref) {
		t.Error("block should be in file store after demotion")
	}

	entry, _ := metaStore.GetBlock(ctx, "TEST", 1)
	if entry.CurrentTier != TierFile {
		t.Fatalf("expected file tier in metadata, got %v", entry.CurrentTier)
	}
}

func TestController_Demote_StoreUnavailable(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, false, false)
	ctx := context.Background()
	blk := makeBlock(t, 1, "TEST", 1, 5)
	ctrl.Ingest(ctx, blk)

	err := ctrl.Demote(ctx, 1, TierMemory, TierFile)
	if err == nil {
		t.Fatal("expected error when file store is nil")
	}
}

func TestController_Demote_WriteFailure(t *testing.T) {
	ctrl, memStore, fileStore, _, _ := newTestController(t, true, true, false)
	ctx := context.Background()
	blk := makeBlock(t, 1, "TEST", 1, 5)
	ctrl.Ingest(ctx, blk)

	// Inject write failure
	fileStore.putErr = fmt.Errorf("disk full")

	err := ctrl.Demote(ctx, 1, TierMemory, TierFile)
	if err == nil {
		t.Fatal("expected error from failed put")
	}

	// Source should still have the block
	ref := BlockRef{Stream: "TEST", BlockID: 1}
	if !memStore.hasBlock(ref) {
		t.Error("block should remain in memory after failed demotion")
	}
}

func TestController_Promote_BlobToFile(t *testing.T) {
	ctrl, _, fileStore, blobStore, _ := newTestController(t, false, true, true)
	ctx := context.Background()
	blk := makeBlock(t, 1, "TEST", 1, 5)

	// Put directly in blob
	ref := BlockRef{Stream: "TEST", BlockID: 1, FirstSeq: 1, LastSeq: 5}
	blobStore.Put(ctx, ref, blk)

	// Record in meta as blob tier
	ctrl.meta.RecordBlock(ctx, meta.BlockEntry{
		Stream: "TEST", BlockID: 1, FirstSeq: 1, LastSeq: 5,
		MsgCount: 5, CurrentTier: TierBlob, CreatedAt: time.Now(),
	})

	if err := ctrl.Promote(ctx, 1, TierBlob, TierFile); err != nil {
		t.Fatal(err)
	}

	if !fileStore.hasBlock(ref) {
		t.Error("block should be in file store after promotion")
	}
	if !blobStore.hasBlock(ref) {
		t.Error("block should remain in blob store after promotion (copy, not move)")
	}
}

func TestController_Retrieve_MemoryTier(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, true, false)
	ctx := context.Background()
	blk := makeBlock(t, 1, "TEST", 1, 5)
	ctrl.Ingest(ctx, blk)

	msg, err := ctrl.Retrieve(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 3 {
		t.Fatalf("expected seq 3, got %d", msg.Sequence)
	}
	if msg.Stream != "TEST" {
		t.Fatalf("expected stream TEST, got %s", msg.Stream)
	}
}

func TestController_Retrieve_FileTier(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, true, false)
	ctx := context.Background()
	blk := makeBlock(t, 1, "TEST", 1, 5)
	ctrl.Ingest(ctx, blk)

	// Demote to file tier
	ctrl.Demote(ctx, 1, TierMemory, TierFile)

	msg, err := ctrl.Retrieve(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 3 {
		t.Fatalf("expected seq 3, got %d", msg.Sequence)
	}

	// Wait briefly for async promote goroutine to complete
	time.Sleep(50 * time.Millisecond)
}

func TestController_Retrieve_NotFound(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, true, false)
	_, err := ctrl.Retrieve(context.Background(), 999)
	if err == nil {
		t.Fatal("expected error for non-existent sequence")
	}
}

func TestController_RetrieveRange(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, true, false)
	ctx := context.Background()

	// Ingest two blocks
	blk1 := makeBlock(t, 1, "TEST", 1, 5)
	blk2 := makeBlock(t, 2, "TEST", 6, 5)
	ctrl.Ingest(ctx, blk1)
	ctrl.Ingest(ctx, blk2)

	msgs, err := ctrl.RetrieveRange(ctx, 3, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 6 {
		t.Fatalf("expected 6 messages (seq 3-8), got %d", len(msgs))
	}
}

func TestController_DemotionCycle_AgePolicy(t *testing.T) {
	metaStore := newTestMeta(t)
	memStore := newMockStore(TierMemory)
	fileStore := newMockStore(TierFile)

	policyCfg := config.TiersConfig{
		Memory: config.MemoryTierConfig{Enabled: true, MaxAge: config.Duration(time.Minute)},
		File:   config.FileTierConfig{Enabled: true},
	}

	ctrl := NewController(ControllerConfig{
		Stream: "TEST",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: policyCfg,
		Logger: zap.NewNop(),
	})

	ctx := context.Background()

	// Create a block with old timestamps
	b := block.NewBuilder("TEST", 1, 1<<20)
	for i := 0; i < 5; i++ {
		b.Add(block.Message{
			Sequence:  uint64(i + 1),
			Subject:   "TEST.test",
			Data:      []byte(fmt.Sprintf("msg-%d", i)),
			Timestamp: time.Now().Add(-10 * time.Minute), // 10 min ago
		})
	}
	blk, _ := b.Seal()

	if err := ctrl.Ingest(ctx, blk); err != nil {
		t.Fatal(err)
	}

	// Run demotion cycle
	if err := ctrl.demotionCycle(ctx); err != nil {
		t.Fatal(err)
	}

	ref := BlockRef{Stream: "TEST", BlockID: 1}
	if memStore.hasBlock(ref) {
		t.Error("old block should be demoted from memory")
	}
	if !fileStore.hasBlock(ref) {
		t.Error("old block should be in file after demotion")
	}
}

func TestController_DemotionCycle_SizePolicy(t *testing.T) {
	metaStore := newTestMeta(t)
	memStore := newMockStore(TierMemory)
	fileStore := newMockStore(TierFile)

	policyCfg := config.TiersConfig{
		Memory: config.MemoryTierConfig{Enabled: true, MaxBytes: config.ByteSize(1)}, // 1 byte limit
		File:   config.FileTierConfig{Enabled: true},
	}

	ctrl := NewController(ControllerConfig{
		Stream: "TEST",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: policyCfg,
		Logger: zap.NewNop(),
	})

	ctx := context.Background()
	blk := makeBlock(t, 1, "TEST", 1, 5)
	ctrl.Ingest(ctx, blk)

	ctrl.demotionCycle(ctx)

	ref := BlockRef{Stream: "TEST", BlockID: 1}
	if memStore.hasBlock(ref) {
		t.Error("block exceeding MaxBytes should be demoted")
	}
	if !fileStore.hasBlock(ref) {
		t.Error("block should be in file after size-based demotion")
	}
}

func TestController_RunDemotionLoop_Cancel(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, true, false)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- ctrl.RunDemotionLoop(ctx, 100*time.Millisecond)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-done
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestController_Concurrent_IngestRetrieve(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, true, false)
	ctx := context.Background()

	var wg sync.WaitGroup

	// Ingest blocks concurrently
	for i := uint64(1); i <= 20; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			blk := makeBlock(t, id, "TEST", id*100, 5)
			if err := ctrl.Ingest(ctx, blk); err != nil {
				t.Errorf("ingest %d: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	// Retrieve concurrently
	for i := uint64(1); i <= 20; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			seq := id*100 + 2
			msg, err := ctrl.Retrieve(ctx, seq)
			if err != nil {
				t.Errorf("retrieve seq %d: %v", seq, err)
				return
			}
			if msg.Sequence != seq {
				t.Errorf("expected seq %d, got %d", seq, msg.Sequence)
			}
		}(i)
	}
	wg.Wait()
}

func TestController_Concurrent_DemoteRetrieve(t *testing.T) {
	ctrl, _, _, _, _ := newTestController(t, true, true, false)
	ctx := context.Background()

	// Ingest several blocks
	for i := uint64(1); i <= 10; i++ {
		blk := makeBlock(t, i, "TEST", i*100, 5)
		ctrl.Ingest(ctx, blk)
	}

	var wg sync.WaitGroup

	// Concurrent demotions
	for i := uint64(1); i <= 10; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			ctrl.Demote(ctx, id, TierMemory, TierFile)
		}(i)
	}

	// Concurrent retrievals
	for i := uint64(1); i <= 10; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			seq := id*100 + 2
			ctrl.Retrieve(ctx, seq) // may fail due to concurrent demotion, that's ok
		}(i)
	}

	wg.Wait()

	// Ensure cleanup for temp dir (via deferred t.TempDir)
	_ = os.TempDir()
}
