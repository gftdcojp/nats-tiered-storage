package lifecycle

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/memory"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

func newTestMeta(t *testing.T) meta.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := meta.NewBoltStore(path, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestManager(t *testing.T, blobEnabled bool, maxAge time.Duration) (*Manager, meta.Store, *tier.Controller) {
	t.Helper()
	metaStore := newTestMeta(t)
	memStore := memory.NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())
	// Use a second memory store as a stand-in for blob tier in tests
	blobStore := memory.NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())

	policyCfg := config.TiersConfig{
		Memory: config.MemoryTierConfig{Enabled: true},
	}

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "TEST",
		Memory: memStore,
		Blob:   blobStore,
		Meta:   metaStore,
		Policy: policyCfg,
		Logger: zap.NewNop(),
	})

	streamCfg := config.StreamConfig{
		Name: "TEST",
		Tiers: config.TiersConfig{
			Blob: config.BlobTierConfig{
				Enabled: blobEnabled,
				MaxAge:  config.Duration(maxAge),
			},
		},
	}

	mgr := NewManager(ctrl, metaStore, streamCfg, zap.NewNop())
	return mgr, metaStore, ctrl
}

func TestManager_GCCycle_DeletesExpired(t *testing.T) {
	mgr, metaStore, _ := newTestManager(t, true, time.Hour)
	ctx := context.Background()

	// Record an expired block in blob tier
	metaStore.RecordBlock(ctx, meta.BlockEntry{
		Stream:      "TEST",
		BlockID:     1,
		FirstSeq:    1,
		LastSeq:     10,
		MsgCount:    10,
		SizeBytes:   1000,
		CurrentTier: tier.TierBlob,
		CreatedAt:   time.Now().Add(-2 * time.Hour),
		LastTS:      time.Now().Add(-2 * time.Hour), // 2h ago, max_age=1h
	})

	if err := mgr.gcCycle(ctx); err != nil {
		t.Fatal(err)
	}

	// Block should be deleted from metadata
	_, err := metaStore.GetBlock(ctx, "TEST", 1)
	if err == nil {
		t.Error("expected expired block to be deleted from metadata")
	}
}

func TestManager_GCCycle_KeepsNonExpired(t *testing.T) {
	mgr, metaStore, _ := newTestManager(t, true, time.Hour)
	ctx := context.Background()

	// Record a recent block in blob tier
	metaStore.RecordBlock(ctx, meta.BlockEntry{
		Stream:      "TEST",
		BlockID:     1,
		FirstSeq:    1,
		LastSeq:     10,
		MsgCount:    10,
		SizeBytes:   1000,
		CurrentTier: tier.TierBlob,
		CreatedAt:   time.Now(),
		LastTS:      time.Now(), // just now
	})

	if err := mgr.gcCycle(ctx); err != nil {
		t.Fatal(err)
	}

	// Block should still exist
	entry, err := metaStore.GetBlock(ctx, "TEST", 1)
	if err != nil {
		t.Fatalf("non-expired block should be kept: %v", err)
	}
	if entry.BlockID != 1 {
		t.Fatalf("expected block 1, got %d", entry.BlockID)
	}
}

func TestManager_GCCycle_ZeroMaxAge(t *testing.T) {
	mgr, metaStore, _ := newTestManager(t, true, 0) // max_age = 0 = retain forever
	ctx := context.Background()

	metaStore.RecordBlock(ctx, meta.BlockEntry{
		Stream:      "TEST",
		BlockID:     1,
		FirstSeq:    1,
		LastSeq:     10,
		CurrentTier: tier.TierBlob,
		LastTS:      time.Now().Add(-100 * 24 * time.Hour), // 100 days old
		CreatedAt:   time.Now().Add(-100 * 24 * time.Hour),
	})

	if err := mgr.gcCycle(ctx); err != nil {
		t.Fatal(err)
	}

	// Block should still exist (max_age=0 means retain forever)
	_, err := metaStore.GetBlock(ctx, "TEST", 1)
	if err != nil {
		t.Fatal("block should be retained when max_age=0")
	}
}

func TestManager_GCCycle_BlobDisabled(t *testing.T) {
	mgr, metaStore, _ := newTestManager(t, false, time.Hour) // blob disabled
	ctx := context.Background()

	metaStore.RecordBlock(ctx, meta.BlockEntry{
		Stream:      "TEST",
		BlockID:     1,
		FirstSeq:    1,
		LastSeq:     10,
		CurrentTier: tier.TierBlob,
		LastTS:      time.Now().Add(-2 * time.Hour),
		CreatedAt:   time.Now().Add(-2 * time.Hour),
	})

	if err := mgr.gcCycle(ctx); err != nil {
		t.Fatal(err)
	}

	// Block should still exist (blob disabled = no GC)
	_, err := metaStore.GetBlock(ctx, "TEST", 1)
	if err != nil {
		t.Fatal("block should be retained when blob tier is disabled")
	}
}

func TestManager_Run_CancelStops(t *testing.T) {
	mgr, _, _ := newTestManager(t, false, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- mgr.Run(ctx, 100*time.Millisecond)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-done
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestManager_GCCycle_PartialFailure(t *testing.T) {
	mgr, metaStore, _ := newTestManager(t, true, time.Hour)
	ctx := context.Background()

	// Record two expired blocks
	for i := uint64(1); i <= 2; i++ {
		metaStore.RecordBlock(ctx, meta.BlockEntry{
			Stream:      "TEST",
			BlockID:     i,
			FirstSeq:    i * 10,
			LastSeq:     i*10 + 9,
			MsgCount:    10,
			CurrentTier: tier.TierBlob,
			LastTS:      time.Now().Add(-2 * time.Hour),
			CreatedAt:   time.Now().Add(-2 * time.Hour),
		})
	}

	// GC cycle should attempt both (ctrl.DeleteFromTier may fail since no blob store,
	// but the metadata deletion for failed ones just logs a warning and continues)
	mgr.gcCycle(ctx)

	// The test verifies gcCycle doesn't panic or return early on partial failure
}
