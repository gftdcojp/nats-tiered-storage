package meta

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/types"
	"go.uber.org/zap"
)

func newTestStore(t *testing.T) *BoltStore {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "nts-meta-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	store, err := NewBoltStore(tmpFile.Name(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestRecordAndLookup(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry := BlockEntry{
		Stream:      "ORDERS",
		BlockID:     1,
		FirstSeq:    100,
		LastSeq:     200,
		FirstTS:     time.Now().Add(-time.Hour),
		LastTS:      time.Now(),
		MsgCount:    101,
		SizeBytes:   8 * 1024 * 1024,
		CurrentTier: types.TierMemory,
		CreatedAt:   time.Now(),
	}

	if err := store.RecordBlock(ctx, entry); err != nil {
		t.Fatalf("RecordBlock failed: %v", err)
	}

	// Lookup by sequence
	found, err := store.LookupBySequence(ctx, "ORDERS", 150)
	if err != nil {
		t.Fatalf("LookupBySequence failed: %v", err)
	}
	if found.BlockID != 1 {
		t.Errorf("expected block ID 1, got %d", found.BlockID)
	}
	if found.CurrentTier != types.TierMemory {
		t.Errorf("expected TierMemory, got %v", found.CurrentTier)
	}
}

func TestUpdateTier(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Write-through: block exists in Memory and File tiers.
	entry := BlockEntry{
		Stream:      "ORDERS",
		BlockID:     1,
		FirstSeq:    100,
		LastSeq:     200,
		FirstTS:     time.Now(),
		LastTS:      time.Now(),
		MsgCount:    101,
		SizeBytes:   1024,
		CurrentTier: types.TierMemory,
		Tiers:       []types.Tier{types.TierMemory, types.TierFile},
		CreatedAt:   time.Now(),
	}
	store.RecordBlock(ctx, entry)

	// Evict from Memory — File remains.
	err := store.UpdateTier(ctx, "ORDERS", 1, types.TierMemory, types.TierFile)
	if err != nil {
		t.Fatalf("UpdateTier failed: %v", err)
	}

	found, err := store.LookupBySequence(ctx, "ORDERS", 100)
	if err != nil {
		t.Fatal(err)
	}
	if found.CurrentTier != types.TierFile {
		t.Errorf("expected TierFile after update, got %v", found.CurrentTier)
	}
	if len(found.Tiers) != 1 || found.Tiers[0] != types.TierFile {
		t.Errorf("expected Tiers=[File], got %v", found.Tiers)
	}
}

func TestUpdateTier_LegacyEntry(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Legacy entry without Tiers field (backward compat).
	entry := BlockEntry{
		Stream:      "ORDERS",
		BlockID:     1,
		FirstSeq:    100,
		LastSeq:     200,
		FirstTS:     time.Now(),
		LastTS:      time.Now(),
		MsgCount:    101,
		SizeBytes:   1024,
		CurrentTier: types.TierMemory,
		CreatedAt:   time.Now(),
	}
	store.RecordBlock(ctx, entry)

	// Evicting the only tier leaves an empty Tiers list.
	err := store.UpdateTier(ctx, "ORDERS", 1, types.TierMemory, types.TierFile)
	if err != nil {
		t.Fatalf("UpdateTier failed: %v", err)
	}

	found, err := store.LookupBySequence(ctx, "ORDERS", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(found.Tiers) != 0 {
		t.Errorf("expected empty Tiers after evicting only tier, got %v", found.Tiers)
	}
}

func TestAddTierPresence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry := BlockEntry{
		Stream:      "ORDERS",
		BlockID:     1,
		FirstSeq:    100,
		LastSeq:     200,
		FirstTS:     time.Now(),
		LastTS:      time.Now(),
		MsgCount:    101,
		SizeBytes:   1024,
		CurrentTier: types.TierFile,
		Tiers:       []types.Tier{types.TierFile},
		CreatedAt:   time.Now(),
	}
	store.RecordBlock(ctx, entry)

	// Promote: add Memory tier.
	err := store.AddTierPresence(ctx, "ORDERS", 1, types.TierMemory)
	if err != nil {
		t.Fatalf("AddTierPresence failed: %v", err)
	}

	found, err := store.GetBlock(ctx, "ORDERS", 1)
	if err != nil {
		t.Fatal(err)
	}
	if found.CurrentTier != types.TierMemory {
		t.Errorf("expected CurrentTier=Memory after promotion, got %v", found.CurrentTier)
	}
	if len(found.Tiers) != 2 || found.Tiers[0] != types.TierMemory || found.Tiers[1] != types.TierFile {
		t.Errorf("expected Tiers=[Memory,File], got %v", found.Tiers)
	}

	// Idempotent: adding same tier again is a no-op.
	err = store.AddTierPresence(ctx, "ORDERS", 1, types.TierMemory)
	if err != nil {
		t.Fatalf("idempotent AddTierPresence failed: %v", err)
	}
	found, _ = store.GetBlock(ctx, "ORDERS", 1)
	if len(found.Tiers) != 2 {
		t.Errorf("expected 2 tiers after idempotent add, got %d", len(found.Tiers))
	}
}

func TestEffectiveTiers_BackwardCompat(t *testing.T) {
	// EffectiveTiers falls back to CurrentTier when Tiers is nil.
	entry := BlockEntry{CurrentTier: types.TierBlob}
	tiers := entry.EffectiveTiers()
	if len(tiers) != 1 || tiers[0] != types.TierBlob {
		t.Errorf("expected [Blob] fallback, got %v", tiers)
	}

	// With explicit Tiers, use that.
	entry.Tiers = []types.Tier{types.TierMemory, types.TierFile, types.TierBlob}
	tiers = entry.EffectiveTiers()
	if len(tiers) != 3 {
		t.Errorf("expected 3 tiers, got %d", len(tiers))
	}
}

func TestListBlocks(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	for i := 0; i < 5; i++ {
		entry := BlockEntry{
			Stream:      "ORDERS",
			BlockID:     uint64(i + 1),
			FirstSeq:    uint64(i*100 + 1),
			LastSeq:     uint64((i + 1) * 100),
			FirstTS:     now.Add(time.Duration(i) * time.Minute),
			LastTS:      now.Add(time.Duration(i+1) * time.Minute),
			MsgCount:    100,
			SizeBytes:   1024,
			CurrentTier: types.Tier(i % 3),
			CreatedAt:   now,
		}
		store.RecordBlock(ctx, entry)
	}

	all, err := store.ListBlocks(ctx, "ORDERS", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Errorf("expected 5 blocks, got %d", len(all))
	}

	memTier := types.TierMemory
	memBlocks, err := store.ListBlocks(ctx, "ORDERS", &memTier)
	if err != nil {
		t.Fatal(err)
	}
	if len(memBlocks) != 2 {
		t.Errorf("expected 2 memory blocks, got %d", len(memBlocks))
	}
}

func TestConsumerState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seq, err := store.GetConsumerState(ctx, "ORDERS")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Errorf("expected 0 for new stream, got %d", seq)
	}

	if err := store.SetConsumerState(ctx, "ORDERS", 500); err != nil {
		t.Fatal(err)
	}

	seq, err = store.GetConsumerState(ctx, "ORDERS")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 500 {
		t.Errorf("expected 500, got %d", seq)
	}
}

func TestDeleteBlock(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry := BlockEntry{
		Stream:      "ORDERS",
		BlockID:     1,
		FirstSeq:    100,
		LastSeq:     200,
		FirstTS:     time.Now(),
		LastTS:      time.Now(),
		MsgCount:    101,
		SizeBytes:   1024,
		CurrentTier: types.TierMemory,
		CreatedAt:   time.Now(),
	}
	store.RecordBlock(ctx, entry)

	if err := store.DeleteBlock(ctx, "ORDERS", 1); err != nil {
		t.Fatal(err)
	}

	all, _ := store.ListBlocks(ctx, "ORDERS", nil)
	if len(all) != 0 {
		t.Errorf("expected 0 blocks after delete, got %d", len(all))
	}
}

func TestPing(t *testing.T) {
	store := newTestStore(t)
	if err := store.Ping(); err != nil {
		t.Fatal(err)
	}
}
