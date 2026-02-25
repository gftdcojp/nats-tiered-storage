package internal_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// TestDurability_MetaStoreRestart verifies that BoltDB metadata survives close and reopen.
func TestDurability_MetaStoreRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "meta.db")
	ctx := context.Background()

	// Open store and write data
	store1, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 5; i++ {
		store1.RecordBlock(ctx, meta.BlockEntry{
			Stream:      "TEST",
			BlockID:     i,
			FirstSeq:    i * 10,
			LastSeq:     i*10 + 9,
			MsgCount:    10,
			SizeBytes:   500,
			CurrentTier: tier.TierMemory,
			CreatedAt:   time.Now(),
		})
	}

	store1.SetConsumerState(ctx, "TEST", 59)
	store1.Close()

	// Reopen and verify
	store2, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	blocks, err := store2.ListBlocks(ctx, "TEST", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 5 {
		t.Fatalf("expected 5 blocks after restart, got %d", len(blocks))
	}

	lastSeq, err := store2.GetConsumerState(ctx, "TEST")
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 59 {
		t.Fatalf("expected consumer state 59, got %d", lastSeq)
	}
}

// TestDurability_FileStoreRestart verifies that file store data survives close and reopen.
func TestDurability_FileStoreRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := config.FileTierConfig{Enabled: true, DataDir: dir}
	ctx := context.Background()

	s1, err := file.NewStore(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 3; i++ {
		blk := makeTestBlock(t, i, "TEST", i*100, 10)
		ref := tier.BlockRef{Stream: "TEST", BlockID: i, FirstSeq: i * 100, LastSeq: i*100 + 9}
		if err := s1.Put(ctx, ref, blk); err != nil {
			t.Fatal(err)
		}
	}
	s1.Close()

	// Reopen
	s2, err := file.NewStore(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for i := uint64(1); i <= 3; i++ {
		ref := tier.BlockRef{Stream: "TEST", BlockID: i, FirstSeq: i * 100, LastSeq: i*100 + 9}
		blk, err := s2.Get(ctx, ref)
		if err != nil {
			t.Fatalf("block %d after restart: %v", i, err)
		}
		if len(blk.Messages) != 10 {
			t.Fatalf("block %d: expected 10 messages, got %d", i, len(blk.Messages))
		}
	}
}

// TestDurability_BlockChecksumFailure verifies that corrupted block data is detected by CRC.
func TestDurability_BlockChecksumFailure(t *testing.T) {
	blk := makeTestBlock(t, 1, "TEST", 1, 50)

	// Verify the block can be decoded
	_, err := block.Decode(blk.Raw)
	if err != nil {
		t.Fatalf("original block should decode: %v", err)
	}

	// Corrupt the raw bytes in the message data area (after the header)
	corrupted := make([]byte, len(blk.Raw))
	copy(corrupted, blk.Raw)
	if len(corrupted) > 200 {
		corrupted[150] ^= 0xFF
		corrupted[151] ^= 0xFF
		corrupted[152] ^= 0xFF
	}

	_, err = block.Decode(corrupted)
	if err == nil {
		t.Fatal("expected CRC error for corrupted block data")
	}
}

// TestDurability_FullTierTransition verifies data integrity through Memory -> File tier transitions.
func TestDurability_FullTierTransition(t *testing.T) {
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
	fileStore, err := file.NewStore(config.FileTierConfig{Enabled: true, DataDir: fileDataDir}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "TEST",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true},
		},
		Logger: zap.NewNop(),
	})

	// Ingest 100 messages across multiple blocks
	for i := uint64(1); i <= 10; i++ {
		blk := makeTestBlock(t, i, "TEST", (i-1)*10+1, 10)
		if err := ctrl.Ingest(ctx, blk); err != nil {
			t.Fatalf("ingest block %d: %v", i, err)
		}
	}

	// Verify all 100 messages from memory
	for seq := uint64(1); seq <= 100; seq++ {
		msg, err := ctrl.Retrieve(ctx, seq)
		if err != nil {
			t.Fatalf("retrieve seq %d from memory: %v", seq, err)
		}
		if msg.Sequence != seq {
			t.Fatalf("expected seq %d, got %d", seq, msg.Sequence)
		}
	}

	// Demote all to file tier
	for i := uint64(1); i <= 10; i++ {
		if err := ctrl.Demote(ctx, i, tier.TierMemory, tier.TierFile); err != nil {
			t.Fatalf("demote block %d: %v", i, err)
		}
	}

	// Verify all 100 messages from file tier
	for seq := uint64(1); seq <= 100; seq++ {
		msg, err := ctrl.Retrieve(ctx, seq)
		if err != nil {
			t.Fatalf("retrieve seq %d from file: %v", seq, err)
		}
		if msg.Sequence != seq {
			t.Fatalf("expected seq %d from file, got %d", seq, msg.Sequence)
		}
	}

	// Verify range retrieval across all blocks
	msgs, err := ctrl.RetrieveRange(ctx, 1, 100)
	if err != nil {
		t.Fatalf("retrieve range: %v", err)
	}
	if len(msgs) != 100 {
		t.Fatalf("expected 100 messages in range, got %d", len(msgs))
	}
}

// TestDurability_ConsumerCheckpoint verifies consumer state persistence.
func TestDurability_ConsumerCheckpoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "meta.db")
	ctx := context.Background()

	store, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Save checkpoints
	for seq := uint64(10); seq <= 100; seq += 10 {
		store.SetConsumerState(ctx, "ORDERS", seq)
	}
	store.Close()

	// Reopen and verify latest checkpoint
	store2, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	seq, err := store2.GetConsumerState(ctx, "ORDERS")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 100 {
		t.Fatalf("expected consumer state 100, got %d", seq)
	}
}

// TestDurability_KVMetaPersistence verifies KV key metadata survives restart.
func TestDurability_KVMetaPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "meta.db")
	ctx := context.Background()

	store1, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	store1.RecordKVKey(ctx, "KV_config", meta.KVKeyEntry{
		Key: "app.port", Bucket: "config", Operation: "PUT",
		LastSequence: 5, LastBlockID: 1, UpdatedAt: time.Now(),
	})
	store1.RecordKVKey(ctx, "KV_config", meta.KVKeyEntry{
		Key: "app.host", Bucket: "config", Operation: "PUT",
		LastSequence: 6, LastBlockID: 1, UpdatedAt: time.Now(),
	})
	store1.Close()

	// Reopen and verify
	store2, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	entry, err := store2.LookupKVKey(ctx, "KV_config", "app.port")
	if err != nil {
		t.Fatalf("lookup after restart: %v", err)
	}
	if entry.LastSequence != 5 {
		t.Fatalf("expected seq 5, got %d", entry.LastSequence)
	}

	keys, err := store2.ListKVKeys(ctx, "KV_config", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

// TestDurability_ObjMetaPersistence verifies Object Store metadata survives restart.
func TestDurability_ObjMetaPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "meta.db")
	ctx := context.Background()

	store1, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	store1.RecordObjMeta(ctx, "OBJ_files", meta.ObjEntry{
		Name: "report.pdf", Bucket: "files", NUID: "abc123",
		Size: 1024, Chunks: 2, ModTime: time.Now(),
	})
	store1.Close()

	store2, err := meta.NewBoltStore(dbPath, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	entry, err := store2.LookupObj(ctx, "OBJ_files", "report.pdf")
	if err != nil {
		t.Fatalf("lookup after restart: %v", err)
	}
	if entry.NUID != "abc123" {
		t.Fatalf("expected nuid abc123, got %s", entry.NUID)
	}
}
