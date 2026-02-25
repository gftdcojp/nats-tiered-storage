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
	"github.com/gftdcojp/nats-tiered-storage/internal/ingest"
	"github.com/gftdcojp/nats-tiered-storage/internal/memory"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"github.com/gftdcojp/nats-tiered-storage/internal/types"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// startEmbeddedNATS starts an embedded nats-server with JetStream enabled.
func startEmbeddedNATS(t *testing.T) (*server.Server, string) {
	t.Helper()
	tmpDir := t.TempDir()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random port
		JetStream: true,
		StoreDir:  filepath.Join(tmpDir, "jetstream"),
		NoLog:     true,
		NoSigs:    true,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create nats-server: %v", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server failed to start")
	}

	url := fmt.Sprintf("nats://127.0.0.1:%d", opts.Port)
	t.Cleanup(func() { ns.Shutdown() })
	return ns, url
}

// TestIntegration_FullPipeline tests the complete flow:
// publish messages -> ingest pipeline -> memory tier -> demotion to file tier -> read back
func TestIntegration_FullPipeline(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	tmpDir := t.TempDir()

	logger := zap.NewNop()

	// Connect to NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}

	// Create JetStream stream
	ctx := context.Background()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Initialize metadata store
	metaPath := filepath.Join(tmpDir, "meta.db")
	metaStore, err := meta.NewBoltStore(metaPath, logger)
	if err != nil {
		t.Fatalf("create meta store: %v", err)
	}
	defer metaStore.Close()

	// Initialize tier stores
	memStore := memory.NewStore(config.MemoryTierConfig{
		Enabled:   true,
		MaxBytes:  config.ByteSize(64 * 1024 * 1024),
		MaxBlocks: 10,
		MaxAge:    config.Duration(10 * time.Second),
	}, logger)

	fileDataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(fileDataDir, 0755)
	fileStore, err := file.NewStore(config.FileTierConfig{
		Enabled:  true,
		DataDir:  fileDataDir,
		MaxBytes: config.ByteSize(256 * 1024 * 1024),
	}, logger)
	if err != nil {
		t.Fatalf("create file store: %v", err)
	}

	// Build tier controller
	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "ORDERS",
		Memory: memStore,
		File:   fileStore,
		Blob:   nil, // no S3 in this test
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{
				Enabled:   true,
				MaxBytes:  config.ByteSize(64 * 1024 * 1024),
				MaxBlocks: 10,
				MaxAge:    config.Duration(10 * time.Second),
			},
			File: config.FileTierConfig{
				Enabled:  true,
				DataDir:  fileDataDir,
				MaxBytes: config.ByteSize(256 * 1024 * 1024),
			},
		},
		Logger: logger,
	})

	// Build ingest pipeline
	streamCfg := config.StreamConfig{
		Name:         "ORDERS",
		ConsumerName: "nts-test-archiver",
		FetchBatch:   64,
		FetchTimeout: config.Duration(2 * time.Second),
		Tiers: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true, DataDir: fileDataDir},
		},
	}
	blockCfg := config.BlockConfig{
		TargetSize:  config.ByteSize(1024), // 1KB for testing (small blocks)
		MaxLinger:   config.Duration(2 * time.Second),
		Compression: "none",
	}

	pipeline := ingest.NewPipeline(ingest.PipelineConfig{
		JS:     js,
		Ctrl:   ctrl,
		Meta:   metaStore,
		Stream: streamCfg,
		Block:  blockCfg,
		Logger: logger,
	})

	// Start ingest pipeline in background
	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	pipelineDone := make(chan error, 1)
	go func() {
		pipelineDone <- pipeline.Run(pipelineCtx)
	}()

	// --- Step 1: Publish messages ---
	t.Log("Publishing 50 messages...")
	for i := 0; i < 50; i++ {
		_, err := js.Publish(ctx, "orders.new", []byte(fmt.Sprintf(`{"order_id": %d, "amount": %.2f}`, i+1, float64(i)*9.99)))
		if err != nil {
			t.Fatalf("publish message %d: %v", i, err)
		}
	}
	t.Log("Published 50 messages successfully")

	// --- Step 2: Wait for ingestion (linger timer will seal partial blocks) ---
	t.Log("Waiting for ingestion...")
	time.Sleep(5 * time.Second)

	// --- Step 3: Verify blocks were created in metadata ---
	blocks, err := metaStore.ListBlocks(ctx, "ORDERS", nil)
	if err != nil {
		t.Fatalf("list blocks: %v", err)
	}
	t.Logf("Blocks created: %d", len(blocks))
	if len(blocks) == 0 {
		t.Fatal("expected at least 1 block to be created")
	}

	// Show block details
	var totalMsgs uint64
	for _, b := range blocks {
		t.Logf("  Block %d: seq=[%d-%d] msgs=%d size=%d tier=%s",
			b.BlockID, b.FirstSeq, b.LastSeq, b.MsgCount, b.SizeBytes, b.CurrentTier.String())
		totalMsgs += b.MsgCount
	}
	t.Logf("Total messages across blocks: %d", totalMsgs)
	if totalMsgs != 50 {
		t.Errorf("expected 50 messages total, got %d", totalMsgs)
	}

	// --- Step 4: Verify messages are in memory tier ---
	memTier := types.TierMemory
	memBlocks, _ := metaStore.ListBlocks(ctx, "ORDERS", &memTier)
	t.Logf("Blocks in memory tier: %d", len(memBlocks))
	if len(memBlocks) == 0 {
		t.Fatal("expected blocks in memory tier")
	}

	// --- Step 5: Read back a message ---
	msg, err := ctrl.Retrieve(ctx, 1)
	if err != nil {
		t.Fatalf("retrieve message seq=1: %v", err)
	}
	t.Logf("Retrieved message: stream=%s subject=%s seq=%d data=%s",
		msg.Stream, msg.Subject, msg.Sequence, string(msg.Data))
	if msg.Sequence != 1 {
		t.Errorf("expected sequence 1, got %d", msg.Sequence)
	}
	if msg.Subject != "orders.new" {
		t.Errorf("expected subject orders.new, got %s", msg.Subject)
	}

	// Read range
	msgs, err := ctrl.RetrieveRange(ctx, 1, 10)
	if err != nil {
		t.Fatalf("retrieve range: %v", err)
	}
	t.Logf("Retrieved %d messages in range [1-10]", len(msgs))
	if len(msgs) != 10 {
		t.Errorf("expected 10 messages, got %d", len(msgs))
	}

	// --- Step 6: Force demotion to file tier ---
	t.Log("Demoting all memory blocks to file tier...")
	for _, b := range memBlocks {
		if err := ctrl.Demote(ctx, b.BlockID, types.TierMemory, types.TierFile); err != nil {
			t.Fatalf("demote block %d: %v", b.BlockID, err)
		}
	}

	// Verify demotion
	fileTier := types.TierFile
	fileBlocks, _ := metaStore.ListBlocks(ctx, "ORDERS", &fileTier)
	t.Logf("Blocks in file tier after demotion: %d", len(fileBlocks))
	if len(fileBlocks) != len(memBlocks) {
		t.Errorf("expected %d blocks in file tier, got %d", len(memBlocks), len(fileBlocks))
	}

	// Verify files exist on disk
	for _, b := range fileBlocks {
		blkPath := filepath.Join(fileDataDir, "ORDERS", fmt.Sprintf("%010d.blk", b.BlockID))
		if _, err := os.Stat(blkPath); os.IsNotExist(err) {
			t.Errorf("block file not found: %s", blkPath)
		}
		idxPath := filepath.Join(fileDataDir, "ORDERS", fmt.Sprintf("%010d.idx", b.BlockID))
		if _, err := os.Stat(idxPath); os.IsNotExist(err) {
			t.Errorf("index file not found: %s", idxPath)
		}
	}

	// --- Step 7: Read back from file tier ---
	msg, err = ctrl.Retrieve(ctx, 25)
	if err != nil {
		t.Fatalf("retrieve message seq=25 from file tier: %v", err)
	}
	t.Logf("Retrieved from file tier: seq=%d subject=%s data=%s",
		msg.Sequence, msg.Subject, string(msg.Data))
	if msg.Sequence != 25 {
		t.Errorf("expected sequence 25, got %d", msg.Sequence)
	}

	// --- Step 8: Consumer state persistence ---
	lastSeq, err := metaStore.GetConsumerState(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("get consumer state: %v", err)
	}
	t.Logf("Last acked sequence: %d", lastSeq)
	if lastSeq != 50 {
		t.Errorf("expected last acked seq 50, got %d", lastSeq)
	}

	// --- Cleanup ---
	pipelineCancel()
	<-pipelineDone

	t.Log("Integration test PASSED")
}

// TestIntegration_BlockEncodeDecode_RoundTrip tests that blocks survive encode->decode
// and messages can be retrieved from decoded blocks.
func TestIntegration_BlockEncodeDecode_RoundTrip(t *testing.T) {
	now := time.Now()
	msgs := make([]block.Message, 100)
	for i := range msgs {
		msgs[i] = block.Message{
			Sequence:  uint64(i + 1),
			Subject:   fmt.Sprintf("orders.item.%d", i%10),
			Data:      []byte(fmt.Sprintf(`{"id":%d,"ts":"%s"}`, i+1, now.Format(time.RFC3339))),
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
		}
		if i%3 == 0 {
			msgs[i].Headers = []byte(fmt.Sprintf("X-Request-Id: req-%d\r\n", i))
		}
	}

	builder := block.NewBuilder("TEST", 42, block.DefaultBlockSize)
	for _, m := range msgs {
		if !builder.Add(m) {
			t.Fatal("builder rejected message unexpectedly")
		}
	}

	blk, err := builder.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	t.Logf("Block: id=%d msgs=%d size=%d bytes", blk.ID, blk.MsgCount, blk.SizeBytes)

	// Decode from raw
	decoded, err := block.Decode(blk.Raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.MsgCount != 100 {
		t.Errorf("decoded msg count: expected 100, got %d", decoded.MsgCount)
	}
	if len(decoded.Messages) != 100 {
		t.Fatalf("decoded messages: expected 100, got %d", len(decoded.Messages))
	}

	// Verify every message
	for i, orig := range msgs {
		got := decoded.Messages[i]
		if got.Sequence != orig.Sequence {
			t.Errorf("msg %d: seq expected %d, got %d", i, orig.Sequence, got.Sequence)
		}
		if got.Subject != orig.Subject {
			t.Errorf("msg %d: subject expected %s, got %s", i, orig.Subject, got.Subject)
		}
		if string(got.Data) != string(orig.Data) {
			t.Errorf("msg %d: data mismatch", i)
		}
		if string(got.Headers) != string(orig.Headers) {
			t.Errorf("msg %d: headers mismatch: %q vs %q", i, got.Headers, orig.Headers)
		}
	}

	// Index lookup
	for _, seq := range []uint64{1, 50, 100} {
		entry, found := decoded.Index.Lookup(seq)
		if !found {
			t.Errorf("index lookup failed for seq %d", seq)
			continue
		}
		if entry.Sequence != seq {
			t.Errorf("index lookup seq %d returned %d", seq, entry.Sequence)
		}
	}

	t.Log("Block round-trip test PASSED")
}
