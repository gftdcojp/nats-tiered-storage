package internal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/file"
	"github.com/gftdcojp/nats-tiered-storage/internal/ingest"
	"github.com/gftdcojp/nats-tiered-storage/internal/memory"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/serve"
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

// TestIntegration_KVStore tests the complete KV Store flow:
// create KV bucket -> put keys -> ingest pipeline -> purge from JetStream -> retrieve via sidecar
func TestIntegration_KVStore(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	tmpDir := t.TempDir()
	logger := zap.NewNop()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()

	// Create KV bucket (this creates the underlying KV_config stream)
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "config",
		History: 5,
	})
	if err != nil {
		t.Fatalf("create KV bucket: %v", err)
	}

	// Put some keys with multiple revisions
	kv.Put(ctx, "app.port", []byte("8080"))
	kv.Put(ctx, "app.port", []byte("9090"))
	kv.Put(ctx, "app.host", []byte("localhost"))
	kv.Put(ctx, "db.url", []byte("postgres://localhost:5432"))
	kv.Delete(ctx, "db.url") // Delete a key

	t.Log("KV keys published")

	// Initialize meta store
	metaStore, err := meta.NewBoltStore(filepath.Join(tmpDir, "meta.db"), logger)
	if err != nil {
		t.Fatalf("meta store: %v", err)
	}
	defer metaStore.Close()

	// Initialize tier stores
	fileDataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(fileDataDir, 0755)
	memStore := memory.NewStore(config.MemoryTierConfig{
		Enabled:   true,
		MaxBytes:  config.ByteSize(64 * 1024 * 1024),
		MaxBlocks: 10,
	}, logger)
	fileStore, _ := file.NewStore(config.FileTierConfig{
		Enabled: true,
		DataDir: fileDataDir,
	}, logger)

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "KV_config",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true, DataDir: fileDataDir},
		},
		Logger: logger,
	})

	streamCfg := config.StreamConfig{
		Name:         "KV_config",
		Type:         config.StreamTypeKV,
		ConsumerName: "nts-test-kv",
		FetchBatch:   64,
		FetchTimeout: config.Duration(2 * time.Second),
		KV:           config.KVArchiveConfig{IndexAllRevisions: true},
		Tiers: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true, DataDir: fileDataDir},
		},
	}

	pipeline := ingest.NewPipeline(ingest.PipelineConfig{
		JS:   js,
		Ctrl: ctrl,
		Meta: metaStore,
		Stream: streamCfg,
		Block: config.BlockConfig{
			TargetSize:  config.ByteSize(1024),
			MaxLinger:   config.Duration(2 * time.Second),
			Compression: "none",
		},
		Logger: logger,
	})

	// Run ingest pipeline
	pipeCtx, pipeCancel := context.WithCancel(ctx)
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- pipeline.Run(pipeCtx) }()

	// Wait for ingestion
	time.Sleep(5 * time.Second)

	// --- Verify KV indexes ---

	// Check latest key entry for app.port
	entry, err := metaStore.LookupKVKey(ctx, "KV_config", "app.port")
	if err != nil {
		t.Fatalf("LookupKVKey app.port: %v", err)
	}
	t.Logf("KV key app.port: seq=%d op=%s", entry.LastSequence, entry.Operation)
	if entry.Operation != "PUT" {
		t.Errorf("app.port operation = %q, want PUT", entry.Operation)
	}

	// Check that the latest value is "9090" (second put)
	msg, err := ctrl.Retrieve(ctx, entry.LastSequence)
	if err != nil {
		t.Fatalf("retrieve app.port: %v", err)
	}
	if string(msg.Data) != "9090" {
		t.Errorf("app.port value = %q, want 9090", string(msg.Data))
	}

	// Check db.url was deleted
	dbEntry, err := metaStore.LookupKVKey(ctx, "KV_config", "db.url")
	if err != nil {
		t.Fatalf("LookupKVKey db.url: %v", err)
	}
	if dbEntry.Operation != "DEL" {
		t.Errorf("db.url operation = %q, want DEL", dbEntry.Operation)
	}

	// Check revisions for app.port (should have 2)
	revs, err := metaStore.ListKVKeyRevisions(ctx, "KV_config", "app.port")
	if err != nil {
		t.Fatalf("ListKVKeyRevisions: %v", err)
	}
	t.Logf("app.port revisions: %d", len(revs))
	if len(revs) != 2 {
		t.Errorf("expected 2 revisions for app.port, got %d", len(revs))
	}

	// Check key listing
	keys, err := metaStore.ListKVKeys(ctx, "KV_config", "app.")
	if err != nil {
		t.Fatalf("ListKVKeys: %v", err)
	}
	t.Logf("Keys with prefix 'app.': %d", len(keys))
	if len(keys) != 2 {
		t.Errorf("expected 2 keys with prefix 'app.', got %d", len(keys))
	}

	// --- Start NATS KV responder and test sidecar retrieval ---
	respCtx, respCancel := context.WithCancel(ctx)
	respDone := make(chan error, 1)
	go func() {
		respDone <- serve.RunNATSKVResponder(respCtx, nc, "nts", []config.StreamConfig{streamCfg}, []*ingest.Pipeline{pipeline}, metaStore, logger)
	}()
	time.Sleep(500 * time.Millisecond) // wait for subscription

	// Request via NATS
	resp, err := nc.Request("nts.kv.config.get.app.port", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("NATS request kv get: %v", err)
	}
	var kvResp map[string]interface{}
	json.Unmarshal(resp.Data, &kvResp)
	t.Logf("NATS KV get response: %v", kvResp)
	if kvResp["value"] != "9090" {
		t.Errorf("NATS KV get value = %v, want 9090", kvResp["value"])
	}

	// Request keys
	resp, err = nc.Request("nts.kv.config.keys", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("NATS request kv keys: %v", err)
	}
	var keysList []string
	json.Unmarshal(resp.Data, &keysList)
	t.Logf("NATS KV keys response: %v", keysList)
	// db.url should be filtered out (it's deleted)
	for _, k := range keysList {
		if k == "db.url" {
			t.Error("deleted key db.url should not appear in keys list")
		}
	}

	respCancel()
	<-respDone
	pipeCancel()
	<-pipeDone

	t.Log("KV Store integration test PASSED")
}

// TestIntegration_ObjectStore tests the complete Object Store flow:
// create Object Store -> put object -> ingest pipeline -> retrieve via sidecar with chunk reassembly
func TestIntegration_ObjectStore(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	tmpDir := t.TempDir()
	logger := zap.NewNop()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()

	// Create Object Store bucket
	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket: "files",
	})
	if err != nil {
		t.Fatalf("create Object Store: %v", err)
	}

	// Put an object (will be split into chunks)
	testData := []byte(strings.Repeat("Hello, NATS Object Store! ", 100)) // ~2.6KB
	_, err = obs.Put(ctx, jetstream.ObjectMeta{Name: "test-doc.txt"}, strings.NewReader(string(testData)))
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	t.Logf("Put object test-doc.txt (%d bytes)", len(testData))

	// Initialize meta store
	metaStore, err := meta.NewBoltStore(filepath.Join(tmpDir, "meta.db"), logger)
	if err != nil {
		t.Fatalf("meta store: %v", err)
	}
	defer metaStore.Close()

	// Initialize tier stores
	fileDataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(fileDataDir, 0755)
	memStore := memory.NewStore(config.MemoryTierConfig{
		Enabled:   true,
		MaxBytes:  config.ByteSize(64 * 1024 * 1024),
		MaxBlocks: 10,
	}, logger)
	fileStore, _ := file.NewStore(config.FileTierConfig{
		Enabled: true,
		DataDir: fileDataDir,
	}, logger)

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "OBJ_files",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true, DataDir: fileDataDir},
		},
		Logger: logger,
	})

	streamCfg := config.StreamConfig{
		Name:         "OBJ_files",
		Type:         config.StreamTypeObjectStore,
		ConsumerName: "nts-test-obj",
		FetchBatch:   64,
		FetchTimeout: config.Duration(2 * time.Second),
		Tiers: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true, DataDir: fileDataDir},
		},
	}

	pipeline := ingest.NewPipeline(ingest.PipelineConfig{
		JS:   js,
		Ctrl: ctrl,
		Meta: metaStore,
		Stream: streamCfg,
		Block: config.BlockConfig{
			TargetSize:  config.ByteSize(8 * 1024 * 1024), // 8MB to fit all in one block
			MaxLinger:   config.Duration(2 * time.Second),
			Compression: "none",
		},
		Logger: logger,
	})

	// Run ingest pipeline
	pipeCtx, pipeCancel := context.WithCancel(ctx)
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- pipeline.Run(pipeCtx) }()

	// Wait for ingestion
	time.Sleep(5 * time.Second)

	// --- Verify Object Store indexes ---

	// Check object metadata
	objEntry, err := metaStore.LookupObj(ctx, "OBJ_files", "test-doc.txt")
	if err != nil {
		t.Fatalf("LookupObj test-doc.txt: %v", err)
	}
	t.Logf("Object: name=%s nuid=%s size=%d chunks=%d digest=%s",
		objEntry.Name, objEntry.NUID, objEntry.Size, objEntry.Chunks, objEntry.Digest)
	if objEntry.Name != "test-doc.txt" {
		t.Errorf("object name = %q, want test-doc.txt", objEntry.Name)
	}
	if objEntry.Deleted {
		t.Error("object should not be marked as deleted")
	}

	// Check chunk index
	chunks, err := metaStore.LookupObjChunks(ctx, "OBJ_files", objEntry.NUID)
	if err != nil {
		t.Fatalf("LookupObjChunks: %v", err)
	}
	t.Logf("Chunks: nuid=%s count=%d total_size=%d", chunks.NUID, len(chunks.ChunkSeqs), chunks.TotalSize)
	if len(chunks.ChunkSeqs) == 0 {
		t.Fatal("expected at least 1 chunk")
	}

	// Check object listing
	objects, err := metaStore.ListObjects(ctx, "OBJ_files")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(objects))
	}

	// --- Start NATS Obj responder and test chunk reassembly ---
	respCtx, respCancel := context.WithCancel(ctx)
	respDone := make(chan error, 1)
	go func() {
		respDone <- serve.RunNATSObjResponder(respCtx, nc, "nts", []config.StreamConfig{streamCfg}, []*ingest.Pipeline{pipeline}, metaStore, logger)
	}()
	time.Sleep(500 * time.Millisecond)

	// Request object info via NATS
	resp, err := nc.Request("nts.obj.files.info.test-doc.txt", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("NATS request obj info: %v", err)
	}
	var infoResp map[string]interface{}
	json.Unmarshal(resp.Data, &infoResp)
	t.Logf("NATS Obj info response: name=%v size=%v chunks=%v", infoResp["name"], infoResp["size"], infoResp["chunks"])

	// Request reassembled object via NATS
	resp, err = nc.Request("nts.obj.files.get.test-doc.txt", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("NATS request obj get: %v", err)
	}

	// Check if it's an error response
	if len(resp.Data) > 0 && resp.Data[0] == '{' {
		var errResp map[string]string
		if json.Unmarshal(resp.Data, &errResp) == nil && errResp["error"] != "" {
			t.Fatalf("NATS obj get returned error: %s", errResp["error"])
		}
	}

	t.Logf("NATS Obj get response: %d bytes", len(resp.Data))
	if string(resp.Data) != string(testData) {
		t.Errorf("reassembled object data mismatch: got %d bytes, want %d bytes",
			len(resp.Data), len(testData))
		// Show first 100 bytes of each for debugging
		got := string(resp.Data)
		want := string(testData)
		if len(got) > 100 {
			got = got[:100]
		}
		if len(want) > 100 {
			want = want[:100]
		}
		t.Logf("  got:  %q...", got)
		t.Logf("  want: %q...", want)
	}

	// Request object list via NATS
	resp, err = nc.Request("nts.obj.files.list", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("NATS request obj list: %v", err)
	}
	var listResp []map[string]interface{}
	json.Unmarshal(resp.Data, &listResp)
	t.Logf("NATS Obj list: %d objects", len(listResp))
	if len(listResp) != 1 {
		t.Errorf("expected 1 object in list, got %d", len(listResp))
	}

	respCancel()
	<-respDone
	pipeCancel()
	<-pipeDone

	t.Log("Object Store integration test PASSED")
}
