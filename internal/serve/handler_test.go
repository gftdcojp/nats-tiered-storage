package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/memory"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

// testPipeline is a minimal pipeline stand-in for handler tests.
// The handler only uses p.Stream() and p.Controller(), so we create
// a real tier.Controller backed by a memory store.
type testPipeline struct {
	stream string
	ctrl   *tier.Controller
}

func newTestSetup(t *testing.T) (*handler, *tier.Controller, meta.Store) {
	t.Helper()
	metaStore := newTestMeta(t)
	memStore := memory.NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())
	fileStore := memory.NewStore(config.MemoryTierConfig{Enabled: true}, zap.NewNop())

	ctrl := tier.NewController(tier.ControllerConfig{
		Stream: "ORDERS",
		Memory: memStore,
		File:   fileStore,
		Meta:   metaStore,
		Policy: config.TiersConfig{
			Memory: config.MemoryTierConfig{Enabled: true},
			File:   config.FileTierConfig{Enabled: true},
		},
		Logger: zap.NewNop(),
	})

	// Ingest some test blocks
	blk := makeBlock(t, 1, "ORDERS", 1, 10)
	if err := ctrl.Ingest(context.Background(), blk); err != nil {
		t.Fatal(err)
	}

	// Build a fake pipeline map using testPipeline
	tp := &testPipeline{stream: "ORDERS", ctrl: ctrl}
	pipeMap := map[string]*testPipeline{"ORDERS": tp}

	// We can't directly set pipelines with the real *ingest.Pipeline type,
	// so we test via the HTTP mux that handler registers routes on.
	_ = pipeMap

	h := &handler{
		pipelines:  nil, // set below
		meta:       metaStore,
		streamCfgs: []config.StreamConfig{{Name: "ORDERS"}},
		logger:     zap.NewNop(),
	}

	return h, ctrl, metaStore
}

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

func makeBlock(t *testing.T, id uint64, stream string, firstSeq uint64, msgCount int) *block.Block {
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

func TestHandler_Status(t *testing.T) {
	h := &handler{
		pipelines: nil,
		logger:    zap.NewNop(),
	}

	req := httptest.NewRequest("GET", "/v1/status", nil)
	w := httptest.NewRecorder()
	h.handleStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", resp["status"])
	}
}

func TestHandler_ListBlocks(t *testing.T) {
	_, _, metaStore := newTestSetup(t)

	h := &handler{meta: metaStore, logger: zap.NewNop()}

	req := httptest.NewRequest("GET", "/v1/blocks/ORDERS", nil)
	req.SetPathValue("stream", "ORDERS")
	w := httptest.NewRecorder()
	h.handleListBlocks(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var blocks []map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &blocks)
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}
}

func TestHandler_GetBlock(t *testing.T) {
	_, _, metaStore := newTestSetup(t)
	h := &handler{meta: metaStore, logger: zap.NewNop()}

	req := httptest.NewRequest("GET", "/v1/blocks/ORDERS/1", nil)
	req.SetPathValue("stream", "ORDERS")
	req.SetPathValue("blockID", "1")
	w := httptest.NewRecorder()
	h.handleGetBlock(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_GetBlock_Invalid(t *testing.T) {
	h := &handler{meta: newTestMeta(t), logger: zap.NewNop()}

	req := httptest.NewRequest("GET", "/v1/blocks/ORDERS/abc", nil)
	req.SetPathValue("stream", "ORDERS")
	req.SetPathValue("blockID", "abc")
	w := httptest.NewRecorder()
	h.handleGetBlock(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid block ID, got %d", w.Code)
	}
}

func TestHandler_Streams(t *testing.T) {
	_, _, metaStore := newTestSetup(t)
	// handleStreams iterates over h.pipelines which we can't easily set up
	// without real Pipeline, but we can test it returns valid JSON with no pipelines
	h := &handler{
		pipelines: nil,
		meta:      metaStore,
		logger:    zap.NewNop(),
	}

	req := httptest.NewRequest("GET", "/v1/streams", nil)
	w := httptest.NewRecorder()
	h.handleStreams(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_StreamStats_NotFound(t *testing.T) {
	h := &handler{
		pipelines: nil,
		meta:      newTestMeta(t),
		logger:    zap.NewNop(),
	}

	req := httptest.NewRequest("GET", "/v1/streams/UNKNOWN/stats", nil)
	req.SetPathValue("stream", "UNKNOWN")
	w := httptest.NewRecorder()
	h.handleStreamStats(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandler_GetMessage_InvalidSeq(t *testing.T) {
	h := &handler{pipelines: nil, meta: newTestMeta(t), logger: zap.NewNop()}

	req := httptest.NewRequest("GET", "/v1/messages/ORDERS/abc", nil)
	req.SetPathValue("stream", "ORDERS")
	req.SetPathValue("seq", "abc")
	w := httptest.NewRecorder()
	h.handleGetMessage(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid seq, got %d", w.Code)
	}
}

func TestHandler_GetMessage_NotFound(t *testing.T) {
	h := &handler{pipelines: nil, meta: newTestMeta(t), logger: zap.NewNop()}

	req := httptest.NewRequest("GET", "/v1/messages/UNKNOWN/1", nil)
	req.SetPathValue("stream", "UNKNOWN")
	req.SetPathValue("seq", "1")
	w := httptest.NewRecorder()
	h.handleGetMessage(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown stream, got %d", w.Code)
	}
}

func TestHandler_KVListKeys(t *testing.T) {
	metaStore := newTestMeta(t)
	ctx := context.Background()

	// Record some KV keys
	metaStore.RecordKVKey(ctx, "KV_config", meta.KVKeyEntry{
		Key: "app.port", Bucket: "config", Operation: "PUT",
		LastSequence: 1, LastBlockID: 1, UpdatedAt: time.Now(),
	})
	metaStore.RecordKVKey(ctx, "KV_config", meta.KVKeyEntry{
		Key: "app.host", Bucket: "config", Operation: "PUT",
		LastSequence: 2, LastBlockID: 1, UpdatedAt: time.Now(),
	})

	h := &handler{
		meta:       metaStore,
		streamCfgs: []config.StreamConfig{{Name: "KV_config", Type: config.StreamTypeKV}},
		logger:     zap.NewNop(),
	}

	req := httptest.NewRequest("GET", "/v1/kv/config/keys", nil)
	req.SetPathValue("bucket", "config")
	w := httptest.NewRecorder()
	h.handleKVListKeys(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var keys []string
	json.Unmarshal(w.Body.Bytes(), &keys)
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestHandler_KVGet_NotFound(t *testing.T) {
	metaStore := newTestMeta(t)
	h := &handler{
		meta:       metaStore,
		streamCfgs: []config.StreamConfig{{Name: "KV_config", Type: config.StreamTypeKV}},
		logger:     zap.NewNop(),
	}

	req := httptest.NewRequest("GET", "/v1/kv/config/get/unknown.key", nil)
	req.SetPathValue("bucket", "config")
	req.SetPathValue("key", "unknown.key")
	w := httptest.NewRecorder()
	h.handleKVGet(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandler_ObjList(t *testing.T) {
	metaStore := newTestMeta(t)
	ctx := context.Background()

	metaStore.RecordObjMeta(ctx, "OBJ_files", meta.ObjEntry{
		Name: "report.pdf", Bucket: "files", NUID: "abc123",
		Size: 1024, Chunks: 1, ModTime: time.Now(),
	})

	h := &handler{
		meta:       metaStore,
		streamCfgs: []config.StreamConfig{{Name: "OBJ_files", Type: config.StreamTypeObjectStore}},
		logger:     zap.NewNop(),
	}

	req := httptest.NewRequest("GET", "/v1/objects/files/list", nil)
	req.SetPathValue("bucket", "files")
	w := httptest.NewRecorder()
	h.handleObjList(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var objects []map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &objects)
	if len(objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objects))
	}
	if objects[0]["name"] != "report.pdf" {
		t.Fatalf("expected report.pdf, got %v", objects[0]["name"])
	}
}

func TestHandler_ObjInfo_NotFound(t *testing.T) {
	metaStore := newTestMeta(t)
	h := &handler{
		meta:       metaStore,
		streamCfgs: []config.StreamConfig{{Name: "OBJ_files", Type: config.StreamTypeObjectStore}},
		logger:     zap.NewNop(),
	}

	req := httptest.NewRequest("GET", "/v1/objects/files/info/unknown.pdf", nil)
	req.SetPathValue("bucket", "files")
	req.SetPathValue("name", "unknown.pdf")
	w := httptest.NewRecorder()
	h.handleObjInfo(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
