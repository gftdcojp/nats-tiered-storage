package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"go.uber.org/zap"
)

// mockS3 is an in-memory S3 implementation for testing.
type mockS3 struct {
	mu      sync.RWMutex
	objects map[string][]byte
	putErr  error
	getErr  error
	delErr  error
	headErr error
}

func newMockS3() *mockS3 {
	return &mockS3{objects: make(map[string][]byte)}
}

func (m *mockS3) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	data, _ := io.ReadAll(params.Body)
	m.mu.Lock()
	m.objects[*params.Key] = data
	m.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.RLock()
	data, ok := m.objects[*params.Key]
	m.mu.RUnlock()
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}

	// Handle range requests
	if params.Range != nil {
		var start, end int64
		fmt.Sscanf(*params.Range, "bytes=%d-%d", &start, &end)
		if start < int64(len(data)) {
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			data = data[start : end+1]
		}
	}

	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: intPtr(int64(len(data))),
	}, nil
}

func (m *mockS3) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.delErr != nil {
		return nil, m.delErr
	}
	m.mu.Lock()
	delete(m.objects, *params.Key)
	m.mu.Unlock()
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headErr != nil {
		return nil, m.headErr
	}
	m.mu.RLock()
	data, ok := m.objects[*params.Key]
	m.mu.RUnlock()
	if !ok {
		return nil, &s3types.NotFound{}
	}
	return &s3.HeadObjectOutput{ContentLength: intPtr(int64(len(data)))}, nil
}

func intPtr(v int64) *int64 { return &v }

func makeTestBlock(t *testing.T, id uint64, stream string, firstSeq uint64, msgCount int) *block.Block {
	t.Helper()
	b := block.NewBuilder(stream, id, 1<<20) // 1MB target
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

func newTestBlobStore(t *testing.T) (*Store, *mockS3) {
	t.Helper()
	mock := newMockS3()
	store := NewStore(mock, "test-bucket", config.BlobTierConfig{
		Prefix: "test",
	}, zap.NewNop())
	t.Cleanup(func() { store.Close() })
	return store, mock
}

func TestBlobStore_PutGet(t *testing.T) {
	store, _ := newTestBlobStore(t)
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
		if msg.Sequence != uint64(i+1) {
			t.Errorf("msg[%d] seq = %d, want %d", i, msg.Sequence, i+1)
		}
	}
}

func TestBlobStore_PutUploadsIndex(t *testing.T) {
	store, mock := newTestBlobStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	blkKey := store.objectKey(ref)
	idxKey := store.indexKey(ref)

	mock.mu.RLock()
	_, hasBlk := mock.objects[blkKey]
	_, hasIdx := mock.objects[idxKey]
	mock.mu.RUnlock()

	if !hasBlk {
		t.Error("block not uploaded to S3")
	}
	if !hasIdx {
		t.Error("index not uploaded to S3")
	}
}

func TestBlobStore_GetMessage_WithCache(t *testing.T) {
	store, _ := newTestBlobStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	// First call downloads index
	msg, err := store.GetMessage(ctx, ref, 3)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 3 {
		t.Fatalf("expected seq 3, got %d", msg.Sequence)
	}

	// Second call should use cache
	msg2, err := store.GetMessage(ctx, ref, 3)
	if err != nil {
		t.Fatal(err)
	}
	if msg2.Sequence != 3 {
		t.Fatalf("expected seq 3 from cache, got %d", msg2.Sequence)
	}
}

func TestBlobStore_GetMessage_RangeRequest(t *testing.T) {
	store, _ := newTestBlobStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 10, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 10, LastSeq: 14}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	msg, err := store.GetMessage(ctx, ref, 12)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 12 {
		t.Fatalf("expected seq 12, got %d", msg.Sequence)
	}
	if msg.Stream != "ORDERS" {
		t.Fatalf("expected stream ORDERS, got %s", msg.Stream)
	}
}

func TestBlobStore_GetMessage_Fallback(t *testing.T) {
	mock := newMockS3()
	store := NewStore(mock, "test-bucket", config.BlobTierConfig{Prefix: "test"}, zap.NewNop())
	defer store.Close()
	ctx := context.Background()

	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	// Delete index from S3 to force fallback
	idxKey := store.indexKey(ref)
	mock.mu.Lock()
	delete(mock.objects, idxKey)
	mock.mu.Unlock()

	msg, err := store.GetMessage(ctx, ref, 3)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 3 {
		t.Fatalf("expected seq 3, got %d", msg.Sequence)
	}
}

func TestBlobStore_Delete(t *testing.T) {
	store, mock := newTestBlobStore(t)
	ctx := context.Background()
	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	// Populate cache
	store.GetMessage(ctx, ref, 1)

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}

	// Verify objects removed from S3
	mock.mu.RLock()
	remaining := len(mock.objects)
	mock.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected 0 objects after delete, got %d", remaining)
	}

	// Verify cache cleared
	store.mu.RLock()
	cacheKey := fmt.Sprintf("%s/%d", ref.Stream, ref.BlockID)
	_, cached := store.indexCache[cacheKey]
	store.mu.RUnlock()
	if cached {
		t.Error("index cache not cleared after delete")
	}
}

func TestBlobStore_Exists(t *testing.T) {
	store, _ := newTestBlobStore(t)
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
}

func TestBlobStore_Race_ConcurrentGet(t *testing.T) {
	store, _ := newTestBlobStore(t)
	ctx := context.Background()

	// Put multiple blocks
	for i := uint64(1); i <= 10; i++ {
		blk := makeTestBlock(t, i, "ORDERS", i*100, 5)
		ref := tier.BlockRef{Stream: "ORDERS", BlockID: i, FirstSeq: i * 100, LastSeq: i*100 + 4}
		if err := store.Put(ctx, ref, blk); err != nil {
			t.Fatal(err)
		}
	}

	// Concurrent GetMessage from different goroutines
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			blockID := uint64(n%10) + 1
			seq := blockID*100 + uint64(n%5)
			ref := tier.BlockRef{Stream: "ORDERS", BlockID: blockID, FirstSeq: blockID * 100, LastSeq: blockID*100 + 4}
			msg, err := store.GetMessage(ctx, ref, seq)
			if err != nil {
				t.Errorf("goroutine %d: %v", n, err)
				return
			}
			if msg.Sequence != seq {
				t.Errorf("goroutine %d: expected seq %d, got %d", n, seq, msg.Sequence)
			}
		}(i)
	}
	wg.Wait()
}

func TestBlobStore_Race_GetAndDelete(t *testing.T) {
	store, _ := newTestBlobStore(t)
	ctx := context.Background()

	blk := makeTestBlock(t, 1, "ORDERS", 1, 10)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 10}
	if err := store.Put(ctx, ref, blk); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			seq := uint64(n%10) + 1
			store.GetMessage(ctx, ref, seq) // ignore errors from concurrent delete
		}(i)
	}

	// Concurrent delete
	wg.Add(1)
	go func() {
		defer wg.Done()
		store.Delete(ctx, ref)
	}()

	wg.Wait()
}

func TestBlobStore_PutS3Error(t *testing.T) {
	mock := newMockS3()
	mock.putErr = fmt.Errorf("simulated S3 error")
	store := NewStore(mock, "test-bucket", config.BlobTierConfig{}, zap.NewNop())
	defer store.Close()

	blk := makeTestBlock(t, 1, "ORDERS", 1, 5)
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}

	err := store.Put(context.Background(), ref, blk)
	if err == nil {
		t.Fatal("expected error from Put")
	}
	if !strings.Contains(err.Error(), "S3") {
		t.Fatalf("expected S3 error, got: %v", err)
	}
}

func TestBlobStore_GetS3Error(t *testing.T) {
	mock := newMockS3()
	mock.getErr = fmt.Errorf("simulated S3 error")
	store := NewStore(mock, "test-bucket", config.BlobTierConfig{}, zap.NewNop())
	defer store.Close()

	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 1, FirstSeq: 1, LastSeq: 5}
	_, err := store.Get(context.Background(), ref)
	if err == nil {
		t.Fatal("expected error from Get")
	}
	if !strings.Contains(err.Error(), "S3") {
		t.Fatalf("expected S3 error, got: %v", err)
	}
}

func TestBlobStore_ObjectKeyFormat(t *testing.T) {
	ref := tier.BlockRef{Stream: "ORDERS", BlockID: 42}

	// With prefix
	store := &Store{cfg: config.BlobTierConfig{Prefix: "prod"}}
	key := store.objectKey(ref)
	if key != "prod/ORDERS/blocks/0000000042.blk" {
		t.Fatalf("unexpected key with prefix: %s", key)
	}
	idxKey := store.indexKey(ref)
	if idxKey != "prod/ORDERS/blocks/0000000042.idx" {
		t.Fatalf("unexpected index key with prefix: %s", idxKey)
	}

	// Without prefix
	store2 := &Store{cfg: config.BlobTierConfig{}}
	key2 := store2.objectKey(ref)
	if key2 != "ORDERS/blocks/0000000042.blk" {
		t.Fatalf("unexpected key without prefix: %s", key2)
	}
}
