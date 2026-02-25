package meta

import (
	"context"
	"testing"
	"time"
)

func TestRecordAndLookupKVKey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry := KVKeyEntry{
		Bucket:       "config",
		Key:          "app.database_url",
		LastSequence: 42,
		LastBlockID:  1,
		Operation:    "PUT",
		Revision:     3,
		UpdatedAt:    time.Now(),
	}

	if err := store.RecordKVKey(ctx, "KV_config", entry); err != nil {
		t.Fatalf("RecordKVKey: %v", err)
	}

	got, err := store.LookupKVKey(ctx, "KV_config", "app.database_url")
	if err != nil {
		t.Fatalf("LookupKVKey: %v", err)
	}
	if got.Key != "app.database_url" {
		t.Errorf("key = %q, want %q", got.Key, "app.database_url")
	}
	if got.LastSequence != 42 {
		t.Errorf("LastSequence = %d, want 42", got.LastSequence)
	}
	if got.Operation != "PUT" {
		t.Errorf("Operation = %q, want %q", got.Operation, "PUT")
	}
	if got.Revision != 3 {
		t.Errorf("Revision = %d, want 3", got.Revision)
	}
}

func TestKVKeyOverwrite(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry1 := KVKeyEntry{
		Bucket:       "config",
		Key:          "app.port",
		LastSequence: 10,
		LastBlockID:  1,
		Operation:    "PUT",
		Revision:     1,
		UpdatedAt:    time.Now(),
	}
	store.RecordKVKey(ctx, "KV_config", entry1)

	entry2 := KVKeyEntry{
		Bucket:       "config",
		Key:          "app.port",
		LastSequence: 20,
		LastBlockID:  2,
		Operation:    "DEL",
		Revision:     2,
		UpdatedAt:    time.Now(),
	}
	store.RecordKVKey(ctx, "KV_config", entry2)

	got, err := store.LookupKVKey(ctx, "KV_config", "app.port")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSequence != 20 {
		t.Errorf("LastSequence = %d, want 20 (overwritten)", got.LastSequence)
	}
	if got.Operation != "DEL" {
		t.Errorf("Operation = %q, want DEL", got.Operation)
	}
}

func TestKVKeyNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.LookupKVKey(ctx, "KV_config", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
}

func TestListKVKeys(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	keys := []string{"app.database", "app.port", "app.host", "cache.ttl"}
	for i, k := range keys {
		store.RecordKVKey(ctx, "KV_config", KVKeyEntry{
			Bucket:       "config",
			Key:          k,
			LastSequence: uint64(i + 1),
			LastBlockID:  1,
			Operation:    "PUT",
			Revision:     1,
			UpdatedAt:    time.Now(),
		})
	}

	// List all
	all, err := store.ListKVKeys(ctx, "KV_config", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Errorf("ListKVKeys all: got %d, want 4", len(all))
	}

	// List with prefix
	appKeys, err := store.ListKVKeys(ctx, "KV_config", "app.")
	if err != nil {
		t.Fatal(err)
	}
	if len(appKeys) != 3 {
		t.Errorf("ListKVKeys app.: got %d, want 3", len(appKeys))
	}

	cacheKeys, err := store.ListKVKeys(ctx, "KV_config", "cache.")
	if err != nil {
		t.Fatal(err)
	}
	if len(cacheKeys) != 1 {
		t.Errorf("ListKVKeys cache.: got %d, want 1", len(cacheKeys))
	}
}

func TestRecordAndListKVRevisions(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Record multiple revisions for the same key
	for i := uint64(1); i <= 5; i++ {
		err := store.RecordKVRevision(ctx, "KV_config", "app.port", KVRevEntry{
			Sequence: i * 10,
			BlockID:  i,
		})
		if err != nil {
			t.Fatalf("RecordKVRevision seq=%d: %v", i*10, err)
		}
	}

	// Also record a revision for a different key
	store.RecordKVRevision(ctx, "KV_config", "app.host", KVRevEntry{Sequence: 100, BlockID: 10})

	revs, err := store.ListKVKeyRevisions(ctx, "KV_config", "app.port")
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 5 {
		t.Errorf("revisions for app.port: got %d, want 5", len(revs))
	}
	// Verify ordering (big-endian uint64 keys → sorted by sequence)
	for i, r := range revs {
		expected := uint64((i + 1) * 10)
		if r.Sequence != expected {
			t.Errorf("rev[%d].Sequence = %d, want %d", i, r.Sequence, expected)
		}
	}

	// Different key should have only 1
	hostRevs, err := store.ListKVKeyRevisions(ctx, "KV_config", "app.host")
	if err != nil {
		t.Fatal(err)
	}
	if len(hostRevs) != 1 {
		t.Errorf("revisions for app.host: got %d, want 1", len(hostRevs))
	}
}

func TestListKVKeysEmptyStream(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	keys, err := store.ListKVKeys(ctx, "nonexistent", "")
	if err != nil {
		t.Fatal(err)
	}
	if keys != nil {
		t.Errorf("expected nil for nonexistent stream, got %v", keys)
	}
}
