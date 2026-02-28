package nts

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestKVStore_Get_HotHit verifies that a key present in JetStream is returned
// directly without contacting the cold-tier sidecar.
func TestKVStore_Get_HotHit(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "hot-hit"})
	if err != nil {
		t.Fatalf("creating KV bucket: %v", err)
	}
	if _, err := kv.Put(ctx, "mykey", []byte("myvalue")); err != nil {
		t.Fatalf("putting key: %v", err)
	}

	store := &KVStore{
		kv:          kv,
		bucket:      "hot-hit",
		nc:          nc,
		prefix:      "nts",
		timeout:     5 * time.Second,
		autoRestore: false,
	}

	entry, err := store.Get(ctx, "mykey")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value()) != "myvalue" {
		t.Errorf("got value %q, want %q", string(entry.Value()), "myvalue")
	}
}

// TestKVStore_Get_ColdFallback_NoRestore verifies that a cold fetch succeeds but
// the key is NOT restored to JetStream when autoRestore is false.
func TestKVStore_Get_ColdFallback_NoRestore(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "cold-norestore"})
	if err != nil {
		t.Fatalf("creating KV bucket: %v", err)
	}

	// Mock cold-tier sidecar
	sub, err := nc.Subscribe("nts.kv.cold-norestore.get.coldkey", func(msg *nats.Msg) {
		resp, _ := json.Marshal(coldKVEntry{
			Bucket:    "cold-norestore",
			Key:       "coldkey",
			Value:     "coldvalue",
			Revision:  1,
			Operation: "PUT",
			Timestamp: time.Now(),
		})
		msg.Respond(resp)
	})
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	store := &KVStore{
		kv:          kv,
		bucket:      "cold-norestore",
		nc:          nc,
		prefix:      "nts",
		timeout:     5 * time.Second,
		autoRestore: false,
	}

	entry, err := store.Get(ctx, "coldkey")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value()) != "coldvalue" {
		t.Errorf("got value %q, want %q", string(entry.Value()), "coldvalue")
	}

	// Key must NOT have been written back to hot tier
	_, err = kv.Get(ctx, "coldkey")
	if err == nil {
		t.Error("key was unexpectedly restored to hot tier with autoRestore=false")
	}
}

// TestKVStore_Get_ColdFallback_AutoRestore verifies that a cold fetch with
// autoRestore=true writes the key back to the hot JetStream KV tier.
func TestKVStore_Get_ColdFallback_AutoRestore(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "cold-restore"})
	if err != nil {
		t.Fatalf("creating KV bucket: %v", err)
	}

	// Mock cold-tier sidecar
	sub, err := nc.Subscribe("nts.kv.cold-restore.get.hotkey", func(msg *nats.Msg) {
		resp, _ := json.Marshal(coldKVEntry{
			Bucket:    "cold-restore",
			Key:       "hotkey",
			Value:     "restoredvalue",
			Revision:  1,
			Operation: "PUT",
			Timestamp: time.Now(),
		})
		msg.Respond(resp)
	})
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	store := &KVStore{
		kv:          kv,
		bucket:      "cold-restore",
		nc:          nc,
		prefix:      "nts",
		timeout:     5 * time.Second,
		autoRestore: true,
	}

	entry, err := store.Get(ctx, "hotkey")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value()) != "restoredvalue" {
		t.Errorf("got value %q, want %q", string(entry.Value()), "restoredvalue")
	}

	// Key MUST have been restored to hot tier
	hotEntry, err := kv.Get(ctx, "hotkey")
	if err != nil {
		t.Fatalf("key was not restored to hot tier: %v", err)
	}
	if string(hotEntry.Value()) != "restoredvalue" {
		t.Errorf("restored value %q, want %q", string(hotEntry.Value()), "restoredvalue")
	}
}

// TestKVStore_Get_DeletedKey_NoRestore verifies that a cold fetch returning a
// DEL marker does NOT trigger a restore even when autoRestore=true.
func TestKVStore_Get_DeletedKey_NoRestore(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "del-norestore"})
	if err != nil {
		t.Fatalf("creating KV bucket: %v", err)
	}

	// Mock cold-tier sidecar returning a DEL marker
	sub, err := nc.Subscribe("nts.kv.del-norestore.get.deletedkey", func(msg *nats.Msg) {
		resp, _ := json.Marshal(coldKVEntry{
			Bucket:    "del-norestore",
			Key:       "deletedkey",
			Value:     "",
			Revision:  2,
			Operation: "DEL",
			Timestamp: time.Now(),
		})
		msg.Respond(resp)
	})
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	store := &KVStore{
		kv:          kv,
		bucket:      "del-norestore",
		nc:          nc,
		prefix:      "nts",
		timeout:     5 * time.Second,
		autoRestore: true,
	}

	entry, err := store.Get(ctx, "deletedkey")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry.Operation() != jetstream.KeyValueDelete {
		t.Errorf("got operation %v, want KeyValueDelete", entry.Operation())
	}

	// DEL key must NOT be restored to hot tier
	_, err = kv.Get(ctx, "deletedkey")
	if err == nil {
		t.Error("DEL key was unexpectedly restored to hot tier with autoRestore=true")
	}
}
