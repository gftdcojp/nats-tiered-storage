package nts

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// KVStore wraps jetstream.KeyValue with transparent cold storage fallback.
type KVStore struct {
	kv      jetstream.KeyValue
	bucket  string
	nc      *nats.Conn
	prefix  string
	timeout time.Duration
}

// Get retrieves the latest value for a key.
// If the key is not found in JetStream, it falls back to the sidecar.
func (s *KVStore) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	entry, err := s.kv.Get(ctx, key)
	if err == nil {
		return entry, nil
	}
	if !isNotFound(err) && !isKeyDeleted(err) {
		return nil, err
	}

	// Fallback to sidecar
	subject := fmt.Sprintf("%s.kv.%s.get.%s", s.prefix, s.bucket, key)
	resp, err := s.nc.Request(subject, nil, s.timeout)
	if err != nil {
		return nil, fmt.Errorf("nts: sidecar request for key %q: %w", key, err)
	}

	var result coldKVEntry
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("nts: decoding sidecar KV response: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("nts: sidecar: %s", result.Error)
	}

	return &kvEntry{
		bucket:    result.Bucket,
		key:       result.Key,
		value:     []byte(result.Value),
		revision:  result.Revision,
		operation: parseKVOp(result.Operation),
		timestamp: result.Timestamp,
	}, nil
}

// Put stores a value for a key (delegates to JetStream).
func (s *KVStore) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return s.kv.Put(ctx, key, value)
}

// Delete marks a key as deleted (delegates to JetStream).
func (s *KVStore) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	return s.kv.Delete(ctx, key, opts...)
}

// Keys returns all keys (combines JetStream and cold storage).
func (s *KVStore) Keys(ctx context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	keys, err := s.kv.Keys(ctx, opts...)
	if err == nil && len(keys) > 0 {
		return keys, nil
	}

	// Fallback to sidecar
	subject := fmt.Sprintf("%s.kv.%s.keys", s.prefix, s.bucket)
	resp, rErr := s.nc.Request(subject, nil, s.timeout)
	if rErr != nil {
		// Return original error if sidecar also fails
		if err != nil {
			return nil, err
		}
		return nil, rErr
	}

	var coldKeys []string
	if err := json.Unmarshal(resp.Data, &coldKeys); err != nil {
		return nil, fmt.Errorf("nts: decoding sidecar keys response: %w", err)
	}
	return coldKeys, nil
}

// History retrieves the history for a key (falls back to sidecar).
func (s *KVStore) History(ctx context.Context, key string, opts ...jetstream.WatchOpt) ([]jetstream.KeyValueEntry, error) {
	entries, err := s.kv.History(ctx, key, opts...)
	if err == nil && len(entries) > 0 {
		return entries, nil
	}

	// Fallback to sidecar
	subject := fmt.Sprintf("%s.kv.%s.history.%s", s.prefix, s.bucket, key)
	resp, rErr := s.nc.Request(subject, nil, s.timeout)
	if rErr != nil {
		if err != nil {
			return nil, err
		}
		return nil, rErr
	}

	var revisions []coldKVHistoryEntry
	if err := json.Unmarshal(resp.Data, &revisions); err != nil {
		return nil, fmt.Errorf("nts: decoding sidecar history response: %w", err)
	}

	result := make([]jetstream.KeyValueEntry, 0, len(revisions))
	for _, r := range revisions {
		result = append(result, &kvEntry{
			bucket:    s.bucket,
			key:       key,
			value:     []byte(r.Value),
			revision:  r.Sequence,
			timestamp: r.Timestamp,
		})
	}
	return result, nil
}

// Underlying returns the wrapped jetstream.KeyValue for direct access.
func (s *KVStore) Underlying() jetstream.KeyValue {
	return s.kv
}

// coldKVEntry is the JSON structure returned by the sidecar for KV get.
type coldKVEntry struct {
	Error     string    `json:"error,omitempty"`
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Sequence  uint64    `json:"sequence"`
	Revision  uint64    `json:"revision"`
	Operation string    `json:"operation"`
	Timestamp time.Time `json:"timestamp"`
}

// coldKVHistoryEntry is a single revision in a history response.
type coldKVHistoryEntry struct {
	Sequence  uint64    `json:"sequence"`
	BlockID   uint64    `json:"block_id"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// kvEntry implements jetstream.KeyValueEntry for cold storage results.
type kvEntry struct {
	bucket    string
	key       string
	value     []byte
	revision  uint64
	operation jetstream.KeyValueOp
	timestamp time.Time
}

func (e *kvEntry) Bucket() string               { return e.bucket }
func (e *kvEntry) Key() string                   { return e.key }
func (e *kvEntry) Value() []byte                 { return e.value }
func (e *kvEntry) Revision() uint64              { return e.revision }
func (e *kvEntry) Created() time.Time            { return e.timestamp }
func (e *kvEntry) Delta() uint64                 { return 0 }
func (e *kvEntry) Operation() jetstream.KeyValueOp { return e.operation }

func parseKVOp(s string) jetstream.KeyValueOp {
	switch s {
	case "DEL":
		return jetstream.KeyValueDelete
	case "PURGE":
		return jetstream.KeyValuePurge
	default:
		return jetstream.KeyValuePut
	}
}
