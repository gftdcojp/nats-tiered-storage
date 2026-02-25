package meta

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

// KVKeyEntry tracks the latest known state of a KV key in cold storage.
type KVKeyEntry struct {
	Bucket       string
	Key          string
	LastSequence uint64
	LastBlockID  uint64
	Operation    string // "PUT", "DEL", "PURGE"
	Revision     uint64
	UpdatedAt    time.Time
}

// KVRevEntry tracks a specific revision of a key.
type KVRevEntry struct {
	Sequence uint64
	BlockID  uint64
}

func encodeKVKeyEntry(entry *KVKeyEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeKVKeyEntry(data []byte) (*KVKeyEntry, error) {
	var entry KVKeyEntry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func encodeKVRevEntry(entry *KVRevEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeKVRevEntry(data []byte) (*KVRevEntry, error) {
	var entry KVRevEntry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// kvRevKey builds a composite key: key\x00 + big-endian seq
func kvRevKey(key string, seq uint64) []byte {
	k := []byte(key)
	result := make([]byte, len(k)+1+8)
	copy(result, k)
	result[len(k)] = 0
	copy(result[len(k)+1:], uint64ToBytes(seq))
	return result
}

// kvRevKeyPrefix returns the prefix for scanning all revisions of a key.
func kvRevKeyPrefix(key string) []byte {
	k := []byte(key)
	result := make([]byte, len(k)+1)
	copy(result, k)
	result[len(k)] = 0
	return result
}

func (s *BoltStore) RecordKVKey(_ context.Context, stream string, entry KVKeyEntry) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := s.ensureStreamBuckets(tx, stream)
		if err != nil {
			return err
		}
		idx := sb.Bucket(subBucketKVKeyIndex)
		if idx == nil {
			return fmt.Errorf("kv_key_index bucket not found for stream %q", stream)
		}
		data, err := encodeKVKeyEntry(&entry)
		if err != nil {
			return err
		}
		return idx.Put([]byte(entry.Key), data)
	})
}

func (s *BoltStore) RecordKVRevision(_ context.Context, stream, key string, entry KVRevEntry) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := s.ensureStreamBuckets(tx, stream)
		if err != nil {
			return err
		}
		idx := sb.Bucket(subBucketKVRevIndex)
		if idx == nil {
			return fmt.Errorf("kv_rev_index bucket not found for stream %q", stream)
		}
		data, err := encodeKVRevEntry(&entry)
		if err != nil {
			return err
		}
		return idx.Put(kvRevKey(key, entry.Sequence), data)
	})
}

func (s *BoltStore) LookupKVKey(_ context.Context, stream, key string) (*KVKeyEntry, error) {
	var entry *KVKeyEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}
		idx := sb.Bucket(subBucketKVKeyIndex)
		if idx == nil {
			return fmt.Errorf("key %q not found", key)
		}
		raw := idx.Get([]byte(key))
		if raw == nil {
			return fmt.Errorf("key %q not found", key)
		}
		var err error
		entry, err = decodeKVKeyEntry(raw)
		return err
	})
	return entry, err
}

func (s *BoltStore) ListKVKeys(_ context.Context, stream, prefix string) ([]KVKeyEntry, error) {
	var entries []KVKeyEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return nil
		}
		idx := sb.Bucket(subBucketKVKeyIndex)
		if idx == nil {
			return nil
		}
		c := idx.Cursor()
		var k, v []byte
		if prefix != "" {
			k, v = c.Seek([]byte(prefix))
		} else {
			k, v = c.First()
		}
		prefixBytes := []byte(prefix)
		for k != nil {
			if prefix != "" && !bytes.HasPrefix(k, prefixBytes) {
				break
			}
			entry, err := decodeKVKeyEntry(v)
			if err != nil {
				return err
			}
			entries = append(entries, *entry)
			k, v = c.Next()
		}
		return nil
	})
	return entries, err
}

func (s *BoltStore) ListKVKeyRevisions(_ context.Context, stream, key string) ([]KVRevEntry, error) {
	var entries []KVRevEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return nil
		}
		idx := sb.Bucket(subBucketKVRevIndex)
		if idx == nil {
			return nil
		}
		prefix := kvRevKeyPrefix(key)
		c := idx.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			entry, err := decodeKVRevEntry(v)
			if err != nil {
				return err
			}
			entries = append(entries, *entry)
		}
		return nil
	})
	return entries, err
}
