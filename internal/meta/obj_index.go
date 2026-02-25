package meta

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

// ObjEntry tracks a complete object's metadata in cold storage.
type ObjEntry struct {
	Bucket      string
	Name        string
	NUID        string
	Size        uint64
	Chunks      int
	Digest      string
	MetaSeq     uint64
	MetaBlockID uint64
	Deleted     bool
	ModTime     time.Time
	UpdatedAt   time.Time
}

// ObjChunkSet tracks the chunk messages for a specific NUID.
type ObjChunkSet struct {
	NUID        string
	ChunkSeqs   []uint64
	ChunkBlocks []uint64
	TotalSize   uint64
}

func encodeObjEntry(entry *ObjEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeObjEntry(data []byte) (*ObjEntry, error) {
	var entry ObjEntry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func encodeObjChunkSet(cs *ObjChunkSet) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cs); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeObjChunkSet(data []byte) (*ObjChunkSet, error) {
	var cs ObjChunkSet
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cs); err != nil {
		return nil, err
	}
	return &cs, nil
}

func (s *BoltStore) RecordObjMeta(_ context.Context, stream string, entry ObjEntry) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := s.ensureStreamBuckets(tx, stream)
		if err != nil {
			return err
		}
		idx := sb.Bucket(subBucketObjIndex)
		if idx == nil {
			return fmt.Errorf("obj_index bucket not found for stream %q", stream)
		}
		data, err := encodeObjEntry(&entry)
		if err != nil {
			return err
		}
		return idx.Put([]byte(entry.Name), data)
	})
}

func (s *BoltStore) RecordObjChunks(_ context.Context, stream string, cs ObjChunkSet) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := s.ensureStreamBuckets(tx, stream)
		if err != nil {
			return err
		}
		idx := sb.Bucket(subBucketObjChunkIndex)
		if idx == nil {
			return fmt.Errorf("obj_chunk_index bucket not found for stream %q", stream)
		}

		// Merge with existing chunks for same NUID (chunks may span multiple blocks)
		existing := idx.Get([]byte(cs.NUID))
		if existing != nil {
			prev, err := decodeObjChunkSet(existing)
			if err == nil {
				prev.ChunkSeqs = append(prev.ChunkSeqs, cs.ChunkSeqs...)
				prev.ChunkBlocks = append(prev.ChunkBlocks, cs.ChunkBlocks...)
				prev.TotalSize += cs.TotalSize
				cs = *prev
			}
		}

		data, err := encodeObjChunkSet(&cs)
		if err != nil {
			return err
		}
		return idx.Put([]byte(cs.NUID), data)
	})
}

func (s *BoltStore) LookupObj(_ context.Context, stream, name string) (*ObjEntry, error) {
	var entry *ObjEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}
		idx := sb.Bucket(subBucketObjIndex)
		if idx == nil {
			return fmt.Errorf("object %q not found", name)
		}
		raw := idx.Get([]byte(name))
		if raw == nil {
			return fmt.Errorf("object %q not found", name)
		}
		var err error
		entry, err = decodeObjEntry(raw)
		return err
	})
	return entry, err
}

func (s *BoltStore) LookupObjChunks(_ context.Context, stream, nuid string) (*ObjChunkSet, error) {
	var cs *ObjChunkSet
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}
		idx := sb.Bucket(subBucketObjChunkIndex)
		if idx == nil {
			return fmt.Errorf("chunks for NUID %q not found", nuid)
		}
		raw := idx.Get([]byte(nuid))
		if raw == nil {
			return fmt.Errorf("chunks for NUID %q not found", nuid)
		}
		var err error
		cs, err = decodeObjChunkSet(raw)
		return err
	})
	return cs, err
}

func (s *BoltStore) ListObjects(_ context.Context, stream string) ([]ObjEntry, error) {
	var entries []ObjEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return nil
		}
		idx := sb.Bucket(subBucketObjIndex)
		if idx == nil {
			return nil
		}
		return idx.ForEach(func(k, v []byte) error {
			entry, err := decodeObjEntry(v)
			if err != nil {
				return err
			}
			entries = append(entries, *entry)
			return nil
		})
	})
	return entries, err
}
