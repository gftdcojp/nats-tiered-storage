package meta

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/types"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// Store provides durable metadata tracking for all blocks across all tiers.
type Store interface {
	RecordBlock(ctx context.Context, entry BlockEntry) error
	UpdateTier(ctx context.Context, stream string, blockID uint64, from, to types.Tier) error
	UpdateS3Key(ctx context.Context, stream string, blockID uint64, s3Key string) error
	LookupBySequence(ctx context.Context, stream string, seq uint64) (*BlockEntry, error)
	LookupBySequenceRange(ctx context.Context, stream string, startSeq, endSeq uint64) ([]BlockEntry, error)
	LookupByTimeRange(ctx context.Context, stream string, start, end time.Time) ([]BlockEntry, error)
	ListBlocks(ctx context.Context, stream string, tierFilter *types.Tier) ([]BlockEntry, error)
	GetBlock(ctx context.Context, stream string, blockID uint64) (*BlockEntry, error)
	DeleteBlock(ctx context.Context, stream string, blockID uint64) error
	GetConsumerState(ctx context.Context, stream string) (uint64, error)
	SetConsumerState(ctx context.Context, stream string, seq uint64) error

	// KV Store index methods
	RecordKVKey(ctx context.Context, stream string, entry KVKeyEntry) error
	RecordKVRevision(ctx context.Context, stream, key string, entry KVRevEntry) error
	LookupKVKey(ctx context.Context, stream, key string) (*KVKeyEntry, error)
	ListKVKeys(ctx context.Context, stream, prefix string) ([]KVKeyEntry, error)
	ListKVKeyRevisions(ctx context.Context, stream, key string) ([]KVRevEntry, error)

	// Object Store index methods
	RecordObjMeta(ctx context.Context, stream string, entry ObjEntry) error
	RecordObjChunks(ctx context.Context, stream string, cs ObjChunkSet) error
	LookupObj(ctx context.Context, stream, name string) (*ObjEntry, error)
	LookupObjChunks(ctx context.Context, stream, nuid string) (*ObjChunkSet, error)
	ListObjects(ctx context.Context, stream string) ([]ObjEntry, error)

	Ping() error
	Close() error
}

// BoltStore implements Store using bbolt (BoltDB).
type BoltStore struct {
	db     *bbolt.DB
	logger *zap.Logger
}

// NewBoltStore opens or creates a BoltDB metadata store.
func NewBoltStore(path string, logger *zap.Logger) (*BoltStore, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening bolt db: %w", err)
	}

	s := &BoltStore{db: db, logger: logger}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return s, nil
}

func (s *BoltStore) initSchema() error {
	if err := s.db.Update(func(tx *bbolt.Tx) error {
		sys, err := tx.CreateBucketIfNotExists(bucketSystem)
		if err != nil {
			return err
		}
		v := sys.Get(keySchemaVersion)
		if v == nil {
			return sys.Put(keySchemaVersion, uint64ToBytes(currentSchemaVersion))
		}
		return nil
	}); err != nil {
		return err
	}
	return s.Migrate()
}

func (s *BoltStore) ensureStreamBuckets(tx *bbolt.Tx, stream string) (*bbolt.Bucket, error) {
	streams, err := tx.CreateBucketIfNotExists(bucketStreams)
	if err != nil {
		return nil, err
	}
	sb, err := streams.CreateBucketIfNotExists(streamBucketName(stream))
	if err != nil {
		return nil, err
	}
	for _, name := range [][]byte{
		subBucketBlocks,
		subBucketSeqIndex,
		subBucketTimeIndex,
		subBucketConsumer,
		subBucketKVKeyIndex,
		subBucketKVRevIndex,
		subBucketObjIndex,
		subBucketObjChunkIndex,
	} {
		if _, err := sb.CreateBucketIfNotExists(name); err != nil {
			return nil, err
		}
	}
	return sb, nil
}

func encodeBlockEntry(entry *BlockEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBlockEntry(data []byte) (*BlockEntry, error) {
	var entry BlockEntry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (s *BoltStore) RecordBlock(_ context.Context, entry BlockEntry) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := s.ensureStreamBuckets(tx, entry.Stream)
		if err != nil {
			return err
		}

		data, err := encodeBlockEntry(&entry)
		if err != nil {
			return err
		}

		blocks := sb.Bucket(subBucketBlocks)
		if err := blocks.Put(uint64ToBytes(entry.BlockID), data); err != nil {
			return err
		}

		// Sequence index: firstSeq -> blockID
		seqIdx := sb.Bucket(subBucketSeqIndex)
		if err := seqIdx.Put(uint64ToBytes(entry.FirstSeq), uint64ToBytes(entry.BlockID)); err != nil {
			return err
		}

		// Time index: firstTS unix nano -> blockID
		timeIdx := sb.Bucket(subBucketTimeIndex)
		if err := timeIdx.Put(int64ToBytes(entry.FirstTS.UnixNano()), uint64ToBytes(entry.BlockID)); err != nil {
			return err
		}

		return nil
	})
}

func (s *BoltStore) UpdateTier(_ context.Context, stream string, blockID uint64, _, to types.Tier) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}

		blocks := sb.Bucket(subBucketBlocks)
		raw := blocks.Get(uint64ToBytes(blockID))
		if raw == nil {
			return fmt.Errorf("block %d not found in stream %q", blockID, stream)
		}

		entry, err := decodeBlockEntry(raw)
		if err != nil {
			return err
		}

		entry.CurrentTier = to
		entry.DemotedAt = time.Now()

		data, err := encodeBlockEntry(entry)
		if err != nil {
			return err
		}

		return blocks.Put(uint64ToBytes(blockID), data)
	})
}

func (s *BoltStore) UpdateS3Key(_ context.Context, stream string, blockID uint64, s3Key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}

		blocks := sb.Bucket(subBucketBlocks)
		raw := blocks.Get(uint64ToBytes(blockID))
		if raw == nil {
			return fmt.Errorf("block %d not found", blockID)
		}

		entry, err := decodeBlockEntry(raw)
		if err != nil {
			return err
		}

		entry.S3Key = s3Key
		data, err := encodeBlockEntry(entry)
		if err != nil {
			return err
		}

		return blocks.Put(uint64ToBytes(blockID), data)
	})
}

func (s *BoltStore) LookupBySequence(_ context.Context, stream string, seq uint64) (*BlockEntry, error) {
	var entry *BlockEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}

		seqIdx := sb.Bucket(subBucketSeqIndex)
		c := seqIdx.Cursor()

		// Find the largest key <= seq
		k, v := c.Seek(uint64ToBytes(seq))
		if k == nil {
			// seq is beyond all indexed sequences; use last
			k, v = c.Last()
		} else if bytesToUint64(k) > seq {
			// Seek found a key > seq, go back one
			k, v = c.Prev()
		}
		if k == nil {
			return fmt.Errorf("sequence %d not found in stream %q", seq, stream)
		}

		blockID := bytesToUint64(v)
		blocks := sb.Bucket(subBucketBlocks)
		raw := blocks.Get(uint64ToBytes(blockID))
		if raw == nil {
			return fmt.Errorf("block %d not found in stream %q", blockID, stream)
		}

		var err error
		entry, err = decodeBlockEntry(raw)
		return err
	})
	return entry, err
}

func (s *BoltStore) LookupBySequenceRange(_ context.Context, stream string, startSeq, endSeq uint64) ([]BlockEntry, error) {
	var entries []BlockEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}

		seqIdx := sb.Bucket(subBucketSeqIndex)
		blocks := sb.Bucket(subBucketBlocks)
		c := seqIdx.Cursor()

		// Find first block that could contain startSeq
		k, v := c.Seek(uint64ToBytes(startSeq))
		if k == nil {
			k, v = c.Last()
		} else if bytesToUint64(k) > startSeq {
			pk, pv := c.Prev()
			if pk != nil {
				k, v = pk, pv
			}
		}

		seen := make(map[uint64]bool)
		for k != nil {
			blockID := bytesToUint64(v)
			if !seen[blockID] {
				raw := blocks.Get(uint64ToBytes(blockID))
				if raw != nil {
					entry, err := decodeBlockEntry(raw)
					if err != nil {
						return err
					}
					if entry.FirstSeq <= endSeq && entry.LastSeq >= startSeq {
						entries = append(entries, *entry)
						seen[blockID] = true
					}
					if entry.FirstSeq > endSeq {
						break
					}
				}
			}
			k, v = c.Next()
		}
		return nil
	})
	return entries, err
}

func (s *BoltStore) LookupByTimeRange(_ context.Context, stream string, start, end time.Time) ([]BlockEntry, error) {
	var entries []BlockEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}

		timeIdx := sb.Bucket(subBucketTimeIndex)
		blocks := sb.Bucket(subBucketBlocks)
		c := timeIdx.Cursor()

		startKey := int64ToBytes(start.UnixNano())
		for k, v := c.Seek(startKey); k != nil; k, v = c.Next() {
			blockID := bytesToUint64(v)
			raw := blocks.Get(uint64ToBytes(blockID))
			if raw == nil {
				continue
			}
			entry, err := decodeBlockEntry(raw)
			if err != nil {
				return err
			}
			if entry.FirstTS.After(end) {
				break
			}
			entries = append(entries, *entry)
		}
		return nil
	})
	return entries, err
}

func (s *BoltStore) ListBlocks(_ context.Context, stream string, tierFilter *types.Tier) ([]BlockEntry, error) {
	var entries []BlockEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return nil
		}

		blocks := sb.Bucket(subBucketBlocks)
		return blocks.ForEach(func(k, v []byte) error {
			entry, err := decodeBlockEntry(v)
			if err != nil {
				return err
			}
			if tierFilter != nil && entry.CurrentTier != *tierFilter {
				return nil
			}
			entries = append(entries, *entry)
			return nil
		})
	})
	return entries, err
}

func (s *BoltStore) GetBlock(_ context.Context, stream string, blockID uint64) (*BlockEntry, error) {
	var entry *BlockEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return fmt.Errorf("stream %q not found", stream)
		}

		blocks := sb.Bucket(subBucketBlocks)
		raw := blocks.Get(uint64ToBytes(blockID))
		if raw == nil {
			return fmt.Errorf("block %d not found", blockID)
		}

		var err error
		entry, err = decodeBlockEntry(raw)
		return err
	})
	return entry, err
}

func (s *BoltStore) DeleteBlock(_ context.Context, stream string, blockID uint64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return nil
		}

		blocks := sb.Bucket(subBucketBlocks)
		raw := blocks.Get(uint64ToBytes(blockID))
		if raw == nil {
			return nil
		}

		entry, err := decodeBlockEntry(raw)
		if err != nil {
			return err
		}

		// Remove from blocks
		if err := blocks.Delete(uint64ToBytes(blockID)); err != nil {
			return err
		}

		// Remove from seq index
		seqIdx := sb.Bucket(subBucketSeqIndex)
		if err := seqIdx.Delete(uint64ToBytes(entry.FirstSeq)); err != nil {
			return err
		}

		// Remove from time index
		timeIdx := sb.Bucket(subBucketTimeIndex)
		if err := timeIdx.Delete(int64ToBytes(entry.FirstTS.UnixNano())); err != nil {
			return err
		}

		return nil
	})
}

func (s *BoltStore) GetConsumerState(_ context.Context, stream string) (uint64, error) {
	var seq uint64
	err := s.db.View(func(tx *bbolt.Tx) error {
		sb := s.getStreamBucket(tx, stream)
		if sb == nil {
			return nil
		}
		consumer := sb.Bucket(subBucketConsumer)
		if consumer == nil {
			return nil
		}
		v := consumer.Get(keyLastAckedSeq)
		if v != nil {
			seq = bytesToUint64(v)
		}
		return nil
	})
	return seq, err
}

func (s *BoltStore) SetConsumerState(_ context.Context, stream string, seq uint64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := s.ensureStreamBuckets(tx, stream)
		if err != nil {
			return err
		}
		consumer := sb.Bucket(subBucketConsumer)
		if err := consumer.Put(keyLastAckedSeq, uint64ToBytes(seq)); err != nil {
			return err
		}
		return consumer.Put(keyLastAckedTS, int64ToBytes(time.Now().UnixNano()))
	})
}

func (s *BoltStore) Ping() error {
	return s.db.View(func(tx *bbolt.Tx) error {
		return nil
	})
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) getStreamBucket(tx *bbolt.Tx, stream string) *bbolt.Bucket {
	streams := tx.Bucket(bucketStreams)
	if streams == nil {
		return nil
	}
	return streams.Bucket(streamBucketName(stream))
}
