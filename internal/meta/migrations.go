package meta

import (
	"fmt"

	"go.etcd.io/bbolt"
)

// Migrate runs any pending schema migrations.
func (s *BoltStore) Migrate() error {
	var version uint64
	s.db.View(func(tx *bbolt.Tx) error {
		sys := tx.Bucket(bucketSystem)
		if sys == nil {
			return nil
		}
		v := sys.Get(keySchemaVersion)
		if v != nil {
			version = bytesToUint64(v)
		}
		return nil
	})

	if version < 2 {
		if err := s.migrateV1toV2(); err != nil {
			return fmt.Errorf("migration v1→v2: %w", err)
		}
	}

	return nil
}

// migrateV1toV2 adds KV and Object Store sub-buckets to all existing stream buckets.
func (s *BoltStore) migrateV1toV2() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		streams := tx.Bucket(bucketStreams)
		if streams != nil {
			// Iterate over all existing stream buckets and add the new sub-buckets
			err := streams.ForEach(func(k, v []byte) error {
				// Skip non-bucket entries (v != nil means it's a key-value, not a nested bucket)
				if v != nil {
					return nil
				}
				sb := streams.Bucket(k)
				if sb == nil {
					return nil
				}
				for _, name := range [][]byte{
					subBucketKVKeyIndex,
					subBucketKVRevIndex,
					subBucketObjIndex,
					subBucketObjChunkIndex,
				} {
					if _, err := sb.CreateBucketIfNotExists(name); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		// Update schema version
		sys := tx.Bucket(bucketSystem)
		if sys == nil {
			return fmt.Errorf("system bucket not found")
		}
		return sys.Put(keySchemaVersion, uint64ToBytes(2))
	})
}
