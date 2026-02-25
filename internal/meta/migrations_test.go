package meta

import (
	"os"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func TestMigrateV1toV2(t *testing.T) {
	// Create a v1 database manually (without the new sub-buckets)
	tmpFile, err := os.CreateTemp("", "nts-migrate-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := bbolt.Open(tmpFile.Name(), 0600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	// Set up v1 schema: system bucket with version=1, and a stream with only the original sub-buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		sys, err := tx.CreateBucketIfNotExists(bucketSystem)
		if err != nil {
			return err
		}
		if err := sys.Put(keySchemaVersion, uint64ToBytes(1)); err != nil {
			return err
		}

		streams, err := tx.CreateBucketIfNotExists(bucketStreams)
		if err != nil {
			return err
		}
		sb, err := streams.CreateBucketIfNotExists([]byte("ORDERS"))
		if err != nil {
			return err
		}
		for _, name := range [][]byte{subBucketBlocks, subBucketSeqIndex, subBucketTimeIndex, subBucketConsumer} {
			if _, err := sb.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Now open with NewBoltStore which should trigger migration
	store, err := NewBoltStore(tmpFile.Name(), zap.NewNop())
	if err != nil {
		t.Fatalf("NewBoltStore after migration: %v", err)
	}
	defer store.Close()

	// Verify schema version is now 2
	var version uint64
	store.db.View(func(tx *bbolt.Tx) error {
		sys := tx.Bucket(bucketSystem)
		v := sys.Get(keySchemaVersion)
		if v != nil {
			version = bytesToUint64(v)
		}
		return nil
	})
	if version != 2 {
		t.Errorf("schema version = %d, want 2", version)
	}

	// Verify new sub-buckets exist on the ORDERS stream
	store.db.View(func(tx *bbolt.Tx) error {
		sb := store.getStreamBucket(tx, "ORDERS")
		if sb == nil {
			t.Fatal("ORDERS stream bucket not found after migration")
		}
		for _, name := range [][]byte{subBucketKVKeyIndex, subBucketKVRevIndex, subBucketObjIndex, subBucketObjChunkIndex} {
			if sb.Bucket(name) == nil {
				t.Errorf("sub-bucket %q not found after migration", string(name))
			}
		}
		return nil
	})
}

func TestMigrateIdempotent(t *testing.T) {
	store := newTestStore(t)

	// Running migrate again should be a no-op (already at v2)
	if err := store.Migrate(); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}

	var version uint64
	store.db.View(func(tx *bbolt.Tx) error {
		sys := tx.Bucket(bucketSystem)
		v := sys.Get(keySchemaVersion)
		if v != nil {
			version = bytesToUint64(v)
		}
		return nil
	})
	if version != 2 {
		t.Errorf("schema version = %d after idempotent migrate, want 2", version)
	}
}
