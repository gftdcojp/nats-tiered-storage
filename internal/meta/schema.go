package meta

import (
	"encoding/binary"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/types"
)

// Bucket names in BoltDB.
var (
	bucketSystem        = []byte("system")
	bucketStreams       = []byte("streams")
	keySchemaVersion    = []byte("schema_version")
	keyLastCompaction   = []byte("last_compaction")
	subBucketBlocks     = []byte("blocks")
	subBucketSeqIndex   = []byte("seq_index")
	subBucketTimeIndex  = []byte("time_index")
	subBucketConsumer   = []byte("consumer_state")
	keyLastAckedSeq     = []byte("last_acked_seq")
	keyLastAckedTS      = []byte("last_acked_ts")

	// Schema v2: KV and Object Store indexes
	subBucketKVKeyIndex    = []byte("kv_key_index")
	subBucketKVRevIndex    = []byte("kv_rev_index")
	subBucketObjIndex      = []byte("obj_index")
	subBucketObjChunkIndex = []byte("obj_chunk_index")
)

const currentSchemaVersion = 2

// BlockEntry is the metadata record for a single block.
type BlockEntry struct {
	Stream      string
	BlockID     uint64
	FirstSeq    uint64
	LastSeq     uint64
	FirstTS     time.Time
	LastTS      time.Time
	MsgCount    uint64
	SizeBytes   int64
	CurrentTier types.Tier   // hottest tier (= Tiers[0]); kept for backward compat
	Tiers       []types.Tier // all tiers holding this block, hot→cold order
	S3Key       string
	CreatedAt   time.Time
	DemotedAt   time.Time
	Subjects    []string
}

// EffectiveTiers returns the list of tiers that hold this block.
// Falls back to []Tier{CurrentTier} for data written before the Tiers field existed.
func (e *BlockEntry) EffectiveTiers() []types.Tier {
	if len(e.Tiers) > 0 {
		return e.Tiers
	}
	return []types.Tier{e.CurrentTier}
}

// Ref returns a BlockRef for this entry.
func (e *BlockEntry) Ref() types.BlockRef {
	return types.BlockRef{
		Stream:   e.Stream,
		BlockID:  e.BlockID,
		FirstSeq: e.FirstSeq,
		LastSeq:  e.LastSeq,
	}
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func streamBucketName(stream string) []byte {
	return []byte(stream)
}
