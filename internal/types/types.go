package types

import "time"

// Tier identifies which storage tier a block resides in.
type Tier int

const (
	TierMemory Tier = iota
	TierFile
	TierBlob
)

func (t Tier) String() string {
	switch t {
	case TierMemory:
		return "memory"
	case TierFile:
		return "file"
	case TierBlob:
		return "blob"
	default:
		return "unknown"
	}
}

// BlockRef uniquely identifies a block within the tiered storage system.
type BlockRef struct {
	Stream   string
	BlockID  uint64
	FirstSeq uint64
	LastSeq  uint64
}

// StoredMessage represents a single message retrieved from any tier.
type StoredMessage struct {
	Stream    string
	Subject   string
	Sequence  uint64
	Data      []byte
	Headers   map[string][]string
	Timestamp time.Time
}

// TierStats reports usage for a single tier.
type TierStats struct {
	Tier        Tier
	BlockCount  int64
	TotalBytes  int64
	CapacityMax int64 // -1 for unlimited (blob tier)
}
