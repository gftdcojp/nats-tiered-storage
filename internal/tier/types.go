package tier

import (
	"context"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/types"
)

// Re-export types for convenience.
type Tier = types.Tier
type BlockRef = types.BlockRef
type StoredMessage = types.StoredMessage
type TierStats = types.TierStats

// Re-export constants.
const (
	TierMemory = types.TierMemory
	TierFile   = types.TierFile
	TierBlob   = types.TierBlob
)

// TierStore is the interface every storage tier must implement.
type TierStore interface {
	Put(ctx context.Context, ref BlockRef, data *block.Block) error
	Get(ctx context.Context, ref BlockRef) (*block.Block, error)
	GetMessage(ctx context.Context, ref BlockRef, seq uint64) (*StoredMessage, error)
	Delete(ctx context.Context, ref BlockRef) error
	Exists(ctx context.Context, ref BlockRef) (bool, error)
	Stats(ctx context.Context) (TierStats, error)
	Close() error
}
