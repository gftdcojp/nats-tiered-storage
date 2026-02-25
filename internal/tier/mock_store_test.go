package tier

import (
	"context"
	"fmt"
	"sync"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
)

// mockTierStore is a thread-safe in-memory TierStore for testing.
type mockTierStore struct {
	mu     sync.Mutex
	blocks map[string]*block.Block // key: "stream/blockID"
	putErr error
	getErr error
	delErr error
	tier   Tier
}

func newMockStore(t Tier) *mockTierStore {
	return &mockTierStore{
		blocks: make(map[string]*block.Block),
		tier:   t,
	}
}

func blockKey(ref BlockRef) string {
	return fmt.Sprintf("%s/%d", ref.Stream, ref.BlockID)
}

func (m *mockTierStore) Put(_ context.Context, ref BlockRef, data *block.Block) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.mu.Lock()
	m.blocks[blockKey(ref)] = data
	m.mu.Unlock()
	return nil
}

func (m *mockTierStore) Get(_ context.Context, ref BlockRef) (*block.Block, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.Lock()
	blk, ok := m.blocks[blockKey(ref)]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("block not found: %s", blockKey(ref))
	}
	return blk, nil
}

func (m *mockTierStore) GetMessage(_ context.Context, ref BlockRef, seq uint64) (*StoredMessage, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.Lock()
	blk, ok := m.blocks[blockKey(ref)]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("block not found: %s", blockKey(ref))
	}
	for _, msg := range blk.Messages {
		if msg.Sequence == seq {
			return &StoredMessage{
				Stream:    blk.Stream,
				Subject:   msg.Subject,
				Sequence:  msg.Sequence,
				Data:      msg.Data,
				Timestamp: msg.Timestamp,
			}, nil
		}
	}
	return nil, fmt.Errorf("sequence %d not found", seq)
}

func (m *mockTierStore) Delete(_ context.Context, ref BlockRef) error {
	if m.delErr != nil {
		return m.delErr
	}
	m.mu.Lock()
	delete(m.blocks, blockKey(ref))
	m.mu.Unlock()
	return nil
}

func (m *mockTierStore) Exists(_ context.Context, ref BlockRef) (bool, error) {
	m.mu.Lock()
	_, ok := m.blocks[blockKey(ref)]
	m.mu.Unlock()
	return ok, nil
}

func (m *mockTierStore) Stats(_ context.Context) (TierStats, error) {
	m.mu.Lock()
	count := int64(len(m.blocks))
	m.mu.Unlock()
	return TierStats{Tier: m.tier, BlockCount: count}, nil
}

func (m *mockTierStore) Close() error {
	return nil
}

func (m *mockTierStore) hasBlock(ref BlockRef) bool {
	m.mu.Lock()
	_, ok := m.blocks[blockKey(ref)]
	m.mu.Unlock()
	return ok
}
