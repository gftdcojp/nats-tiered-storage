package block

import (
	"sync"
	"time"
)

// Builder accumulates messages into a block, respecting the target size.
type Builder struct {
	mu         sync.Mutex
	targetSize int64
	stream     string
	blockID    uint64
	messages   []Message
	curSize    int64
}

// NewBuilder creates a builder targeting the given block size.
func NewBuilder(stream string, blockID uint64, targetSize int64) *Builder {
	return &Builder{
		targetSize: targetSize,
		stream:     stream,
		blockID:    blockID,
		messages:   make([]Message, 0, 256),
		curSize:    BlockHeaderSize,
	}
}

// Add appends a message. Returns false if the message would exceed targetSize,
// indicating the caller should Seal() the current block and start a new one.
func (b *Builder) Add(msg Message) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	msgSize := int64(MsgHeaderSize + len(msg.Subject) + 4 + len(msg.Headers) + len(msg.Data) + ChecksumSize)
	if b.curSize+msgSize > b.targetSize && len(b.messages) > 0 {
		return false
	}

	b.messages = append(b.messages, msg)
	b.curSize += msgSize
	return true
}

// Seal finalizes the block: builds the index, encodes to binary format,
// and returns the immutable Block.
func (b *Builder) Seal() (*Block, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.messages) == 0 {
		return nil, nil
	}

	blk := &Block{
		ID:       b.blockID,
		Stream:   b.stream,
		FirstSeq: b.messages[0].Sequence,
		LastSeq:  b.messages[len(b.messages)-1].Sequence,
		FirstTS:  b.messages[0].Timestamp,
		LastTS:   b.messages[len(b.messages)-1].Timestamp,
		MsgCount: uint64(len(b.messages)),
		Messages: b.messages,
	}

	if err := blk.Encode(); err != nil {
		return nil, err
	}

	return blk, nil
}

// CurrentSize returns bytes accumulated so far.
func (b *Builder) CurrentSize() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.curSize
}

// MessageCount returns number of messages accumulated.
func (b *Builder) MessageCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages)
}

// LastTimestamp returns the timestamp of the last message, or zero if empty.
func (b *Builder) LastTimestamp() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.messages) == 0 {
		return time.Time{}
	}
	return b.messages[len(b.messages)-1].Timestamp
}
