package block

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// BlockIndex provides efficient lookup of messages within a block.
type BlockIndex struct {
	Entries []IndexEntry
}

// IndexEntry maps a sequence number to its position in the block.
type IndexEntry struct {
	Sequence uint64
	Offset   int64
	Size     int32
}

// Lookup finds the offset and size for a given sequence number.
func (idx *BlockIndex) Lookup(seq uint64) (IndexEntry, bool) {
	i := sort.Search(len(idx.Entries), func(i int) bool {
		return idx.Entries[i].Sequence >= seq
	})
	if i < len(idx.Entries) && idx.Entries[i].Sequence == seq {
		return idx.Entries[i], true
	}
	return IndexEntry{}, false
}

// Encode serializes the index to a compact binary format for S3 sidecar storage.
// Format: [4 bytes entry_count][repeated: 8 seq + 8 offset + 4 size]
func (idx *BlockIndex) Encode() []byte {
	buf := make([]byte, 4+len(idx.Entries)*20)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(idx.Entries)))

	pos := 4
	for _, e := range idx.Entries {
		binary.BigEndian.PutUint64(buf[pos:pos+8], e.Sequence)
		binary.BigEndian.PutUint64(buf[pos+8:pos+16], uint64(e.Offset))
		binary.BigEndian.PutUint32(buf[pos+16:pos+20], uint32(e.Size))
		pos += 20
	}
	return buf
}

// DecodeIndex parses a binary-encoded index.
func DecodeIndex(data []byte) (*BlockIndex, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("index too small: %d bytes", len(data))
	}

	count := int(binary.BigEndian.Uint32(data[0:4]))
	if len(data) < 4+count*20 {
		return nil, fmt.Errorf("index truncated: expected %d entries, got %d bytes", count, len(data))
	}

	entries := make([]IndexEntry, count)
	pos := 4
	for i := 0; i < count; i++ {
		entries[i] = IndexEntry{
			Sequence: binary.BigEndian.Uint64(data[pos : pos+8]),
			Offset:   int64(binary.BigEndian.Uint64(data[pos+8 : pos+16])),
			Size:     int32(binary.BigEndian.Uint32(data[pos+16 : pos+20])),
		}
		pos += 20
	}

	return &BlockIndex{Entries: entries}, nil
}
