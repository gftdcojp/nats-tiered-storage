package block

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	// DefaultBlockSize matches NATS JetStream's default large block size.
	DefaultBlockSize = 8 * 1024 * 1024 // 8MB

	// MsgHeaderSize is the fixed header for each encoded message.
	// Layout: [4 bytes total_size][8 bytes sequence][8 bytes timestamp_ns][2 bytes subject_len]
	MsgHeaderSize = 22

	// ChecksumSize is the trailing CRC32 checksum per message.
	ChecksumSize = 4

	// BlockMagic identifies the block format.
	BlockMagic = uint32(0x4E545342) // "NTSB" - NATS Tiered Storage Block

	// BlockHeaderSize: [4 magic][4 version][8 block_id][8 first_seq][8 last_seq][8 msg_count][8 size]
	BlockHeaderSize = 48
)

// Block represents an immutable collection of messages.
type Block struct {
	ID        uint64
	Stream    string
	FirstSeq  uint64
	LastSeq   uint64
	FirstTS   time.Time
	LastTS    time.Time
	MsgCount  uint64
	SizeBytes int64
	Messages  []Message
	Index     *BlockIndex
	// Raw is the encoded block in binary format.
	Raw []byte
}

// Message represents a single message within a block.
type Message struct {
	Sequence  uint64
	Subject   string
	Headers   []byte
	Data      []byte
	Timestamp time.Time
	// Offset is set after encoding, pointing into Raw.
	Offset int64
	Size   int32
}

// Encode serializes the block to binary format, populating Raw.
func (b *Block) Encode() error {
	// Calculate total size
	totalSize := BlockHeaderSize
	for _, m := range b.Messages {
		totalSize += MsgHeaderSize + len(m.Subject) + 4 + len(m.Headers) + len(m.Data) + ChecksumSize
	}

	buf := make([]byte, 0, totalSize)

	// Block header
	hdr := make([]byte, BlockHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], BlockMagic)
	binary.BigEndian.PutUint32(hdr[4:8], 1) // version
	binary.BigEndian.PutUint64(hdr[8:16], b.ID)
	binary.BigEndian.PutUint64(hdr[16:24], b.FirstSeq)
	binary.BigEndian.PutUint64(hdr[24:32], b.LastSeq)
	binary.BigEndian.PutUint64(hdr[32:40], b.MsgCount)
	binary.BigEndian.PutUint64(hdr[40:48], uint64(b.SizeBytes))
	buf = append(buf, hdr...)

	// Build index
	idx := &BlockIndex{
		Entries: make([]IndexEntry, 0, len(b.Messages)),
	}

	// Encode each message
	for i := range b.Messages {
		m := &b.Messages[i]
		offset := int64(len(buf))

		subjectBytes := []byte(m.Subject)
		headerLen := len(m.Headers)
		dataLen := len(m.Data)
		msgSize := MsgHeaderSize + len(subjectBytes) + 4 + headerLen + dataLen + ChecksumSize

		// Message header
		msgHdr := make([]byte, MsgHeaderSize)
		binary.BigEndian.PutUint32(msgHdr[0:4], uint32(msgSize))
		binary.BigEndian.PutUint64(msgHdr[4:12], m.Sequence)
		binary.BigEndian.PutUint64(msgHdr[12:20], uint64(m.Timestamp.UnixNano()))
		binary.BigEndian.PutUint16(msgHdr[20:22], uint16(len(subjectBytes)))
		buf = append(buf, msgHdr...)

		// Subject
		buf = append(buf, subjectBytes...)

		// Headers (variable length, preceded by 4-byte length)
		hdrLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(hdrLenBuf, uint32(headerLen))
		buf = append(buf, hdrLenBuf...)
		if headerLen > 0 {
			buf = append(buf, m.Headers...)
		}

		// Data
		buf = append(buf, m.Data...)

		// CRC32 checksum
		crc := crc32.ChecksumIEEE(buf[offset:])
		crcBuf := make([]byte, ChecksumSize)
		binary.BigEndian.PutUint32(crcBuf, crc)
		buf = append(buf, crcBuf...)

		m.Offset = offset
		m.Size = int32(msgSize)

		idx.Entries = append(idx.Entries, IndexEntry{
			Sequence: m.Sequence,
			Offset:   offset,
			Size:     int32(msgSize),
		})
	}

	b.Raw = buf
	b.Index = idx
	b.SizeBytes = int64(len(buf))
	return nil
}

// Decode parses a raw byte slice into a Block.
func Decode(raw []byte) (*Block, error) {
	if len(raw) < BlockHeaderSize {
		return nil, fmt.Errorf("block too small: %d bytes", len(raw))
	}

	magic := binary.BigEndian.Uint32(raw[0:4])
	if magic != BlockMagic {
		return nil, fmt.Errorf("invalid block magic: 0x%08X", magic)
	}

	version := binary.BigEndian.Uint32(raw[4:8])
	if version != 1 {
		return nil, fmt.Errorf("unsupported block version: %d", version)
	}

	b := &Block{
		ID:        binary.BigEndian.Uint64(raw[8:16]),
		FirstSeq:  binary.BigEndian.Uint64(raw[16:24]),
		LastSeq:   binary.BigEndian.Uint64(raw[24:32]),
		MsgCount:  binary.BigEndian.Uint64(raw[32:40]),
		SizeBytes: int64(binary.BigEndian.Uint64(raw[40:48])),
		Raw:       raw,
	}

	idx := &BlockIndex{}

	pos := BlockHeaderSize
	for pos < len(raw) {
		if pos+MsgHeaderSize > len(raw) {
			return nil, fmt.Errorf("truncated message at offset %d", pos)
		}

		msgSize := int(binary.BigEndian.Uint32(raw[pos : pos+4]))
		seq := binary.BigEndian.Uint64(raw[pos+4 : pos+12])
		tsNano := binary.BigEndian.Uint64(raw[pos+12 : pos+20])
		subjectLen := int(binary.BigEndian.Uint16(raw[pos+20 : pos+22]))

		msgStart := pos
		pos += MsgHeaderSize

		if pos+subjectLen > len(raw) {
			return nil, fmt.Errorf("truncated subject at offset %d", pos)
		}
		subject := string(raw[pos : pos+subjectLen])
		pos += subjectLen

		// Headers length
		if pos+4 > len(raw) {
			return nil, fmt.Errorf("truncated header length at offset %d", pos)
		}
		headerLen := int(binary.BigEndian.Uint32(raw[pos : pos+4]))
		pos += 4

		var headers []byte
		if headerLen > 0 {
			if pos+headerLen > len(raw) {
				return nil, fmt.Errorf("truncated headers at offset %d", pos)
			}
			headers = make([]byte, headerLen)
			copy(headers, raw[pos:pos+headerLen])
			pos += headerLen
		}

		// Data: remaining bytes before checksum
		dataLen := msgSize - MsgHeaderSize - subjectLen - 4 - headerLen - ChecksumSize
		if dataLen < 0 || pos+dataLen > len(raw) {
			return nil, fmt.Errorf("invalid data length at offset %d", pos)
		}
		data := make([]byte, dataLen)
		copy(data, raw[pos:pos+dataLen])
		pos += dataLen

		// Verify checksum
		if pos+ChecksumSize > len(raw) {
			return nil, fmt.Errorf("truncated checksum at offset %d", pos)
		}
		expectedCRC := binary.BigEndian.Uint32(raw[pos : pos+ChecksumSize])
		actualCRC := crc32.ChecksumIEEE(raw[msgStart:pos])
		if expectedCRC != actualCRC {
			return nil, fmt.Errorf("checksum mismatch at offset %d: expected 0x%08X, got 0x%08X", msgStart, expectedCRC, actualCRC)
		}
		pos += ChecksumSize

		msg := Message{
			Sequence:  seq,
			Subject:   subject,
			Headers:   headers,
			Data:      data,
			Timestamp: time.Unix(0, int64(tsNano)),
			Offset:    int64(msgStart),
			Size:      int32(msgSize),
		}

		b.Messages = append(b.Messages, msg)
		idx.Entries = append(idx.Entries, IndexEntry{
			Sequence: seq,
			Offset:   int64(msgStart),
			Size:     int32(msgSize),
		})

		if len(b.Messages) == 1 {
			b.FirstTS = msg.Timestamp
		}
		b.LastTS = msg.Timestamp
	}

	b.Index = idx
	return b, nil
}

// DecodeMessage decodes a single message from raw bytes at a given offset.
func DecodeMessage(raw []byte) (*Message, error) {
	if len(raw) < MsgHeaderSize {
		return nil, fmt.Errorf("message too small: %d bytes", len(raw))
	}

	seq := binary.BigEndian.Uint64(raw[4:12])
	tsNano := binary.BigEndian.Uint64(raw[12:20])
	subjectLen := int(binary.BigEndian.Uint16(raw[20:22]))

	pos := MsgHeaderSize
	if pos+subjectLen > len(raw) {
		return nil, fmt.Errorf("truncated subject")
	}
	subject := string(raw[pos : pos+subjectLen])
	pos += subjectLen

	if pos+4 > len(raw) {
		return nil, fmt.Errorf("truncated header length")
	}
	headerLen := int(binary.BigEndian.Uint32(raw[pos : pos+4]))
	pos += 4

	var headers []byte
	if headerLen > 0 {
		if pos+headerLen > len(raw) {
			return nil, fmt.Errorf("truncated headers")
		}
		headers = make([]byte, headerLen)
		copy(headers, raw[pos:pos+headerLen])
		pos += headerLen
	}

	dataLen := len(raw) - pos - ChecksumSize
	if dataLen < 0 {
		return nil, fmt.Errorf("invalid data length")
	}
	data := make([]byte, dataLen)
	copy(data, raw[pos:pos+dataLen])

	return &Message{
		Sequence:  seq,
		Subject:   subject,
		Headers:   headers,
		Data:      data,
		Timestamp: time.Unix(0, int64(tsNano)),
	}, nil
}
