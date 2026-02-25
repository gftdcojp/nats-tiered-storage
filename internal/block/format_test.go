package block

import (
	"testing"
	"time"
)

func TestBlockEncodeAndDecode(t *testing.T) {
	now := time.Now().Truncate(time.Nanosecond)

	blk := &Block{
		ID:     1,
		Stream: "ORDERS",
		Messages: []Message{
			{
				Sequence:  100,
				Subject:   "orders.new",
				Data:      []byte(`{"id": 1, "amount": 42.50}`),
				Timestamp: now,
			},
			{
				Sequence:  101,
				Subject:   "orders.update",
				Headers:   []byte("Nats-Msg-Id: abc123\r\n"),
				Data:      []byte(`{"id": 1, "status": "shipped"}`),
				Timestamp: now.Add(time.Second),
			},
			{
				Sequence:  102,
				Subject:   "orders.cancel",
				Data:      []byte(`{"id": 2}`),
				Timestamp: now.Add(2 * time.Second),
			},
		},
	}
	blk.FirstSeq = 100
	blk.LastSeq = 102
	blk.FirstTS = now
	blk.LastTS = now.Add(2 * time.Second)
	blk.MsgCount = 3

	if err := blk.Encode(); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	if len(blk.Raw) == 0 {
		t.Fatal("encoded block is empty")
	}
	if blk.Index == nil {
		t.Fatal("index is nil after encode")
	}
	if len(blk.Index.Entries) != 3 {
		t.Fatalf("expected 3 index entries, got %d", len(blk.Index.Entries))
	}

	// Decode
	decoded, err := Decode(blk.Raw)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ID != 1 {
		t.Errorf("expected block ID 1, got %d", decoded.ID)
	}
	if decoded.FirstSeq != 100 {
		t.Errorf("expected FirstSeq 100, got %d", decoded.FirstSeq)
	}
	if decoded.LastSeq != 102 {
		t.Errorf("expected LastSeq 102, got %d", decoded.LastSeq)
	}
	if decoded.MsgCount != 3 {
		t.Errorf("expected MsgCount 3, got %d", decoded.MsgCount)
	}
	if len(decoded.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(decoded.Messages))
	}

	// Verify messages
	msg0 := decoded.Messages[0]
	if msg0.Sequence != 100 {
		t.Errorf("msg0: expected seq 100, got %d", msg0.Sequence)
	}
	if msg0.Subject != "orders.new" {
		t.Errorf("msg0: expected subject orders.new, got %s", msg0.Subject)
	}
	if string(msg0.Data) != `{"id": 1, "amount": 42.50}` {
		t.Errorf("msg0: unexpected data: %s", msg0.Data)
	}

	msg1 := decoded.Messages[1]
	if msg1.Sequence != 101 {
		t.Errorf("msg1: expected seq 101, got %d", msg1.Sequence)
	}
	if string(msg1.Headers) != "Nats-Msg-Id: abc123\r\n" {
		t.Errorf("msg1: unexpected headers: %q", msg1.Headers)
	}

	// Index should also be decoded
	if decoded.Index == nil {
		t.Fatal("decoded index is nil")
	}
	entry, found := decoded.Index.Lookup(101)
	if !found {
		t.Fatal("expected to find seq 101 in index")
	}
	if entry.Sequence != 101 {
		t.Errorf("index lookup returned wrong sequence: %d", entry.Sequence)
	}
}

func TestBlockDecodeInvalidMagic(t *testing.T) {
	data := make([]byte, BlockHeaderSize)
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestBlockDecodeTooSmall(t *testing.T) {
	_, err := Decode([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for too small data")
	}
}
