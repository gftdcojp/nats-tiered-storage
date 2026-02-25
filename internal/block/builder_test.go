package block

import (
	"testing"
	"time"
)

func TestBuilderBasic(t *testing.T) {
	b := NewBuilder("TEST", 1, DefaultBlockSize)

	msg := Message{
		Sequence:  1,
		Subject:   "test.subject",
		Data:      []byte("hello world"),
		Timestamp: time.Now(),
	}

	if !b.Add(msg) {
		t.Fatal("failed to add first message")
	}

	if b.MessageCount() != 1 {
		t.Errorf("expected 1 message, got %d", b.MessageCount())
	}

	blk, err := b.Seal()
	if err != nil {
		t.Fatalf("seal failed: %v", err)
	}
	if blk == nil {
		t.Fatal("sealed block is nil")
	}
	if blk.MsgCount != 1 {
		t.Errorf("expected 1 msg, got %d", blk.MsgCount)
	}
	if blk.FirstSeq != 1 {
		t.Errorf("expected FirstSeq 1, got %d", blk.FirstSeq)
	}
}

func TestBuilderRejectsWhenFull(t *testing.T) {
	// Small target size
	b := NewBuilder("TEST", 1, 200)

	// Add messages until one is rejected
	accepted := 0
	for i := 0; i < 100; i++ {
		msg := Message{
			Sequence:  uint64(i + 1),
			Subject:   "test.subject",
			Data:      make([]byte, 50),
			Timestamp: time.Now(),
		}
		if b.Add(msg) {
			accepted++
		} else {
			break
		}
	}

	if accepted == 0 {
		t.Fatal("should have accepted at least one message")
	}
	if accepted >= 100 {
		t.Fatal("should have rejected some messages")
	}
}

func TestBuilderSealEmpty(t *testing.T) {
	b := NewBuilder("TEST", 1, DefaultBlockSize)
	blk, err := b.Seal()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if blk != nil {
		t.Fatal("expected nil block for empty builder")
	}
}
