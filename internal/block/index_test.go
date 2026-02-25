package block

import "testing"

func TestBlockIndexLookup(t *testing.T) {
	idx := &BlockIndex{
		Entries: []IndexEntry{
			{Sequence: 10, Offset: 48, Size: 100},
			{Sequence: 11, Offset: 148, Size: 120},
			{Sequence: 12, Offset: 268, Size: 80},
		},
	}

	entry, found := idx.Lookup(11)
	if !found {
		t.Fatal("expected to find seq 11")
	}
	if entry.Offset != 148 {
		t.Errorf("expected offset 148, got %d", entry.Offset)
	}

	_, found = idx.Lookup(99)
	if found {
		t.Fatal("should not find seq 99")
	}
}

func TestBlockIndexEncodeDecode(t *testing.T) {
	original := &BlockIndex{
		Entries: []IndexEntry{
			{Sequence: 100, Offset: 48, Size: 200},
			{Sequence: 101, Offset: 248, Size: 150},
			{Sequence: 102, Offset: 398, Size: 300},
		},
	}

	encoded := original.Encode()
	decoded, err := DecodeIndex(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(decoded.Entries))
	}

	for i, e := range decoded.Entries {
		if e.Sequence != original.Entries[i].Sequence {
			t.Errorf("entry %d: sequence mismatch: %d vs %d", i, e.Sequence, original.Entries[i].Sequence)
		}
		if e.Offset != original.Entries[i].Offset {
			t.Errorf("entry %d: offset mismatch: %d vs %d", i, e.Offset, original.Entries[i].Offset)
		}
		if e.Size != original.Entries[i].Size {
			t.Errorf("entry %d: size mismatch: %d vs %d", i, e.Size, original.Entries[i].Size)
		}
	}
}

func TestDecodeIndexTooSmall(t *testing.T) {
	_, err := DecodeIndex([]byte{1})
	if err == nil {
		t.Fatal("expected error")
	}
}
