package meta

import (
	"context"
	"testing"
	"time"
)

func TestRecordAndLookupObj(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry := ObjEntry{
		Bucket:      "files",
		Name:        "report.pdf",
		NUID:        "abc123",
		Size:        1024 * 1024,
		Chunks:      8,
		Digest:      "sha256=deadbeef",
		MetaSeq:     100,
		MetaBlockID: 5,
		Deleted:     false,
		ModTime:     time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := store.RecordObjMeta(ctx, "OBJ_files", entry); err != nil {
		t.Fatalf("RecordObjMeta: %v", err)
	}

	got, err := store.LookupObj(ctx, "OBJ_files", "report.pdf")
	if err != nil {
		t.Fatalf("LookupObj: %v", err)
	}
	if got.Name != "report.pdf" {
		t.Errorf("Name = %q, want %q", got.Name, "report.pdf")
	}
	if got.NUID != "abc123" {
		t.Errorf("NUID = %q, want %q", got.NUID, "abc123")
	}
	if got.Size != 1024*1024 {
		t.Errorf("Size = %d, want %d", got.Size, 1024*1024)
	}
	if got.Chunks != 8 {
		t.Errorf("Chunks = %d, want 8", got.Chunks)
	}
	if got.Digest != "sha256=deadbeef" {
		t.Errorf("Digest = %q, want %q", got.Digest, "sha256=deadbeef")
	}
}

func TestObjNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.LookupObj(ctx, "OBJ_files", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent object")
	}
}

func TestRecordAndLookupObjChunks(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	cs := ObjChunkSet{
		NUID:        "abc123",
		ChunkSeqs:   []uint64{101, 102, 103, 104},
		ChunkBlocks: []uint64{5, 5, 5, 6},
		TotalSize:   512 * 1024,
	}

	if err := store.RecordObjChunks(ctx, "OBJ_files", cs); err != nil {
		t.Fatalf("RecordObjChunks: %v", err)
	}

	got, err := store.LookupObjChunks(ctx, "OBJ_files", "abc123")
	if err != nil {
		t.Fatalf("LookupObjChunks: %v", err)
	}
	if len(got.ChunkSeqs) != 4 {
		t.Errorf("ChunkSeqs len = %d, want 4", len(got.ChunkSeqs))
	}
	if got.TotalSize != 512*1024 {
		t.Errorf("TotalSize = %d, want %d", got.TotalSize, 512*1024)
	}
}

func TestObjChunksMerge(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// First batch of chunks (from block 1)
	cs1 := ObjChunkSet{
		NUID:        "abc123",
		ChunkSeqs:   []uint64{101, 102},
		ChunkBlocks: []uint64{1, 1},
		TotalSize:   256 * 1024,
	}
	if err := store.RecordObjChunks(ctx, "OBJ_files", cs1); err != nil {
		t.Fatal(err)
	}

	// Second batch (from block 2) — should merge with existing
	cs2 := ObjChunkSet{
		NUID:        "abc123",
		ChunkSeqs:   []uint64{103, 104},
		ChunkBlocks: []uint64{2, 2},
		TotalSize:   256 * 1024,
	}
	if err := store.RecordObjChunks(ctx, "OBJ_files", cs2); err != nil {
		t.Fatal(err)
	}

	got, err := store.LookupObjChunks(ctx, "OBJ_files", "abc123")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ChunkSeqs) != 4 {
		t.Errorf("merged ChunkSeqs len = %d, want 4", len(got.ChunkSeqs))
	}
	if got.TotalSize != 512*1024 {
		t.Errorf("merged TotalSize = %d, want %d", got.TotalSize, 512*1024)
	}
	// Verify ordering: [101, 102, 103, 104]
	expected := []uint64{101, 102, 103, 104}
	for i, s := range got.ChunkSeqs {
		if s != expected[i] {
			t.Errorf("ChunkSeqs[%d] = %d, want %d", i, s, expected[i])
		}
	}
}

func TestObjChunksNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.LookupObjChunks(ctx, "OBJ_files", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent chunks")
	}
}

func TestListObjects(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	names := []string{"report.pdf", "image.png", "data.csv"}
	for i, name := range names {
		store.RecordObjMeta(ctx, "OBJ_files", ObjEntry{
			Bucket:      "files",
			Name:        name,
			NUID:        "nuid-" + name,
			Size:        uint64((i + 1) * 1024),
			Chunks:      i + 1,
			MetaSeq:     uint64(i + 1),
			MetaBlockID: 1,
			ModTime:     time.Now(),
			UpdatedAt:   time.Now(),
		})
	}

	objs, err := store.ListObjects(ctx, "OBJ_files")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 3 {
		t.Errorf("ListObjects: got %d, want 3", len(objs))
	}
}

func TestListObjectsEmptyStream(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	objs, err := store.ListObjects(ctx, "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if objs != nil {
		t.Errorf("expected nil for nonexistent stream, got %v", objs)
	}
}

func TestObjOverwrite(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// First version
	store.RecordObjMeta(ctx, "OBJ_files", ObjEntry{
		Bucket:      "files",
		Name:        "report.pdf",
		NUID:        "nuid-v1",
		Size:        1024,
		Chunks:      1,
		MetaSeq:     10,
		MetaBlockID: 1,
		ModTime:     time.Now(),
		UpdatedAt:   time.Now(),
	})

	// Updated version (same name, new NUID)
	store.RecordObjMeta(ctx, "OBJ_files", ObjEntry{
		Bucket:      "files",
		Name:        "report.pdf",
		NUID:        "nuid-v2",
		Size:        2048,
		Chunks:      2,
		MetaSeq:     20,
		MetaBlockID: 2,
		ModTime:     time.Now(),
		UpdatedAt:   time.Now(),
	})

	got, err := store.LookupObj(ctx, "OBJ_files", "report.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if got.NUID != "nuid-v2" {
		t.Errorf("NUID = %q, want nuid-v2 (overwritten)", got.NUID)
	}
	if got.Size != 2048 {
		t.Errorf("Size = %d, want 2048", got.Size)
	}
}
