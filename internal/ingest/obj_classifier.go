package ingest

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
)

// ParseObjMetaSubject parses an Object Store metadata subject.
// Pattern: $O.{bucket}.M.{name}
// Returns bucket, name, ok.
func ParseObjMetaSubject(subject string) (bucket, name string, ok bool) {
	if !strings.HasPrefix(subject, "$O.") {
		return "", "", false
	}
	rest := subject[3:] // after "$O."
	parts := strings.SplitN(rest, ".", 3)
	if len(parts) < 3 || parts[1] != "M" {
		return "", "", false
	}
	return parts[0], parts[2], true
}

// ParseObjChunkSubject parses an Object Store chunk subject.
// Pattern: $O.{bucket}.C.{nuid}
// Returns bucket, nuid, ok.
func ParseObjChunkSubject(subject string) (bucket, nuid string, ok bool) {
	if !strings.HasPrefix(subject, "$O.") {
		return "", "", false
	}
	rest := subject[3:] // after "$O."
	parts := strings.SplitN(rest, ".", 3)
	if len(parts) < 3 || parts[1] != "C" {
		return "", "", false
	}
	return parts[0], parts[2], true
}

// objectInfo is the minimal structure from NATS Object Store metadata messages.
type objectInfo struct {
	Name    string `json:"name"`
	Bucket  string `json:"bucket"`
	NUID    string `json:"nuid"`
	Size    uint64 `json:"size"`
	Chunks  int    `json:"chunks"`
	Digest  string `json:"digest"`
	Deleted bool   `json:"deleted"`
}

// indexObjBlock scans messages in a sealed block and records Object Store metadata and chunk entries.
func indexObjBlock(store meta.Store, stream string, blk *block.Block) error {
	// Collect chunk sequences per NUID from this block
	chunksByNUID := make(map[string]*meta.ObjChunkSet)

	for _, msg := range blk.Messages {
		// Check for metadata messages
		if bucket, name, ok := ParseObjMetaSubject(msg.Subject); ok {
			var info objectInfo
			if err := json.Unmarshal(msg.Data, &info); err != nil {
				// Skip invalid metadata
				continue
			}
			entry := meta.ObjEntry{
				Bucket:      bucket,
				Name:        name,
				NUID:        info.NUID,
				Size:        info.Size,
				Chunks:      info.Chunks,
				Digest:      info.Digest,
				MetaSeq:     msg.Sequence,
				MetaBlockID: blk.ID,
				Deleted:     info.Deleted,
				ModTime:     msg.Timestamp,
				UpdatedAt:   time.Now(),
			}
			if err := store.RecordObjMeta(nil, stream, entry); err != nil {
				return err
			}
			continue
		}

		// Check for chunk messages
		if _, nuid, ok := ParseObjChunkSubject(msg.Subject); ok {
			cs, exists := chunksByNUID[nuid]
			if !exists {
				cs = &meta.ObjChunkSet{NUID: nuid}
				chunksByNUID[nuid] = cs
			}
			cs.ChunkSeqs = append(cs.ChunkSeqs, msg.Sequence)
			cs.ChunkBlocks = append(cs.ChunkBlocks, blk.ID)
			cs.TotalSize += uint64(len(msg.Data))
		}
	}

	// Persist chunk sets
	for _, cs := range chunksByNUID {
		if err := store.RecordObjChunks(nil, stream, *cs); err != nil {
			return err
		}
	}

	return nil
}
