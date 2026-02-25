package ingest

import (
	"strings"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
)

// ParseKVSubject extracts the bucket and key from a KV subject.
// KV subjects follow the pattern: $KV.{bucket}.{key}
// Returns bucket, key, ok.
func ParseKVSubject(subject string) (bucket, key string, ok bool) {
	if !strings.HasPrefix(subject, "$KV.") {
		return "", "", false
	}
	rest := subject[4:] // after "$KV."
	dot := strings.IndexByte(rest, '.')
	if dot < 0 || dot == len(rest)-1 {
		return "", "", false
	}
	return rest[:dot], rest[dot+1:], true
}

// ExtractKVOperation reads the KV-Operation header from raw header bytes.
// Returns "PUT" if no KV-Operation header is found.
func ExtractKVOperation(headers []byte) string {
	if len(headers) == 0 {
		return "PUT"
	}
	for _, line := range strings.Split(string(headers), "\r\n") {
		if strings.HasPrefix(line, "KV-Operation: ") {
			return strings.TrimPrefix(line, "KV-Operation: ")
		}
	}
	return "PUT"
}

// indexKVBlock scans messages in a sealed block and records KV key entries and revisions.
func indexKVBlock(store meta.Store, stream string, blk *block.Block, indexAllRevisions bool) error {
	for _, msg := range blk.Messages {
		bucket, key, ok := ParseKVSubject(msg.Subject)
		if !ok {
			continue
		}

		op := ExtractKVOperation(msg.Headers)

		// Always update the latest key entry
		entry := meta.KVKeyEntry{
			Bucket:       bucket,
			Key:          key,
			LastSequence: msg.Sequence,
			LastBlockID:  blk.ID,
			Operation:    op,
			Revision:     msg.Sequence,
			UpdatedAt:    time.Now(),
		}
		if err := store.RecordKVKey(nil, stream, entry); err != nil {
			return err
		}

		// Optionally record each revision
		if indexAllRevisions {
			rev := meta.KVRevEntry{
				Sequence: msg.Sequence,
				BlockID:  blk.ID,
			}
			if err := store.RecordKVRevision(nil, stream, key, rev); err != nil {
				return err
			}
		}
	}
	return nil
}
