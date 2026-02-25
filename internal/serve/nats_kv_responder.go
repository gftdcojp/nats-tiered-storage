package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/ingest"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// RunNATSKVResponder subscribes to KV-specific NATS request-reply subjects.
// Subjects:
//   - nts.kv.{bucket}.get.{key}       — get latest value
//   - nts.kv.{bucket}.history.{key}   — get all revisions
//   - nts.kv.{bucket}.keys            — list keys
func RunNATSKVResponder(ctx context.Context, nc *nats.Conn, prefix string, streamCfgs []config.StreamConfig, pipelines []*ingest.Pipeline, metaStore meta.Store, logger *zap.Logger) error {
	if prefix == "" {
		prefix = "nts"
	}

	// Build mapping: KV bucket name → stream name + pipeline
	type kvStream struct {
		streamName string
		pipeline   *ingest.Pipeline
	}
	bucketMap := make(map[string]kvStream)
	pipeMap := make(map[string]*ingest.Pipeline)
	for _, p := range pipelines {
		pipeMap[p.Stream()] = p
	}
	for _, sc := range streamCfgs {
		if sc.ResolvedType() == config.StreamTypeKV {
			bucket := sc.ResolvedKVBucket()
			bucketMap[bucket] = kvStream{streamName: sc.Name, pipeline: pipeMap[sc.Name]}
		}
	}

	if len(bucketMap) == 0 {
		return nil
	}

	subject := prefix + ".kv.>"
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		// Parse: nts.kv.{bucket}.{op}[.{key}]
		parts := strings.Split(msg.Subject, ".")
		if len(parts) < 4 {
			msg.Respond(errorJSON("invalid subject format"))
			return
		}
		bucket := parts[2]
		op := parts[3]

		ks, ok := bucketMap[bucket]
		if !ok {
			msg.Respond(errorJSON(fmt.Sprintf("KV bucket %q not found", bucket)))
			return
		}

		switch op {
		case "get":
			if len(parts) < 5 {
				msg.Respond(errorJSON("missing key"))
				return
			}
			key := strings.Join(parts[4:], ".")
			handleKVGet(ctx, msg, metaStore, ks.streamName, ks.pipeline, key)

		case "history":
			if len(parts) < 5 {
				msg.Respond(errorJSON("missing key"))
				return
			}
			key := strings.Join(parts[4:], ".")
			handleKVHistory(ctx, msg, metaStore, ks.streamName, ks.pipeline, key)

		case "keys":
			kvPrefix := ""
			if len(parts) >= 5 {
				kvPrefix = strings.Join(parts[4:], ".")
			}
			handleKVKeys(ctx, msg, metaStore, ks.streamName, kvPrefix)

		default:
			msg.Respond(errorJSON(fmt.Sprintf("unknown KV operation %q", op)))
		}
	})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", subject, err)
	}

	logger.Info("NATS KV responder started", zap.String("subject", subject), zap.Int("buckets", len(bucketMap)))

	<-ctx.Done()
	sub.Unsubscribe()
	return nil
}

func handleKVGet(ctx context.Context, msg *nats.Msg, store meta.Store, stream string, p *ingest.Pipeline, key string) {
	entry, err := store.LookupKVKey(ctx, stream, key)
	if err != nil {
		msg.Respond(errorJSON(err.Error()))
		return
	}

	// Retrieve the actual message from the tier controller
	stored, err := p.Controller().Retrieve(ctx, entry.LastSequence)
	if err != nil {
		msg.Respond(errorJSON(fmt.Sprintf("retrieving value: %s", err.Error())))
		return
	}

	resp, _ := json.Marshal(map[string]interface{}{
		"bucket":    entry.Bucket,
		"key":       entry.Key,
		"value":     string(stored.Data),
		"sequence":  entry.LastSequence,
		"revision":  entry.Revision,
		"operation": entry.Operation,
		"timestamp": stored.Timestamp,
	})
	msg.Respond(resp)
}

func handleKVHistory(ctx context.Context, msg *nats.Msg, store meta.Store, stream string, p *ingest.Pipeline, key string) {
	revs, err := store.ListKVKeyRevisions(ctx, stream, key)
	if err != nil {
		msg.Respond(errorJSON(err.Error()))
		return
	}

	var entries []map[string]interface{}
	for _, rev := range revs {
		stored, err := p.Controller().Retrieve(ctx, rev.Sequence)
		if err != nil {
			continue
		}
		entries = append(entries, map[string]interface{}{
			"sequence":  rev.Sequence,
			"block_id":  rev.BlockID,
			"value":     string(stored.Data),
			"timestamp": stored.Timestamp,
		})
	}

	resp, _ := json.Marshal(entries)
	msg.Respond(resp)
}

func handleKVKeys(ctx context.Context, msg *nats.Msg, store meta.Store, stream, prefix string) {
	entries, err := store.ListKVKeys(ctx, stream, prefix)
	if err != nil {
		msg.Respond(errorJSON(err.Error()))
		return
	}

	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Operation != "DEL" && e.Operation != "PURGE" {
			keys = append(keys, e.Key)
		}
	}

	resp, _ := json.Marshal(keys)
	msg.Respond(resp)
}

func errorJSON(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return b
}
