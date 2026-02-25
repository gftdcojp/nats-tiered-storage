package serve

import (
	"bytes"
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

// RunNATSObjResponder subscribes to Object Store-specific NATS request-reply subjects.
// Subjects:
//   - nts.obj.{bucket}.get.{name}   — get object (reassembled chunks)
//   - nts.obj.{bucket}.info.{name}  — get object metadata
//   - nts.obj.{bucket}.list         — list objects
func RunNATSObjResponder(ctx context.Context, nc *nats.Conn, prefix string, streamCfgs []config.StreamConfig, pipelines []*ingest.Pipeline, metaStore meta.Store, logger *zap.Logger) error {
	if prefix == "" {
		prefix = "nts"
	}

	type objStream struct {
		streamName string
		pipeline   *ingest.Pipeline
	}
	bucketMap := make(map[string]objStream)
	pipeMap := make(map[string]*ingest.Pipeline)
	for _, p := range pipelines {
		pipeMap[p.Stream()] = p
	}
	for _, sc := range streamCfgs {
		if sc.ResolvedType() == config.StreamTypeObjectStore {
			bucket := sc.ResolvedObjBucket()
			bucketMap[bucket] = objStream{streamName: sc.Name, pipeline: pipeMap[sc.Name]}
		}
	}

	if len(bucketMap) == 0 {
		return nil
	}

	subject := prefix + ".obj.>"
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		parts := strings.Split(msg.Subject, ".")
		if len(parts) < 4 {
			msg.Respond(errorJSON("invalid subject format"))
			return
		}
		bucket := parts[2]
		op := parts[3]

		os, ok := bucketMap[bucket]
		if !ok {
			msg.Respond(errorJSON(fmt.Sprintf("Object Store bucket %q not found", bucket)))
			return
		}

		switch op {
		case "get":
			if len(parts) < 5 {
				msg.Respond(errorJSON("missing object name"))
				return
			}
			name := strings.Join(parts[4:], ".")
			handleObjGet(ctx, msg, metaStore, os.streamName, os.pipeline, name)

		case "info":
			if len(parts) < 5 {
				msg.Respond(errorJSON("missing object name"))
				return
			}
			name := strings.Join(parts[4:], ".")
			handleObjInfo(ctx, msg, metaStore, os.streamName, name)

		case "list":
			handleObjList(ctx, msg, metaStore, os.streamName)

		default:
			msg.Respond(errorJSON(fmt.Sprintf("unknown Object Store operation %q", op)))
		}
	})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", subject, err)
	}

	logger.Info("NATS Object Store responder started", zap.String("subject", subject), zap.Int("buckets", len(bucketMap)))

	<-ctx.Done()
	sub.Unsubscribe()
	return nil
}

func handleObjGet(ctx context.Context, msg *nats.Msg, store meta.Store, stream string, p *ingest.Pipeline, name string) {
	entry, err := store.LookupObj(ctx, stream, name)
	if err != nil {
		msg.Respond(errorJSON(err.Error()))
		return
	}

	if entry.Deleted {
		msg.Respond(errorJSON(fmt.Sprintf("object %q has been deleted", name)))
		return
	}

	chunks, err := store.LookupObjChunks(ctx, stream, entry.NUID)
	if err != nil {
		msg.Respond(errorJSON(fmt.Sprintf("looking up chunks: %s", err.Error())))
		return
	}

	// Reassemble chunks
	var buf bytes.Buffer
	for _, seq := range chunks.ChunkSeqs {
		stored, err := p.Controller().Retrieve(ctx, seq)
		if err != nil {
			msg.Respond(errorJSON(fmt.Sprintf("retrieving chunk seq=%d: %s", seq, err.Error())))
			return
		}
		buf.Write(stored.Data)
	}

	// For NATS response, we can only send objects up to the NATS max message size.
	// If the object is too large, return an error suggesting to use the HTTP API.
	if buf.Len() > 1024*1024 {
		msg.Respond(errorJSON("object too large for NATS response; use HTTP API /v1/objects/{bucket}/get/{name}"))
		return
	}

	msg.Respond(buf.Bytes())
}

func handleObjInfo(ctx context.Context, msg *nats.Msg, store meta.Store, stream, name string) {
	entry, err := store.LookupObj(ctx, stream, name)
	if err != nil {
		msg.Respond(errorJSON(err.Error()))
		return
	}

	resp, _ := json.Marshal(map[string]interface{}{
		"name":    entry.Name,
		"bucket":  entry.Bucket,
		"nuid":    entry.NUID,
		"size":    entry.Size,
		"chunks":  entry.Chunks,
		"digest":  entry.Digest,
		"deleted": entry.Deleted,
		"modtime": entry.ModTime,
	})
	msg.Respond(resp)
}

func handleObjList(ctx context.Context, msg *nats.Msg, store meta.Store, stream string) {
	entries, err := store.ListObjects(ctx, stream)
	if err != nil {
		msg.Respond(errorJSON(err.Error()))
		return
	}

	var objects []map[string]interface{}
	for _, e := range entries {
		objects = append(objects, map[string]interface{}{
			"name":    e.Name,
			"size":    e.Size,
			"chunks":  e.Chunks,
			"digest":  e.Digest,
			"deleted": e.Deleted,
			"modtime": e.ModTime,
		})
	}

	resp, _ := json.Marshal(objects)
	msg.Respond(resp)
}
