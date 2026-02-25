package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/ingest"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// RunNATSResponder subscribes to NATS request-reply subjects for cold data retrieval.
// Subject pattern: {prefix}.get.{stream}.{sequence}
func RunNATSResponder(ctx context.Context, nc *nats.Conn, cfg config.NATSResponderConfig, pipelines []*ingest.Pipeline, metaStore meta.Store, logger *zap.Logger) error {
	pipeMap := make(map[string]*ingest.Pipeline)
	for _, p := range pipelines {
		pipeMap[p.Stream()] = p
	}

	prefix := cfg.SubjectPrefix
	if prefix == "" {
		prefix = "nts"
	}

	// Subscribe to: nts.get.>
	subject := prefix + ".get.>"
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		parts := strings.Split(msg.Subject, ".")
		// Expected: {prefix}.get.{stream}.{seq}
		if len(parts) < 4 {
			msg.Respond([]byte(`{"error":"invalid subject format"}`))
			return
		}

		stream := parts[2]
		seqStr := parts[3]
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			msg.Respond([]byte(fmt.Sprintf(`{"error":"invalid sequence: %s"}`, seqStr)))
			return
		}

		p, ok := pipeMap[stream]
		if !ok {
			msg.Respond([]byte(fmt.Sprintf(`{"error":"stream %s not found"}`, stream)))
			return
		}

		stored, err := p.Controller().Retrieve(ctx, seq)
		if err != nil {
			msg.Respond([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
			return
		}

		resp, _ := json.Marshal(map[string]interface{}{
			"stream":    stored.Stream,
			"subject":   stored.Subject,
			"sequence":  stored.Sequence,
			"data":      string(stored.Data),
			"timestamp": stored.Timestamp,
		})
		msg.Respond(resp)
	})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", subject, err)
	}

	logger.Info("NATS responder started", zap.String("subject", subject))

	<-ctx.Done()
	sub.Unsubscribe()
	return nil
}
