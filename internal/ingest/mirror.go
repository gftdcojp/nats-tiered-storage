package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/metrics"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	// mirrorPrefix is the naming prefix for auto-created mirror streams.
	mirrorPrefix = "NTS_MIRROR_"

	// defaultMirrorMaxAge is the fallback MaxAge for mirror streams when
	// no tier has a configured max_age.
	defaultMirrorMaxAge = 72 * time.Hour
)

// MirrorStreamName returns the mirror stream name for the given source stream.
func MirrorStreamName(source string) string {
	return mirrorPrefix + source
}

// resolveResult holds the outcome of mirror resolution.
type resolveResult struct {
	// ConsumeStream is the stream name to create the consumer on.
	ConsumeStream string
	// IsMirror is true when a mirror stream was created or reused.
	IsMirror bool
}

// resolveConsumerStream checks whether the target stream is a WorkQueue
// with an existing consumer that would conflict with ours. If so, it
// creates (or reuses) a Limits-retention mirror and returns its name.
func resolveConsumerStream(
	ctx context.Context,
	js jetstream.JetStream,
	streamCfg config.StreamConfig,
	logger *zap.Logger,
) (resolveResult, error) {
	original := streamCfg.Name

	// If auto_mirror is disabled, always use the original stream.
	if !streamCfg.AutoMirrorEnabled() {
		return resolveResult{ConsumeStream: original}, nil
	}

	// Fetch stream info.
	stream, err := js.Stream(ctx, original)
	if err != nil {
		return resolveResult{}, fmt.Errorf("fetching stream %s: %w", original, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return resolveResult{}, fmt.Errorf("getting info for stream %s: %w", original, err)
	}

	// Only WorkQueue retention needs mirror handling.
	if info.Config.Retention != jetstream.WorkQueuePolicy {
		return resolveResult{ConsumeStream: original}, nil
	}

	// Check if our consumer already exists on the original stream.
	_, err = js.Consumer(ctx, original, streamCfg.ConsumerName)
	if err == nil {
		// Our consumer exists — we can CreateOrUpdate without conflict.
		logger.Debug("our consumer already exists on WorkQueue stream, no mirror needed",
			zap.String("stream", original),
			zap.String("consumer", streamCfg.ConsumerName),
		)
		return resolveResult{ConsumeStream: original}, nil
	}

	// If no existing consumers at all, we can be the first — no conflict.
	if info.State.Consumers == 0 {
		return resolveResult{ConsumeStream: original}, nil
	}

	// Conflict: WorkQueue stream has existing consumer(s) and ours isn't one of them.
	mirrorName := MirrorStreamName(original)
	logger.Info("WorkQueue stream has existing consumers, creating mirror",
		zap.String("source_stream", original),
		zap.String("mirror_stream", mirrorName),
		zap.Int("existing_consumers", info.State.Consumers),
	)

	maxAge := streamCfg.Tiers.MaxTierRetention(defaultMirrorMaxAge)

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      mirrorName,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    maxAge,
		Mirror: &jetstream.StreamSource{
			Name: original,
		},
	})
	if err != nil {
		return resolveResult{}, fmt.Errorf("creating mirror stream %s: %w", mirrorName, err)
	}

	metrics.MirrorStreamsCreated.WithLabelValues(original).Inc()
	logger.Info("mirror stream ready",
		zap.String("mirror", mirrorName),
		zap.Duration("max_age", maxAge),
	)

	return resolveResult{ConsumeStream: mirrorName, IsMirror: true}, nil
}
