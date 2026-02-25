package ingest

// Consumer management utilities for JetStream pull consumers.
// The primary consumer logic is in pipeline.go.
// This file provides helpers for consumer introspection.

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerInfo returns information about the ingest consumer.
func ConsumerInfo(ctx context.Context, js jetstream.JetStream, stream, consumer string) (*jetstream.ConsumerInfo, error) {
	cons, err := js.Consumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("getting consumer %s on stream %s: %w", consumer, stream, err)
	}
	return cons.Info(ctx)
}
