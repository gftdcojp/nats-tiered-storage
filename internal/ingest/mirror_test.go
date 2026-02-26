package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

func startEmbeddedNATS(t *testing.T) (*server.Server, string) {
	t.Helper()
	tmpDir := t.TempDir()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  filepath.Join(tmpDir, "jetstream"),
		NoLog:     true,
		NoSigs:    true,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create nats-server: %v", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server failed to start")
	}

	url := fmt.Sprintf("nats://127.0.0.1:%d", opts.Port)
	t.Cleanup(func() { ns.Shutdown() })
	return ns, url
}

func connectJS(t *testing.T, natsURL string) (jetstream.JetStream, func()) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatalf("jetstream: %v", err)
	}
	return js, func() { nc.Close() }
}

func TestMirrorStreamName(t *testing.T) {
	tests := []struct {
		source string
		want   string
	}{
		{"ORDERS", "NTS_MIRROR_ORDERS"},
		{"EVENTS", "NTS_MIRROR_EVENTS"},
		{"KV_config", "NTS_MIRROR_KV_config"},
	}
	for _, tt := range tests {
		got := MirrorStreamName(tt.source)
		if got != tt.want {
			t.Errorf("MirrorStreamName(%q) = %q, want %q", tt.source, got, tt.want)
		}
	}
}

func TestResolveConsumerStream_LimitsPolicy_NoMirror(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	js, cleanup := connectJS(t, natsURL)
	defer cleanup()

	ctx := context.Background()

	// Create a Limits retention stream (default).
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "ORDERS",
		Subjects:  []string{"orders.>"},
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	cfg := config.StreamConfig{
		Name:         "ORDERS",
		ConsumerName: "nts-archiver",
	}
	res, err := resolveConsumerStream(ctx, js, cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("resolveConsumerStream: %v", err)
	}
	if res.ConsumeStream != "ORDERS" {
		t.Errorf("ConsumeStream = %q, want ORDERS", res.ConsumeStream)
	}
	if res.IsMirror {
		t.Error("IsMirror should be false for Limits stream")
	}
}

func TestResolveConsumerStream_WorkQueue_NoExistingConsumers(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	js, cleanup := connectJS(t, natsURL)
	defer cleanup()

	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TASKS",
		Subjects:  []string{"tasks.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	cfg := config.StreamConfig{
		Name:         "TASKS",
		ConsumerName: "nts-archiver",
	}
	res, err := resolveConsumerStream(ctx, js, cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("resolveConsumerStream: %v", err)
	}
	if res.ConsumeStream != "TASKS" {
		t.Errorf("ConsumeStream = %q, want TASKS", res.ConsumeStream)
	}
	if res.IsMirror {
		t.Error("IsMirror should be false when no existing consumers")
	}
}

func TestResolveConsumerStream_WorkQueue_WithExistingConsumer(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	js, cleanup := connectJS(t, natsURL)
	defer cleanup()

	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TASKS",
		Subjects:  []string{"tasks.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Create an existing consumer (simulates another service).
	_, err = js.CreateConsumer(ctx, "TASKS", jetstream.ConsumerConfig{
		Durable:   "other-service",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("create existing consumer: %v", err)
	}

	cfg := config.StreamConfig{
		Name:         "TASKS",
		ConsumerName: "nts-archiver",
		Tiers: config.TiersConfig{
			File: config.FileTierConfig{
				Enabled: true,
				MaxAge:  config.Duration(48 * time.Hour),
			},
		},
	}
	res, err := resolveConsumerStream(ctx, js, cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("resolveConsumerStream: %v", err)
	}
	if res.ConsumeStream != "NTS_MIRROR_TASKS" {
		t.Errorf("ConsumeStream = %q, want NTS_MIRROR_TASKS", res.ConsumeStream)
	}
	if !res.IsMirror {
		t.Error("IsMirror should be true")
	}

	// Verify mirror stream was created with correct config.
	mirrorStream, err := js.Stream(ctx, "NTS_MIRROR_TASKS")
	if err != nil {
		t.Fatalf("get mirror stream: %v", err)
	}
	mirrorInfo, err := mirrorStream.Info(ctx)
	if err != nil {
		t.Fatalf("mirror stream info: %v", err)
	}
	if mirrorInfo.Config.Retention != jetstream.LimitsPolicy {
		t.Errorf("mirror retention = %v, want LimitsPolicy", mirrorInfo.Config.Retention)
	}
	if mirrorInfo.Config.Mirror == nil || mirrorInfo.Config.Mirror.Name != "TASKS" {
		t.Error("mirror source should be TASKS")
	}
	if mirrorInfo.Config.MaxAge != 48*time.Hour {
		t.Errorf("mirror MaxAge = %v, want 48h", mirrorInfo.Config.MaxAge)
	}
}

func TestResolveConsumerStream_WorkQueue_OurConsumerExists(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	js, cleanup := connectJS(t, natsURL)
	defer cleanup()

	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TASKS",
		Subjects:  []string{"tasks.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Create our own consumer first.
	_, err = js.CreateConsumer(ctx, "TASKS", jetstream.ConsumerConfig{
		Durable:   "nts-archiver",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("create our consumer: %v", err)
	}

	cfg := config.StreamConfig{
		Name:         "TASKS",
		ConsumerName: "nts-archiver",
	}
	res, err := resolveConsumerStream(ctx, js, cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("resolveConsumerStream: %v", err)
	}
	if res.ConsumeStream != "TASKS" {
		t.Errorf("ConsumeStream = %q, want TASKS", res.ConsumeStream)
	}
	if res.IsMirror {
		t.Error("IsMirror should be false when our consumer already exists")
	}
}

func TestResolveConsumerStream_AutoMirrorDisabled(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	js, cleanup := connectJS(t, natsURL)
	defer cleanup()

	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TASKS",
		Subjects:  []string{"tasks.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Create an existing consumer.
	_, err = js.CreateConsumer(ctx, "TASKS", jetstream.ConsumerConfig{
		Durable:   "other-service",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("create existing consumer: %v", err)
	}

	disabled := false
	cfg := config.StreamConfig{
		Name:         "TASKS",
		ConsumerName: "nts-archiver",
		AutoMirror:   &disabled,
	}
	res, err := resolveConsumerStream(ctx, js, cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("resolveConsumerStream: %v", err)
	}
	if res.ConsumeStream != "TASKS" {
		t.Errorf("ConsumeStream = %q, want TASKS (auto_mirror disabled)", res.ConsumeStream)
	}
	if res.IsMirror {
		t.Error("IsMirror should be false when auto_mirror is disabled")
	}
}

func TestResolveConsumerStream_MirrorAlreadyExists(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	js, cleanup := connectJS(t, natsURL)
	defer cleanup()

	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TASKS",
		Subjects:  []string{"tasks.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Pre-create the mirror stream.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "NTS_MIRROR_TASKS",
		Retention: jetstream.LimitsPolicy,
		MaxAge:    72 * time.Hour,
		Mirror:    &jetstream.StreamSource{Name: "TASKS"},
	})
	if err != nil {
		t.Fatalf("pre-create mirror: %v", err)
	}

	// Create an existing consumer on the original.
	_, err = js.CreateConsumer(ctx, "TASKS", jetstream.ConsumerConfig{
		Durable:   "other-service",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("create existing consumer: %v", err)
	}

	cfg := config.StreamConfig{
		Name:         "TASKS",
		ConsumerName: "nts-archiver",
	}
	res, err := resolveConsumerStream(ctx, js, cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("resolveConsumerStream: %v", err)
	}
	if res.ConsumeStream != "NTS_MIRROR_TASKS" {
		t.Errorf("ConsumeStream = %q, want NTS_MIRROR_TASKS", res.ConsumeStream)
	}
	if !res.IsMirror {
		t.Error("IsMirror should be true for reused mirror")
	}
}
