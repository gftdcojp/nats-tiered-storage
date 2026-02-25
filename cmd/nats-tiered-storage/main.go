package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gftdcojp/nats-tiered-storage/internal/blob"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/file"
	"github.com/gftdcojp/nats-tiered-storage/internal/ingest"
	"github.com/gftdcojp/nats-tiered-storage/internal/lifecycle"
	"github.com/gftdcojp/nats-tiered-storage/internal/memory"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/metrics"
	"github.com/gftdcojp/nats-tiered-storage/internal/serve"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"github.com/gftdcojp/nats-tiered-storage/pkg/natsutil"
	"github.com/gftdcojp/nats-tiered-storage/pkg/s3util"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	showVersion := flag.Bool("version", false, "show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("nats-tiered-storage %s\n", version)
		os.Exit(0)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := newLogger(cfg.Observability.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	if err := run(cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal("fatal error", zap.Error(err))
	}
}

func run(cfg *config.Config, logger *zap.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Connect to NATS
	nc, err := natsutil.Connect(cfg.NATS, logger.Named("nats"))
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream context: %w", err)
	}

	// Initialize metadata store
	metaStore, err := meta.NewBoltStore(cfg.Metadata.Path, logger.Named("meta"))
	if err != nil {
		return fmt.Errorf("opening metadata store: %w", err)
	}
	defer metaStore.Close()

	// Initialize S3 client (shared across streams)
	var s3Client *s3util.Client
	for _, sc := range cfg.Streams {
		if sc.Tiers.Blob.Enabled {
			s3Client, err = s3util.NewClient(ctx, sc.Tiers.Blob)
			if err != nil {
				return fmt.Errorf("creating S3 client: %w", err)
			}
			break
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	var pipelines []*ingest.Pipeline

	for _, sc := range cfg.Streams {
		sc := sc

		// Build tier stores for this stream
		memStore := memory.NewStore(sc.Tiers.Memory, logger.Named("memory").With(zap.String("stream", sc.Name)))
		fileStore, err := file.NewStore(sc.Tiers.File, logger.Named("file").With(zap.String("stream", sc.Name)))
		if err != nil {
			return fmt.Errorf("creating file store for stream %s: %w", sc.Name, err)
		}

		var blobStore tier.TierStore
		if sc.Tiers.Blob.Enabled && s3Client != nil {
			blobStore = blob.NewStore(s3Client, sc.Tiers.Blob, logger.Named("blob").With(zap.String("stream", sc.Name)))
		}

		// Build tier controller
		ctrl := tier.NewController(tier.ControllerConfig{
			Stream:  sc.Name,
			Memory:  memStore,
			File:    fileStore,
			Blob:    blobStore,
			Meta:    metaStore,
			Policy:  sc.Tiers,
			Logger:  logger.Named("tier").With(zap.String("stream", sc.Name)),
		})

		// Start demotion loop
		g.Go(func() error { return ctrl.RunDemotionLoop(gctx, cfg.Policy.EvalInterval.Duration()) })

		// Start retention loop
		gcMgr := lifecycle.NewManager(ctrl, metaStore, sc, logger.Named("lifecycle").With(zap.String("stream", sc.Name)))
		g.Go(func() error { return gcMgr.Run(gctx, cfg.Policy.EvalInterval.Duration()) })

		// Build ingest pipeline
		p := ingest.NewPipeline(ingest.PipelineConfig{
			JS:       js,
			Ctrl:     ctrl,
			Meta:     metaStore,
			Stream:   sc,
			Block:    cfg.Block,
			Logger:   logger.Named("ingest").With(zap.String("stream", sc.Name)),
		})
		pipelines = append(pipelines, p)
		g.Go(func() error { return p.Run(gctx) })
	}

	// Start HTTP API
	if cfg.API.Enabled {
		g.Go(func() error {
			return serve.RunHTTP(gctx, cfg.API, pipelines, metaStore, logger.Named("api"))
		})
	}

	// Start NATS responder
	if cfg.API.NATSResponder.Enabled {
		g.Go(func() error {
			return serve.RunNATSResponder(gctx, nc, cfg.API.NATSResponder, pipelines, metaStore, logger.Named("nats-responder"))
		})
	}

	// Start metrics server
	if cfg.Observability.Metrics.Enabled {
		g.Go(func() error { return metrics.RunServer(gctx, cfg.Observability.Metrics) })
	}

	// Start health server
	if cfg.Observability.Health.Enabled {
		healthChecker := metrics.NewHealthChecker(nc, metaStore, s3Client)
		g.Go(func() error {
			return metrics.RunHealthServer(gctx, cfg.Observability.Health, healthChecker)
		})
	}

	logger.Info("nats-tiered-storage started",
		zap.String("version", version),
		zap.Int("streams", len(cfg.Streams)),
		zap.String("nats_url", cfg.NATS.URL),
	)

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	// Graceful shutdown: flush unsealed blocks
	logger.Info("shutting down, flushing unsealed blocks...")
	for _, p := range pipelines {
		if err := p.FlushAndClose(context.Background()); err != nil {
			logger.Error("error flushing pipeline", zap.Error(err))
		}
	}

	return nil
}

func newLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
	var zapCfg zap.Config
	if cfg.Format == "console" {
		zapCfg = zap.NewDevelopmentConfig()
	} else {
		zapCfg = zap.NewProductionConfig()
	}

	switch cfg.Level {
	case "debug":
		zapCfg.Level.SetLevel(zap.DebugLevel)
	case "info":
		zapCfg.Level.SetLevel(zap.InfoLevel)
	case "warn":
		zapCfg.Level.SetLevel(zap.WarnLevel)
	case "error":
		zapCfg.Level.SetLevel(zap.ErrorLevel)
	}

	return zapCfg.Build()
}
