package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Ingest metrics
	MessagesIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nts_messages_ingested_total",
		Help: "Total messages ingested from JetStream",
	}, []string{"stream"})

	BlocksSealed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nts_blocks_sealed_total",
		Help: "Total blocks sealed",
	}, []string{"stream"})

	BlockSealDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nts_block_seal_duration_seconds",
		Help:    "Time to seal a block",
		Buckets: prometheus.DefBuckets,
	}, []string{"stream"})

	// Tier metrics
	TierBlockCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nts_tier_block_count",
		Help: "Number of blocks in each tier",
	}, []string{"stream", "tier"})

	TierBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nts_tier_bytes",
		Help: "Total bytes stored in each tier",
	}, []string{"stream", "tier"})

	DemotionOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nts_demotion_ops_total",
		Help: "Number of block demotions",
	}, []string{"stream", "from_tier", "to_tier"})

	PromotionOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nts_promotion_ops_total",
		Help: "Number of block promotions",
	}, []string{"stream", "from_tier", "to_tier"})

	// S3 metrics
	S3UploadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nts_s3_upload_duration_seconds",
		Help:    "S3 upload latency",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30},
	}, []string{"stream"})

	S3UploadErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nts_s3_upload_errors_total",
		Help: "S3 upload failures",
	}, []string{"stream", "error_type"})

	S3DownloadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nts_s3_download_duration_seconds",
		Help:    "S3 download latency",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5},
	}, []string{"stream"})

	// Read path metrics
	ReadRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nts_read_requests_total",
		Help: "Read requests by tier",
	}, []string{"stream", "tier"})

	ReadLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nts_read_latency_seconds",
		Help:    "Read request latency",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	}, []string{"stream", "tier"})

	// Consumer lag
	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nts_consumer_lag_messages",
		Help: "Messages pending in JetStream not yet ingested",
	}, []string{"stream"})
)

// RunServer starts the Prometheus metrics HTTP server.
func RunServer(ctx context.Context, cfg config.MetricsConfig) error {
	mux := http.NewServeMux()
	path := cfg.Path
	if path == "" {
		path = "/metrics"
	}
	mux.Handle(path, promhttp.Handler())

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
