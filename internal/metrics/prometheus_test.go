package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsServer_MetricsEndpoint(t *testing.T) {
	// Touch some metrics so they appear in the output.
	// Vec metrics only show up after WithLabelValues() is called.
	MessagesIngested.WithLabelValues("TEST").Add(0)
	BlocksSealed.WithLabelValues("TEST").Add(0)
	TierBlockCount.WithLabelValues("TEST", "memory").Set(0)
	TierBytes.WithLabelValues("TEST", "memory").Set(0)
	DemotionOps.WithLabelValues("TEST", "memory", "file").Add(0)
	PromotionOps.WithLabelValues("TEST", "file", "memory").Add(0)
	S3UploadDuration.WithLabelValues("TEST").Observe(0)
	S3UploadErrors.WithLabelValues("TEST", "timeout").Add(0)
	S3DownloadDuration.WithLabelValues("TEST").Observe(0)
	ReadRequests.WithLabelValues("TEST", "memory").Add(0)
	ReadLatency.WithLabelValues("TEST", "memory").Observe(0)
	ConsumerLag.WithLabelValues("TEST").Set(0)
	KVIndexOps.WithLabelValues("TEST", "record_key").Add(0)
	KVGetRequests.WithLabelValues("config", "ok").Add(0)
	ObjIndexOps.WithLabelValues("TEST", "record_meta").Add(0)
	ObjGetRequests.WithLabelValues("files", "ok").Add(0)
	ObjReassemblyDuration.WithLabelValues("files").Observe(0)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	body := w.Body.String()

	// Verify that our custom nts_ metrics are registered and visible
	expectedMetrics := []string{
		"nts_messages_ingested_total",
		"nts_blocks_sealed_total",
		"nts_tier_block_count",
		"nts_tier_bytes",
		"nts_demotion_ops_total",
		"nts_promotion_ops_total",
		"nts_s3_upload_duration_seconds",
		"nts_s3_upload_errors_total",
		"nts_s3_download_duration_seconds",
		"nts_read_requests_total",
		"nts_read_latency_seconds",
		"nts_consumer_lag_messages",
		"nts_kv_index_ops_total",
		"nts_kv_get_requests_total",
		"nts_obj_index_ops_total",
		"nts_obj_get_requests_total",
		"nts_obj_reassembly_duration_seconds",
	}

	for _, name := range expectedMetrics {
		if !strings.Contains(body, name) {
			t.Errorf("expected /metrics to contain %q", name)
		}
	}

	// Verify content type includes text/plain (Prometheus exposition format)
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") && !strings.Contains(ct, "text/openmetrics") {
		t.Errorf("expected text/plain or openmetrics content type, got %s", ct)
	}
}
