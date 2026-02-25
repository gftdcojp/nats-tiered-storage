package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/pkg/s3util"
	"github.com/nats-io/nats.go"
)

// HealthStatus represents the overall health state.
type HealthStatus struct {
	OK     bool    `json:"ok"`
	Checks []Check `json:"checks,omitempty"`
}

// Check represents an individual health check.
type Check struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// HealthChecker runs health probes.
type HealthChecker struct {
	natsConn *nats.Conn
	meta     meta.Store
	s3Client *s3util.Client
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(nc *nats.Conn, metaStore meta.Store, s3Client *s3util.Client) *HealthChecker {
	return &HealthChecker{
		natsConn: nc,
		meta:     metaStore,
		s3Client: s3Client,
	}
}

// Liveness checks if the process is alive.
func (h *HealthChecker) Liveness() HealthStatus {
	return HealthStatus{OK: true}
}

// Readiness checks if the service can handle requests.
func (h *HealthChecker) Readiness() HealthStatus {
	status := HealthStatus{OK: true}

	// Check NATS connection
	if h.natsConn != nil && !h.natsConn.IsConnected() {
		status.OK = false
		status.Checks = append(status.Checks, Check{
			Name: "nats", Status: "disconnected",
		})
	} else {
		status.Checks = append(status.Checks, Check{
			Name: "nats", Status: "connected",
		})
	}

	// Check metadata store
	if h.meta != nil {
		if err := h.meta.Ping(); err != nil {
			status.OK = false
			status.Checks = append(status.Checks, Check{
				Name: "metadata", Status: "error", Error: err.Error(),
			})
		} else {
			status.Checks = append(status.Checks, Check{
				Name: "metadata", Status: "ok",
			})
		}
	}

	// Check S3
	if h.s3Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.s3Client.Ping(ctx); err != nil {
			status.OK = false
			status.Checks = append(status.Checks, Check{
				Name: "s3", Status: "error", Error: err.Error(),
			})
		} else {
			status.Checks = append(status.Checks, Check{
				Name: "s3", Status: "ok",
			})
		}
	}

	return status
}

// RunHealthServer starts the health check HTTP server.
func RunHealthServer(ctx context.Context, cfg config.HealthConfig, checker *HealthChecker) error {
	mux := http.NewServeMux()

	livenessPath := cfg.LivenessPath
	if livenessPath == "" {
		livenessPath = "/healthz"
	}
	readinessPath := cfg.ReadinessPath
	if readinessPath == "" {
		readinessPath = "/readyz"
	}

	mux.HandleFunc(livenessPath, func(w http.ResponseWriter, r *http.Request) {
		status := checker.Liveness()
		code := http.StatusOK
		if !status.OK {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc(readinessPath, func(w http.ResponseWriter, r *http.Request) {
		status := checker.Readiness()
		code := http.StatusOK
		if !status.OK {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(status)
	})

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
