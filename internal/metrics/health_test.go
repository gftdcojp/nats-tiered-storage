package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func startEmbeddedNATS(t *testing.T) (*server.Server, string) {
	t.Helper()
	tmpDir := t.TempDir()

	opts := &server.Options{
		Host:     "127.0.0.1",
		Port:     -1,
		NoLog:    true,
		NoSigs:   true,
		StoreDir: filepath.Join(tmpDir, "jetstream"),
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create nats-server: %v", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server failed to start")
	}

	t.Cleanup(func() { ns.Shutdown() })
	return ns, ns.ClientURL()
}

func newTestMeta(t *testing.T) meta.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := meta.NewBoltStore(path, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestHealthChecker_Liveness(t *testing.T) {
	checker := NewHealthChecker(nil, nil, nil)
	status := checker.Liveness()
	if !status.OK {
		t.Fatal("liveness should always return OK=true")
	}
}

func TestHealthChecker_Readiness_AllOK(t *testing.T) {
	_, url := startEmbeddedNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	metaStore := newTestMeta(t)
	checker := NewHealthChecker(nc, metaStore, nil)

	status := checker.Readiness()
	if !status.OK {
		t.Fatalf("expected readiness OK=true, got checks: %+v", status.Checks)
	}

	// Verify individual checks
	found := map[string]bool{}
	for _, c := range status.Checks {
		found[c.Name] = true
		if c.Name == "nats" && c.Status != "connected" {
			t.Fatalf("expected nats connected, got %s", c.Status)
		}
		if c.Name == "metadata" && c.Status != "ok" {
			t.Fatalf("expected metadata ok, got %s", c.Status)
		}
	}
	if !found["nats"] {
		t.Error("nats check missing")
	}
	if !found["metadata"] {
		t.Error("metadata check missing")
	}
}

func TestHealthChecker_Readiness_NATSDown(t *testing.T) {
	ns, url := startEmbeddedNATS(t)
	nc, err := nats.Connect(url, nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Shut down the server to make the connection stale
	ns.Shutdown()
	time.Sleep(100 * time.Millisecond)

	checker := NewHealthChecker(nc, nil, nil)
	status := checker.Readiness()
	if status.OK {
		t.Fatal("expected readiness OK=false when NATS is down")
	}

	for _, c := range status.Checks {
		if c.Name == "nats" && c.Status != "disconnected" {
			t.Fatalf("expected nats disconnected, got %s", c.Status)
		}
	}
}

func TestHealthChecker_Readiness_MetaError(t *testing.T) {
	metaStore := newTestMeta(t)
	// Close the store to make Ping fail
	metaStore.Close()

	checker := NewHealthChecker(nil, metaStore, nil)
	status := checker.Readiness()
	if status.OK {
		t.Fatal("expected readiness OK=false when meta store is closed")
	}

	for _, c := range status.Checks {
		if c.Name == "metadata" {
			if c.Status != "error" {
				t.Fatalf("expected metadata error, got %s", c.Status)
			}
			if c.Error == "" {
				t.Fatal("expected error message for metadata check")
			}
		}
	}
}

func TestHealthChecker_Readiness_NilDeps(t *testing.T) {
	checker := NewHealthChecker(nil, nil, nil)
	// Should not panic
	status := checker.Readiness()
	if !status.OK {
		t.Fatal("expected readiness OK=true with nil dependencies (no checks fail)")
	}
}

func TestHealthServer_Endpoints(t *testing.T) {
	_, url := startEmbeddedNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	metaStore := newTestMeta(t)
	checker := NewHealthChecker(nc, metaStore, nil)

	cfg := config.HealthConfig{
		LivenessPath:  "/healthz",
		ReadinessPath: "/readyz",
	}

	// Build the same mux that RunHealthServer would build
	mux := http.NewServeMux()
	mux.HandleFunc(cfg.LivenessPath, func(w http.ResponseWriter, r *http.Request) {
		status := checker.Liveness()
		code := http.StatusOK
		if !status.OK {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc(cfg.ReadinessPath, func(w http.ResponseWriter, r *http.Request) {
		status := checker.Readiness()
		code := http.StatusOK
		if !status.OK {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(status)
	})

	// Test liveness
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("liveness: expected 200, got %d", w.Code)
	}
	var liveResp HealthStatus
	json.Unmarshal(w.Body.Bytes(), &liveResp)
	if !liveResp.OK {
		t.Fatal("liveness response should have OK=true")
	}

	// Test readiness
	req = httptest.NewRequest("GET", "/readyz", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("readiness: expected 200, got %d", w.Code)
	}
	var readyResp HealthStatus
	json.Unmarshal(w.Body.Bytes(), &readyResp)
	if !readyResp.OK {
		t.Fatalf("readiness response should have OK=true, checks: %+v", readyResp.Checks)
	}
}
