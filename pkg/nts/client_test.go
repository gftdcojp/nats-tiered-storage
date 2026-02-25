package nts

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
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

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})

	return ns, nc, js
}

func TestClient_New_Valid(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)

	client, err := New(Config{NC: nc, JS: js})
	if err != nil {
		t.Fatal(err)
	}
	if client.prefix != "nts" {
		t.Fatalf("expected default prefix 'nts', got %q", client.prefix)
	}
	if client.timeout != 5*time.Second {
		t.Fatalf("expected default timeout 5s, got %v", client.timeout)
	}
}

func TestClient_New_CustomConfig(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)

	client, err := New(Config{
		NC:            nc,
		JS:            js,
		SubjectPrefix: "myapp",
		Timeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.prefix != "myapp" {
		t.Fatalf("expected prefix 'myapp', got %q", client.prefix)
	}
	if client.timeout != 10*time.Second {
		t.Fatalf("expected timeout 10s, got %v", client.timeout)
	}
}

func TestClient_New_MissingNC(t *testing.T) {
	_, _, js := startEmbeddedNATS(t)

	_, err := New(Config{NC: nil, JS: js})
	if err == nil {
		t.Fatal("expected error for nil NC")
	}
}

func TestClient_New_MissingJS(t *testing.T) {
	_, nc, _ := startEmbeddedNATS(t)

	_, err := New(Config{NC: nc, JS: nil})
	if err == nil {
		t.Fatal("expected error for nil JS")
	}
}

func TestClient_GetMessage_Sidecar(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	// Create a stream so the client can get a reference
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"ORDERS.>"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Set up mock sidecar responder
	sub, err := nc.Subscribe("nts.get.ORDERS.42", func(msg *nats.Msg) {
		resp := map[string]interface{}{
			"stream":    "ORDERS",
			"subject":   "ORDERS.new",
			"sequence":  42,
			"data":      "hello from cold storage",
			"timestamp": time.Now().Format(time.RFC3339Nano),
		}
		data, _ := json.Marshal(resp)
		msg.Respond(data)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	client, _ := New(Config{NC: nc, JS: js})
	// Seq 42 doesn't exist in JetStream, so it should fall back to sidecar
	stored, err := client.GetMessage(ctx, "ORDERS", 42)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Sequence != 42 {
		t.Fatalf("expected seq 42, got %d", stored.Sequence)
	}
	if string(stored.Data) != "hello from cold storage" {
		t.Fatalf("expected cold data, got %q", stored.Data)
	}
}

func TestClient_KV_Get_Sidecar(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	// Create a KV bucket
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "config",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Put then purge a key so Get returns ErrKeyDeleted
	kv.Put(ctx, "app.port", []byte("8080"))
	kv.Purge(ctx, "app.port")

	// Set up mock sidecar responder
	sub, err := nc.Subscribe("nts.kv.config.get.app.port", func(msg *nats.Msg) {
		resp := coldKVEntry{
			Bucket:    "config",
			Key:       "app.port",
			Value:     "3000",
			Revision:  5,
			Operation: "PUT",
			Timestamp: time.Now(),
		}
		data, _ := json.Marshal(resp)
		msg.Respond(data)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	client, _ := New(Config{NC: nc, JS: js})
	kvStore, err := client.KeyValue(ctx, "config")
	if err != nil {
		t.Fatal(err)
	}

	entry, err := kvStore.Get(ctx, "app.port")
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Value()) != "3000" {
		t.Fatalf("expected '3000', got %q", entry.Value())
	}
	if entry.Revision() != 5 {
		t.Fatalf("expected revision 5, got %d", entry.Revision())
	}
}

func TestClient_KV_Keys_Sidecar(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	// Create an empty KV bucket
	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "settings",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Mock sidecar returns keys from cold storage
	sub, err := nc.Subscribe("nts.kv.settings.keys", func(msg *nats.Msg) {
		keys := []string{"db.host", "db.port", "cache.ttl"}
		data, _ := json.Marshal(keys)
		msg.Respond(data)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	client, _ := New(Config{NC: nc, JS: js})
	kvStore, err := client.KeyValue(ctx, "settings")
	if err != nil {
		t.Fatal(err)
	}

	keys, err := kvStore.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
}

func TestClient_Obj_Get_Sidecar(t *testing.T) {
	_, nc, js := startEmbeddedNATS(t)
	ctx := context.Background()

	// Create an Object Store bucket
	_, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket: "files",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Mock sidecar returns object data
	sub, err := nc.Subscribe("nts.obj.files.get.report.pdf", func(msg *nats.Msg) {
		// Return raw binary (not JSON)
		msg.Respond([]byte("PDF-binary-content-here"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	client, _ := New(Config{NC: nc, JS: js})
	objStore, err := client.ObjectStore(ctx, "files")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := objStore.Get(ctx, "report.pdf")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "PDF-binary-content-here" {
		t.Fatalf("expected PDF content, got %q", data)
	}
}
