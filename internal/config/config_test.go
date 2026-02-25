package config

import (
	"os"
	"testing"
)

func TestLoadAndValidate(t *testing.T) {
	yaml := `
nats:
  url: "nats://localhost:4222"

streams:
  - name: "ORDERS"
    consumer_name: "nts-archiver"
    tiers:
      memory:
        enabled: true
        max_bytes: "128MB"
        max_age: "5m"
      file:
        enabled: true
        data_dir: "/tmp/nts/test"
        max_bytes: "1GB"
        max_age: "1h"
      blob:
        enabled: false

metadata:
  path: "/tmp/nts/test-meta.db"
`
	tmpFile, err := os.CreateTemp("", "nts-config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(yaml)
	tmpFile.Close()

	cfg, err := Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	if cfg.NATS.URL != "nats://localhost:4222" {
		t.Errorf("unexpected NATS URL: %s", cfg.NATS.URL)
	}
	if len(cfg.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(cfg.Streams))
	}
	if cfg.Streams[0].Name != "ORDERS" {
		t.Errorf("unexpected stream name: %s", cfg.Streams[0].Name)
	}
	if int64(cfg.Streams[0].Tiers.Memory.MaxBytes) != 128*1024*1024 {
		t.Errorf("unexpected max_bytes: %d", cfg.Streams[0].Tiers.Memory.MaxBytes)
	}
}

func TestValidateNoStreams(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Streams = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for no streams")
	}
}

func TestValidateNoTiers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Streams = []StreamConfig{{Name: "TEST"}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for no enabled tiers")
	}
}

func TestParseByteSizes(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"1KB", 1024},
		{"256MB", 256 * 1024 * 1024},
		{"10GB", 10 * 1024 * 1024 * 1024},
		{"1TB", 1024 * 1024 * 1024 * 1024},
		{"100B", 100},
	}
	for _, tt := range tests {
		result, err := parseByteSize(tt.input)
		if err != nil {
			t.Errorf("parseByteSize(%q) error: %v", tt.input, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("parseByteSize(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}
