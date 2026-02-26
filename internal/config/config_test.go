package config

import (
	"os"
	"testing"
	"time"
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

func TestAutoMirrorDefault(t *testing.T) {
	sc := StreamConfig{Name: "ORDERS"}
	if !sc.AutoMirrorEnabled() {
		t.Error("AutoMirrorEnabled() should be true by default (nil)")
	}

	enabled := true
	sc.AutoMirror = &enabled
	if !sc.AutoMirrorEnabled() {
		t.Error("AutoMirrorEnabled() should be true when explicitly set to true")
	}
}

func TestAutoMirrorExplicitFalse(t *testing.T) {
	disabled := false
	sc := StreamConfig{
		Name:       "ORDERS",
		AutoMirror: &disabled,
	}
	if sc.AutoMirrorEnabled() {
		t.Error("AutoMirrorEnabled() should be false when explicitly set to false")
	}
}

func TestMaxTierRetention(t *testing.T) {
	defaultAge := 72 * time.Hour

	t.Run("no tiers enabled", func(t *testing.T) {
		tc := TiersConfig{}
		got := tc.MaxTierRetention(defaultAge)
		if got != defaultAge {
			t.Errorf("MaxTierRetention = %v, want default %v", got, defaultAge)
		}
	})

	t.Run("memory only", func(t *testing.T) {
		tc := TiersConfig{
			Memory: MemoryTierConfig{Enabled: true, MaxAge: Duration(5 * time.Minute)},
		}
		got := tc.MaxTierRetention(defaultAge)
		if got != 5*time.Minute {
			t.Errorf("MaxTierRetention = %v, want 5m", got)
		}
	})

	t.Run("file is largest", func(t *testing.T) {
		tc := TiersConfig{
			Memory: MemoryTierConfig{Enabled: true, MaxAge: Duration(5 * time.Minute)},
			File:   FileTierConfig{Enabled: true, MaxAge: Duration(24 * time.Hour)},
		}
		got := tc.MaxTierRetention(defaultAge)
		if got != 24*time.Hour {
			t.Errorf("MaxTierRetention = %v, want 24h", got)
		}
	})

	t.Run("blob is largest", func(t *testing.T) {
		tc := TiersConfig{
			Memory: MemoryTierConfig{Enabled: true, MaxAge: Duration(5 * time.Minute)},
			File:   FileTierConfig{Enabled: true, MaxAge: Duration(24 * time.Hour)},
			Blob:   BlobTierConfig{Enabled: true, MaxAge: Duration(720 * time.Hour)},
		}
		got := tc.MaxTierRetention(defaultAge)
		if got != 720*time.Hour {
			t.Errorf("MaxTierRetention = %v, want 720h", got)
		}
	})

	t.Run("enabled but zero max_age falls back to default", func(t *testing.T) {
		tc := TiersConfig{
			Memory: MemoryTierConfig{Enabled: true},
			File:   FileTierConfig{Enabled: true},
		}
		got := tc.MaxTierRetention(defaultAge)
		if got != defaultAge {
			t.Errorf("MaxTierRetention = %v, want default %v", got, defaultAge)
		}
	})

	t.Run("disabled tier ignored", func(t *testing.T) {
		tc := TiersConfig{
			Memory: MemoryTierConfig{Enabled: true, MaxAge: Duration(5 * time.Minute)},
			File:   FileTierConfig{Enabled: false, MaxAge: Duration(999 * time.Hour)},
		}
		got := tc.MaxTierRetention(defaultAge)
		if got != 5*time.Minute {
			t.Errorf("MaxTierRetention = %v, want 5m (disabled file tier should be ignored)", got)
		}
	})
}
