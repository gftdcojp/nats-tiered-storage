package ingest

import (
	"testing"
)

func TestParseKVSubject(t *testing.T) {
	tests := []struct {
		subject string
		bucket  string
		key     string
		ok      bool
	}{
		{"$KV.config.app.database_url", "config", "app.database_url", true},
		{"$KV.mystore.simple", "mystore", "simple", true},
		{"$KV.b.k", "b", "k", true},
		{"$KV.config.", "", "", false},     // empty key
		{"$KV.config", "", "", false},      // no key
		{"$KV.", "", "", false},            // no bucket
		{"ORDERS.new", "", "", false},      // not a KV subject
		{"", "", "", false},               // empty
	}

	for _, tt := range tests {
		bucket, key, ok := ParseKVSubject(tt.subject)
		if ok != tt.ok {
			t.Errorf("ParseKVSubject(%q) ok = %v, want %v", tt.subject, ok, tt.ok)
			continue
		}
		if ok {
			if bucket != tt.bucket {
				t.Errorf("ParseKVSubject(%q) bucket = %q, want %q", tt.subject, bucket, tt.bucket)
			}
			if key != tt.key {
				t.Errorf("ParseKVSubject(%q) key = %q, want %q", tt.subject, key, tt.key)
			}
		}
	}
}

func TestExtractKVOperation(t *testing.T) {
	tests := []struct {
		headers  string
		expected string
	}{
		{"", "PUT"},
		{"KV-Operation: DEL\r\n", "DEL"},
		{"KV-Operation: PURGE\r\n", "PURGE"},
		{"Content-Type: text/plain\r\nKV-Operation: DEL\r\n", "DEL"},
		{"Content-Type: text/plain\r\n", "PUT"},
	}

	for _, tt := range tests {
		got := ExtractKVOperation([]byte(tt.headers))
		if got != tt.expected {
			t.Errorf("ExtractKVOperation(%q) = %q, want %q", tt.headers, got, tt.expected)
		}
	}
}
