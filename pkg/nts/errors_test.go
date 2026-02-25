package nts

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestIsNotFound_MsgNotFound(t *testing.T) {
	if !isNotFound(jetstream.ErrMsgNotFound) {
		t.Error("expected true for ErrMsgNotFound")
	}
}

func TestIsNotFound_KeyNotFound(t *testing.T) {
	if !isNotFound(jetstream.ErrKeyNotFound) {
		t.Error("expected true for ErrKeyNotFound")
	}
}

func TestIsNotFound_StringMatch(t *testing.T) {
	err := fmt.Errorf("the thing not found in store")
	if !isNotFound(err) {
		t.Error("expected true for error containing 'not found'")
	}
}

func TestIsNotFound_Nil(t *testing.T) {
	if isNotFound(nil) {
		t.Error("expected false for nil")
	}
}

func TestIsNotFound_OtherError(t *testing.T) {
	err := fmt.Errorf("connection timeout")
	if isNotFound(err) {
		t.Error("expected false for unrelated error")
	}
}

func TestIsKeyDeleted_True(t *testing.T) {
	if !isKeyDeleted(jetstream.ErrKeyDeleted) {
		t.Error("expected true for ErrKeyDeleted")
	}
}

func TestIsKeyDeleted_Nil(t *testing.T) {
	if isKeyDeleted(nil) {
		t.Error("expected false for nil")
	}
}

func TestParseKVOp(t *testing.T) {
	tests := []struct {
		input string
		want  jetstream.KeyValueOp
	}{
		{"PUT", jetstream.KeyValuePut},
		{"DEL", jetstream.KeyValueDelete},
		{"PURGE", jetstream.KeyValuePurge},
		{"", jetstream.KeyValuePut},       // default
		{"UNKNOWN", jetstream.KeyValuePut}, // default
	}

	for _, tt := range tests {
		got := parseKVOp(tt.input)
		if got != tt.want {
			t.Errorf("parseKVOp(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
