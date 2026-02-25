package nts

import (
	"errors"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// isNotFound checks if an error indicates a "not found" condition
// across JetStream message, KV, and Object Store operations.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return true
	}
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return true
	}
	// jetstream.ErrObjectNotFound may not exist in all versions;
	// also check the error string as a fallback.
	errStr := err.Error()
	if strings.Contains(errStr, "not found") {
		return true
	}
	return false
}

// isKeyDeleted checks if a KV entry represents a deleted key.
func isKeyDeleted(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, jetstream.ErrKeyDeleted)
}
