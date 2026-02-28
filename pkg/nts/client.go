package nts

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config configures the NTS transparent client.
type Config struct {
	// NC is the NATS connection.
	NC *nats.Conn

	// JS is the JetStream context.
	JS jetstream.JetStream

	// SubjectPrefix is the prefix for sidecar request subjects.
	// Defaults to "nts".
	SubjectPrefix string

	// Timeout for sidecar requests. Defaults to 5s.
	Timeout time.Duration

	// AutoRestore, if true, causes KVStore.Get to restore evicted keys back to
	// the hot NATS KV tier after a successful cold fetch. Defaults to false for
	// backward compatibility.
	AutoRestore bool
}

// Client provides transparent access to NATS data with cold storage fallback.
type Client struct {
	nc          *nats.Conn
	js          jetstream.JetStream
	prefix      string
	timeout     time.Duration
	autoRestore bool
}

// New creates a new NTS transparent client.
func New(cfg Config) (*Client, error) {
	if cfg.NC == nil {
		return nil, fmt.Errorf("nts: NC (NATS connection) is required")
	}
	if cfg.JS == nil {
		return nil, fmt.Errorf("nts: JS (JetStream context) is required")
	}
	prefix := cfg.SubjectPrefix
	if prefix == "" {
		prefix = "nts"
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		nc:          cfg.NC,
		js:          cfg.JS,
		prefix:      prefix,
		timeout:     timeout,
		autoRestore: cfg.AutoRestore,
	}, nil
}

// KeyValue returns a KV store wrapper with transparent cold fallback.
func (c *Client) KeyValue(ctx context.Context, bucket string) (*KVStore, error) {
	kv, err := c.js.KeyValue(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("nts: opening KV bucket %q: %w", bucket, err)
	}
	return &KVStore{
		kv:          kv,
		bucket:      bucket,
		nc:          c.nc,
		prefix:      c.prefix,
		timeout:     c.timeout,
		autoRestore: c.autoRestore,
	}, nil
}

// ObjectStore returns an Object Store wrapper with transparent cold fallback.
func (c *Client) ObjectStore(ctx context.Context, bucket string) (*ObjStore, error) {
	obs, err := c.js.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("nts: opening Object Store bucket %q: %w", bucket, err)
	}
	return &ObjStore{
		obs:     obs,
		bucket:  bucket,
		nc:      c.nc,
		prefix:  c.prefix,
		timeout: c.timeout,
	}, nil
}

// GetMessage retrieves a single message by sequence, falling back to sidecar.
func (c *Client) GetMessage(ctx context.Context, stream string, seq uint64) (*StoredMessage, error) {
	// Try JetStream direct get first
	s, err := c.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("nts: stream %q: %w", stream, err)
	}

	msg, err := s.GetMsg(ctx, seq)
	if err == nil {
		return &StoredMessage{
			Stream:    stream,
			Subject:   msg.Subject,
			Sequence:  seq,
			Data:      msg.Data,
			Headers:   msg.Header,
			Timestamp: msg.Time,
		}, nil
	}

	if !isNotFound(err) {
		return nil, err
	}

	// Fallback to sidecar
	subject := fmt.Sprintf("%s.get.%s.%d", c.prefix, stream, seq)
	resp, err := c.nc.Request(subject, nil, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("nts: sidecar request: %w", err)
	}

	var result struct {
		Error     string    `json:"error"`
		Stream    string    `json:"stream"`
		Subject   string    `json:"subject"`
		Sequence  uint64    `json:"sequence"`
		Data      string    `json:"data"`
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("nts: decoding sidecar response: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("nts: sidecar: %s", result.Error)
	}

	return &StoredMessage{
		Stream:    result.Stream,
		Subject:   result.Subject,
		Sequence:  result.Sequence,
		Data:      []byte(result.Data),
		Timestamp: result.Timestamp,
	}, nil
}

// StoredMessage represents a message retrieved from either JetStream or cold storage.
type StoredMessage struct {
	Stream    string
	Subject   string
	Sequence  uint64
	Data      []byte
	Headers   nats.Header
	Timestamp time.Time
}
