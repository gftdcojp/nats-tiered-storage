package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NATS          NATSConfig          `yaml:"nats"`
	Streams       []StreamConfig      `yaml:"streams"`
	Block         BlockConfig         `yaml:"block"`
	Policy        PolicyConfig        `yaml:"policy"`
	Metadata      MetadataConfig      `yaml:"metadata"`
	API           APIConfig           `yaml:"api"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type NATSConfig struct {
	URL            string    `yaml:"url"`
	CredentialsFile string   `yaml:"credentials_file"`
	NKeySeedFile   string    `yaml:"nkey_seed_file"`
	TLS            TLSConfig `yaml:"tls"`
	ConnectionName string    `yaml:"connection_name"`
	MaxReconnects  int       `yaml:"max_reconnects"`
	ReconnectWait  Duration  `yaml:"reconnect_wait"`
}

type TLSConfig struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// StreamType identifies the type of JetStream stream being archived.
type StreamType string

const (
	StreamTypeRaw         StreamType = "stream"
	StreamTypeKV          StreamType = "kv"
	StreamTypeObjectStore StreamType = "objectstore"
)

// FetchRetryConfig controls exponential backoff behaviour when JetStream fetch
// requests fail (e.g. "no responders available for request").
type FetchRetryConfig struct {
	// InitialDelay is the backoff duration after the first consecutive error.
	// Subsequent delays double up to MaxDelay. Defaults to 1s.
	InitialDelay Duration `yaml:"initial_delay"`
	// MaxDelay caps the exponential backoff. Defaults to 60s.
	MaxDelay Duration `yaml:"max_delay"`
}

type StreamConfig struct {
	Name         string           `yaml:"name"`
	Type         StreamType       `yaml:"type"`
	Subjects     []string         `yaml:"subjects"`
	ConsumerName string           `yaml:"consumer_name"`
	AutoMirror   *bool            `yaml:"auto_mirror"`
	FetchBatch   int              `yaml:"fetch_batch"`
	FetchTimeout Duration         `yaml:"fetch_timeout"`
	Retry        FetchRetryConfig `yaml:"retry"`
	Tiers        TiersConfig      `yaml:"tiers"`
	KV           KVArchiveConfig  `yaml:"kv"`
	ObjectStore  ObjArchiveConfig `yaml:"objectstore"`
}

// AutoMirrorEnabled returns whether auto-mirroring is enabled for this stream.
// Defaults to true when not explicitly set.
func (sc *StreamConfig) AutoMirrorEnabled() bool {
	if sc.AutoMirror == nil {
		return true
	}
	return *sc.AutoMirror
}

type KVArchiveConfig struct {
	BucketName        string `yaml:"bucket_name"`
	IndexAllRevisions bool   `yaml:"index_all_revisions"`
}

type ObjArchiveConfig struct {
	BucketName string `yaml:"bucket_name"`
}

// DetectStreamType auto-detects the stream type from the stream name prefix.
func DetectStreamType(name string) StreamType {
	if len(name) > 3 && name[:3] == "KV_" {
		return StreamTypeKV
	}
	if len(name) > 4 && name[:4] == "OBJ_" {
		return StreamTypeObjectStore
	}
	return StreamTypeRaw
}

// ResolvedType returns the stream type, auto-detecting if not explicitly set.
func (sc *StreamConfig) ResolvedType() StreamType {
	if sc.Type != "" {
		return sc.Type
	}
	return DetectStreamType(sc.Name)
}

// ResolvedKVBucket returns the KV bucket name, auto-deriving from stream name.
func (sc *StreamConfig) ResolvedKVBucket() string {
	if sc.KV.BucketName != "" {
		return sc.KV.BucketName
	}
	if len(sc.Name) > 3 && sc.Name[:3] == "KV_" {
		return sc.Name[3:]
	}
	return sc.Name
}

// ResolvedObjBucket returns the Object Store bucket name, auto-deriving from stream name.
func (sc *StreamConfig) ResolvedObjBucket() string {
	if sc.ObjectStore.BucketName != "" {
		return sc.ObjectStore.BucketName
	}
	if len(sc.Name) > 4 && sc.Name[:4] == "OBJ_" {
		return sc.Name[4:]
	}
	return sc.Name
}

type TiersConfig struct {
	Memory MemoryTierConfig `yaml:"memory"`
	File   FileTierConfig   `yaml:"file"`
	Blob   BlobTierConfig   `yaml:"blob"`
}

// MaxTierRetention returns the maximum retention duration across all enabled tiers.
// If no tier has a configured max_age, the provided defaultAge is returned.
func (tc *TiersConfig) MaxTierRetention(defaultAge time.Duration) time.Duration {
	var max time.Duration
	if tc.Memory.Enabled && tc.Memory.MaxAge.Duration() > max {
		max = tc.Memory.MaxAge.Duration()
	}
	if tc.File.Enabled && tc.File.MaxAge.Duration() > max {
		max = tc.File.MaxAge.Duration()
	}
	if tc.Blob.Enabled && tc.Blob.MaxAge.Duration() > max {
		max = tc.Blob.MaxAge.Duration()
	}
	if max == 0 {
		return defaultAge
	}
	return max
}

type MemoryTierConfig struct {
	Enabled   bool     `yaml:"enabled"`
	MaxBytes  ByteSize `yaml:"max_bytes"`
	MaxBlocks int      `yaml:"max_blocks"`
	MaxAge    Duration `yaml:"max_age"`
}

type FileTierConfig struct {
	Enabled   bool     `yaml:"enabled"`
	DataDir   string   `yaml:"data_dir"`
	MaxBytes  ByteSize `yaml:"max_bytes"`
	MaxBlocks int      `yaml:"max_blocks"`
	MaxAge    Duration `yaml:"max_age"`
}

type BlobTierConfig struct {
	Enabled        bool     `yaml:"enabled"`
	Endpoint       string   `yaml:"endpoint"`
	Region         string   `yaml:"region"`
	Bucket         string   `yaml:"bucket"`
	Prefix         string   `yaml:"prefix"`
	AccessKeyID    string   `yaml:"access_key_id"`
	SecretAccessKey string  `yaml:"secret_access_key"`
	ForcePathStyle bool     `yaml:"force_path_style"`
	StorageClass   string   `yaml:"storage_class"`
	MaxAge         Duration `yaml:"max_age"`
	Multipart      bool     `yaml:"multipart"`
}

type BlockConfig struct {
	TargetSize  ByteSize `yaml:"target_size"`
	MaxLinger   Duration `yaml:"max_linger"`
	Compression string   `yaml:"compression"`
}

type PolicyConfig struct {
	EvalInterval Duration `yaml:"eval_interval"`
}

type MetadataConfig struct {
	Path   string `yaml:"path"`
	NoSync bool   `yaml:"no_sync"`
}

type APIConfig struct {
	Enabled       bool              `yaml:"enabled"`
	Listen        string            `yaml:"listen"`
	NATSResponder NATSResponderConfig `yaml:"nats_responder"`
}

type NATSResponderConfig struct {
	Enabled       bool   `yaml:"enabled"`
	SubjectPrefix string `yaml:"subject_prefix"`
}

type ObservabilityConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Health  HealthConfig  `yaml:"health"`
	Logging LoggingConfig `yaml:"logging"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
	Path    string `yaml:"path"`
}

type HealthConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Listen        string `yaml:"listen"`
	LivenessPath  string `yaml:"liveness_path"`
	ReadinessPath string `yaml:"readiness_path"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.NATS.URL == "" {
		return fmt.Errorf("nats.url is required")
	}

	if len(c.Streams) == 0 {
		return fmt.Errorf("at least one stream must be configured")
	}

	for i, sc := range c.Streams {
		if sc.Name == "" {
			return fmt.Errorf("streams[%d].name is required", i)
		}
		if !sc.Tiers.Memory.Enabled && !sc.Tiers.File.Enabled && !sc.Tiers.Blob.Enabled {
			return fmt.Errorf("streams[%d] (%s): at least one tier must be enabled", i, sc.Name)
		}
		if sc.Tiers.File.Enabled && sc.Tiers.File.DataDir == "" {
			return fmt.Errorf("streams[%d] (%s): file tier requires data_dir", i, sc.Name)
		}
		if sc.Tiers.Blob.Enabled {
			if sc.Tiers.Blob.Endpoint == "" {
				return fmt.Errorf("streams[%d] (%s): blob tier requires endpoint", i, sc.Name)
			}
			if sc.Tiers.Blob.Bucket == "" {
				return fmt.Errorf("streams[%d] (%s): blob tier requires bucket", i, sc.Name)
			}
		}
	}

	if c.Block.TargetSize < 256*1024 || c.Block.TargetSize > 16*1024*1024 {
		return fmt.Errorf("block.target_size must be between 256KB and 16MB, got %d", c.Block.TargetSize)
	}

	if c.Block.MaxLinger <= 0 {
		return fmt.Errorf("block.max_linger must be > 0")
	}

	if c.Metadata.Path == "" {
		return fmt.Errorf("metadata.path is required")
	}

	return nil
}

// Duration wraps time.Duration for YAML unmarshaling of strings like "5m", "24h".
type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// ByteSize wraps int64 for YAML unmarshaling of strings like "256MB", "10GB".
type ByteSize int64

func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		// Try as integer
		var n int64
		if err2 := value.Decode(&n); err2 != nil {
			return err
		}
		*b = ByteSize(n)
		return nil
	}
	parsed, err := parseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(parsed)
	return nil
}

func parseByteSize(s string) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty byte size")
	}

	var multiplier int64 = 1
	numStr := s

	switch {
	case len(s) >= 2 && s[len(s)-2:] == "KB":
		multiplier = 1024
		numStr = s[:len(s)-2]
	case len(s) >= 2 && s[len(s)-2:] == "MB":
		multiplier = 1024 * 1024
		numStr = s[:len(s)-2]
	case len(s) >= 2 && s[len(s)-2:] == "GB":
		multiplier = 1024 * 1024 * 1024
		numStr = s[:len(s)-2]
	case len(s) >= 2 && s[len(s)-2:] == "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = s[:len(s)-2]
	case s[len(s)-1] == 'B':
		numStr = s[:len(s)-1]
	}

	var n int64
	_, err := fmt.Sscanf(numStr, "%d", &n)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	return n * multiplier, nil
}
