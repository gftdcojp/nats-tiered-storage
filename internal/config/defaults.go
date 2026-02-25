package config

import "time"

func DefaultConfig() *Config {
	return &Config{
		NATS: NATSConfig{
			URL:            "nats://localhost:4222",
			ConnectionName: "nats-tiered-storage",
			MaxReconnects:  -1,
			ReconnectWait:  Duration(2 * time.Second),
		},
		Block: BlockConfig{
			TargetSize:  ByteSize(8 * 1024 * 1024), // 8MB
			MaxLinger:   Duration(30 * time.Second),
			Compression: "s2",
		},
		Policy: PolicyConfig{
			EvalInterval: Duration(30 * time.Second),
		},
		Metadata: MetadataConfig{
			Path: "/var/lib/nts/meta.db",
		},
		API: APIConfig{
			Enabled: true,
			Listen:  ":8080",
			NATSResponder: NATSResponderConfig{
				Enabled:       false,
				SubjectPrefix: "nts",
			},
		},
		Observability: ObservabilityConfig{
			Metrics: MetricsConfig{
				Enabled: true,
				Listen:  ":9090",
				Path:    "/metrics",
			},
			Health: HealthConfig{
				Enabled:       true,
				Listen:        ":8081",
				LivenessPath:  "/healthz",
				ReadinessPath: "/readyz",
			},
			Logging: LoggingConfig{
				Level:  "info",
				Format: "json",
				Output: "stderr",
			},
		},
	}
}
