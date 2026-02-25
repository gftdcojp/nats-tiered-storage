// Package natsutil provides helpers for establishing NATS connections
// with TLS, credentials, NKey, and reconnection handling.
package natsutil

import (
	"fmt"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// Connect establishes a connection to NATS with the given configuration.
func Connect(cfg config.NATSConfig, logger *zap.Logger) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(cfg.ConnectionName),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait.Duration()),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				logger.Warn("NATS disconnected", zap.Error(err))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Info("NATS connection closed")
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			logger.Error("NATS async error", zap.Error(err))
		}),
		nats.ReconnectBufSize(16 * 1024 * 1024), // 16MB reconnect buffer
		nats.PingInterval(20 * time.Second),
	}

	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}

	if cfg.NKeySeedFile != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.NKeySeedFile)
		if err != nil {
			return nil, fmt.Errorf("loading nkey seed: %w", err)
		}
		opts = append(opts, opt)
	}

	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		opts = append(opts, nats.ClientCert(cfg.TLS.CertFile, cfg.TLS.KeyFile))
	}
	if cfg.TLS.CAFile != "" {
		opts = append(opts, nats.RootCAs(cfg.TLS.CAFile))
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", cfg.URL, err)
	}

	logger.Info("connected to NATS",
		zap.String("url", nc.ConnectedUrl()),
		zap.String("server_id", nc.ConnectedServerId()),
	)

	return nc, nil
}
