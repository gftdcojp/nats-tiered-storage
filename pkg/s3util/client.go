// Package s3util provides a factory for creating AWS S3-compatible clients
// for use with blob tier storage (AWS S3, MinIO, Cloudflare R2).
package s3util

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
)

// Client wraps the AWS S3 client.
type Client struct {
	S3     *s3.Client
	Bucket string
	Prefix string
}

// NewClient creates a new S3-compatible client from blob tier config.
func NewClient(ctx context.Context, cfg config.BlobTierConfig) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &Client{
		S3:     client,
		Bucket: cfg.Bucket,
		Prefix: cfg.Prefix,
	}, nil
}

// Ping checks connectivity by performing a HeadBucket operation.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.S3.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: &c.Bucket,
	})
	return err
}
