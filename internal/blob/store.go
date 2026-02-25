package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"github.com/gftdcojp/nats-tiered-storage/pkg/s3util"
	"go.uber.org/zap"
)

// Store implements tier.TierStore for S3-compatible object storage.
type Store struct {
	client *s3util.Client
	cfg    config.BlobTierConfig
	logger *zap.Logger
	// indexCache caches block indexes in memory for efficient single-message lookups.
	indexCache map[string]*block.BlockIndex
}

func NewStore(client *s3util.Client, cfg config.BlobTierConfig, logger *zap.Logger) *Store {
	return &Store{
		client:     client,
		cfg:        cfg,
		logger:     logger,
		indexCache: make(map[string]*block.BlockIndex),
	}
}

func (s *Store) objectKey(ref tier.BlockRef) string {
	if s.cfg.Prefix != "" {
		return fmt.Sprintf("%s/%s/blocks/%010d.blk", s.cfg.Prefix, ref.Stream, ref.BlockID)
	}
	return fmt.Sprintf("%s/blocks/%010d.blk", ref.Stream, ref.BlockID)
}

func (s *Store) indexKey(ref tier.BlockRef) string {
	if s.cfg.Prefix != "" {
		return fmt.Sprintf("%s/%s/blocks/%010d.idx", s.cfg.Prefix, ref.Stream, ref.BlockID)
	}
	return fmt.Sprintf("%s/blocks/%010d.idx", ref.Stream, ref.BlockID)
}

func (s *Store) Put(ctx context.Context, ref tier.BlockRef, data *block.Block) error {
	key := s.objectKey(ref)

	metadata := map[string]string{
		"nts-stream":    ref.Stream,
		"nts-block-id":  strconv.FormatUint(ref.BlockID, 10),
		"nts-first-seq": strconv.FormatUint(ref.FirstSeq, 10),
		"nts-last-seq":  strconv.FormatUint(ref.LastSeq, 10),
		"nts-msg-count": strconv.FormatUint(data.MsgCount, 10),
	}

	input := &s3.PutObjectInput{
		Bucket:      &s.client.Bucket,
		Key:         &key,
		Body:        bytes.NewReader(data.Raw),
		ContentType: aws.String("application/octet-stream"),
		Metadata:    metadata,
	}

	if s.cfg.StorageClass != "" {
		// StorageClass is set via the types package
	}

	_, err := s.client.S3.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("uploading block to S3: %w", err)
	}

	// Upload index sidecar
	if data.Index != nil {
		idxKey := s.indexKey(ref)
		idxData := data.Index.Encode()
		_, err := s.client.S3.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &s.client.Bucket,
			Key:         &idxKey,
			Body:        bytes.NewReader(idxData),
			ContentType: aws.String("application/octet-stream"),
		})
		if err != nil {
			s.logger.Warn("failed to upload index sidecar", zap.Error(err), zap.String("key", idxKey))
		}
	}

	s.logger.Debug("block uploaded to S3",
		zap.Uint64("block_id", ref.BlockID),
		zap.String("key", key),
		zap.Int64("size", data.SizeBytes),
	)

	return nil
}

func (s *Store) Get(ctx context.Context, ref tier.BlockRef) (*block.Block, error) {
	key := s.objectKey(ref)
	resp, err := s.client.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.client.Bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("downloading block from S3: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading S3 response: %w", err)
	}

	return block.Decode(raw)
}

func (s *Store) GetMessage(ctx context.Context, ref tier.BlockRef, seq uint64) (*tier.StoredMessage, error) {
	// Try cached index first
	idx, err := s.getBlockIndex(ctx, ref)
	if err == nil {
		entry, found := idx.Lookup(seq)
		if found {
			return s.readMessageRange(ctx, ref, entry.Offset, entry.Size)
		}
	}

	// Fallback: download full block
	blk, err := s.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	for _, msg := range blk.Messages {
		if msg.Sequence == seq {
			return &tier.StoredMessage{
				Stream:    blk.Stream,
				Subject:   msg.Subject,
				Sequence:  msg.Sequence,
				Data:      msg.Data,
				Timestamp: msg.Timestamp,
			}, nil
		}
	}
	return nil, fmt.Errorf("sequence %d not found in block %d", seq, ref.BlockID)
}

func (s *Store) getBlockIndex(ctx context.Context, ref tier.BlockRef) (*block.BlockIndex, error) {
	cacheKey := fmt.Sprintf("%s/%d", ref.Stream, ref.BlockID)
	if idx, ok := s.indexCache[cacheKey]; ok {
		return idx, nil
	}

	idxKey := s.indexKey(ref)
	resp, err := s.client.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.client.Bucket,
		Key:    &idxKey,
	})
	if err != nil {
		return nil, fmt.Errorf("downloading index from S3: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	idx, err := block.DecodeIndex(data)
	if err != nil {
		return nil, err
	}

	s.indexCache[cacheKey] = idx
	return idx, nil
}

func (s *Store) readMessageRange(ctx context.Context, ref tier.BlockRef, offset int64, size int32) (*tier.StoredMessage, error) {
	key := s.objectKey(ref)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1)

	resp, err := s.client.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.client.Bucket,
		Key:    &key,
		Range:  &rangeHeader,
	})
	if err != nil {
		return nil, fmt.Errorf("S3 range request: %w", err)
	}
	defer resp.Body.Close()

	msgBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	msg, err := block.DecodeMessage(msgBytes)
	if err != nil {
		return nil, err
	}

	return &tier.StoredMessage{
		Stream:    ref.Stream,
		Subject:   msg.Subject,
		Sequence:  msg.Sequence,
		Data:      msg.Data,
		Timestamp: msg.Timestamp,
	}, nil
}

func (s *Store) Delete(ctx context.Context, ref tier.BlockRef) error {
	key := s.objectKey(ref)
	_, err := s.client.S3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.client.Bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("deleting block from S3: %w", err)
	}

	// Also delete index
	idxKey := s.indexKey(ref)
	s.client.S3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.client.Bucket,
		Key:    &idxKey,
	})

	// Remove from cache
	cacheKey := fmt.Sprintf("%s/%d", ref.Stream, ref.BlockID)
	delete(s.indexCache, cacheKey)

	return nil
}

func (s *Store) Exists(ctx context.Context, ref tier.BlockRef) (bool, error) {
	key := s.objectKey(ref)
	_, err := s.client.S3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.client.Bucket,
		Key:    &key,
	})
	if err != nil {
		return false, nil // treat any error as not found
	}
	return true, nil
}

func (s *Store) Stats(_ context.Context) (tier.TierStats, error) {
	return tier.TierStats{
		Tier:        tier.TierBlob,
		CapacityMax: -1, // unlimited
	}, nil
}

func (s *Store) Close() error {
	s.indexCache = nil
	return nil
}
