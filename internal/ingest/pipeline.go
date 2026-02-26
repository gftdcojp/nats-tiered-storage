package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/block"
	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/gftdcojp/nats-tiered-storage/internal/metrics"
	"github.com/gftdcojp/nats-tiered-storage/internal/tier"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// PipelineConfig holds dependencies for the ingest pipeline.
type PipelineConfig struct {
	JS     jetstream.JetStream
	Ctrl   *tier.Controller
	Meta   meta.Store
	Stream config.StreamConfig
	Block  config.BlockConfig
	Logger *zap.Logger
}

// Pipeline manages the ingestion of messages from a JetStream stream.
type Pipeline struct {
	js            jetstream.JetStream
	ctrl          *tier.Controller
	meta          meta.Store
	streamCfg     config.StreamConfig
	blockCfg      config.BlockConfig
	logger        *zap.Logger
	builder       *block.Builder
	lingerTimer   *time.Timer
	pendingAcks   []jetstream.Msg
	mu            sync.Mutex
	nextID        atomic.Uint64
	consumeStream string // stream to create consumer on (may be mirror)
	isMirror      bool   // true when consuming from an auto-created mirror
}

// NewPipeline creates a new ingest pipeline.
func NewPipeline(cfg PipelineConfig) *Pipeline {
	p := &Pipeline{
		js:        cfg.JS,
		ctrl:      cfg.Ctrl,
		meta:      cfg.Meta,
		streamCfg: cfg.Stream,
		blockCfg:  cfg.Block,
		logger:    cfg.Logger,
	}
	p.nextID.Store(1)
	return p
}

// Run starts the ingest loop, consuming from JetStream.
func (p *Pipeline) Run(ctx context.Context) error {
	// Restore block ID from metadata
	lastSeq, _ := p.meta.GetConsumerState(ctx, p.streamCfg.Name)
	if lastSeq > 0 {
		p.logger.Info("resuming from last acked sequence", zap.Uint64("seq", lastSeq))
	}

	// Resolve which stream to consume from (may create a mirror for WorkQueue streams).
	res, err := resolveConsumerStream(ctx, p.js, p.streamCfg, p.logger)
	if err != nil {
		return fmt.Errorf("resolving consumer stream for %s: %w", p.streamCfg.Name, err)
	}
	p.consumeStream = res.ConsumeStream
	p.isMirror = res.IsMirror

	// Create or get durable pull consumer
	consumerCfg := jetstream.ConsumerConfig{
		Durable:       p.streamCfg.ConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxAckPending: p.streamCfg.FetchBatch * 4,
	}
	// Only set FilterSubjects when consuming from the original stream.
	// Mirror streams replicate all messages, so filtering is unnecessary.
	if !p.isMirror && len(p.streamCfg.Subjects) > 0 {
		consumerCfg.FilterSubjects = p.streamCfg.Subjects
	}

	cons, err := p.js.CreateOrUpdateConsumer(ctx, p.consumeStream, consumerCfg)
	if err != nil {
		return fmt.Errorf("creating consumer %s on stream %s: %w", p.streamCfg.ConsumerName, p.consumeStream, err)
	}

	p.logger.Info("ingest pipeline started",
		zap.String("stream", p.streamCfg.Name),
		zap.String("consume_stream", p.consumeStream),
		zap.Bool("is_mirror", p.isMirror),
		zap.String("consumer", p.streamCfg.ConsumerName),
		zap.Int("fetch_batch", p.streamCfg.FetchBatch),
	)

	// Initialize first builder
	targetSize := int64(p.blockCfg.TargetSize)
	p.builder = block.NewBuilder(p.streamCfg.Name, p.nextBlockID(), targetSize)

	fetchTimeout := p.streamCfg.FetchTimeout.Duration()
	if fetchTimeout == 0 {
		fetchTimeout = 5 * time.Second
	}

	batchSize := p.streamCfg.FetchBatch
	if batchSize == 0 {
		batchSize = 256
	}

	for {
		select {
		case <-ctx.Done():
			return p.FlushAndClose(context.Background())
		default:
		}

		msgs, err := cons.Fetch(batchSize, jetstream.FetchMaxWait(fetchTimeout))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return p.FlushAndClose(context.Background())
			}
			p.logger.Warn("fetch error, retrying", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		for msg := range msgs.Messages() {
			msgMeta, err := msg.Metadata()
			if err != nil {
				p.logger.Warn("failed to get message metadata", zap.Error(err))
				continue
			}

			m := block.Message{
				Sequence:  msgMeta.Sequence.Stream,
				Subject:   msg.Subject(),
				Data:      msg.Data(),
				Timestamp: msgMeta.Timestamp,
			}

			// Extract raw headers
			if hdrs := msg.Headers(); hdrs != nil {
				var hdrBytes []byte
				for k, vals := range hdrs {
					for _, v := range vals {
						hdrBytes = append(hdrBytes, []byte(k+": "+v+"\r\n")...)
					}
				}
				m.Headers = hdrBytes
			}

			p.mu.Lock()
			if !p.builder.Add(m) {
				// Block is full, seal and ingest
				if err := p.sealAndIngest(ctx); err != nil {
					p.mu.Unlock()
					return err
				}
				// Start new block
				p.builder = block.NewBuilder(p.streamCfg.Name, p.nextBlockID(), targetSize)
				p.builder.Add(m)
			}
			p.pendingAcks = append(p.pendingAcks, msg)
			p.resetLingerTimer(ctx)
			p.mu.Unlock()
		}

		if err := msgs.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
			p.logger.Warn("batch error", zap.Error(err))
		}
	}
}

// FlushAndClose seals any pending block and acknowledges messages.
func (p *Pipeline) FlushAndClose(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.lingerTimer != nil {
		p.lingerTimer.Stop()
	}

	if p.builder != nil && p.builder.MessageCount() > 0 {
		return p.sealAndIngest(ctx)
	}
	return nil
}

func (p *Pipeline) sealAndIngest(ctx context.Context) error {
	sealStart := time.Now()
	blk, err := p.builder.Seal()
	if err != nil {
		return fmt.Errorf("sealing block: %w", err)
	}
	if blk == nil {
		return nil
	}

	metrics.BlockSealDuration.WithLabelValues(p.streamCfg.Name).Observe(time.Since(sealStart).Seconds())

	if err := p.ctrl.Ingest(ctx, blk); err != nil {
		return fmt.Errorf("ingesting block: %w", err)
	}

	// Update metrics
	metrics.BlocksSealed.WithLabelValues(p.streamCfg.Name).Inc()
	metrics.MessagesIngested.WithLabelValues(p.streamCfg.Name).Add(float64(blk.MsgCount))

	// ACK after successful ingest
	for _, msg := range p.pendingAcks {
		if err := msg.Ack(); err != nil {
			p.logger.Warn("failed to ack message", zap.Error(err))
		}
	}

	// Persist consumer checkpoint
	if err := p.meta.SetConsumerState(ctx, p.streamCfg.Name, blk.LastSeq); err != nil {
		p.logger.Warn("failed to persist consumer state", zap.Error(err))
	}

	// Secondary indexing for KV and Object Store streams
	switch p.streamCfg.ResolvedType() {
	case config.StreamTypeKV:
		if err := indexKVBlock(p.meta, p.streamCfg.Name, blk, p.streamCfg.KV.IndexAllRevisions); err != nil {
			p.logger.Warn("KV indexing failed", zap.Error(err))
		}
	case config.StreamTypeObjectStore:
		if err := indexObjBlock(p.meta, p.streamCfg.Name, blk); err != nil {
			p.logger.Warn("Object Store indexing failed", zap.Error(err))
		}
	}

	p.logger.Info("block sealed and ingested",
		zap.Uint64("block_id", blk.ID),
		zap.Uint64("first_seq", blk.FirstSeq),
		zap.Uint64("last_seq", blk.LastSeq),
		zap.Uint64("msg_count", blk.MsgCount),
		zap.Int64("size_bytes", blk.SizeBytes),
	)

	p.pendingAcks = p.pendingAcks[:0]
	return nil
}

func (p *Pipeline) resetLingerTimer(ctx context.Context) {
	if p.lingerTimer != nil {
		p.lingerTimer.Stop()
	}
	maxLinger := p.blockCfg.MaxLinger.Duration()
	if maxLinger == 0 {
		maxLinger = 30 * time.Second
	}
	p.lingerTimer = time.AfterFunc(maxLinger, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.builder != nil && p.builder.MessageCount() > 0 {
			if err := p.sealAndIngest(ctx); err != nil {
				p.logger.Error("linger flush error", zap.Error(err))
				return
			}
			p.builder = block.NewBuilder(p.streamCfg.Name, p.nextBlockID(), int64(p.blockCfg.TargetSize))
		}
	})
}

func (p *Pipeline) nextBlockID() uint64 {
	return p.nextID.Add(1) - 1
}

// Stream returns the stream name this pipeline is managing.
func (p *Pipeline) Stream() string {
	return p.streamCfg.Name
}

// Controller returns the tier controller.
func (p *Pipeline) Controller() *tier.Controller {
	return p.ctrl
}
