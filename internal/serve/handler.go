package serve

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/internal/config"
	"github.com/gftdcojp/nats-tiered-storage/internal/ingest"
	"github.com/gftdcojp/nats-tiered-storage/internal/meta"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type handler struct {
	pipelines  map[string]*ingest.Pipeline
	meta       meta.Store
	streamCfgs []config.StreamConfig
	js         jetstream.JetStream
	logger     *zap.Logger
}

// RunHTTP starts the HTTP API server.
func RunHTTP(ctx context.Context, cfg config.APIConfig, pipelines []*ingest.Pipeline, streamCfgs []config.StreamConfig, metaStore meta.Store, js jetstream.JetStream, logger *zap.Logger) error {
	pipeMap := make(map[string]*ingest.Pipeline)
	for _, p := range pipelines {
		pipeMap[p.Stream()] = p
	}

	h := &handler{
		pipelines:  pipeMap,
		meta:       metaStore,
		streamCfgs: streamCfgs,
		js:         js,
		logger:     logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", h.handleStatus)
	mux.HandleFunc("GET /v1/streams", h.handleStreams)
	mux.HandleFunc("GET /v1/streams/{stream}/stats", h.handleStreamStats)
	mux.HandleFunc("GET /v1/messages/{stream}/{seq}", h.handleGetMessage)
	mux.HandleFunc("GET /v1/messages/{stream}", h.handleGetMessages)
	mux.HandleFunc("GET /v1/blocks/{stream}", h.handleListBlocks)
	mux.HandleFunc("GET /v1/blocks/{stream}/{blockID}", h.handleGetBlock)
	mux.HandleFunc("POST /v1/admin/demote/{stream}/{blockID}", h.handleDemote)
	mux.HandleFunc("POST /v1/admin/promote/{stream}/{blockID}", h.handlePromote)

	// KV and Object Store routes
	h.registerKVRoutes(mux)
	h.registerObjRoutes(mux)

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	logger.Info("HTTP API listening", zap.String("addr", cfg.Listen))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (h *handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"status":  "ok",
		"streams": len(h.pipelines),
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *handler) handleStreams(w http.ResponseWriter, r *http.Request) {
	var streams []map[string]interface{}
	for name := range h.pipelines {
		blocks, _ := h.meta.ListBlocks(r.Context(), name, nil)
		var memCount, fileCount, blobCount int
		for _, b := range blocks {
			for _, t := range b.EffectiveTiers() {
				switch t {
				case 0:
					memCount++
				case 1:
					fileCount++
				case 2:
					blobCount++
				}
			}
		}
		streams = append(streams, map[string]interface{}{
			"name":          name,
			"total_blocks":  len(blocks),
			"memory_blocks": memCount,
			"file_blocks":   fileCount,
			"blob_blocks":   blobCount,
		})
	}
	writeJSON(w, http.StatusOK, streams)
}

func (h *handler) handleStreamStats(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	p, ok := h.pipelines[stream]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	blocks, _ := h.meta.ListBlocks(r.Context(), stream, nil)
	var memBytes, fileBytes, blobBytes int64
	var memCount, fileCount, blobCount int
	for _, b := range blocks {
		for _, t := range b.EffectiveTiers() {
			switch t {
			case 0:
				memCount++
				memBytes += b.SizeBytes
			case 1:
				fileCount++
				fileBytes += b.SizeBytes
			case 2:
				blobCount++
				blobBytes += b.SizeBytes
			}
		}
	}

	_ = p // used for stream verification
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"stream": stream,
		"tiers": map[string]interface{}{
			"memory": map[string]interface{}{"blocks": memCount, "bytes": memBytes},
			"file":   map[string]interface{}{"blocks": fileCount, "bytes": fileBytes},
			"blob":   map[string]interface{}{"blocks": blobCount, "bytes": blobBytes},
		},
		"total_blocks": len(blocks),
	})
}

func (h *handler) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	seqStr := r.PathValue("seq")
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid sequence"})
		return
	}

	p, ok := h.pipelines[stream]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	msg, err := p.Controller().Retrieve(r.Context(), seq)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"stream":    msg.Stream,
		"subject":   msg.Subject,
		"sequence":  msg.Sequence,
		"data":      string(msg.Data),
		"headers":   msg.Headers,
		"timestamp": msg.Timestamp,
	})
}

func (h *handler) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	p, ok := h.pipelines[stream]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	startStr := r.URL.Query().Get("start")
	countStr := r.URL.Query().Get("count")

	start, _ := strconv.ParseUint(startStr, 10, 64)
	count, _ := strconv.ParseUint(countStr, 10, 64)
	if count == 0 {
		count = 100
	}

	msgs, err := p.Controller().RetrieveRange(r.Context(), start, start+count-1)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	var result []map[string]interface{}
	for _, msg := range msgs {
		result = append(result, map[string]interface{}{
			"sequence":  msg.Sequence,
			"subject":   msg.Subject,
			"data":      string(msg.Data),
			"timestamp": msg.Timestamp,
		})
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) handleListBlocks(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	blocks, err := h.meta.ListBlocks(r.Context(), stream, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	var result []map[string]interface{}
	for _, b := range blocks {
		tierNames := make([]string, 0, len(b.EffectiveTiers()))
		for _, t := range b.EffectiveTiers() {
			tierNames = append(tierNames, t.String())
		}
		result = append(result, map[string]interface{}{
			"block_id":   b.BlockID,
			"first_seq":  b.FirstSeq,
			"last_seq":   b.LastSeq,
			"msg_count":  b.MsgCount,
			"size_bytes": b.SizeBytes,
			"tier":       b.CurrentTier.String(),
			"tiers":      tierNames,
			"created_at": b.CreatedAt,
			"age":        time.Since(b.CreatedAt).String(),
		})
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	blockIDStr := r.PathValue("blockID")
	blockID, err := strconv.ParseUint(blockIDStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid block ID"})
		return
	}

	entry, err := h.meta.GetBlock(r.Context(), stream, blockID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, entry)
}

func (h *handler) handleDemote(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	blockIDStr := r.PathValue("blockID")
	blockID, err := strconv.ParseUint(blockIDStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid block ID"})
		return
	}

	p, ok := h.pipelines[stream]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	entry, err := h.meta.GetBlock(r.Context(), stream, blockID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	tiers := entry.EffectiveTiers()
	if len(tiers) <= 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "block exists in only one tier, cannot evict"})
		return
	}

	hottest := tiers[0]
	nextTier := tiers[1] // the tier it will fall through to after eviction
	if err := p.Controller().Demote(r.Context(), blockID, hottest, nextTier); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "evicted", "from": hottest.String()})
}

func (h *handler) handlePromote(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	blockIDStr := r.PathValue("blockID")
	blockID, err := strconv.ParseUint(blockIDStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid block ID"})
		return
	}

	p, ok := h.pipelines[stream]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	entry, err := h.meta.GetBlock(r.Context(), stream, blockID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	tiers := entry.EffectiveTiers()
	hottest := tiers[0]
	if hottest == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "block is already in hottest tier (memory)"})
		return
	}

	prevTier := hottest - 1
	if err := p.Controller().Promote(r.Context(), blockID, hottest, prevTier); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "promoted", "to": prevTier.String()})
}

// resolveKVBucket finds the stream name and pipeline for a KV bucket name.
func (h *handler) resolveKVBucket(bucket string) (string, *ingest.Pipeline) {
	for _, sc := range h.streamCfgs {
		if sc.ResolvedType() == config.StreamTypeKV && sc.ResolvedKVBucket() == bucket {
			return sc.Name, h.pipelines[sc.Name]
		}
	}
	return "", nil
}

// resolveObjBucket finds the stream name and pipeline for an Object Store bucket name.
func (h *handler) resolveObjBucket(bucket string) (string, *ingest.Pipeline) {
	for _, sc := range h.streamCfgs {
		if sc.ResolvedType() == config.StreamTypeObjectStore && sc.ResolvedObjBucket() == bucket {
			return sc.Name, h.pipelines[sc.Name]
		}
	}
	return "", nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
