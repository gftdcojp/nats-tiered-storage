package serve

import (
	"net/http"
)

// registerKVRoutes registers KV Store HTTP API routes.
func (h *handler) registerKVRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/kv/{bucket}/get/{key...}", h.handleKVGet)
	mux.HandleFunc("GET /v1/kv/{bucket}/history/{key...}", h.handleKVHistory)
	mux.HandleFunc("GET /v1/kv/{bucket}/keys", h.handleKVListKeys)
}

func (h *handler) handleKVGet(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	stream, p := h.resolveKVBucket(bucket)
	if p == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "KV bucket not found"})
		return
	}

	entry, err := h.meta.LookupKVKey(r.Context(), stream, key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	stored, err := p.Controller().Retrieve(r.Context(), entry.LastSequence)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"bucket":    entry.Bucket,
		"key":       entry.Key,
		"value":     string(stored.Data),
		"sequence":  entry.LastSequence,
		"revision":  entry.Revision,
		"operation": entry.Operation,
		"timestamp": stored.Timestamp,
	})
}

func (h *handler) handleKVHistory(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	stream, p := h.resolveKVBucket(bucket)
	if p == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "KV bucket not found"})
		return
	}

	revs, err := h.meta.ListKVKeyRevisions(r.Context(), stream, key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	var entries []map[string]interface{}
	for _, rev := range revs {
		stored, err := p.Controller().Retrieve(r.Context(), rev.Sequence)
		if err != nil {
			continue
		}
		entries = append(entries, map[string]interface{}{
			"sequence":  rev.Sequence,
			"block_id":  rev.BlockID,
			"value":     string(stored.Data),
			"timestamp": stored.Timestamp,
		})
	}

	writeJSON(w, http.StatusOK, entries)
}

func (h *handler) handleKVListKeys(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	prefix := r.URL.Query().Get("prefix")

	stream, _ := h.resolveKVBucket(bucket)
	if stream == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "KV bucket not found"})
		return
	}

	entries, err := h.meta.ListKVKeys(r.Context(), stream, prefix)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Operation != "DEL" && e.Operation != "PURGE" {
			keys = append(keys, e.Key)
		}
	}

	writeJSON(w, http.StatusOK, keys)
}
