package serve

import (
	"fmt"
	"net/http"

	"go.uber.org/zap"
)

// registerObjRoutes registers Object Store HTTP API routes.
func (h *handler) registerObjRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/objects/{bucket}/get/{name...}", h.handleObjGet)
	mux.HandleFunc("GET /v1/objects/{bucket}/info/{name...}", h.handleObjInfo)
	mux.HandleFunc("GET /v1/objects/{bucket}/list", h.handleObjList)
}

func (h *handler) handleObjGet(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	name := r.PathValue("name")

	stream, p := h.resolveObjBucket(bucket)
	if p == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Object Store bucket not found"})
		return
	}

	entry, err := h.meta.LookupObj(r.Context(), stream, name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	if entry.Deleted {
		writeJSON(w, http.StatusGone, map[string]string{"error": fmt.Sprintf("object %q has been deleted", name)})
		return
	}

	chunks, err := h.meta.LookupObjChunks(r.Context(), stream, entry.NUID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Stream the object as binary
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Nts-Object-Name", entry.Name)
	w.Header().Set("X-Nts-Object-Size", fmt.Sprintf("%d", entry.Size))
	w.Header().Set("X-Nts-Object-Digest", entry.Digest)
	w.Header().Set("X-Nts-Object-Chunks", fmt.Sprintf("%d", entry.Chunks))

	for _, seq := range chunks.ChunkSeqs {
		stored, err := p.Controller().Retrieve(r.Context(), seq)
		if err != nil {
			h.logger.Error("failed to retrieve chunk",
				zap.Uint64("seq", seq),
				zap.Error(err),
			)
			return
		}
		if _, err := w.Write(stored.Data); err != nil {
			return
		}
	}
}

func (h *handler) handleObjInfo(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	name := r.PathValue("name")

	stream, _ := h.resolveObjBucket(bucket)
	if stream == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Object Store bucket not found"})
		return
	}

	entry, err := h.meta.LookupObj(r.Context(), stream, name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":    entry.Name,
		"bucket":  entry.Bucket,
		"nuid":    entry.NUID,
		"size":    entry.Size,
		"chunks":  entry.Chunks,
		"digest":  entry.Digest,
		"deleted": entry.Deleted,
		"modtime": entry.ModTime,
	})
}

func (h *handler) handleObjList(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")

	stream, _ := h.resolveObjBucket(bucket)
	if stream == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Object Store bucket not found"})
		return
	}

	entries, err := h.meta.ListObjects(r.Context(), stream)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	var objects []map[string]interface{}
	for _, e := range entries {
		objects = append(objects, map[string]interface{}{
			"name":    e.Name,
			"size":    e.Size,
			"chunks":  e.Chunks,
			"digest":  e.Digest,
			"deleted": e.Deleted,
			"modtime": e.ModTime,
		})
	}

	writeJSON(w, http.StatusOK, objects)
}

