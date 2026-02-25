package nts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ObjStore wraps jetstream.ObjectStore with transparent cold storage fallback.
type ObjStore struct {
	obs     jetstream.ObjectStore
	bucket  string
	nc      *nats.Conn
	prefix  string
	timeout time.Duration
}

// Get retrieves an object by name.
// Falls back to sidecar if the object is not found in JetStream.
func (s *ObjStore) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	result, err := s.obs.Get(ctx, name)
	if err == nil {
		return result, nil
	}
	if !isNotFound(err) {
		return nil, err
	}

	// Fallback to sidecar — request reassembled object
	subject := fmt.Sprintf("%s.obj.%s.get.%s", s.prefix, s.bucket, name)
	resp, err := s.nc.Request(subject, nil, s.timeout)
	if err != nil {
		return nil, fmt.Errorf("nts: sidecar request for object %q: %w", name, err)
	}

	// Check if response is a JSON error
	if len(resp.Data) > 0 && resp.Data[0] == '{' {
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(resp.Data, &errResp) == nil && errResp.Error != "" {
			return nil, fmt.Errorf("nts: sidecar: %s", errResp.Error)
		}
	}

	return io.NopCloser(bytes.NewReader(resp.Data)), nil
}

// GetInfo retrieves object metadata.
// Falls back to sidecar if not found in JetStream.
func (s *ObjStore) GetInfo(ctx context.Context, name string) (*ObjInfo, error) {
	info, err := s.obs.GetInfo(ctx, name)
	if err == nil {
		return &ObjInfo{
			Name:    info.Name,
			Bucket:  info.Bucket,
			Size:    info.Size,
			Chunks:  uint32(info.Chunks),
			Digest:  info.Digest,
			Deleted: info.Deleted,
			ModTime: info.ModTime,
		}, nil
	}
	if !isNotFound(err) {
		return nil, err
	}

	// Fallback to sidecar
	subject := fmt.Sprintf("%s.obj.%s.info.%s", s.prefix, s.bucket, name)
	resp, err := s.nc.Request(subject, nil, s.timeout)
	if err != nil {
		return nil, fmt.Errorf("nts: sidecar request for object info %q: %w", name, err)
	}

	var result coldObjInfo
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("nts: decoding sidecar response: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("nts: sidecar: %s", result.Error)
	}

	return &ObjInfo{
		Name:    result.Name,
		Bucket:  result.Bucket,
		NUID:    result.NUID,
		Size:    result.Size,
		Chunks:  uint32(result.Chunks),
		Digest:  result.Digest,
		Deleted: result.Deleted,
		ModTime: result.ModTime,
	}, nil
}

// Put stores an object (delegates to JetStream).
func (s *ObjStore) Put(ctx context.Context, meta jetstream.ObjectMeta, reader io.Reader) (*jetstream.ObjectInfo, error) {
	return s.obs.Put(ctx, meta, reader)
}

// Delete deletes an object (delegates to JetStream).
func (s *ObjStore) Delete(ctx context.Context, name string) error {
	return s.obs.Delete(ctx, name)
}

// List returns all objects (combines JetStream and cold storage).
func (s *ObjStore) List(ctx context.Context) ([]*ObjInfo, error) {
	// Try JetStream first
	infos, err := s.obs.List(ctx)
	if err == nil && len(infos) > 0 {
		result := make([]*ObjInfo, 0, len(infos))
		for _, info := range infos {
			result = append(result, &ObjInfo{
				Name:    info.Name,
				Bucket:  info.Bucket,
				Size:    info.Size,
				Chunks:  uint32(info.Chunks),
				Digest:  info.Digest,
				Deleted: info.Deleted,
				ModTime: info.ModTime,
			})
		}
		return result, nil
	}

	// Fallback to sidecar
	subject := fmt.Sprintf("%s.obj.%s.list", s.prefix, s.bucket)
	resp, rErr := s.nc.Request(subject, nil, s.timeout)
	if rErr != nil {
		if err != nil {
			return nil, err
		}
		return nil, rErr
	}

	var coldList []coldObjListEntry
	if err := json.Unmarshal(resp.Data, &coldList); err != nil {
		return nil, fmt.Errorf("nts: decoding sidecar list response: %w", err)
	}

	result := make([]*ObjInfo, 0, len(coldList))
	for _, e := range coldList {
		result = append(result, &ObjInfo{
			Name:    e.Name,
			Bucket:  s.bucket,
			Size:    e.Size,
			Chunks:  uint32(e.Chunks),
			Digest:  e.Digest,
			Deleted: e.Deleted,
			ModTime: e.ModTime,
		})
	}
	return result, nil
}

// Underlying returns the wrapped jetstream.ObjectStore for direct access.
func (s *ObjStore) Underlying() jetstream.ObjectStore {
	return s.obs
}

// ObjInfo represents object metadata.
type ObjInfo struct {
	Name    string
	Bucket  string
	NUID    string
	Size    uint64
	Chunks  uint32
	Digest  string
	Deleted bool
	ModTime time.Time
}

type coldObjInfo struct {
	Error   string    `json:"error,omitempty"`
	Name    string    `json:"name"`
	Bucket  string    `json:"bucket"`
	NUID    string    `json:"nuid"`
	Size    uint64    `json:"size"`
	Chunks  int       `json:"chunks"`
	Digest  string    `json:"digest"`
	Deleted bool      `json:"deleted"`
	ModTime time.Time `json:"modtime"`
}

type coldObjListEntry struct {
	Name    string    `json:"name"`
	Size    uint64    `json:"size"`
	Chunks  int       `json:"chunks"`
	Digest  string    `json:"digest"`
	Deleted bool      `json:"deleted"`
	ModTime time.Time `json:"modtime"`
}
