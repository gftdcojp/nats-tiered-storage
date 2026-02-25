package ingest

import (
	"testing"
)

func TestParseObjMetaSubject(t *testing.T) {
	tests := []struct {
		subject string
		bucket  string
		name    string
		ok      bool
	}{
		{"$O.files.M.report.pdf", "files", "report.pdf", true},
		{"$O.mystore.M.doc", "mystore", "doc", true},
		{"$O.files.C.abc123", "", "", false}, // chunk, not meta
		{"$O.files.X.abc", "", "", false},    // unknown marker
		{"$O.files", "", "", false},          // incomplete
		{"ORDERS.new", "", "", false},        // not obj subject
	}

	for _, tt := range tests {
		bucket, name, ok := ParseObjMetaSubject(tt.subject)
		if ok != tt.ok {
			t.Errorf("ParseObjMetaSubject(%q) ok = %v, want %v", tt.subject, ok, tt.ok)
			continue
		}
		if ok {
			if bucket != tt.bucket {
				t.Errorf("ParseObjMetaSubject(%q) bucket = %q, want %q", tt.subject, bucket, tt.bucket)
			}
			if name != tt.name {
				t.Errorf("ParseObjMetaSubject(%q) name = %q, want %q", tt.subject, name, tt.name)
			}
		}
	}
}

func TestParseObjChunkSubject(t *testing.T) {
	tests := []struct {
		subject string
		bucket  string
		nuid    string
		ok      bool
	}{
		{"$O.files.C.abc123", "files", "abc123", true},
		{"$O.mystore.C.xyz", "mystore", "xyz", true},
		{"$O.files.M.report", "", "", false}, // meta, not chunk
		{"$O.files", "", "", false},          // incomplete
		{"ORDERS.new", "", "", false},        // not obj subject
	}

	for _, tt := range tests {
		bucket, nuid, ok := ParseObjChunkSubject(tt.subject)
		if ok != tt.ok {
			t.Errorf("ParseObjChunkSubject(%q) ok = %v, want %v", tt.subject, ok, tt.ok)
			continue
		}
		if ok {
			if bucket != tt.bucket {
				t.Errorf("ParseObjChunkSubject(%q) bucket = %q, want %q", tt.subject, bucket, tt.bucket)
			}
			if nuid != tt.nuid {
				t.Errorf("ParseObjChunkSubject(%q) nuid = %q, want %q", tt.subject, nuid, tt.nuid)
			}
		}
	}
}
