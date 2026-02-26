// Package nts provides a transparent client for NATS JetStream with automatic
// fallback to cold (tiered) storage via the nats-tiered-storage sidecar.
//
// When data has been moved from JetStream to tiered storage (Memory → File → S3),
// this client seamlessly retrieves it by falling back to the sidecar's
// NATS request-reply interface.
//
// # Installation
//
//	go get github.com/gftdcojp/nats-tiered-storage/pkg/nts
//
// # Basic Usage
//
//	nc, _ := nats.Connect("nats://localhost:4222")
//	js, _ := jetstream.New(nc)
//
//	client, _ := nts.New(nts.Config{NC: nc, JS: js})
//
//	// KV Store — falls back to sidecar when key is purged from JetStream
//	kv, _ := client.KeyValue(ctx, "config")
//	entry, _ := kv.Get(ctx, "app.port")
//	fmt.Println(string(entry.Value()))
//
//	// Object Store — reassembles chunks from cold storage
//	obs, _ := client.ObjectStore(ctx, "files")
//	reader, _ := obs.Get(ctx, "report.pdf")
//	io.Copy(dst, reader)
//
//	// Direct message retrieval
//	msg, _ := client.GetMessage(ctx, "ORDERS", 42)
//
// # Architecture
//
// The sidecar uses a write-through fan-out strategy: each ingested block is
// written to all enabled tiers simultaneously (Memory, File, and/or S3). Reads
// fall through from the hottest to the coldest tier until a hit is found. Policy-
// based eviction removes blocks from hotter tiers over time, while colder copies
// remain intact. This eliminates data-copy during demotion and provides automatic
// fallthrough when a tier has evicted a block (e.g. memory LRU eviction).
//
// The client first attempts a native JetStream operation (direct get, KV get, etc.).
// If the data is not found (purged, expired, or deleted), it falls back to a
// NATS request-reply call to the sidecar, which retrieves the data from whichever
// tier currently holds it.
//
// The sidecar handles WorkQueue retention streams transparently: when a WorkQueue
// stream already has consumers, the sidecar auto-creates a Limits-retention mirror
// (NTS_MIRROR_{stream}) and consumes from it to avoid consumer conflicts.
//
// The subject prefix for sidecar requests defaults to "nts" and can be configured
// via [Config.SubjectPrefix].
//
// # Sidecar Subjects
//
//	nts.get.{stream}.{seq}           — retrieve message by sequence
//	nts.kv.{bucket}.get.{key}        — get KV value
//	nts.kv.{bucket}.keys             — list KV keys
//	nts.kv.{bucket}.history.{key}    — get KV key history
//	nts.obj.{bucket}.get.{name}      — get object (reassembled)
//	nts.obj.{bucket}.info.{name}     — get object metadata
//	nts.obj.{bucket}.list            — list objects
package nts
