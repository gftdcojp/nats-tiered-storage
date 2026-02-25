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
// The client first attempts a native JetStream operation (direct get, KV get, etc.).
// If the data is not found (purged, expired, or deleted), it falls back to a
// NATS request-reply call to the nats-tiered-storage sidecar, which retrieves
// the data from whichever tier currently holds it.
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
