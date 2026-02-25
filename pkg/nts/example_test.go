package nts_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/gftdcojp/nats-tiered-storage/pkg/nts"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func Example() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	client, err := nts.New(nts.Config{
		NC: nc,
		JS: js,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Retrieve a message — falls back to cold storage if not in JetStream
	msg, err := client.GetMessage(ctx, "ORDERS", 42)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("seq=%d data=%s\n", msg.Sequence, msg.Data)
}

func ExampleClient_KeyValue() {
	nc, _ := nats.Connect("nats://localhost:4222")
	js, _ := jetstream.New(nc)
	client, _ := nts.New(nts.Config{NC: nc, JS: js})
	ctx := context.Background()

	// Wrap a KV bucket with transparent cold fallback
	kv, err := client.KeyValue(ctx, "config")
	if err != nil {
		log.Fatal(err)
	}

	// Get returns from JetStream first, falls back to sidecar
	entry, err := kv.Get(ctx, "app.port")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("key=%s value=%s revision=%d\n",
		entry.Key(), entry.Value(), entry.Revision())

	// Put/Delete delegate directly to JetStream
	kv.Put(ctx, "app.port", []byte("9090"))

	// Keys combines JetStream and cold storage
	keys, _ := kv.Keys(ctx)
	fmt.Println("keys:", keys)
}

func ExampleClient_ObjectStore() {
	nc, _ := nats.Connect("nats://localhost:4222")
	js, _ := jetstream.New(nc)
	client, _ := nts.New(nts.Config{NC: nc, JS: js})
	ctx := context.Background()

	// Wrap an Object Store bucket with transparent cold fallback
	obs, err := client.ObjectStore(ctx, "files")
	if err != nil {
		log.Fatal(err)
	}

	// Get reassembles chunks from cold storage if not in JetStream
	reader, err := obs.Get(ctx, "report.pdf")
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	fmt.Printf("object size: %d bytes\n", len(data))
}

func ExampleNew() {
	nc, _ := nats.Connect("nats://localhost:4222")
	js, _ := jetstream.New(nc)

	// Default configuration (prefix: "nts", timeout: 5s)
	client, _ := nts.New(nts.Config{NC: nc, JS: js})
	_ = client

	// Custom configuration
	client, _ = nts.New(nts.Config{
		NC:            nc,
		JS:            js,
		SubjectPrefix: "myapp.cold", // custom sidecar subject prefix
		Timeout:       10 * time.Second,
	})
	_ = client
}
