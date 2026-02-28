package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
)

var version = "dev"

func main() {
	addr := flag.String("addr", "http://localhost:8080", "nats-tiered-storage API address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "version":
		fmt.Printf("nts-ctl %s\n", version)
	case "status":
		cmdStatus(*addr)
	case "streams":
		cmdStreams(*addr)
	case "stream":
		if len(args) < 3 || args[1] != "info" {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl stream info <name>")
			os.Exit(1)
		}
		cmdStreamInfo(*addr, args[2])
	case "blocks":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl blocks <stream>")
			os.Exit(1)
		}
		cmdBlocks(*addr, args[1])
	case "demote":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl demote <stream> <blockID>")
			os.Exit(1)
		}
		cmdDemote(*addr, args[1], args[2])
	case "promote":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl promote <stream> <blockID>")
			os.Exit(1)
		}
		cmdPromote(*addr, args[1], args[2])
	case "kv":
		cmdKV(*addr, args[1:])
	case "obj":
		cmdObj(*addr, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `nts-ctl - NATS Tiered Storage management CLI

Usage:
  nts-ctl [flags] <command> [args]

Commands:
  status                        Show overall status
  streams                       List managed streams
  stream info <name>            Show tier breakdown for a stream
  blocks <stream>               List all blocks with tier info
  demote <stream> <id>          Force-demote a specific block
  promote <stream> <id>         Force-promote a specific block
  kv get <bucket> <key>         Get a KV value from cold storage
  kv keys <bucket> [prefix]     List KV keys in cold storage
  kv history <bucket> <key>     Show key revision history
  kv restore <bucket> <key>     Restore a KV key from cold storage back to hot tier
  obj get <bucket> <name>       Get an object from cold storage (to stdout)
  obj info <bucket> <name>      Show object metadata
  obj list <bucket>             List objects in cold storage
  version                       Show version

Flags:
  -addr string   API address (default "http://localhost:8080")`)
}

func cmdStatus(addr string) {
	resp, err := http.Get(addr + "/v1/status")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	printJSON(resp.Body)
}

func cmdStreams(addr string) {
	resp, err := http.Get(addr + "/v1/streams")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var streams []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&streams); err != nil {
		fmt.Fprintf(os.Stderr, "error decoding response: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tBLOCKS\tMEMORY\tFILE\tBLOB")
	for _, s := range streams {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\n",
			s["name"], s["total_blocks"], s["memory_blocks"], s["file_blocks"], s["blob_blocks"])
	}
	w.Flush()
}

func cmdStreamInfo(addr, name string) {
	resp, err := http.Get(addr + "/v1/streams/" + name + "/stats")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	printJSON(resp.Body)
}

func cmdBlocks(addr, stream string) {
	resp, err := http.Get(addr + "/v1/blocks/" + stream)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var blocks []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		fmt.Fprintf(os.Stderr, "error decoding response: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BLOCK_ID\tFIRST_SEQ\tLAST_SEQ\tMSGS\tSIZE\tTIER\tAGE")
	for _, b := range blocks {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			b["block_id"], b["first_seq"], b["last_seq"],
			b["msg_count"], b["size_bytes"], b["tier"], b["age"])
	}
	w.Flush()
}

func cmdDemote(addr, stream, blockID string) {
	resp, err := http.Post(addr+"/v1/admin/demote/"+stream+"/"+blockID, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	printJSON(resp.Body)
}

func cmdPromote(addr, stream, blockID string) {
	resp, err := http.Post(addr+"/v1/admin/promote/"+stream+"/"+blockID, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	printJSON(resp.Body)
}

func cmdKV(addr string, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: nts-ctl kv <get|keys|history|restore> <bucket> [key|prefix]")
		os.Exit(1)
	}
	op := args[0]
	bucket := args[1]

	switch op {
	case "get":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl kv get <bucket> <key>")
			os.Exit(1)
		}
		resp, err := http.Get(addr + "/v1/kv/" + bucket + "/get/" + args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printJSON(resp.Body)

	case "keys":
		url := addr + "/v1/kv/" + bucket + "/keys"
		if len(args) >= 3 {
			url += "?prefix=" + args[2]
		}
		resp, err := http.Get(url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		var keys []string
		if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
			fmt.Fprintf(os.Stderr, "error decoding response: %v\n", err)
			os.Exit(1)
		}
		for _, k := range keys {
			fmt.Println(k)
		}

	case "history":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl kv history <bucket> <key>")
			os.Exit(1)
		}
		resp, err := http.Get(addr + "/v1/kv/" + bucket + "/history/" + args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printJSON(resp.Body)

	case "restore":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl kv restore <bucket> <key>")
			os.Exit(1)
		}
		resp, err := http.Post(addr+"/v1/kv/"+bucket+"/restore/"+args[2], "", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printJSON(resp.Body)

	default:
		fmt.Fprintf(os.Stderr, "unknown kv command: %s\n", op)
		os.Exit(1)
	}
}

func cmdObj(addr string, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: nts-ctl obj <get|info|list> <bucket> [name]")
		os.Exit(1)
	}
	op := args[0]
	bucket := args[1]

	switch op {
	case "get":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl obj get <bucket> <name>")
			os.Exit(1)
		}
		resp, err := http.Get(addr + "/v1/objects/" + bucket + "/get/" + args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.Header.Get("Content-Type") == "application/octet-stream" {
			io.Copy(os.Stdout, resp.Body)
		} else {
			printJSON(resp.Body)
		}

	case "info":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: nts-ctl obj info <bucket> <name>")
			os.Exit(1)
		}
		resp, err := http.Get(addr + "/v1/objects/" + bucket + "/info/" + args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printJSON(resp.Body)

	case "list":
		resp, err := http.Get(addr + "/v1/objects/" + bucket + "/list")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		var objects []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&objects); err != nil {
			fmt.Fprintf(os.Stderr, "error decoding response: %v\n", err)
			os.Exit(1)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tSIZE\tCHUNKS\tDIGEST\tDELETED")
		for _, o := range objects {
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\n",
				o["name"], o["size"], o["chunks"], o["digest"], o["deleted"])
		}
		w.Flush()

	default:
		fmt.Fprintf(os.Stderr, "unknown obj command: %s\n", op)
		os.Exit(1)
	}
}

func printJSON(r io.Reader) {
	var v interface{}
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		fmt.Fprintf(os.Stderr, "error decoding response: %v\n", err)
		return
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
