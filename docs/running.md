# Running a cluster

Phase 4 turns the replicated log into real processes: `paxosd` is one node, the `client` package is the Go SDK. There is one shard and no TLS yet.

## Build

```sh
go build -o bin/paxosd ./cmd/paxosd
```

## Start three nodes locally

Each node needs its ID, the address of every node (including itself), and its own data directory. Run each command in its own terminal:

```sh
PEERS=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003

bin/paxosd -id 1 -peers $PEERS -data-dir data/1
bin/paxosd -id 2 -peers $PEERS -data-dir data/2
bin/paxosd -id 3 -peers $PEERS -data-dir data/3
```

Within a few hundred milliseconds one node wins the election. Each node serves both the peer service (Paxos messages) and the client service (Get and Put) on its address.

| Flag | Default | Meaning |
|---|---|---|
| `-id` | required | this node's ID, must appear in `-peers` |
| `-peers` | required | every node as `id=host:port`, comma separated, identical on all nodes |
| `-data-dir` | required | where `node-<id>.db` (SQLite) lives |
| `-listen` | the node's address in `-peers` | address to bind, if different (for example `0.0.0.0:7001`) |
| `-tick` | `10ms` | wall-clock length of one logical Paxos tick |
| `-rpc-timeout` | `200ms` | deadline for each peer RPC |

With the default tick, the leader sends a heartbeat every 40 ms and a follower starts an election after 200 to 400 ms without one (4 ticks and 20 to 40 ticks; see DECISIONS.md entry 15).

Stop a node with Ctrl-C or `kill <pid>` (SIGTERM). It stops accepting requests, waits up to 2 seconds for in-flight ones, stops its event loop and closes its database. Start it again with the same flags and it recovers from its data directory (see docs/storage.md) and catches up from the leader.

## Use the SDK

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Shrutij516/paxos-txn-store/client"
)

func main() {
	c, err := client.New([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}, client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Put(ctx, "greeting", "hello"); err != nil {
		log.Fatal(err)
	}
	v, err := c.Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(v) // hello
}
```

What the SDK does for you:

- **Leader discovery.** Any node can be contacted. A follower answers "not leader" with the leader's address, and the client retries there at once. The client remembers the leader until a request to it fails.
- **Retries.** Each attempt has a deadline (`AttemptTimeout`, default 1 s). After a failure or timeout the client moves to the next node and waits a randomized, exponentially growing backoff (20 ms up to 500 ms). It keeps going until the call's context ends.
- **Exactly once.** The client picks a random client ID and numbers its operations. Every retry of an operation reuses its sequence number, and the servers' dedup table applies it at most once, returning the original result to later retries.
- **One request at a time.** Calls on one `Client` are serialized, because dedup assumes one outstanding request per client (see docs/multipaxos.md). For concurrency, create one `Client` per goroutine.

Reads go through the log like writes, so `Get` is linearizable.

## Regenerating protobuf code

The generated files in `proto/` are committed. After editing a `.proto` file, install `protoc` (29.x), then:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
make proto
```

## How it fits together

- `proto/paxos/v1`: peer messages, one `Envelope` with a `oneof` per Paxos message type, and the `Peer` service.
- `proto/kv/v1`: the client `KV` service.
- `internal/wire`: converts between the protobuf types and the plain Go structs in `internal/paxos`. The Paxos core never sees protobuf.
- `internal/transport/grpc.go`: implements `paxos.Transport`. Each peer has a bounded queue drained by one goroutine that sends one unary RPC per message under a deadline. A full queue drops messages, so a slow or dead peer never blocks the node.
- `internal/server`: one node. A single event-loop goroutine owns the replica and the key-value store. gRPC handlers, the ticker and the transport only send it events over channels.
- `cmd/paxosd`: flags, signal handling.
- `client`: the SDK.
