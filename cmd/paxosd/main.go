// Command paxosd runs one node of the sharded transaction store. The node
// hosts one replica of every shard listed in the cluster config file:
//
//	paxosd -config cluster.json -id 1 -data-dir ./data/1
//
// It serves both the peer (Paxos and two-phase commit) and the client
// (Txn) gRPC services on its address from the config, or on -listen if
// given. SIGTERM or SIGINT shuts it down gracefully.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("paxosd: %v", err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("paxosd", flag.ContinueOnError)
	config := fs.String("config", "", "cluster config file (JSON: shards and every node's id and addr)")
	id := fs.Int("id", 0, "this node's ID (must appear in the config)")
	listen := fs.String("listen", "", "listen address (default: this node's addr in the config)")
	dataDir := fs.String("data-dir", "", "directory for this node's SQLite files, one per shard")
	tick := fs.Duration("tick", server.DefaultTick, "wall-clock length of one logical Paxos tick")
	rpcTimeout := fs.Duration("rpc-timeout", server.DefaultRPCTimeout, "deadline for each peer RPC")
	inDoubt := fs.Duration("in-doubt-wait", server.DefaultInDoubtWait,
		"how long a prepared transaction waits for its coordinator's decision before asking for it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *config == "" || *id <= 0 || *dataDir == "" {
		return fmt.Errorf("-config, -id and -data-dir are required")
	}
	if *inDoubt <= 0 || *tick <= 0 {
		return fmt.Errorf("-in-doubt-wait and -tick must be positive")
	}
	cl, err := server.LoadCluster(*config)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	n, err := server.Start(server.Config{
		ID: paxos.NodeID(*id), Cluster: cl, Listen: *listen, DataDir: *dataDir,
		Tick: *tick, RPCTimeout: *rpcTimeout, InDoubtWait: *inDoubt,
	})
	if err != nil {
		return err
	}
	log.Printf("paxosd: node %d serving %d shards on %s (tick %v)", *id, n.Shards(), n.Addr(), *tick)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	<-ctx.Done()
	log.Printf("paxosd: node %d shutting down", *id)
	start := time.Now()
	n.Stop()
	log.Printf("paxosd: node %d stopped in %v", *id, time.Since(start).Round(time.Millisecond))
	return nil
}
