// Command paxosd runs one node of the replicated key-value store.
//
//	paxosd -id 1 -peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data-dir ./data/1
//
// It serves both the peer (Paxos) and the client (KV) gRPC services on its
// address from -peers, or on -listen if given. SIGTERM or SIGINT shuts it
// down gracefully.
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
	id := fs.Int("id", 0, "this node's ID (must appear in -peers)")
	peers := fs.String("peers", "", "all nodes as id=host:port, comma separated")
	listen := fs.String("listen", "", "listen address (default: this node's address in -peers)")
	dataDir := fs.String("data-dir", "", "directory for this node's SQLite database")
	tick := fs.Duration("tick", server.DefaultTick, "wall-clock length of one logical Paxos tick")
	rpcTimeout := fs.Duration("rpc-timeout", server.DefaultRPCTimeout, "deadline for each peer RPC")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id <= 0 || *dataDir == "" {
		return fmt.Errorf("-id and -data-dir are required")
	}
	pm, err := server.ParsePeers(*peers)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	n, err := server.Start(server.Config{
		ID: paxos.NodeID(*id), Peers: pm, Listen: *listen, DataDir: *dataDir,
		Tick: *tick, RPCTimeout: *rpcTimeout,
	})
	if err != nil {
		return err
	}
	log.Printf("paxosd: node %d serving on %s (tick %v)", *id, n.Addr(), *tick)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	<-ctx.Done()
	log.Printf("paxosd: node %d shutting down", *id)
	start := time.Now()
	n.Stop()
	log.Printf("paxosd: node %d stopped in %v", *id, time.Since(start).Round(time.Millisecond))
	return nil
}
