// Package testcluster starts an in-process cluster of server nodes on
// localhost for tests.
package testcluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// Cluster is a set of nodes, each with its own data dir, each hosting one
// replica of every shard.
type Cluster struct {
	t     testing.TB
	cfgs  map[paxos.NodeID]server.Config
	Nodes map[paxos.NodeID]*server.Node // nil while stopped
	IDs   []paxos.NodeID
	Addrs []string // in ID order
}

// Start launches n nodes with the given number of shards. opts, if not
// nil, adjusts each config before start.
func Start(t testing.TB, n, shards int, opts func(*server.Config)) *Cluster {
	t.Helper()
	c := &Cluster{t: t, cfgs: map[paxos.NodeID]server.Config{}, Nodes: map[paxos.NodeID]*server.Node{}}
	cl := server.Cluster{Shards: shards}
	lis := map[paxos.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := paxos.NodeID(i)
		lis[id] = l
		cl.Nodes = append(cl.Nodes, server.NodeConfig{ID: id, Addr: l.Addr().String()})
		c.IDs = append(c.IDs, id)
		c.Addrs = append(c.Addrs, l.Addr().String())
	}
	for _, id := range c.IDs {
		cfg := server.Config{ID: id, Cluster: cl, DataDir: t.TempDir(), Tick: 5 * time.Millisecond}
		if opts != nil {
			opts(&cfg)
		}
		c.cfgs[id] = cfg
		cfg.Listener = lis[id]
		node, err := server.Start(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.Nodes[id] = node
	}
	t.Cleanup(c.StopAll)
	return c
}

// Stop stops one node; its data dir is kept.
func (c *Cluster) Stop(id paxos.NodeID) {
	if n := c.Nodes[id]; n != nil {
		n.Stop()
		c.Nodes[id] = nil
	}
}

// Restart starts a stopped node again on the same address and data dir.
func (c *Cluster) Restart(id paxos.NodeID) {
	c.t.Helper()
	var err error
	// The port was just released; retry briefly in case it is still busy.
	for i := 0; i < 50; i++ {
		var n *server.Node
		if n, err = server.Start(c.cfgs[id]); err == nil {
			c.Nodes[id] = n
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("restart node %d: %v", id, err)
}

// StopAll stops every running node.
func (c *Cluster) StopAll() {
	for _, id := range c.IDs {
		c.Stop(id)
	}
}

// Leader waits until exactly one running node believes it leads shard sh
// and returns its ID.
func (c *Cluster) Leader(ctx context.Context, sh txn.ShardID) (paxos.NodeID, error) {
	for {
		var leaders []paxos.NodeID
		for _, id := range c.IDs {
			if n := c.Nodes[id]; n != nil {
				var is bool
				n.Inspect(sh, func(_ *paxos.Replica, _ *txn.SM, ts *txn.Server) { is = ts.Leading() })
				if is {
					leaders = append(leaders, id)
				}
			}
		}
		if len(leaders) == 1 {
			return leaders[0], nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Addr returns a node's address.
func (c *Cluster) Addr(id paxos.NodeID) string { return c.cfgs[id].Cluster.Addrs()[id] }

// Commit returns a running node's commit index for shard sh.
func (c *Cluster) Commit(id paxos.NodeID, sh txn.ShardID) uint64 {
	var commit uint64
	c.Nodes[id].Inspect(sh, func(r *paxos.Replica, _ *txn.SM, _ *txn.Server) { commit = r.Commit() })
	return commit
}

// DataDir returns a node's data directory.
func (c *Cluster) DataDir(id paxos.NodeID) string { return c.cfgs[id].DataDir }
