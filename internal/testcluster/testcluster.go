// Package testcluster starts an in-process cluster of server nodes on
// localhost for tests.
package testcluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
)

// Cluster is a set of nodes, each with its own data dir.
type Cluster struct {
	t     testing.TB
	cfgs  map[paxos.NodeID]server.Config
	Nodes map[paxos.NodeID]*server.Node // nil while stopped
	IDs   []paxos.NodeID
	Addrs []string // in ID order
}

// Start launches n nodes. opts, if not nil, adjusts each config before start.
func Start(t testing.TB, n int, opts func(*server.Config)) *Cluster {
	t.Helper()
	c := &Cluster{t: t, cfgs: map[paxos.NodeID]server.Config{}, Nodes: map[paxos.NodeID]*server.Node{}}
	peers := map[paxos.NodeID]string{}
	lis := map[paxos.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := paxos.NodeID(i)
		lis[id] = l
		peers[id] = l.Addr().String()
		c.IDs = append(c.IDs, id)
		c.Addrs = append(c.Addrs, peers[id])
	}
	for _, id := range c.IDs {
		cfg := server.Config{ID: id, Peers: peers, DataDir: t.TempDir(), Tick: 5 * time.Millisecond}
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

// Leader waits until exactly one running node believes it leads and
// returns its ID.
func (c *Cluster) Leader(ctx context.Context) (paxos.NodeID, error) {
	for {
		var leaders []paxos.NodeID
		for _, id := range c.IDs {
			if n := c.Nodes[id]; n != nil {
				var is bool
				n.Inspect(func(r *paxos.Replica, _ *kv.Store) { is = r.IsLeader() })
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
func (c *Cluster) Addr(id paxos.NodeID) string { return c.cfgs[id].Peers[id] }

// Commit returns a running node's commit index.
func (c *Cluster) Commit(id paxos.NodeID) uint64 {
	var commit uint64
	c.Nodes[id].Inspect(func(r *paxos.Replica, _ *kv.Store) { commit = r.Commit() })
	return commit
}
