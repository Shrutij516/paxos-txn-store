package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// Cluster is the shard membership shared by every node, read from a JSON
// config file:
//
//	{
//	  "shards": 3,
//	  "nodes": [
//	    {"id": 1, "addr": "127.0.0.1:7001"},
//	    {"id": 2, "addr": "127.0.0.1:7002"},
//	    {"id": 3, "addr": "127.0.0.1:7003"}
//	  ]
//	}
//
// Every node hosts one replica of every shard, so each shard is a Paxos
// group of all the nodes. Keys map to shards by txn.ShardOf.
type Cluster struct {
	Shards int          `json:"shards"`
	Nodes  []NodeConfig `json:"nodes"`
}

// NodeConfig is one node's ID and the address it serves on.
type NodeConfig struct {
	ID   paxos.NodeID `json:"id"`
	Addr string       `json:"addr"`
}

// maxShards bounds the shard count; each shard is a goroutine and a SQLite
// file on every node.
const maxShards = 1024

// LoadCluster reads and validates a cluster config file.
func LoadCluster(path string) (Cluster, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Cluster{}, err
	}
	return ParseCluster(b)
}

// ParseCluster parses and validates a cluster config. Unknown fields are
// errors, so a misspelled key is not silently ignored.
func ParseCluster(b []byte) (Cluster, error) {
	var c Cluster
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Cluster{}, fmt.Errorf("server: cluster config: %w", err)
	}
	return c, c.Validate()
}

// Validate checks the shard count, and that node IDs are positive and
// unique and every node has an address.
func (c Cluster) Validate() error {
	if c.Shards < 1 || c.Shards > maxShards {
		return fmt.Errorf("server: cluster config: shards must be 1 to %d, got %d", maxShards, c.Shards)
	}
	if len(c.Nodes) == 0 {
		return errors.New("server: cluster config: no nodes")
	}
	seen := map[paxos.NodeID]bool{}
	for _, n := range c.Nodes {
		if n.ID <= 0 {
			return fmt.Errorf("server: cluster config: node id must be positive, got %d", n.ID)
		}
		if seen[n.ID] {
			return fmt.Errorf("server: cluster config: duplicate node id %d", n.ID)
		}
		if n.Addr == "" {
			return fmt.Errorf("server: cluster config: node %d has no addr", n.ID)
		}
		seen[n.ID] = true
	}
	return nil
}

// IDs returns the node IDs in ascending order.
func (c Cluster) IDs() []paxos.NodeID {
	out := make([]paxos.NodeID, len(c.Nodes))
	for i, n := range c.Nodes {
		out[i] = n.ID
	}
	slices.Sort(out)
	return out
}

// Addrs maps node IDs to addresses.
func (c Cluster) Addrs() map[paxos.NodeID]string {
	out := make(map[paxos.NodeID]string, len(c.Nodes))
	for _, n := range c.Nodes {
		out[n.ID] = n.Addr
	}
	return out
}

// ShardNodes returns the replicas of a shard: every node.
func (c Cluster) ShardNodes(txn.ShardID) []paxos.NodeID { return c.IDs() }
