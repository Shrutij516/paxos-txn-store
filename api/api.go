// Package api holds the public types shared by the Go SDK (package client)
// and the servers: transaction IDs, the key-to-shard function and the
// outcome errors. The wire request and response types are the generated
// protobuf types in proto/txn/v1. Nothing here imports the module's
// internal packages, so programs outside this module can use the SDK.
package api

import (
	"errors"
	"hash/fnv"
)

// TxnID identifies one attempt of a transaction. A retried transaction
// gets a new ID but keeps its start timestamp.
type TxnID uint64

// ShardOf maps a key to its shard, 0..shards-1, by FNV-1a hash. Clients and
// servers must agree on it: the SDK routes every request with it and the
// servers reject a key sent to the wrong shard.
func ShardOf(key string, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shards))
}

var (
	// ErrAborted means the transaction did not commit and never will; it
	// can be run again.
	ErrAborted = errors.New("transaction aborted")
	// ErrUnknown means the commit request may have reached a coordinator
	// but its outcome was not learned in time: the transaction may or may
	// not have committed.
	ErrUnknown = errors.New("commit outcome unknown")
	// ErrDone is returned when a transaction is used after it finished.
	ErrDone = errors.New("transaction already finished")
	// ErrNoAddrs is returned when a client is given no server address.
	ErrNoAddrs = errors.New("no server addresses")
)
