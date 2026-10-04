// Package client is the Go SDK for the replicated key-value store.
//
// A Client finds the leader by following redirects, retries failed or slow
// attempts with backoff, and tags every operation with its client ID and a
// sequence number so that a retried request is applied at most once. A
// Client has at most one request outstanding: concurrent calls on the same
// Client wait for each other. Use one Client per concurrent caller.
package client

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/Shrutij516/paxos-txn-store/proto/kv/v1"
)

// Options tune a Client. Zero fields take the defaults.
type Options struct {
	// AttemptTimeout bounds a single RPC attempt. Default 1s.
	AttemptTimeout time.Duration
	// BackoffBase and BackoffMax bound the randomized exponential backoff
	// between failed attempts. Defaults 20ms and 500ms.
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// ClientID overrides the random client ID. It must be non-zero and must
	// not be shared with another live client.
	ClientID uint64
}

// Client talks to one cluster.
type Client struct {
	opts  Options
	addrs []string
	conns map[string]kvv1.KVClient
	raw   []*grpc.ClientConn

	mu       sync.Mutex // held for the whole of each operation
	clientID uint64
	seq      uint64
	leader   string // last known leader address, "" if unknown
	next     int    // round-robin position when the leader is unknown
	rng      *rand.Rand

	// Counters for tests and debugging; guarded by mu.
	attempts, redirects, retries int
}

// ErrNoAddrs is returned by New when no server address is given.
var ErrNoAddrs = errors.New("client: no server addresses")

// New creates a client for the cluster whose nodes listen on addrs.
// Connections are made lazily.
func New(addrs []string, opts Options) (*Client, error) {
	if len(addrs) == 0 {
		return nil, ErrNoAddrs
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = time.Second
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = 20 * time.Millisecond
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = 500 * time.Millisecond
	}
	c := &Client{opts: opts, addrs: append([]string(nil), addrs...), conns: make(map[string]kvv1.KVClient)}
	c.clientID = opts.ClientID
	if c.clientID == 0 {
		c.clientID = randomID()
	}
	c.rng = rand.New(rand.NewPCG(c.clientID, randomID()))
	return c, nil
}

func randomID() uint64 {
	var b [8]byte
	for {
		if _, err := crand.Read(b[:]); err != nil {
			return uint64(time.Now().UnixNano()) | 1
		}
		// Keep the top bit clear: SQLite stores IDs as signed integers.
		if id := binary.LittleEndian.Uint64(b[:]) >> 1; id != 0 {
			return id
		}
	}
}

// ID returns the client's ID.
func (c *Client) ID() uint64 { return c.clientID }

// Leader returns the last known leader address, or "" if none is known.
func (c *Client) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

// Close closes all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, cc := range c.raw {
		if err := cc.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.raw, c.conns = nil, map[string]kvv1.KVClient{}
	return first
}

func (c *Client) conn(addr string) (kvv1.KVClient, error) {
	if k, ok := c.conns[addr]; ok {
		return k, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.raw = append(c.raw, cc)
	k := kvv1.NewKVClient(cc)
	c.conns[addr] = k
	return k, nil
}

// Get returns the value of key, or "" if it was never set. The read goes
// through the replicated log, so it is linearizable.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	var value string
	err := c.do(ctx, func(ctx context.Context, k kvv1.KVClient, seq uint64) (bool, string, error) {
		r, err := k.Get(ctx, &kvv1.GetRequest{ClientId: c.clientID, Seq: seq, Key: key})
		if err != nil {
			return false, "", err
		}
		value = r.GetValue()
		return r.GetOk(), r.GetLeaderAddr(), nil
	})
	return value, err
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	return c.do(ctx, func(ctx context.Context, k kvv1.KVClient, seq uint64) (bool, string, error) {
		r, err := k.Put(ctx, &kvv1.PutRequest{ClientId: c.clientID, Seq: seq, Key: key, Value: value})
		if err != nil {
			return false, "", err
		}
		return r.GetOk(), r.GetLeaderAddr(), nil
	})
}

// attemptFn runs one RPC. It returns ok=true on success, or ok=false with
// the leader address the server suggested ("" if none).
type attemptFn func(ctx context.Context, k kvv1.KVClient, seq uint64) (ok bool, leader string, err error)

// do runs one operation to completion: a fresh sequence number, then
// attempts until one succeeds or ctx ends. Every attempt reuses the same
// sequence number, so the cluster applies the operation at most once.
func (c *Client) do(ctx context.Context, attempt attemptFn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	seq := c.seq
	backoff := c.opts.BackoffBase
	hops := 0 // redirects followed since the last backoff
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i > 0 {
			c.retries++
		}
		addr := c.target()
		k, err := c.conn(addr)
		if err == nil {
			c.attempts++
			actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
			var ok bool
			var hint string
			ok, hint, err = attempt(actx, k, seq)
			cancel()
			if err == nil && ok {
				c.leader = addr
				return nil
			}
			if err == nil && hint != "" && hint != addr && hops < len(c.addrs) {
				// Redirect: try the named leader right away. Stale hints can
				// point in a circle, so only a few hops skip the backoff.
				c.redirects++
				hops++
				c.leader = hint
				continue
			}
		}
		// Failure, timeout, or no leader known: forget the leader, move on
		// to the next node, and back off with jitter.
		c.leader = ""
		c.next++
		hops = 0
		if err := sleep(ctx, backoff/2+time.Duration(c.rng.Int64N(int64(backoff/2)+1))); err != nil {
			return err
		}
		backoff = min(backoff*2, c.opts.BackoffMax)
	}
}

func (c *Client) target() string {
	if c.leader != "" {
		return c.leader
	}
	return c.addrs[c.next%len(c.addrs)]
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
