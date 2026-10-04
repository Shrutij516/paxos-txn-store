// Package client is the Go SDK for the sharded transaction store.
//
// A transaction reads and writes keys on any shards and then commits:
//
//	err := c.Run(ctx, func(ctx context.Context, t *client.Txn) error {
//		v, err := t.Read(ctx, "alice")
//		if err != nil {
//			return err
//		}
//		return t.Write(ctx, "alice", v+"!")
//	})
//
// Reads and writes go to the leader of the key's shard, and Commit goes to
// the leader of the coordinator shard (the lowest shard the transaction
// touched). The Client caches the leader of each shard, follows redirects
// and retries failed attempts with backoff. Run retries a transaction that
// aborted, with backoff, up to a maximum number of attempts.
//
// Commit returns nil (committed), an error wrapping ErrAborted (did not
// commit), or one wrapping ErrUnknown: the request may have reached a
// coordinator, but no answer arrived before the context ended. Such a
// transaction may have committed or not; Run returns it to the caller
// rather than retrying, since running it again could apply it twice.
//
// A Client is safe for concurrent use; a Txn is not.
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Shrutij516/paxos-txn-store/internal/txn"
	txnv1 "github.com/Shrutij516/paxos-txn-store/proto/txn/v1"
)

// Options tune a Client. Zero fields take the defaults.
type Options struct {
	// AttemptTimeout bounds a single RPC attempt. Default 1s.
	AttemptTimeout time.Duration
	// BackoffBase and BackoffMax bound the randomized exponential backoff
	// between failed attempts of one call, and between attempts of a
	// transaction in Run. Defaults 20ms and 500ms.
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// MaxAttempts is how many times Run tries a transaction that aborts.
	// Default 10.
	MaxAttempts int
	// OnAttempt, if set, is called by Run after each attempt with the
	// transaction and its result (nil if it committed).
	OnAttempt func(t *Txn, err error)
}

var (
	// ErrAborted means the transaction did not commit and never will;
	// it can be run again.
	ErrAborted = errors.New("client: transaction aborted")
	// ErrUnknown means the commit request may have reached a coordinator
	// but its outcome was not learned in time: the transaction may or may
	// not have committed.
	ErrUnknown = errors.New("client: commit outcome unknown")
	// ErrNoAddrs is returned by New when no server address is given.
	ErrNoAddrs = errors.New("client: no server addresses")
	// ErrDone is returned when a transaction is used after it finished.
	ErrDone = errors.New("client: transaction already finished")
)

// Client talks to one cluster.
type Client struct {
	opts  Options
	addrs []string

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	leaders map[txn.ShardID]string // cached leader address per shard
	next    int                    // round-robin position when a leader is unknown
	shards  int                    // learned from Begin
	rng     *rand.Rand
}

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
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 10
	}
	return &Client{
		opts: opts, addrs: slices.Clone(addrs),
		conns: map[string]*grpc.ClientConn{}, leaders: map[txn.ShardID]string{},
		rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}, nil
}

// Close closes all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.conns = map[string]*grpc.ClientConn{}
	return first
}

// Leader returns the cached leader address of a shard, "" if unknown.
func (c *Client) Leader(sh int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaders[txn.ShardID(sh)]
}

// Shards returns the number of shards, 0 before the first Begin.
func (c *Client) Shards() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.shards
}

func (c *Client) stub(addr string) (txnv1.TxnClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns[addr] = conn
	}
	return txnv1.NewTxnClient(conn), nil
}

// target picks the node to try for a shard: the cached leader, or the next
// node round-robin.
func (c *Client) target(sh txn.ShardID) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if a, ok := c.leaders[sh]; ok {
		return a
	}
	a := c.addrs[c.next%len(c.addrs)]
	c.next++
	return a
}

// redirect records what a failed attempt at addr taught about the shard's
// leader. It reports whether the hint names a new node to try right away.
func (c *Client) redirect(sh txn.ShardID, addr string, hint *txnv1.LeaderHint) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h := hint.GetAddr(); h != "" && h != addr {
		c.leaders[sh] = h
		return true
	}
	if c.leaders[sh] == addr {
		delete(c.leaders, sh)
	}
	return false
}

func (c *Client) learned(sh txn.ShardID, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaders[sh] = addr
}

func (c *Client) sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	d = d/2 + time.Duration(c.rng.Int64N(int64(d/2)+1))
	c.mu.Unlock()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// outcome is what one RPC attempt at a shard's leader produced.
type outcome int

const (
	done      outcome = iota // a final answer
	notLeader                // the node does not lead the shard
	failed                   // transport error or timeout
)

// onShard calls f at the leader of shard sh until it gives a final answer
// or ctx ends. f returns the attempt's outcome, a leader hint for
// notLeader, and an error for failed.
func (c *Client) onShard(ctx context.Context, sh txn.ShardID,
	f func(ctx context.Context, stub txnv1.TxnClient) (outcome, *txnv1.LeaderHint, error)) error {
	backoff := c.opts.BackoffBase
	var last error
	for {
		if err := ctx.Err(); err != nil {
			if last == nil {
				last = err
			}
			return fmt.Errorf("client: shard %d: %w (last error: %v)", sh, err, last)
		}
		addr := c.target(sh)
		stub, err := c.stub(addr)
		if err != nil {
			return err
		}
		actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		out, hint, err := f(actx, stub)
		cancel()
		switch out {
		case done:
			c.learned(sh, addr)
			return nil
		case notLeader:
			last = fmt.Errorf("%s does not lead shard %d", addr, sh)
			if c.redirect(sh, addr, hint) {
				continue // a fresh hint: follow it at once
			}
		case failed:
			if neverSent(err) {
				return err // the request itself is wrong; retrying cannot help
			}
			last = err
			c.redirect(sh, addr, nil)
		}
		if err := c.sleep(ctx, backoff); err != nil {
			continue // reported at the top of the loop
		}
		backoff = min(backoff*2, c.opts.BackoffMax)
	}
}

// Txn is one attempt of a transaction. Reads see the latest committed value
// (or this transaction's own write) under a shared lock held until commit;
// writes are buffered and sent with Commit.
type Txn struct {
	c      *Client
	meta   txn.Meta
	shards int
	reads  map[string]uint64 // key -> version read
	vals   map[string]string // key -> value read
	writes map[string]string
	touch  map[txn.ShardID]bool
	done   bool
}

// Begin starts a transaction attempt.
func (c *Client) Begin(ctx context.Context) (*Txn, error) { return c.begin(ctx, 0) }

// begin starts an attempt; a non-zero ts keeps an earlier attempt's age.
func (c *Client) begin(ctx context.Context, ts uint64) (*Txn, error) {
	backoff := c.opts.BackoffBase
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("client: begin: %w (last error: %v)", err, last)
		}
		addr := c.target(0)
		stub, err := c.stub(addr)
		if err != nil {
			return nil, err
		}
		actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		r, err := stub.Begin(actx, &txnv1.BeginRequest{Ts: ts})
		cancel()
		if err == nil && r.GetShards() > 0 {
			c.mu.Lock()
			c.shards = int(r.GetShards())
			c.mu.Unlock()
			return &Txn{
				c: c, meta: txn.Meta{ID: txn.ID(r.GetTxn().GetId()), TS: r.GetTxn().GetTs()}, shards: int(r.GetShards()),
				reads: map[string]uint64{}, vals: map[string]string{}, writes: map[string]string{}, touch: map[txn.ShardID]bool{},
			}, nil
		}
		last = err
		c.redirect(0, addr, nil)
		if c.sleep(ctx, backoff) == nil {
			backoff = min(backoff*2, c.opts.BackoffMax)
		}
	}
}

// ID returns the attempt's transaction ID.
func (t *Txn) ID() uint64 { return uint64(t.meta.ID) }

// ReadVersions returns the version of every key this attempt read from a
// shard (a key it wrote before reading is not included).
func (t *Txn) ReadVersions() map[string]uint64 { return maps(t.reads) }

// ReadValues returns the value of every key this attempt read from a shard.
func (t *Txn) ReadValues() map[string]string { return maps(t.vals) }

// Shards returns the shards this attempt read or wrote.
func (t *Txn) Shards() []int {
	out := make([]int, 0, len(t.touch))
	for sh := range t.touch {
		out = append(out, int(sh))
	}
	slices.Sort(out)
	return out
}

func maps[V any](m map[string]V) map[string]V {
	out := make(map[string]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (t *Txn) shardOf(key string) txn.ShardID { return txn.ShardOf(key, t.shards) }

// fail ends the attempt after the server said it aborted.
func (t *Txn) fail(ctx context.Context) error {
	t.Abort(ctx)
	return ErrAborted
}

// Read returns the committed value of key ("" if it was never written).
// It returns an error wrapping ErrAborted if the transaction was aborted;
// the transaction is then finished.
func (t *Txn) Read(ctx context.Context, key string) (string, error) {
	if t.done {
		return "", ErrDone
	}
	if v, ok := t.writes[key]; ok {
		return v, nil
	}
	if v, ok := t.vals[key]; ok {
		return v, nil // repeatable: the shared lock is still held
	}
	sh := t.shardOf(key)
	t.touch[sh] = true
	var resp *txnv1.ReadResponse
	err := t.c.onShard(ctx, sh, func(ctx context.Context, stub txnv1.TxnClient) (outcome, *txnv1.LeaderHint, error) {
		r, err := stub.Read(ctx, &txnv1.ReadRequest{Txn: t.protoMeta(), Key: key})
		return classify(r.GetStatus(), r.GetLeader(), err, func() { resp = r })
	})
	if err != nil {
		return "", err
	}
	if resp.GetStatus() == txnv1.Status_STATUS_ABORTED {
		return "", t.fail(ctx)
	}
	t.reads[key], t.vals[key] = resp.GetVersion(), resp.GetValue()
	return resp.GetValue(), nil
}

// Write buffers a write of key. It tells the shard's leader first, which
// fails fast if the transaction was already aborted there.
func (t *Txn) Write(ctx context.Context, key, value string) error {
	if t.done {
		return ErrDone
	}
	sh := t.shardOf(key)
	t.touch[sh] = true
	var st txnv1.Status
	err := t.c.onShard(ctx, sh, func(ctx context.Context, stub txnv1.TxnClient) (outcome, *txnv1.LeaderHint, error) {
		r, err := stub.Write(ctx, &txnv1.WriteRequest{Txn: t.protoMeta(), Key: key})
		return classify(r.GetStatus(), r.GetLeader(), err, func() { st = r.GetStatus() })
	})
	if err != nil {
		return err
	}
	if st == txnv1.Status_STATUS_ABORTED {
		return t.fail(ctx)
	}
	t.writes[key] = value
	return nil
}

// classify maps an RPC result to an outcome; keep runs on a final answer.
func classify(st txnv1.Status, hint *txnv1.LeaderHint, err error, keep func()) (outcome, *txnv1.LeaderHint, error) {
	switch {
	case err != nil:
		return failed, nil, err
	case st == txnv1.Status_STATUS_NOT_LEADER:
		return notLeader, hint, nil
	case st == txnv1.Status_STATUS_OK || st == txnv1.Status_STATUS_ABORTED:
		keep()
		return done, nil, nil
	}
	return failed, nil, fmt.Errorf("client: unexpected status %v", st)
}

func (t *Txn) protoMeta() *txnv1.TxnMeta { return &txnv1.TxnMeta{Id: uint64(t.meta.ID), Ts: t.meta.TS} }

// parts groups the read set and the writes by shard, lowest shard first.
func (t *Txn) parts() []*txnv1.Part {
	by := map[txn.ShardID]*txnv1.Part{}
	get := func(k string) *txnv1.Part {
		sh := t.shardOf(k)
		if by[sh] == nil {
			by[sh] = &txnv1.Part{Shard: uint32(sh)}
		}
		return by[sh]
	}
	for k, v := range t.reads {
		p := get(k)
		if p.Reads == nil {
			p.Reads = map[string]uint64{}
		}
		p.Reads[k] = v
	}
	for k, v := range t.writes {
		p := get(k)
		if p.Writes == nil {
			p.Writes = map[string]string{}
		}
		p.Writes[k] = v
	}
	out := make([]*txnv1.Part, 0, len(by))
	for _, p := range by {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b *txnv1.Part) int { return int(a.Shard) - int(b.Shard) })
	return out
}

// Commit commits the transaction. It returns nil if it committed, an error
// wrapping ErrAborted if it did not, and one wrapping ErrUnknown if the
// outcome could not be learned before ctx ended. Commit retries the same
// request (same transaction ID) after errors and at new leaders; the
// servers answer a repeated commit from the log, so this never commits
// twice.
func (t *Txn) Commit(ctx context.Context) error {
	if t.done {
		return ErrDone
	}
	t.done = true
	parts := t.parts()
	if len(parts) == 0 {
		return nil // touched nothing
	}
	coord := txn.ShardID(parts[0].GetShard())
	req := &txnv1.CommitRequest{Txn: t.protoMeta(), Parts: parts}
	var st txnv1.Status
	maybeSent := false // some attempt may have reached a coordinator
	err := t.c.onShard(ctx, coord, func(ctx context.Context, stub txnv1.TxnClient) (outcome, *txnv1.LeaderHint, error) {
		r, err := stub.Commit(ctx, req)
		if err != nil && !neverSent(err) {
			maybeSent = true
		}
		return classify(r.GetStatus(), r.GetLeader(), err, func() { st = r.GetStatus() })
	})
	switch {
	case neverSent(err):
		return err
	case err != nil && maybeSent:
		return fmt.Errorf("%w: %v", ErrUnknown, err)
	case err != nil:
		// Every attempt was refused before reaching a coordinator.
		return fmt.Errorf("%w: commit never reached a leader: %v", ErrAborted, err)
	case st == txnv1.Status_STATUS_ABORTED:
		return ErrAborted
	}
	return nil
}

// neverSent reports whether an RPC error proves the request was not
// processed: the server rejected it as invalid.
func neverSent(err error) bool { return err != nil && status.Code(err) == codes.InvalidArgument }

// Abort gives up the transaction and asks the leaders of the shards it
// touched to drop its locks. It is best effort (locks also expire) and
// never fails. It runs even if ctx has ended, since a transaction often
// aborts because its deadline passed, and its locks would otherwise block
// others until they expire; each request is bounded by AttemptTimeout.
func (t *Txn) Abort(ctx context.Context) {
	if t.done {
		return
	}
	t.done = true
	ctx = context.WithoutCancel(ctx)
	for _, sh := range t.Shards() {
		sh := txn.ShardID(sh)
		actx, cancel := context.WithTimeout(ctx, t.c.opts.AttemptTimeout)
		if stub, err := t.c.stub(t.c.target(sh)); err == nil {
			_, _ = stub.Abort(actx, &txnv1.AbortRequest{Txn: uint64(t.meta.ID), Shard: uint32(sh)})
		}
		cancel()
	}
}

// Run runs fn in a transaction and commits it. If the transaction aborts
// (fn or Commit returns an error wrapping ErrAborted), Run waits a
// randomized, growing backoff and runs fn again in a new attempt that keeps
// the first attempt's start timestamp, so it ages and eventually wins
// conflicts. It gives up after MaxAttempts and returns the last error. Any
// other error from fn aborts the attempt and is returned as is, as is a
// commit with an unknown outcome (ErrUnknown), which must not be retried
// blindly. fn may run several times, so it must not have side effects
// beyond the transaction.
func (c *Client) Run(ctx context.Context, fn func(ctx context.Context, t *Txn) error) error {
	var ts uint64
	backoff := c.opts.BackoffBase
	for attempt := 1; ; attempt++ {
		t, err := c.begin(ctx, ts)
		if err != nil {
			return err
		}
		ts = t.meta.TS
		if err = fn(ctx, t); err == nil {
			err = t.Commit(ctx)
		} else {
			t.Abort(ctx)
		}
		if c.opts.OnAttempt != nil {
			c.opts.OnAttempt(t, err)
		}
		if !errors.Is(err, ErrAborted) || attempt >= c.opts.MaxAttempts {
			return err
		}
		if c.sleep(ctx, backoff) != nil {
			return err
		}
		backoff = min(backoff*2, c.opts.BackoffMax)
	}
}
