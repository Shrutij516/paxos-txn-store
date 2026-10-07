package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
)

func attr(s sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// TestCrossShardTxnIsOneTrace: the client, the coordinator shard and the
// participant shard all add spans to one trace for one cross-shard
// transaction, including the two-phase commit phases and Paxos rounds. The
// shards are led by different nodes, so the trace context crosses the
// network in the peer envelope.
func TestCrossShardTxnIsOneTrace(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	// cut, when non-zero, is a node whose shard 2 peer messages are dropped.
	var cut atomic.Int64
	drop := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if env, ok := req.(*paxosv1.Envelope); ok && env.GetShard() == 2 {
			if n := cut.Load(); n != 0 && env.GetFrom() != env.GetTo() && (int64(env.GetFrom()) == n || int64(env.GetTo()) == n) {
				return &paxosv1.SendAck{}, nil
			}
		}
		return h(ctx, req)
	})
	c := testcluster.Start(t, 3, shards, func(cfg *server.Config) {
		cfg.TracerProvider = tp
		cfg.ServerOptions = []grpc.ServerOption{drop}
	})
	cl := newClient(t, c, client.Options{TracerProvider: tp})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, d := keyOn(0, 0), keyOn(2, 0)
	if err := put(ctx, cl, map[string]string{a: "10", d: "10"}); err != nil {
		t.Fatal(err)
	}
	// If one node leads both shards, cut that node off from shard 2's peer
	// traffic (in both directions) until the other two elect a shard 2
	// leader between them. Shard 0 is untouched. Once the cut heals, the old
	// leader sees the higher ballot and steps down for shard 2.
	l0, err := c.Leader(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if l2, err := c.Leader(ctx, 2); err != nil {
		t.Fatal(err)
	} else if l2 == l0 {
		cut.Store(int64(l0))
		deadline := time.Now().Add(20 * time.Second)
		for moved := false; !moved; {
			for _, id := range c.IDs {
				if id != l0 {
					c.Nodes[id].Inspect(2, func(_ *paxos.Replica, _ *txn.SM, ts *txn.Server) { moved = moved || ts.Leading() })
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("shard 2 elected no leader while node", l0, "was cut off")
			}
			time.Sleep(10 * time.Millisecond)
		}
		cut.Store(0)
	}
	if l2, err := c.Leader(ctx, 2); err != nil || l2 == l0 {
		t.Fatalf("shard 2 leader %d (err %v), shard 0 leader %d", l2, err, l0)
	}
	if now, err := c.Leader(ctx, 0); err != nil || now != l0 {
		t.Fatalf("shard 0 leader moved from %d to %d (err %v)", l0, now, err)
	}
	exp.Reset()
	var id string
	err = cl.Run(ctx, func(ctx context.Context, tx *client.Txn) error {
		id = strconv.FormatUint(uint64(tx.ID()), 10)
		if _, err := tx.Read(ctx, a); err != nil {
			return err
		}
		if err := tx.Write(ctx, a, "9"); err != nil {
			return err
		}
		return tx.Write(ctx, d, "11")
	})
	if err != nil {
		t.Fatal(err)
	}

	// Server spans end on the shards' event loops, shortly after the reply.
	deadline := time.Now().Add(10 * time.Second)
	for {
		spans := exp.GetSpans().Snapshots()
		var root sdktrace.ReadOnlySpan
		for _, s := range spans {
			if v, ok := attr(s, "txn.id"); ok && v.AsString() == id && s.Name() == "txn" {
				root = s
			}
		}
		if root == nil {
			t.Fatalf("no client span for txn %s", id)
		}
		names := map[string]int{}
		shardsSeen, nodes := map[int64]bool{}, map[int64]bool{}
		for _, s := range spans {
			v, ok := attr(s, "txn.id")
			if ok && v.AsString() == id && s.SpanContext().TraceID() != root.SpanContext().TraceID() {
				t.Fatalf("span %q of txn %s is in another trace", s.Name(), id)
			}
			if s.SpanContext().TraceID() != root.SpanContext().TraceID() {
				continue
			}
			names[s.Name()]++
			if v, ok := attr(s, "shard"); ok {
				shardsSeen[v.AsInt64()] = true
			}
			if v, ok := attr(s, "node"); ok {
				nodes[v.AsInt64()] = true
			}
		}
		done := len(shardsSeen) >= 2 && len(nodes) >= 2 && names["2pc.prepare"] == 1 && names["2pc.decide"] == 1 &&
			names["2pc.participant.prepare"] >= 2 && names["paxos.round"] >= 3
		if done {
			t.Logf("one trace for txn %s: spans %v; shards %d, nodes %d", id, names, len(shardsSeen), len(nodes))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace of txn %s has spans %v from shards %v, nodes %v", id, names, shardsSeen, nodes)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent writers.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Split(strings.TrimSpace(s.b.String()), "\n")
}

// TestRequestLogsCarryTraceID: at debug level every Txn request is logged
// as JSON with the trace ID of the transaction that made it, and leader
// changes are logged at info level.
func TestRequestLogsCarryTraceID(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c := testcluster.Start(t, 3, shards, func(cfg *server.Config) {
		cfg.TracerProvider = tp
		cfg.Logger = logger
	})
	cl := newClient(t, c, client.Options{TracerProvider: tp})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var id string
	err := cl.Run(ctx, func(ctx context.Context, tx *client.Txn) error {
		id = strconv.FormatUint(uint64(tx.ID()), 10)
		return tx.Write(ctx, keyOn(1, 0), "v")
	})
	if err != nil {
		t.Fatal(err)
	}
	for sh := 0; sh < shards; sh++ { // every shard has elected, and logged it
		if _, err := c.Leader(ctx, txn.ShardID(sh)); err != nil {
			t.Fatal(err)
		}
	}
	var traceID string
	for _, s := range exp.GetSpans().Snapshots() {
		if v, ok := attr(s, "txn.id"); ok && v.AsString() == id && s.Name() == "txn" {
			traceID = s.SpanContext().TraceID().String()
		}
	}
	methods, leaders := map[string]bool{}, 0
	for _, line := range logs.lines() {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		switch rec["msg"] {
		case "request":
			if rec["trace_id"] == traceID {
				methods[rec["method"].(string)] = true
			}
		case "became leader":
			if rec["shard"] == nil || rec["node"] == nil {
				t.Fatalf("leader log without shard and node: %q", line)
			}
			leaders++
		}
	}
	for _, m := range []string{"/txn.v1.Txn/Begin", "/txn.v1.Txn/Write", "/txn.v1.Txn/Commit"} {
		if !methods[m] {
			t.Fatalf("no %s request log with trace ID %s; logged %v", m, traceID, methods)
		}
	}
	if leaders < shards {
		t.Fatalf("%d leader elections logged, want at least one per shard", leaders)
	}
}
