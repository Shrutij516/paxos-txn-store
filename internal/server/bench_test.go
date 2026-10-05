package server_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// benchKey returns a key on shard sh unique to worker w.
func benchKey(w int, sh txn.ShardID) string {
	for i := 0; ; i++ {
		if k := fmt.Sprintf("w%d-%d", w, i); txn.ShardOf(k, shards) == sh {
			return k
		}
	}
}

// BenchmarkTracingOverhead measures committed transactions per second on an
// in-process 3-node, 3-shard cluster with tracing off, and with every node
// and client tracing at sampling ratio 0.1 and 1.0. Spans go through the
// SDK's batch processor to an exporter that discards them, so the numbers
// include creating, recording and batching spans but no network export.
// Eight workers each run cross-shard transfers on their own two keys, so
// transactions do not conflict and contention does not hide the overhead.
//
//	go test -run xxx -bench TracingOverhead -benchtime 3s ./internal/server/
func BenchmarkTracingOverhead(b *testing.B) {
	const workers = 8
	for _, mode := range []struct {
		name  string
		ratio float64
	}{{"off", -1}, {"sample0.1", 0.1}, {"sample1.0", 1}} {
		b.Run(mode.name, func(b *testing.B) {
			var tp trace.TracerProvider
			if mode.ratio >= 0 {
				sdk := sdktrace.NewTracerProvider(
					sdktrace.WithBatcher(tracetest.NewNoopExporter()),
					sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(mode.ratio))))
				defer func() { _ = sdk.Shutdown(context.Background()) }()
				tp = sdk
			}
			c := testcluster.Start(b, 3, shards, func(cfg *server.Config) {
				cfg.Tick = server.DefaultTick
				cfg.TracerProvider = tp
			})
			ctx := context.Background()
			var clients []*client.Client
			for w := 0; w < workers; w++ {
				cl, err := client.New(c.Addrs, client.Options{TracerProvider: tp, MaxAttempts: 100})
				if err != nil {
					b.Fatal(err)
				}
				defer func() { _ = cl.Close() }()
				clients = append(clients, cl)
				if err := put(ctx, cl, map[string]string{benchKey(w, 0): "1000000", benchKey(w, 2): "0"}); err != nil {
					b.Fatal(err)
				}
			}
			var next atomic.Int64
			var wg sync.WaitGroup
			b.ResetTimer()
			start := time.Now()
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for next.Add(1) <= int64(b.N) {
						if _, err := transfer(ctx, clients[w], benchKey(w, 0), benchKey(w, 2), 1); err != nil {
							b.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "txns/s")
		})
	}
}
