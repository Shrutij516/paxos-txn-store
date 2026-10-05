// Command paxosd runs one node of the sharded transaction store. The node
// hosts one replica of every shard listed in the cluster config file:
//
//	paxosd -config cluster.json -id 1 -data-dir ./data/1
//
// It serves both the peer (Paxos and two-phase commit) and the client
// (Txn) gRPC services on its address from the config, or on -listen if
// given. With -metrics-listen it serves Prometheus metrics on /metrics, and
// with -otlp-endpoint it exports traces over OTLP/gRPC. Logs are JSON on
// stderr. SIGTERM or SIGINT shuts it down gracefully.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
)

// DefaultTraceSample is the fraction of new traces sampled (DECISIONS.md
// entry 28). Traces a client starts follow the client's decision.
const DefaultTraceSample = 0.01

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("paxosd failed", "err", err.Error())
		os.Exit(1)
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
	metricsListen := fs.String("metrics-listen", "", "address for the Prometheus /metrics endpoint (empty: no endpoint)")
	otlp := fs.String("otlp-endpoint", "", "OTLP/gRPC collector address (host:port) for traces (empty: no tracing)")
	sample := fs.Float64("trace-sample", DefaultTraceSample,
		"fraction of new traces to sample, 0 to 1; traces a client starts follow the client's decision")
	logLevel := fs.String("log-level", "info", "debug, info, warn or error; debug logs every client request with its trace ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *config == "" || *id <= 0 || *dataDir == "" {
		return fmt.Errorf("-config, -id and -data-dir are required")
	}
	if *inDoubt <= 0 || *tick <= 0 {
		return fmt.Errorf("-in-doubt-wait and -tick must be positive")
	}
	if *sample < 0 || *sample > 1 {
		return fmt.Errorf("-trace-sample must be between 0 and 1")
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	mainLog := logger.With("node", *id) // the server adds node to its own lines
	cl, err := server.LoadCluster(*config)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	cfg := server.Config{
		ID: paxos.NodeID(*id), Cluster: cl, Listen: *listen, DataDir: *dataDir,
		Tick: *tick, RPCTimeout: *rpcTimeout, InDoubtWait: *inDoubt, Logger: logger,
	}
	var tp *sdktrace.TracerProvider
	if *otlp != "" {
		exp, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithEndpoint(*otlp), otlptracegrpc.WithInsecure())
		if err != nil {
			return err
		}
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(*sample))),
			sdktrace.WithResource(resource.NewSchemaless(
				attribute.String("service.name", "paxosd"), attribute.Int("node", *id))),
		)
		cfg.TracerProvider = tp
	}
	n, err := server.Start(cfg)
	if err != nil {
		return err
	}
	var metricsSrv *http.Server
	if *metricsListen != "" {
		lis, err := net.Listen("tcp", *metricsListen)
		if err != nil {
			n.Stop()
			return err
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(n.Registry(), promhttp.HandlerOpts{}))
		metricsSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := metricsSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				mainLog.Error("metrics endpoint failed", "err", err.Error())
			}
		}()
		mainLog.Info("serving metrics", "addr", lis.Addr().String())
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	<-ctx.Done()
	start := time.Now()
	n.Stop()
	if metricsSrv != nil {
		_ = metricsSrv.Close()
	}
	if tp != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = tp.Shutdown(sctx) // flushes buffered spans
		cancel()
	}
	mainLog.Info("stopped", "took_ms", time.Since(start).Milliseconds())
	return nil
}
