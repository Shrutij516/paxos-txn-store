#!/usr/bin/env bash
# Steady-state load test against a local 3-node cluster of native paxosd
# processes (docs/benchmarks.md). For every config in the matrix it runs
# RUNS fresh clusters (empty data directories), each with loadgen in
# benchmark mode, then prints the mean and spread of each metric.
#
# Usage: scripts/bench.sh [out-dir]          (default bench-out)
#   RUNS=5 WARMUP=10s DURATION=30s KEYS=10000 HOT_KEYS=16 HOT_FRACTION=0.9
#   CLIENTS="8 32" PROFILES="low high" CROSS="0 0.5" READ_RATIO=0
#   KEEP=1 keeps every run's data directories (for the offline checkers);
#   by default only the first run of each config keeps them.
#   SUMMARY_ONLY=1 re-prints the summary of an existing out-dir.
set -euo pipefail

out=${1:-bench-out}
RUNS=${RUNS:-5} WARMUP=${WARMUP:-10s} DURATION=${DURATION:-30s}
KEYS=${KEYS:-10000} HOT_KEYS=${HOT_KEYS:-16} HOT_FRACTION=${HOT_FRACTION:-0.9}
CLIENTS=${CLIENTS:-"8 32"} PROFILES=${PROFILES:-"low high"} CROSS=${CROSS:-"0 0.5"}
READ_RATIO=${READ_RATIO:-0} KEEP=${KEEP:-0}
repo=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$out"
out=$(cd "$out" && pwd)

summary() {
  python3 - "$out" <<'PY'
import json, statistics, sys, glob, os
out = sys.argv[1]
rows = {}
for f in sorted(glob.glob(os.path.join(out, "runs", "*", "report.json"))):
    r = json.load(open(f))
    c = r["config"]
    key = (c["profile"], c["cross_shard_ratio"], c["clients"])
    rows.setdefault(key, []).append(r)
metrics = [("txn/s", lambda r: r["committed_per_sec"]),
           ("p50 ms", lambda r: r["latency_ms"]["p50"]),
           ("p90 ms", lambda r: r["latency_ms"]["p90"]),
           ("p99 ms", lambda r: r["latency_ms"]["p99"]),
           ("max ms", lambda r: r["latency_ms"]["max"]),
           ("abort %", lambda r: 100 * r["abort_rate"]),
           ("errors", lambda r: r["errors"])]
noisy = []
print("| profile | cross | clients | runs | " + " | ".join(m for m, _ in metrics) + " | top abort reasons |")
print("|---|---|---|---|" + "---|" * len(metrics) + "---|")
for key in sorted(rows, key=lambda k: (k[0] != "low", k[1], k[2])):
    rs = rows[key]
    cells = []
    for name, get in metrics:
        xs = [get(r) for r in rs]
        mean = statistics.fmean(xs)
        sd = statistics.stdev(xs) if len(xs) > 1 else 0.0
        cells.append(f"{mean:.1f} ({min(xs):.1f}–{max(xs):.1f}, sd {sd:.1f})")
        if name in ("txn/s", "p50 ms", "p99 ms") and mean > 0 and (max(xs) - min(xs)) / mean > 0.20:
            noisy.append(f"{key[0]}/cross={key[1]}/clients={key[2]}: {name} spread {(max(xs)-min(xs))/mean*100:.0f}% of mean ({[round(x,1) for x in xs]})")
    reasons = {}
    for r in rs:
        for k, v in (r.get("aborts_by_reason") or {}).items():
            reasons[k] = reasons.get(k, 0) + v / len(rs)
    top = ", ".join(f"{k} {v:.0f}" for k, v in sorted(reasons.items(), key=lambda kv: -kv[1])[:3]) or "none"
    print(f"| {key[0]} | {key[1]} | {key[2]} | {len(rs)} | " + " | ".join(cells) + f" | {top} |")
print()
print("Cells: mean (min–max, sd) over the runs. Latency: client.Run start to commit ack, committed transactions.")
print("Configs whose runs differ by more than 20% of the mean (txn/s, p50, p99):")
print("\n".join("- " + n for n in noisy) if noisy else "- none")
PY
}

if [[ ${SUMMARY_ONLY:-0} == 1 ]]; then
  summary
  exit 0
fi

bin="$out/bin"
mkdir -p "$bin"
(cd "$repo" && go build -o "$bin/paxosd" ./cmd/paxosd && go build -o "$bin/loadgen" ./cmd/loadgen)

pids=()
stop_nodes() {
  for p in "${pids[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
  pids=()
}
trap stop_nodes EXIT

# settle <metrics addr>...: wait (up to 2 minutes) until every node answers
# /readyz and no transaction is in doubt anywhere, as the chaos runner does
# before stopping the nodes. Stopping earlier leaves cross-shard commits
# decided at the coordinator but not yet applied at a participant, which
# the offline checkers then report as violations (DECISIONS.md I6).
settle() {
  local deadline=$((SECONDS + 120)) m doubt ready
  while (( SECONDS < deadline )); do
    ready=1 doubt=0
    for m in "$@"; do
      curl -sf -o /dev/null "http://$m/readyz" || { ready=0; break; }
      doubt=$(curl -s "http://$m/metrics" | awk -v d="$doubt" '/^txn_in_doubt[{ ]/ {d += $NF} END {print d}')
    done
    if (( ready == 1 )) && [[ $doubt == 0 ]]; then return 0; fi
    sleep 0.2
  done
  echo "settle: not settled after 2 minutes (in doubt: $doubt, all ready: $ready)" >&2
  return 1
}

# one_run <dir> <profile> <cross> <clients> <seed>
one_run() {
  local dir=$1 profile=$2 cross=$3 clients=$4 seed=$5
  rm -rf "$dir" && mkdir -p "$dir"
  local addrs=() metrics=()
  printf '{"shards": 3, "nodes": [' > "$dir/cluster.json"
  for i in 1 2 3; do
    addrs+=("127.0.0.1:1700$i") metrics+=("127.0.0.1:1910$i")
    printf '{"id": %d, "addr": "127.0.0.1:1700%d"}%s' "$i" "$i" "$([[ $i == 3 ]] || echo ,)" >> "$dir/cluster.json"
  done
  printf ']}\n' >> "$dir/cluster.json"
  for i in 1 2 3; do
    "$bin/paxosd" -config "$dir/cluster.json" -id "$i" -data-dir "$dir/data/n$i" \
      -metrics-listen "127.0.0.1:1910$i" -trace-sample 0 > "$dir/n$i.log" 2>&1 &
    pids+=($!)
  done
  for m in "${metrics[@]}"; do
    for _ in $(seq 1 200); do
      curl -sf -o /dev/null "http://$m/readyz" && break
      sleep 0.1
    done
  done
  local hot=()
  [[ $profile == high ]] && hot=(-hot-keys "$HOT_KEYS" -hot-fraction "$HOT_FRACTION")
  "$bin/loadgen" -addrs "$(IFS=,; echo "${addrs[*]}")" -metrics-addrs "$(IFS=,; echo "${metrics[*]}")" \
    -clients "$clients" -short-clients 0 -audit-percent 0 -keys "$KEYS" -profile "$profile" "${hot[@]}" \
    -cross-shard-ratio "$cross" -read-ratio "$READ_RATIO" -warmup "$WARMUP" -duration "$DURATION" \
    -seed "$seed" -out "$dir/history.jsonl" -report "$dir/report.json" > "$dir/loadgen.log" 2>&1
  settle "${metrics[@]}" || touch "$dir/NOT_SETTLED"
  stop_nodes
}

total=0
for profile in $PROFILES; do for cross in $CROSS; do for clients in $CLIENTS; do
  for run in $(seq 1 "$RUNS"); do
    total=$((total + 1))
    name="$profile-cross$cross-c$clients-r$run"
    dir="$out/runs/$name"
    start=$(date +%s)
    one_run "$dir" "$profile" "$cross" "$clients" "$run"
    printf '%s: %.1f txn/s, p99 %.1f ms (%ds)\n' "$name" \
      "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["committed_per_sec"])' "$dir/report.json")" \
      "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["latency_ms"]["p99"])' "$dir/report.json")" \
      $(($(date +%s) - start))
    if [[ $KEEP != 1 && $run != 1 ]]; then rm -rf "$dir/data"; fi
  done
done; done; done
echo "$total runs in $out/runs"
summary
