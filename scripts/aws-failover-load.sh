#!/usr/bin/env bash
# Failover under load (docs/aws.md "Failover under load", DECISIONS.md
# entry 46). Runs the benchmark load against the cluster, SIGKILLs the node
# leading the most shards in the middle of the measured window, restarts it
# later, and collects what is needed to report failover time per shard,
# throughput and latency before, during and after the kill, and the
# restarted node's time to ready.
#
#   aws-failover-load.sh run aws <out-dir>
#       From .github/workflows/aws-loadtest.yml, in deploy/terraform/cluster
#       with AWS credentials: drives the nodes over SSM (AWS-RunShellScript).
#   aws-failover-load.sh run local <out-dir>
#       The same sequence against three native paxosd processes this script
#       starts on 127.0.0.1 (PAXOSD_BIN, LOADGEN_BIN; for testing the logic).
#   aws-failover-load.sh node <step> [args]
#       One step on a node. `run` sends the script with the step to run.
#
# Knobs (environment): WARMUP=10 DURATION=120 KILL_AT=60 (seconds into the
# measured window) DOWN=30 (seconds the victim stays down) CLIENTS=32
# KEYS=10000 CROSS=0.5 PROFILE=low.
#
# Caveat: loadgen runs on node 1 and shares its CPU (2 vCPUs on a
# t3.small) with node 1's paxosd and the observability containers. Node 1 is
# never the victim, so loadgen survives the kill.
#
# Outputs in <out-dir>: events.env (victim, kill and restart times),
# leaders.txt (paxos_is_leader samples), timeline.jsonl, report.json,
# history.jsonl, loadgen.log, data/nodeN/ (each node's SQLite files after
# the run) and the node steps' output. scripts/aws-failover-report.py turns
# them into the summary.
set -euo pipefail

WARMUP=${WARMUP:-10} DURATION=${DURATION:-120} KILL_AT=${KILL_AT:-60} DOWN=${DOWN:-30}
CLIENTS=${CLIENTS:-32} KEYS=${KEYS:-10000} CROSS=${CROSS:-0.5} PROFILE=${PROFILE:-low}
NODE_WORK=${NODE_WORK:-/var/tmp/paxos-loadtest} # on the node: one directory per run

now_ms() { date +%s%3N; }
log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

# ---- Steps on a node ----
# MODE=aws: the node runs paxosd in the Docker container "paxosd"
# (user_data.sh.tftpl), metrics on 127.0.0.1:9100, loadgen from the same
# image. MODE=local: native processes, described by LOCAL_DIR.

# loadgen_args <run-dir> <addrs> <maddrs>
loadgen_args() {
  local dir=$1
  echo -addrs "$2" -metrics-addrs "$3" -clients "$CLIENTS" -short-clients 0 -audit-percent 0 \
    -keys "$KEYS" -profile "$PROFILE" -cross-shard-ratio "$CROSS" -warmup "${WARMUP}s" -duration "${DURATION}s" \
    -out "$dir/history.jsonl" -report "$dir/report.json" -timeline "$dir/timeline.jsonl"
}

# node start <run> <image> <addrs> <maddrs>: starts loadgen and the leader
# sampler in the background and returns.
node_start() {
  local run=$1 image=$2 addrs=$3 maddrs=$4 dir=$NODE_WORK/$1
  mkdir -p "$dir"
  if [[ $MODE == aws ]]; then
    chown 65532:65532 "$dir" # the image's nonroot user writes the outputs
    # loadgen -h prints its usage and exits 1; an image without it fails
    # to start the entrypoint.
    local usage
    usage=$(docker run --rm --entrypoint /loadgen "$image" -h 2>&1 || true)
    if [[ $usage != *"Usage of loadgen"* ]]; then
      echo "the running image has no /loadgen: deploy an image built from this repository at Phase 9c or later" >&2
      exit 1
    fi
    # shellcheck disable=SC2046 # loadgen_args is a word list on purpose
    docker run -d --name "loadgen-$run" --network host -v "$dir:/out" --entrypoint /loadgen "$image" \
      $(loadgen_args /out "$addrs" "$maddrs") >/dev/null
  else
    # shellcheck disable=SC2046
    setsid "$LOADGEN_BIN" $(loadgen_args "$dir" "$addrs" "$maddrs") >"$dir/loadgen.log" 2>&1 </dev/null &
    echo $! >"$dir/loadgen.pid"
  fi
  # Sampler: every 100 ms, each node's paxos_is_leader series as
  # "<unix ms> <node> <shard> <value>". A node that does not answer within
  # 200 ms is skipped for that round.
  cat >"$dir/sampler.sh" <<SAMPLER
#!/bin/bash
IFS=, read -ra ms <<< "$maddrs"
while [ ! -e "$dir/stop-sampler" ]; do
  for m in "\${ms[@]}"; do
    t=\$(date +%s%3N)
    curl -s -m 0.2 "http://\$m/metrics" | awk -v t="\$t" '/^paxos_is_leader\\{/ {
      match(\$0, /node="[0-9]+"/); n = substr(\$0, RSTART + 6, RLENGTH - 7)
      match(\$0, /shard="[0-9]+"/); s = substr(\$0, RSTART + 7, RLENGTH - 8)
      print t, n, s, \$NF }'
  done >> "$dir/leaders.txt"
  sleep 0.1
done
SAMPLER
  chmod 755 "$dir/sampler.sh"
  # A transient systemd unit, so the sampler outlives the SSM command.
  if [[ $MODE == aws ]] && [[ -d /run/systemd/system ]]; then
    systemd-run --quiet --collect --unit "paxos-sampler-$run" "$dir/sampler.sh"
  else
    setsid "$dir/sampler.sh" >/dev/null 2>&1 </dev/null &
  fi
  echo "started_ms=$(now_ms)"
}

loadgen_running() {
  local run=$1
  if [[ $MODE == aws ]]; then
    [[ $(docker inspect -f '{{.State.Running}}' "loadgen-$run" 2>/dev/null) == true ]]
  else
    kill -0 "$(cat "$NODE_WORK/$run/loadgen.pid")" 2>/dev/null
  fi
}

# node status <run>: the measured window (from the timeline header) once
# loadgen has opened the accounts, and whether loadgen still runs.
node_status() {
  local run=$1 dir=$NODE_WORK/$1 h=
  [[ -s $dir/timeline.jsonl ]] && h=$(head -n 1 "$dir/timeline.jsonl")
  if [[ $h == *window_start_unix_ms* ]]; then
    echo "window_start_ms=$(sed -E 's/.*"window_start_unix_ms":([0-9]+).*/\1/' <<<"$h")"
    echo "window_end_ms=$(sed -E 's/.*"window_end_unix_ms":([0-9]+).*/\1/' <<<"$h")"
  fi
  if loadgen_running "$run"; then echo "loadgen=running"; else echo "loadgen=exited"; fi
}

# node leaders <maddrs>: "leader <shard> <node>" for every shard with
# exactly one node reporting paxos_is_leader 1.
node_leaders() {
  local m
  IFS=, read -ra ms <<<"$1"
  for m in "${ms[@]}"; do
    curl -s -m 2 "http://$m/metrics" | awk '/^paxos_is_leader\{/ && $NF == 1 {
      match($0, /node="[0-9]+"/); n = substr($0, RSTART + 6, RLENGTH - 7)
      match($0, /shard="[0-9]+"/); s = substr($0, RSTART + 7, RLENGTH - 8)
      print "leader", s, n }'
  done | sort -k2,2n | awk '{c[$2]++; l[$2] = $0} END {for (s in c) if (c[s] == 1) print l[s]}' | sort -k2,2n
}

# node kill: SIGKILL this node's paxosd, with Docker's restart policy off
# so it stays down.
node_kill() {
  local t
  if [[ $MODE == aws ]]; then
    docker update --restart=no paxosd >/dev/null
    t=$(now_ms)
    docker kill -s KILL paxosd >/dev/null
  else
    t=$(now_ms)
    kill -KILL "$(cat "$LOCAL_DIR/n${NODE:?}.pid")"
  fi
  echo "kill_ms=$t"
}

# node restart: start paxosd again (restart policy back on) and wait for
# /readyz 200: every shard on the node has a leader and is caught up.
node_restart() {
  local t ready='' maddr=127.0.0.1:9100
  if [[ $MODE == aws ]]; then
    docker update --restart unless-stopped paxosd >/dev/null
    t=$(now_ms)
    docker start paxosd >/dev/null
  else
    maddr=127.0.0.1:1910$NODE
    t=$(now_ms)
    # shellcheck disable=SC2046 # the saved command line is a word list
    setsid $(cat "$LOCAL_DIR/n$NODE.cmd") >>"$LOCAL_DIR/n$NODE.log" 2>&1 </dev/null &
    echo $! >"$LOCAL_DIR/n$NODE.pid"
  fi
  for _ in $(seq 1 3000); do # 5 minutes
    if [[ $(curl -s -m 1 -o /dev/null -w '%{http_code}' "http://$maddr/readyz" || true) == 200 ]]; then
      ready=$(now_ms)
      break
    fi
    sleep 0.1
  done
  echo "restart_ms=$t"
  echo "ready_ms=${ready:-none}"
}

# node finish <run> <maddrs>: wait for loadgen to end, stop the sampler,
# and wait (up to 5 minutes) until every node is ready with no transaction
# in doubt, so the logs are final for the checkers.
node_finish() {
  local run=$1 dir=$NODE_WORK/$1 m doubt ready settled=
  IFS=, read -ra ms <<<"$2"
  while loadgen_running "$run"; do sleep 1; done
  touch "$dir/stop-sampler"
  if [[ $MODE == aws ]]; then
    echo "loadgen_exit=$(docker inspect -f '{{.State.ExitCode}}' "loadgen-$run")"
    docker logs "loadgen-$run" >"$dir/loadgen.log" 2>&1
    docker rm "loadgen-$run" >/dev/null
  fi
  for _ in $(seq 1 1500); do
    ready=1 doubt=0
    for m in "${ms[@]}"; do
      [[ $(curl -s -m 1 -o /dev/null -w '%{http_code}' "http://$m/readyz" || true) == 200 ]] || ready=0
      doubt=$(curl -s -m 1 "http://$m/metrics" | awk -v d="$doubt" '/^txn_in_doubt[{ ]/ {d += $NF} END {print d}')
    done
    if (( ready == 1 )) && [[ $doubt == 0 ]]; then settled=$(now_ms); break; fi
    sleep 0.2
  done
  echo "settled_ms=${settled:-none}"
  tail -n 3 "$dir/loadgen.log"
}

# node upload <run> <bucket> <region> <node name> (aws only): node 1
# uploads the run's outputs, every node a copy of its SQLite files (the
# cluster is idle after `finish`, so the files are final). The instance
# role may write only under runs/ in that bucket (its bucket policy).
node_upload() {
  local run=$1 bucket=$2 region=$3 name=$4 dir=$NODE_WORK/$1
  local dest=s3://$bucket/runs/$run
  if [[ -d $dir ]]; then
    tar -C "$dir" -czf - --exclude sampler.sh --exclude stop-sampler . |
      aws s3 cp --region "$region" --only-show-errors - "$dest/run.tar.gz"
  fi
  (cd /data && tar -czf - node-*-shard-*.db*) | aws s3 cp --region "$region" --only-show-errors - "$dest/data-$name.tar.gz"
  echo "uploaded to $dest"
}

node_main() {
  local step=$1
  shift
  case $step in
    start) node_start "$@" ;;
    status) node_status "$@" ;;
    leaders) node_leaders "$@" ;;
    kill) node_kill ;;
    restart) node_restart ;;
    finish) node_finish "$@" ;;
    upload) node_upload "$@" ;;
    *) echo "unknown step $step" >&2; exit 2 ;;
  esac
}

# ---- The run (workflow runner, or local) ----

# on <node 1-3> <step> [args]: run a node step and print its output.
on() {
  local n=$1
  shift
  if [[ $MODE == local ]]; then
    MODE=local NODE=$n LOCAL_DIR=$LOCAL_DIR NODE_WORK=$NODE_WORK "$SELF" node "$@"
    return
  fi
  local id=${IDS[$n]} cmd status
  cmd=$(aws ssm send-command --document-name AWS-RunShellScript --instance-ids "$id" \
    --comment "failover-load $1" --query Command.CommandId --output text \
    --parameters "$(ssm_params "$@")")
  while :; do
    status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" --query Status --output text 2>/dev/null || echo Pending)
    case $status in Pending | InProgress | Delayed) sleep 1 ;; *) break ;; esac
  done
  aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" --query StandardOutputContent --output text
  if [[ $status != Success ]]; then
    aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" --query StandardErrorContent --output text >&2
    log "node$n $1: $status"
    return 1
  fi
}

# ssm_params <step> [args]: AWS-RunShellScript parameters that run this
# script's node step. The script travels gzipped and base64-encoded inside
# the command, so no quoting of it matters and it stays well under SSM's
# parameter size limit.
ssm_params() {
  local script args
  script=$(gzip -9c "$SELF" | base64 -w0)
  args=$(printf '%q ' "$@")
  jq -n --arg s "$script" --arg a "$args" '{
    executionTimeout: ["900"],
    commands: ["set -euo pipefail", "f=$(mktemp)", "echo \($s) | base64 -d | gunzip > \"$f\"", "MODE=aws bash \"$f\" node \($a)"]}'
}

# sleep_until <unix ms>
sleep_until() {
  local d=$(( $1 - $(now_ms) ))
  (( d > 0 )) && sleep "$(awk -v d="$d" 'BEGIN {printf "%.3f", d / 1000}')"
  return 0
}

local_cluster() {
  local i
  LOCAL_DIR=$OUT/local
  NODE_WORK=$LOCAL_DIR/work
  mkdir -p "$LOCAL_DIR" "$NODE_WORK"
  printf '{"shards": 3, "nodes": [{"id": 1, "addr": "127.0.0.1:17001"}, {"id": 2, "addr": "127.0.0.1:17002"}, {"id": 3, "addr": "127.0.0.1:17003"}]}\n' >"$LOCAL_DIR/cluster.json"
  for i in 1 2 3; do
    echo "$PAXOSD_BIN -config $LOCAL_DIR/cluster.json -id $i -data-dir $LOCAL_DIR/data/n$i -metrics-listen 127.0.0.1:1910$i -trace-sample 0" >"$LOCAL_DIR/n$i.cmd"
    # shellcheck disable=SC2046
    setsid $(cat "$LOCAL_DIR/n$i.cmd") >"$LOCAL_DIR/n$i.log" 2>&1 </dev/null &
    echo $! >"$LOCAL_DIR/n$i.pid"
  done
  trap 'for i in 1 2 3; do kill -TERM "$(cat "$LOCAL_DIR/n$i.pid")" 2>/dev/null || true; done' EXIT
  for i in 1 2 3; do
    for _ in $(seq 1 200); do
      curl -sf -o /dev/null "http://127.0.0.1:1910$i/readyz" && break
      sleep 0.1
    done
  done
  ADDRS=127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003
  MADDRS=127.0.0.1:19101,127.0.0.1:19102,127.0.0.1:19103
  IMAGE=none
}

run_main() {
  MODE=$1 OUT=$2
  mkdir -p "$OUT"
  OUT=$(cd "$OUT" && pwd)
  SELF=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
  RUN=${RUN_ID:-run-$(date -u +%Y%m%dT%H%M%SZ)}
  local ev=$OUT/events.env
  : >"$ev"
  if [[ $MODE == local ]]; then
    : "${PAXOSD_BIN:?set PAXOSD_BIN}" "${LOADGEN_BIN:?set LOADGEN_BIN}"
    local_cluster
  else
    : "${LOADTEST_BUCKET:?set LOADTEST_BUCKET}" "${AWS_REGION:?set AWS_REGION}"
    declare -gA IDS
    local ids ips n
    ids=$(terraform output -json instance_ids)
    ips=$(terraform output -json private_ips)
    for n in 1 2 3; do
      IDS[$n]=$(jq -r ".node$n // empty" <<<"$ids")
      [[ ${IDS[$n]} == i-* ]] || { echo "no instance for node$n in the state: is the cluster deployed?" >&2; exit 1; }
    done
    ADDRS=$(jq -r '[.node1, .node2, .node3] | map(. + ":7001") | join(",")' <<<"$ips")
    MADDRS=$(jq -r '[.node1, .node2, .node3] | map(. + ":9100") | join(",")' <<<"$ips")
    # The image node 1 runs, pinned by digest (paxosd-run records it).
    local digest
    digest=$(on 1 digest | tr -d '[:space:]')
    [[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "node1 reports no image digest: $digest" >&2; exit 1; }
    IMAGE=ghcr.io/shrutij516/paxos-txn-store@$digest
    echo "image=$IMAGE" >>"$ev"
    # standard credits throttle a node that has spent them (incident I19).
    echo "cpu_credits=$(aws ec2 describe-instance-credit-specifications --instance-ids "${IDS[1]}" "${IDS[2]}" "${IDS[3]}" \
      --query 'InstanceCreditSpecifications[].CpuCredits' --output text | tr -s '\t ' '\n' | sort -u | paste -sd/)" >>"$ev"
  fi
  {
    echo "run=$RUN"
    echo "warmup_s=$WARMUP duration_s=$DURATION kill_at_s=$KILL_AT down_s=$DOWN clients=$CLIENTS keys=$KEYS cross=$CROSS profile=$PROFILE"
  } >>"$ev"

  log "starting loadgen and the leader sampler on node1 ($RUN)"
  on 1 start "$RUN" "$IMAGE" "$ADDRS" "$MADDRS" | tee -a "$OUT/node-steps.log"

  # Wait for loadgen to open the accounts and start its clock.
  local st win=
  for _ in $(seq 1 120); do
    st=$(on 1 status "$RUN")
    win=$(sed -n 's/^window_start_ms=//p' <<<"$st")
    [[ -n $win ]] && break
    [[ $st == *loadgen=exited* ]] && { echo "loadgen exited before its measured window" >&2; on 1 finish "$RUN" "$MADDRS" >&2 || true; exit 1; }
    sleep 2
  done
  [[ -n $win ]] || { echo "loadgen did not start its window within 4 minutes" >&2; exit 1; }
  echo "window_start_ms=$win" >>"$ev"
  sed -n 's/^window_end_ms=/window_end_ms=/p' <<<"$st" >>"$ev"
  log "measured window starts at $win; kill at +${KILL_AT}s"

  sleep_until $((win + KILL_AT * 1000))
  # The victim: the node leading the most shards; node 1 runs loadgen, so
  # if it is node 1 take the next one (most shards among nodes 2 and 3).
  local leaders victim
  leaders=$(on 1 leaders "$MADDRS")
  echo "before the kill:"$'\n'"$leaders" >>"$OUT/node-steps.log"
  # Ties go to the lower node ID.
  local most_any
  most_any=$(awk '{c[$3]++} END {m = 1; for (n = 2; n <= 3; n++) if (c[n] > c[m]) m = n; print m}' <<<"$leaders")
  victim=$most_any
  if [[ $victim == 1 ]]; then
    victim=$(awk '{c[$3]++} END {print (c[3] > c[2]) ? 3 : 2}' <<<"$leaders")
  fi
  local victim_shards
  victim_shards=$(awk -v v="$victim" '$3 == v {printf "%s%s", sep, $2; sep = ","}' <<<"$leaders")
  {
    echo "leaders_before_kill=$(awk '{printf "%s%s:%s", sep, $2, $3; sep = ","}' <<<"$leaders")"
    echo "most_shards_node=$most_any"
    echo "victim=$victim"
    echo "victim_shards=$victim_shards"
  } >>"$ev"
  log "leaders: $(tr '\n' ' ' <<<"$leaders"); victim node$victim (leads shards ${victim_shards:-none})"
  on "$victim" kill | tee -a "$OUT/node-steps.log" >>"$ev"
  local kill_ms
  kill_ms=$(sed -n 's/^kill_ms=//p' "$ev")
  log "node$victim killed at $kill_ms"

  sleep_until $((kill_ms + DOWN * 1000))
  log "restarting node$victim"
  on "$victim" restart | tee -a "$OUT/node-steps.log" >>"$ev"

  log "waiting for loadgen to finish and the cluster to settle"
  on 1 finish "$RUN" "$MADDRS" | tee -a "$OUT/node-steps.log" | grep -E '^(settled_ms|loadgen_exit)=' >>"$ev" || true

  if [[ $MODE == local ]]; then
    cp "$NODE_WORK/$RUN"/{timeline.jsonl,report.json,history.jsonl,loadgen.log,leaders.txt} "$OUT/"
    for n in 1 2 3; do
      mkdir -p "$OUT/data/node$n"
      cp "$LOCAL_DIR/data/n$n"/node-*-shard-*.db* "$OUT/data/node$n/"
    done
    cp "$LOCAL_DIR/cluster.json" "$OUT/cluster.json"
  else
    for n in 1 2 3; do on "$n" upload "$RUN" "$LOADTEST_BUCKET" "$AWS_REGION" "node$n" >>"$OUT/node-steps.log"; done
    aws s3 cp --only-show-errors "s3://$LOADTEST_BUCKET/runs/$RUN/run.tar.gz" - | tar -C "$OUT" -xzf -
    for n in 1 2 3; do
      mkdir -p "$OUT/data/node$n"
      aws s3 cp --only-show-errors "s3://$LOADTEST_BUCKET/runs/$RUN/data-node$n.tar.gz" - | tar -C "$OUT/data/node$n" -xzf -
    done
  fi
  log "done: $OUT"
}

case ${1:-} in
  node)
    shift
    MODE=${MODE:-aws}
    if [[ ${1:-} == digest ]]; then cat /etc/paxosd/image-digest; exit 0; fi
    node_main "$@"
    ;;
  ssm-params) # for testing the command encoding
    shift
    SELF=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
    ssm_params "$@"
    ;;
  run)
    if [[ ${2:-} != aws && ${2:-} != local ]] || [[ -z ${3:-} ]]; then
      echo "usage: $0 run aws|local <out-dir>" >&2
      exit 2
    fi
    run_main "$2" "$3"
    ;;
  *)
    sed -n '2,32p' "$0" >&2
    exit 2
    ;;
esac
