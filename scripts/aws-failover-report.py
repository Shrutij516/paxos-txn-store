#!/usr/bin/env python3
"""Summarize a failover-under-load run (scripts/aws-failover-load.sh).

    scripts/aws-failover-report.py <out-dir>

Reads events.env, leaders.txt, timeline.jsonl, history.jsonl and report.json
from the run's
output directory and writes summary.md and summary.json there (and prints
the Markdown). Phases of the measured window, by the wall-clock time each
transaction finished:

  before        window start .. kill
  during        kill .. kill + 10 s
  after         kill + 10 s .. the restarted node is ready (two nodes)
  after rejoin  restarted node ready .. window end (three nodes)

Failover time per shard the victim led: from the kill to the first sample
(every ~100 ms, on node 1) in which another node reports paxos_is_leader 1
for that shard. Kill time is taken on the victim, samples on node 1; both
clocks are NTP-synchronized (Amazon Time Sync on EC2).
"""
import json
import os
import sys

DURING_MS = 10_000


def read_env(path):
    ev = {}
    for line in open(path):
        for tok in line.split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                ev[k] = v
    return ev


def pct(xs, q):
    if not xs:
        return None
    s = sorted(xs)
    return s[max(0, int(q * len(s) + 0.999999) - 1)]


def main(out):
    ev = read_env(os.path.join(out, "events.env"))
    kill = int(ev["kill_ms"])
    victim = ev["victim"]
    restart = int(ev["restart_ms"]) if ev.get("restart_ms", "none").isdigit() else None
    ready = int(ev["ready_ms"]) if ev.get("ready_ms", "none").isdigit() else None

    lines = open(os.path.join(out, "timeline.jsonl")).read().splitlines()
    header = json.loads(lines[0])
    w0, w1 = header["window_start_unix_ms"], header["window_end_unix_ms"]
    txns = [json.loads(l) for l in lines[1:] if l.strip()]
    # Aborted attempts (each retried by the client) from the history, whose
    # times are nanoseconds since loadgen started.
    t0 = header["start_unix_ms"]
    aborts = []
    for l in open(os.path.join(out, "history.jsonl")):
        a = json.loads(l).get("attempt")
        if a and not a.get("setup") and a["outcome"] == "aborted":
            aborts.append(t0 + a["finish"] / 1e6)

    phases = [("before", w0, kill), ("during", kill, kill + DURING_MS)]
    if ready:
        phases += [("after", kill + DURING_MS, ready), ("after rejoin", ready, w1)]
    else:
        phases += [("after", kill + DURING_MS, w1)]
    rows = []
    for name, a, b in phases:
        a, b = max(a, w0), min(b, w1)
        if b <= a:
            continue
        sel = [t for t in txns if a <= t["t"] < b]
        lat = [t["ms"] for t in sel if t["outcome"] == "committed"]
        rows.append({
            "phase": name,
            "seconds": round((b - a) / 1000, 1),
            "committed_per_sec": round(len(lat) / ((b - a) / 1000), 1),
            "p50_ms": pct(lat, 0.50),
            "p99_ms": pct(lat, 0.99),
            "max_ms": max(lat) if lat else None,
            "aborted_attempts": sum(a <= x < b for x in aborts),
            "errors": sum(t["outcome"] in ("unknown", "error") for t in sel),
        })

    # Longest stretch without a commit, from the kill on.
    ends = sorted(t["t"] for t in txns if t["outcome"] == "committed" and t["t"] >= kill)
    gap, prev = 0, kill
    for e in ends:
        gap, prev = max(gap, e - prev), e

    failover = {}
    led = [s for s in ev.get("victim_shards", "").split(",") if s]
    samples = []
    path = os.path.join(out, "leaders.txt")
    if os.path.exists(path):
        for line in open(path):
            p = line.split()
            if len(p) == 4 and p[0].isdigit():
                samples.append((int(p[0]), p[1], p[2], p[3]))
    samples.sort()
    for s in led:
        hit = next(((t, n) for t, n, sh, v in samples if t >= kill and sh == s and n != victim and v in ("1", "1.0")), None)
        failover[s] = {"ms": hit[0] - kill, "new_leader": hit[1]} if hit else {"ms": None, "new_leader": None}

    rep = json.load(open(os.path.join(out, "report.json")))
    summary = {
        "run": ev.get("run"),
        "image": ev.get("image"),
        "cpu_credits": ev.get("cpu_credits"),
        "config": {k: ev.get(k) for k in ("warmup_s", "duration_s", "kill_at_s", "down_s", "clients", "keys", "cross", "profile")},
        "leaders_before_kill": ev.get("leaders_before_kill"),
        "most_shards_node": ev.get("most_shards_node"),
        "victim": victim,
        "victim_shards": led,
        "failover_ms": failover,
        "longest_commit_gap_after_kill_ms": gap,
        "rejoin_s": round((ready - restart) / 1000, 2) if ready and restart else None,
        "phases": rows,
        "whole_window": {"committed_per_sec": rep["committed_per_sec"], "p50_ms": rep["latency_ms"]["p50"], "p99_ms": rep["latency_ms"]["p99"],
                         "aborted": rep["aborted"], "unknown": rep["unknown"], "failed": rep["failed"], "errors": rep["errors"]},
        "settled": ev.get("settled_ms", "none") != "none",
    }
    json.dump(summary, open(os.path.join(out, "summary.json"), "w"), indent=2)

    f = lambda v, d=1: "n/a" if v is None else (f"{v:.{d}f}" if isinstance(v, float) else str(v))
    md = []
    md.append(f"### Failover under load: run {summary['run']}")
    md.append("")
    where = f"Image `{summary['image']}`, CPU credits `{summary['cpu_credits']}`" if summary["image"] else "Local processes"
    md.append(f"{where}. Load: {ev.get('clients')} clients, {ev.get('profile')} contention, cross-shard {ev.get('cross')}, "
              f"{ev.get('keys')} accounts; {ev.get('warmup_s')} s warmup, {ev.get('duration_s')} s measured. "
              f"Leaders before the kill (shard:node): {ev.get('leaders_before_kill')}.")
    md.append("")
    note = "" if victim == summary["most_shards_node"] else f" (node{summary['most_shards_node']} led the most, but runs loadgen)"
    md.append(f"Killed node{victim}{note}, leading shards {', '.join(led) or 'none'}, {ev.get('kill_at_s')} s into the window; restarted {ev.get('down_s')} s later.")
    md.append("")
    md.append("| Shard | Failover (ms) | New leader |")
    md.append("|---|---|---|")
    for s in led:
        fo = failover[s]
        md.append(f"| {s} | {f(fo['ms'])} | {'node' + fo['new_leader'] if fo['new_leader'] else 'none seen'} |")
    if not led:
        md.append("| - | the victim led no shard | - |")
    md.append("")
    md.append("| Phase | Seconds | Committed/s | p50 ms | p99 ms | max ms | Aborted attempts | Errors |")
    md.append("|---|---|---|---|---|---|---|---|")
    for r in rows:
        md.append(f"| {r['phase']} | {r['seconds']} | {r['committed_per_sec']} | {f(r['p50_ms'])} | {f(r['p99_ms'])} | {f(r['max_ms'])} | {r['aborted_attempts']} | {r['errors']} |")
    ww = summary["whole_window"]
    md.append(f"| whole window | {ev.get('duration_s')} | {ww['committed_per_sec']:.1f} | {ww['p50_ms']:.1f} | {ww['p99_ms']:.1f} | | {ww['aborted']} | {ww['errors']} |")
    md.append("")
    md.append(f"- **Rejoin:** node{victim} ready (/readyz 200) {f(summary['rejoin_s'], 2)} s after its restart.")
    md.append(f"- **Longest stretch without a commit after the kill:** {gap} ms.")
    md.append(f"- **Errors in the window** (unknown outcome or failed): {ww['errors']} ({ww['unknown']} unknown, {ww['failed']} failed). "
              "Errors per phase count transactions that ended without committing after all their attempts; aborted attempts are retried.")
    md.append("- Latency is end to end per committed transaction (retries included), attributed to the phase in which it finished.")
    md.append(f"- **Settled** (every node ready, nothing in doubt) before the logs were copied: {'yes' if summary['settled'] else 'NO'}.")
    md.append("- Caveat: loadgen runs on node 1 and shares its CPU with node 1's paxosd.")
    if summary["cpu_credits"] and "standard" in summary["cpu_credits"]:
        md.append("- **Warning:** the nodes use standard CPU credits; a node that has spent them is throttled to 20% of each vCPU "
                  "(incident I19). Check CloudWatch CPUCreditBalance, or deploy with cpu_credits = unlimited.")
    text = "\n".join(md) + "\n"
    open(os.path.join(out, "summary.md"), "w").write(text)
    print(text)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
