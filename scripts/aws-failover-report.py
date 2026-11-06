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

Failover time per shard the victim led: from the kill to the first read
(each node is read every ~100 ms, from node 1) in which another node
reports paxos_is_leader 1 for that shard. Kill time is taken on the victim,
reads on node 1; both clocks are NTP-synchronized (Amazon Time Sync on EC2).

A read that times out or fails tells nothing about leadership, so a gap in
a survivor's successful reads can hide when the new leader took over and
make a failover look slower than it was (incident I22). A failover time is
"measured" only if every survivor's successful reads (reads.txt) are at
most GAP_MS apart from READ_MS before the kill until READ_MS after the new
leader was first seen; otherwise it is "uncertain", with each survivor's
longest unread gap in that span.
"""
import json
import os
import sys

DURING_MS = 10_000
READ_MS = 100  # the sampler's read interval
GAP_MS = 2 * READ_MS  # successful reads further apart than this miss a sample


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
    # Every read, per node: (start ms, outcome, latency ms).
    reads = {}
    path = os.path.join(out, "reads.txt")
    if os.path.exists(path):
        for line in open(path):
            p = line.split()
            if len(p) == 4 and p[0].isdigit():
                reads.setdefault(p[1], []).append((int(p[0]), p[2], int(p[3])))
    for r in reads.values():
        r.sort()
    survivors = sorted(n for n in reads if n != victim)

    def longest_gap(n, a, b):
        """Longest stretch overlapping [a, b] between successful reads of node n."""
        ok = [t for t, o, _ in reads.get(n, []) if o == "ok"]
        before = [t for t in ok if t <= a]
        pts = ([before[-1]] if before else [a]) + [t for t in ok if a < t < b]
        after = [t for t in ok if t >= b]
        pts += [after[0]] if after else [b]
        return max(y - x for x, y in zip(pts, pts[1:])) if len(pts) > 1 else b - a

    for s in led:
        hit = next(((t, n) for t, n, sh, v in samples if t >= kill and sh == s and n != victim and v in ("1", "1.0")), None)
        if not hit:
            end = (ready or kill + DURING_MS)
            failover[s] = {"ms": None, "new_leader": None, "status": "uncertain",
                           "gaps_ms": {n: longest_gap(n, kill, end) for n in survivors}, "earliest_ms": None}
            continue
        t, n = hit
        gaps = {m: longest_gap(m, kill - READ_MS, t + READ_MS) for m in survivors}
        prev = [x for x, o, _ in reads.get(n, []) if o == "ok" and x < t]
        status = "measured" if reads and survivors and all(g <= GAP_MS for g in gaps.values()) else "uncertain"
        failover[s] = {"ms": t - kill, "new_leader": n, "status": status, "gaps_ms": gaps,
                       # The take-over happened after the new leader's previous successful read.
                       "earliest_ms": max(prev[-1] - kill, 0) if prev else None}

    # Per node, over the whole run: read outcomes and latency.
    read_stats = {}
    for n, r in sorted(reads.items()):
        lat = [l for _, o, l in r if o == "ok"]
        read_stats[n] = {"ok": len(lat), "timeout": sum(o == "timeout" for _, o, _ in r), "error": sum(o == "error" for _, o, _ in r),
                         "p50_ms": pct(lat, 0.5), "p99_ms": pct(lat, 0.99), "max_ms": max(lat) if lat else None,
                         "longest_gap_after_kill_ms": longest_gap(n, kill, w1) if n != victim else None}

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
        "reads": read_stats,
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
    md.append("| Shard | Failover (ms) | Status | New leader | Longest unread gap per survivor (ms) |")
    md.append("|---|---|---|---|---|")
    for s in led:
        fo = failover[s]
        gaps = ", ".join(f"node{n} {g}" for n, g in fo["gaps_ms"].items()) or "no read log"
        rng = f(fo["ms"]) if fo["status"] == "measured" or fo["ms"] is None else f"{f(fo['earliest_ms'])} to {f(fo['ms'])}"
        md.append(f"| {s} | {rng} | {fo['status']} | {'node' + fo['new_leader'] if fo['new_leader'] else 'none seen'} | {gaps} |")
    if not led:
        md.append("| - | the victim led no shard | - | - | - |")
    md.append("")
    md.append(f"Measured: every survivor read successfully at least every {GAP_MS} ms from {READ_MS} ms before the kill to {READ_MS} ms after the new leader "
              "was first seen. Uncertain: a survivor went unread longer than that, so the take-over may have come earlier; the range runs from the new "
              "leader's previous successful read to the read that showed it leading.")
    md.append("")
    if read_stats:
        md.append("| Node | Reads ok | Timeouts | Errors | Read p50 ms | p99 ms | max ms | Longest unread gap after the kill (ms) |")
        md.append("|---|---|---|---|---|---|---|---|")
        for n, r in read_stats.items():
            gap_txt = "victim" if r["longest_gap_after_kill_ms"] is None else r["longest_gap_after_kill_ms"]
            md.append(f"| node{n} | {r['ok']} | {r['timeout']} | {r['error']} | {f(r['p50_ms'])} | {f(r['p99_ms'])} | {f(r['max_ms'])} | {gap_txt} |")
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
