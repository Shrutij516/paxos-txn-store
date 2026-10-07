// Command chaos runs a fault-injection test against the compose cluster in
// deploy/compose.yaml (see docs/chaos.md):
//
//	chaos -duration 2m -loadgen ./bin/loadgen -out chaos-out
//
// It starts loadgen against the cluster, and for the whole run injects one
// fault at a time on a random node, each held for a few seconds and then
// undone: SIGKILL (docker kill, then docker start), network delay and
// packet loss (tc netem), and a partition that cuts the node off from
// everyone (netem dropping every packet). tc runs in a short-lived sidecar
// container that shares the node's network namespace and has NET_ADMIN,
// since the node image has no shell or tools. When the kernel has no netem
// (some sandboxes), delay and loss are skipped and the partition uses
// iptables instead.
//
// By default it first recreates the stack with empty volumes (-fresh), so
// the history covers every transaction in the logs. When the run ends it
// heals everything, waits until every node is ready
// and no transaction is in doubt, stops the nodes with SIGTERM, copies
// their data out, and runs the checkers (internal/chaos.Check) on the
// history. It writes report.json, faults.json, history.jsonl, loadgen.log
// and compose.log to -out and exits 1 on any violation.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Shrutij516/paxos-txn-store/history"
	"github.com/Shrutij516/paxos-txn-store/internal/chaos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
)

type config struct {
	compose, project, cluster string
	services                  []string
	addrs, metrics            string
	loadgen                   string
	loadgenArgs               []string
	duration                  time.Duration
	minGap, maxGap            time.Duration
	minHold, maxHold          time.Duration
	settle                    time.Duration
	netemImage                string
	fresh                     bool
	out                       string
	seed                      uint64
}

func main() {
	var c config
	var services, loadgenArgs string
	flag.StringVar(&c.compose, "compose", "deploy/compose.yaml", "compose file of the running cluster")
	flag.StringVar(&c.project, "project", "paxos", "compose project name")
	flag.StringVar(&c.cluster, "cluster", "deploy/cluster.json", "cluster config the nodes use (for the shard count)")
	flag.StringVar(&services, "services", "node1,node2,node3", "compose services of the paxosd nodes")
	flag.StringVar(&c.addrs, "addrs", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "node addresses for loadgen")
	flag.StringVar(&c.metrics, "metrics", "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103", "node metrics addresses, in -services order")
	flag.StringVar(&c.loadgen, "loadgen", "loadgen", "loadgen binary")
	flag.StringVar(&loadgenArgs, "loadgen-args", "", "extra loadgen flags, space separated")
	flag.DurationVar(&c.duration, "duration", 2*time.Minute, "how long the workload and the faults run")
	flag.DurationVar(&c.minGap, "min-gap", 2*time.Second, "shortest pause between faults")
	flag.DurationVar(&c.maxGap, "max-gap", 6*time.Second, "longest pause between faults")
	flag.DurationVar(&c.minHold, "min-hold", 2*time.Second, "shortest time a fault lasts")
	flag.DurationVar(&c.maxHold, "max-hold", 8*time.Second, "longest time a fault lasts")
	flag.DurationVar(&c.settle, "settle", 2*time.Minute, "how long the cluster may take to become ready with nothing in doubt after the run")
	flag.StringVar(&c.netemImage, "netem-image", "mirror.gcr.io/nicolaka/netshoot:v0.14", "image with tc and iptables for the fault sidecar")
	flag.BoolVar(&c.fresh, "fresh", true, "recreate the cluster with empty volumes first (docker compose down -v, then up --wait); the history must cover every transaction in the logs")
	flag.StringVar(&c.out, "out", "chaos-out", "output directory")
	flag.Uint64Var(&c.seed, "seed", uint64(time.Now().UnixNano()), "random seed for the fault schedule")
	flag.Parse()
	c.services = strings.Split(services, ",")
	c.loadgenArgs = strings.Fields(loadgenArgs)
	if err := run(c); err != nil {
		log.Fatalf("chaos: %v", err)
	}
}

// fault is one injected fault, for faults.json.
type fault struct {
	Kind   string  `json:"kind"`
	Node   string  `json:"node"`
	Detail string  `json:"detail,omitempty"`
	Start  float64 `json:"start_s"`
	End    float64 `json:"end_s"`
}

func run(c config) error {
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		return err
	}
	cl, err := server.LoadCluster(c.cluster)
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(c.seed, 0xc4a05))
	d := docker{file: c.compose, project: c.project}
	if c.fresh {
		log.Printf("chaos: recreating the cluster with empty volumes")
		if err := d.composeCmd("down", "-v", "--remove-orphans"); err != nil {
			return err
		}
		if err := d.composeCmd("up", "-d", "--no-build", "--wait"); err != nil {
			return err
		}
	}
	ids := map[string]string{}
	for _, s := range c.services {
		if ids[s], err = d.containerID(s); err != nil {
			return err
		}
	}
	netem := d.netemWorks(c.netemImage, ids[c.services[0]])
	log.Printf("chaos: seed %d, duration %v, netem available: %v", c.seed, c.duration, netem)

	// The workload.
	hist := filepath.Join(c.out, "history.jsonl")
	lg, err := os.Create(filepath.Join(c.out, "loadgen.log"))
	if err != nil {
		return err
	}
	defer func() { _ = lg.Close() }()
	args := append([]string{"-addrs", c.addrs, "-duration", c.duration.String(), "-out", hist, "-seed", fmt.Sprint(c.seed % 1000000)}, c.loadgenArgs...)
	load := exec.Command(c.loadgen, args...)
	load.Stdout, load.Stderr = lg, lg
	if err := load.Start(); err != nil {
		return err
	}
	loadDone := make(chan error, 1)
	go func() { loadDone <- load.Wait() }()

	// The faults, one at a time, until the workload's time is up.
	start := time.Now()
	since := func() float64 { return time.Since(start).Seconds() }
	kinds := []string{"kill", "partition"}
	if netem {
		kinds = append(kinds, "delay", "loss")
	}
	var faults []fault
	deadline := start.Add(c.duration)
	pick := func(lo, hi time.Duration) time.Duration { return lo + time.Duration(rng.Int64N(int64(hi-lo)+1)) }
	for {
		time.Sleep(pick(c.minGap, c.maxGap))
		hold := pick(c.minHold, c.maxHold)
		if time.Now().Add(hold).After(deadline) {
			break
		}
		f := fault{Kind: kinds[rng.IntN(len(kinds))], Node: c.services[rng.IntN(len(c.services))], Start: since()}
		id := ids[f.Node]
		undo, err := inject(d, c.netemImage, netem, f.Kind, id, ids, rng, &f)
		if err != nil {
			return fmt.Errorf("injecting %s on %s: %w", f.Kind, f.Node, err)
		}
		log.Printf("chaos: t=%.1fs %s %s %s for %v", f.Start, f.Kind, f.Node, f.Detail, hold.Round(100*time.Millisecond))
		time.Sleep(hold)
		if err := undo(); err != nil {
			return fmt.Errorf("undoing %s on %s: %w", f.Kind, f.Node, err)
		}
		f.End = since()
		faults = append(faults, f)
	}
	if err := writeJSON(filepath.Join(c.out, "faults.json"), faults); err != nil {
		return err
	}
	select {
	case err := <-loadDone:
		if err != nil {
			return fmt.Errorf("loadgen failed (see loadgen.log): %w", err)
		}
	case <-time.After(c.duration + 3*time.Minute):
		_ = load.Process.Kill()
		return errors.New("loadgen did not finish")
	}

	// Settle, stop cleanly, copy the data out and check.
	var violations []string
	if err := settle(strings.Split(c.metrics, ","), c.settle); err != nil {
		violations = append(violations, "cluster did not settle after the run: "+err.Error())
	}
	_ = d.saveLogs(filepath.Join(c.out, "compose.log"))
	if err := d.composeCmd(append([]string{"stop", "-t", "10"}, c.services...)...); err != nil {
		return err
	}
	var dirs []string
	for _, s := range c.services {
		dir := filepath.Join(c.out, "data", s)
		_ = os.RemoveAll(dir)
		if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
			return err
		}
		if err := d.run("cp", ids[s]+":/data", dir); err != nil {
			return err
		}
		dirs = append(dirs, dir)
	}
	f, err := os.Open(hist)
	if err != nil {
		return err
	}
	meta, atts, err := history.Read(f)
	_ = f.Close()
	if err != nil {
		return err
	}
	rep := chaos.Report{}
	if sms, err := chaos.LoadShards(dirs, cl.Shards); err != nil {
		violations = append(violations, err.Error())
	} else {
		rep = chaos.Check(meta, atts, sms)
	}
	rep.Violations = append(violations, rep.Violations...)
	counts := map[string]int{}
	for _, f := range faults {
		counts[f.Kind]++
	}
	summary := struct {
		Seed     uint64         `json:"seed"`
		Duration string         `json:"duration"`
		Netem    bool           `json:"netem"`
		Faults   map[string]int `json:"faults"`
		chaos.Report
	}{c.seed, c.duration.String(), netem, counts, rep}
	if err := writeJSON(filepath.Join(c.out, "report.json"), summary); err != nil {
		return err
	}
	log.Printf("chaos: faults %v; %d attempts, %d committed (%d cross-shard), %d aborted, %d unknown (%d committed), %d failed before commit; %d committed txns checked",
		counts, rep.Attempts, rep.Committed, rep.CrossShard, rep.Aborted, rep.Unknown, rep.UnknownCommitted, rep.Failed, rep.CheckedTxns)
	if len(rep.Violations) > 0 {
		for _, v := range rep.Violations {
			log.Printf("chaos: VIOLATION: %s", v)
		}
		return fmt.Errorf("%d violations (see %s)", len(rep.Violations), c.out)
	}
	log.Printf("chaos: no violations")
	return nil
}

// inject applies one fault to the node with container id and returns how
// to undo it.
func inject(d docker, image string, netem bool, kind, id string, ids map[string]string, rng *rand.Rand, f *fault) (func() error, error) {
	tc := func(args ...string) error { return d.sidecar(image, id, append([]string{"tc"}, args...)...) }
	clearTC := func() error { return tc("qdisc", "del", "dev", "eth0", "root") }
	switch kind {
	case "kill":
		if err := d.run("kill", "-s", "KILL", id); err != nil {
			return nil, err
		}
		return func() error { return d.run("start", id) }, nil
	case "delay":
		ms := 50 + rng.IntN(250)
		f.Detail = fmt.Sprintf("%dms +-%dms", ms, ms/2)
		return clearTC, tc("qdisc", "add", "dev", "eth0", "root", "netem", "delay", fmt.Sprintf("%dms", ms), fmt.Sprintf("%dms", ms/2))
	case "loss":
		pct := 10 + rng.IntN(31)
		f.Detail = fmt.Sprintf("%d%%", pct)
		return clearTC, tc("qdisc", "add", "dev", "eth0", "root", "netem", "loss", fmt.Sprintf("%d%%", pct))
	case "partition":
		if netem {
			f.Detail = "netem loss 100%"
			return clearTC, tc("qdisc", "add", "dev", "eth0", "root", "netem", "loss", "100%")
		}
		// No netem: drop all traffic to and from the other nodes.
		var rules []string
		for _, other := range ids {
			if other == id {
				continue
			}
			ip, err := d.ip(other)
			if err != nil {
				return nil, err
			}
			rules = append(rules, fmt.Sprintf("iptables -A INPUT -s %s -j DROP && iptables -A OUTPUT -d %s -j DROP", ip, ip))
		}
		f.Detail = "iptables"
		undo := func() error { return d.sidecar(image, id, "iptables", "-F") }
		return undo, d.sidecar(image, id, "sh", "-c", strings.Join(rules, " && "))
	}
	return nil, fmt.Errorf("unknown fault %q", kind)
}

// settle waits until every node answers /readyz and the cluster has no
// transaction in doubt.
func settle(metrics []string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	hc := http.Client{Timeout: 2 * time.Second}
	var last error
	for time.Now().Before(deadline) {
		last = nil
		inDoubt := 0.0
		for _, m := range metrics {
			resp, err := hc.Get("http://" + m + "/readyz")
			if err != nil {
				last = err
				break
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				last = fmt.Errorf("%s not ready", m)
				break
			}
			n, err := gauge(hc, m, "txn_in_doubt")
			if err != nil {
				last = err
				break
			}
			inDoubt += n
		}
		if last == nil && inDoubt > 0 {
			last = fmt.Errorf("%v transactions in doubt", inDoubt)
		}
		if last == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return last
}

// gauge sums a metric's samples from a node's /metrics.
func gauge(hc http.Client, addr, name string) (float64, error) {
	resp, err := hc.Get("http://" + addr + "/metrics")
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	total := 0.0
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		var v float64
		if _, err := fmt.Sscan(line[strings.LastIndexByte(line, ' ')+1:], &v); err == nil {
			total += v
		}
	}
	return total, sc.Err()
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// docker drives the docker CLI.
type docker struct{ file, project string }

func (d docker) run(args ...string) error {
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (d docker) output(args ...string) (string, error) {
	out, err := exec.Command("docker", args...).Output()
	if err != nil {
		return "", fmt.Errorf("docker %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out)), nil
}

func (d docker) composeCmd(args ...string) error {
	return d.run(append([]string{"compose", "-f", d.file, "-p", d.project}, args...)...)
}

func (d docker) containerID(service string) (string, error) {
	id, err := d.output("compose", "-f", d.file, "-p", d.project, "ps", "-aq", service)
	if err == nil && id == "" {
		err = fmt.Errorf("no container for service %s; is the stack up?", service)
	}
	return id, err
}

func (d docker) ip(id string) (string, error) {
	return d.output("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", id)
}

// sidecar runs a command in a throwaway container that shares container
// id's network namespace and may change its network configuration.
func (d docker) sidecar(image, id string, cmd ...string) error {
	return d.run(append([]string{"run", "--rm", "--net", "container:" + id, "--cap-add", "NET_ADMIN", image}, cmd...)...)
}

// netemWorks reports whether the kernel supports netem, by adding and
// removing a 1ms delay on one node.
func (d docker) netemWorks(image, id string) bool {
	if d.sidecar(image, id, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "1ms") != nil {
		return false
	}
	return d.sidecar(image, id, "tc", "qdisc", "del", "dev", "eth0", "root") == nil
}

func (d docker) saveLogs(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	cmd := exec.Command("docker", "compose", "-f", d.file, "-p", d.project, "logs", "--no-color", "--timestamps")
	cmd.Stdout, cmd.Stderr = f, io.Discard
	return cmd.Run()
}
