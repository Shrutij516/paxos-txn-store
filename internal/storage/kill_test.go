package storage

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// killChildEnv, when set to a database path, turns the test binary into the
// writer child.
const killChildEnv = "PAXOS_KILL_CHILD_DB"

func killEntry(i uint64) paxos.SlotEntry {
	return paxos.SlotEntry{Slot: i, Ballot: paxos.Ballot{Round: i, Node: 1},
		Entry: paxos.Entry{ClientID: 1, Seq: i, Cmd: paxos.Value(fmt.Sprintf("P\x00k\x00v%d", i))}}
}

// TestKillChildWriter is not a test on its own. When the parent re-executes
// the test binary with PAXOS_KILL_CHILD_DB set, this function becomes the
// child: it writes entries in a loop and prints "confirmed <i>" only after
// the write for i has returned, i.e. after its transaction committed with
// synchronous=FULL. It runs until it is killed.
func TestKillChildWriter(t *testing.T) {
	path := os.Getenv(killChildEnv)
	if path == "" {
		t.Skip("helper process for TestSQLiteSurvivesSIGKILL")
	}
	s, err := OpenSQLite(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	for i := uint64(1); ; i++ {
		e := killEntry(i)
		// Each confirmed index covers a promise plus accepted entry (one
		// transaction) and a committed entry plus commit index (another).
		if err := s.SaveAccept(e.Ballot, e); err != nil {
			fmt.Fprintln(os.Stderr, "accept:", err)
			os.Exit(2)
		}
		if err := s.AppendCommitted([]paxos.SlotEntry{{Slot: i, Entry: e.Entry}}); err != nil {
			fmt.Fprintln(os.Stderr, "commit:", err)
			os.Exit(2)
		}
		if _, err := fmt.Fprintf(os.Stdout, "confirmed %d\n", i); err != nil { // os.Stdout is unbuffered
			os.Exit(0) // the parent stopped reading
		}
	}
}

// TestSQLiteSurvivesSIGKILL re-executes the test binary as a writer child,
// kills it with SIGKILL at a random point, reopens the database, checks its
// integrity, and verifies that every write the child confirmed is present.
// 20 iterations, 3 under -short.
func TestSQLiteSurvivesSIGKILL(t *testing.T) {
	iters := 20
	if testing.Short() {
		iters = 3
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for it := 0; it < iters; it++ {
		seed := uint64(it + 1)
		rng := rand.New(rand.NewPCG(seed, 99))
		killAfter := 1 + rng.IntN(300)                            // confirmations to wait for
		extra := time.Duration(rng.IntN(2000)) * time.Microsecond // then a random extra delay
		path := filepath.Join(t.TempDir(), "kill.db")

		cmd := exec.Command(exe, "-test.run=^TestKillChildWriter$", "-test.count=1")
		cmd.Env = append(os.Environ(), killChildEnv+"="+path)
		cmd.Stderr = os.Stderr
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		var confirmed uint64
		sc := bufio.NewScanner(out)
		killed := false
		for sc.Scan() {
			line := sc.Text()
			n, ok := strings.CutPrefix(line, "confirmed ")
			if !ok {
				continue // test framework output
			}
			v, err := strconv.ParseUint(n, 10, 64)
			if err != nil {
				t.Fatalf("iter %d (seed %d): bad line %q", it, seed, line)
			}
			confirmed = v
			if !killed && int(v) >= killAfter {
				time.Sleep(extra)
				if err := cmd.Process.Kill(); err != nil { // SIGKILL on Unix
					t.Fatal(err)
				}
				killed = true
			}
			// Keep reading: lines printed before the kill are confirmations too.
		}
		_ = cmd.Wait()
		if !killed {
			t.Fatalf("iter %d (seed %d): child exited on its own after %d confirmations", it, seed, confirmed)
		}

		s, err := OpenSQLite(path)
		if err != nil {
			t.Fatalf("iter %d (seed %d): reopen after kill: %v", it, seed, err)
		}
		if res, err := s.Pragma("integrity_check"); err != nil || res != "ok" {
			t.Fatalf("iter %d (seed %d): integrity_check = %q, %v", it, seed, res, err)
		}
		acc, err := s.LoadAccepted()
		if err != nil {
			t.Fatal(err)
		}
		com, err := s.LoadCommitted()
		if err != nil {
			t.Fatal(err)
		}
		prom, _ := s.LoadPromised()
		if uint64(len(acc)) < confirmed || uint64(len(com)) < confirmed {
			t.Fatalf("iter %d (seed %d): confirmed %d but found %d accepted and %d committed",
				it, seed, confirmed, len(acc), len(com))
		}
		for i := uint64(1); i <= confirmed; i++ {
			want := killEntry(i)
			if acc[i-1] != want || com[i-1].Slot != i || com[i-1].Entry != want.Entry {
				t.Fatalf("iter %d (seed %d): entry %d wrong after kill: accepted %+v committed %+v", it, seed, i, acc[i-1], com[i-1])
			}
		}
		if prom.Round < confirmed {
			t.Fatalf("iter %d (seed %d): promise %v older than confirmed write %d", it, seed, prom, confirmed)
		}
		_ = s.Close()
		t.Logf("iter %d (seed %d): killed after %d confirmations; %d accepted, %d committed on disk, integrity ok",
			it, seed, confirmed, len(acc), len(com))
	}
}
