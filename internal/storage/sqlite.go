package storage

import (
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite" // pure Go SQLite driver, registers "sqlite"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// SchemaVersion is the schema this code writes and understands.
const SchemaVersion = 1

// migrations[i] upgrades a database from schema version i to i+1. Version 0
// means an empty file.
var migrations = []string{
	`CREATE TABLE acceptor (
		id              INTEGER PRIMARY KEY CHECK (id = 1),
		promised_round  INTEGER NOT NULL,
		promised_node   INTEGER NOT NULL,
		accepted_round  INTEGER NOT NULL,
		accepted_node   INTEGER NOT NULL,
		value           BLOB    NOT NULL
	);
	CREATE TABLE proposer (
		id     INTEGER PRIMARY KEY CHECK (id = 1),
		round  INTEGER NOT NULL
	);
	CREATE TABLE log_promise (
		id     INTEGER PRIMARY KEY CHECK (id = 1),
		round  INTEGER NOT NULL,
		node   INTEGER NOT NULL
	);
	CREATE TABLE log_accepted (
		slot          INTEGER PRIMARY KEY,
		ballot_round  INTEGER NOT NULL,
		ballot_node   INTEGER NOT NULL,
		noop          INTEGER NOT NULL,
		client_id     INTEGER NOT NULL,
		seq           INTEGER NOT NULL,
		cmd           BLOB    NOT NULL
	);
	CREATE TABLE log_committed (
		slot       INTEGER PRIMARY KEY,
		noop       INTEGER NOT NULL,
		client_id  INTEGER NOT NULL,
		seq        INTEGER NOT NULL,
		cmd        BLOB    NOT NULL
	);
	CREATE TABLE log_commit (
		id            INTEGER PRIMARY KEY CHECK (id = 1),
		commit_index  INTEGER NOT NULL
	);`,
}

// Sync selects SQLite's synchronous pragma. Use SyncFull. SyncNormal exists
// only so the benchmarks can show what FULL costs; in WAL mode NORMAL can
// lose the last commits on power loss, which would break Paxos promises.
type Sync string

// Supported synchronous settings.
const (
	SyncFull   Sync = "FULL"
	SyncNormal Sync = "NORMAL"
)

// faultPoint names where a fault hook runs inside a write transaction.
type faultPoint int

const (
	beforeCommit faultPoint = iota // all statements executed, not yet committed
	afterCommit                    // committed and durable, caller not yet told
)

// SQLite is a durable paxos.Storage and paxos.LogStorage backed by one
// SQLite file in WAL mode with synchronous=FULL. Every Save or Append is one
// transaction, so a crash leaves either the old state or the new one. It is
// not safe for concurrent use.
type SQLite struct {
	db   *sql.DB
	path string

	// fault, if set, runs at each faultPoint of every write. An error at
	// beforeCommit rolls the transaction back; at afterCommit the write
	// stays durable but the error is still returned. It is unexported and
	// only set by this package's tests.
	fault func(faultPoint) error
}

var (
	_ paxos.Storage    = (*SQLite)(nil)
	_ paxos.LogStorage = (*SQLite)(nil)
)

// OpenSQLite opens or creates the database at path with synchronous=FULL.
func OpenSQLite(path string) (*SQLite, error) { return OpenSQLiteSync(path, SyncFull) }

// OpenSQLiteSync is OpenSQLite with an explicit synchronous setting.
func OpenSQLiteSync(path string, sync Sync) (*SQLite, error) {
	if sync != SyncFull && sync != SyncNormal {
		return nil, fmt.Errorf("storage: unsupported synchronous mode %q", sync)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(%s)&_pragma=busy_timeout(5000)", path, sync)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: pragmas are per connection, and writes are serialized.
	db.SetMaxOpenConns(1)
	s := &SQLite{db: db, path: path}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *SQLite) Close() error { return s.db.Close() }

// Path returns the database file path.
func (s *SQLite) Path() string { return s.path }

// Pragma returns the current value of a pragma, for tests and diagnostics.
func (s *SQLite) Pragma(name string) (string, error) {
	var v string
	err := s.db.QueryRow("PRAGMA " + name).Scan(&v)
	return v, err
}

func (s *SQLite) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (
		id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)`); err != nil {
		return err
	}
	var v int
	err := s.db.QueryRow(`SELECT version FROM schema_version WHERE id = 1`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		v = 0
	} else if err != nil {
		return err
	}
	if v > SchemaVersion {
		return fmt.Errorf("storage: database schema version %d is newer than supported %d", v, SchemaVersion)
	}
	for ; v < SchemaVersion; v++ {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[v]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("storage: migrating to version %d: %w", v+1, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_version (id, version) VALUES (1, ?)
			ON CONFLICT (id) DO UPDATE SET version = excluded.version`, v+1); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// Version returns the schema version stored in the database.
func (s *SQLite) Version() (int, error) {
	var v int
	err := s.db.QueryRow(`SELECT version FROM schema_version WHERE id = 1`).Scan(&v)
	return v, err
}

// write runs fn in one transaction and calls the fault hook around commit.
func (s *SQLite) write(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if s.fault != nil {
		if err := s.fault(beforeCommit); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if s.fault != nil {
		return s.fault(afterCommit)
	}
	return nil
}

// ---- paxos.Storage ----

// LoadAcceptor implements paxos.Storage.
func (s *SQLite) LoadAcceptor() (paxos.AcceptorState, error) {
	var st paxos.AcceptorState
	var pn, an int32
	var v []byte
	err := s.db.QueryRow(`SELECT promised_round, promised_node, accepted_round, accepted_node, value
		FROM acceptor WHERE id = 1`).Scan(&st.Promised.Round, &pn, &st.Accepted.Round, &an, &v)
	if errors.Is(err, sql.ErrNoRows) {
		return paxos.AcceptorState{}, nil
	}
	st.Promised.Node, st.Accepted.Node, st.Value = paxos.NodeID(pn), paxos.NodeID(an), paxos.Value(v)
	return st, err
}

// SaveAcceptor implements paxos.Storage.
func (s *SQLite) SaveAcceptor(st paxos.AcceptorState) error {
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO acceptor (id, promised_round, promised_node, accepted_round, accepted_node, value)
			VALUES (1, ?, ?, ?, ?, ?)
			ON CONFLICT (id) DO UPDATE SET promised_round = excluded.promised_round,
				promised_node = excluded.promised_node, accepted_round = excluded.accepted_round,
				accepted_node = excluded.accepted_node, value = excluded.value`,
			st.Promised.Round, int32(st.Promised.Node), st.Accepted.Round, int32(st.Accepted.Node), []byte(st.Value))
		return err
	})
}

// LoadRound implements paxos.Storage and paxos.LogStorage.
func (s *SQLite) LoadRound() (uint64, error) {
	var r uint64
	err := s.db.QueryRow(`SELECT round FROM proposer WHERE id = 1`).Scan(&r)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return r, err
}

// SaveRound implements paxos.Storage and paxos.LogStorage.
func (s *SQLite) SaveRound(r uint64) error {
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO proposer (id, round) VALUES (1, ?)
			ON CONFLICT (id) DO UPDATE SET round = excluded.round`, r)
		return err
	})
}

// ---- paxos.LogStorage ----

// LoadPromised implements paxos.LogStorage.
func (s *SQLite) LoadPromised() (paxos.Ballot, error) {
	var b paxos.Ballot
	var n int32
	err := s.db.QueryRow(`SELECT round, node FROM log_promise WHERE id = 1`).Scan(&b.Round, &n)
	if errors.Is(err, sql.ErrNoRows) {
		return paxos.Ballot{}, nil
	}
	b.Node = paxos.NodeID(n)
	return b, err
}

// SavePromised implements paxos.LogStorage.
func (s *SQLite) SavePromised(b paxos.Ballot) error {
	return s.write(func(tx *sql.Tx) error { return upsertPromise(tx, b) })
}

func upsertPromise(tx *sql.Tx, b paxos.Ballot) error {
	_, err := tx.Exec(`INSERT INTO log_promise (id, round, node) VALUES (1, ?, ?)
		ON CONFLICT (id) DO UPDATE SET round = excluded.round, node = excluded.node`, b.Round, int32(b.Node))
	return err
}

// LoadAccepted implements paxos.LogStorage.
func (s *SQLite) LoadAccepted() ([]paxos.SlotEntry, error) {
	rows, err := s.db.Query(`SELECT slot, ballot_round, ballot_node, noop, client_id, seq, cmd
		FROM log_accepted ORDER BY slot`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []paxos.SlotEntry
	for rows.Next() {
		var se paxos.SlotEntry
		var node int32
		var cmd []byte
		if err := rows.Scan(&se.Slot, &se.Ballot.Round, &node, &se.Entry.Noop, &se.Entry.ClientID, &se.Entry.Seq, &cmd); err != nil {
			return nil, err
		}
		se.Ballot.Node, se.Entry.Cmd = paxos.NodeID(node), paxos.Value(cmd)
		out = append(out, se)
	}
	return out, rows.Err()
}

// SaveAccept implements paxos.LogStorage. The promise and the accepted
// entry are written in one transaction.
func (s *SQLite) SaveAccept(promised paxos.Ballot, se paxos.SlotEntry) error {
	return s.write(func(tx *sql.Tx) error {
		if err := upsertPromise(tx, promised); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO log_accepted (slot, ballot_round, ballot_node, noop, client_id, seq, cmd)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (slot) DO UPDATE SET ballot_round = excluded.ballot_round,
				ballot_node = excluded.ballot_node, noop = excluded.noop, client_id = excluded.client_id,
				seq = excluded.seq, cmd = excluded.cmd`,
			se.Slot, se.Ballot.Round, int32(se.Ballot.Node), se.Entry.Noop, se.Entry.ClientID, se.Entry.Seq, []byte(se.Entry.Cmd))
		return err
	})
}

func (s *SQLite) commitIndex(q interface {
	QueryRow(string, ...any) *sql.Row
}) (uint64, error) {
	var c uint64
	err := q.QueryRow(`SELECT commit_index FROM log_commit WHERE id = 1`).Scan(&c)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return c, err
}

// LoadCommitted implements paxos.LogStorage.
func (s *SQLite) LoadCommitted() ([]paxos.SlotEntry, error) {
	c, err := s.commitIndex(s.db)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT slot, noop, client_id, seq, cmd FROM log_committed
		WHERE slot <= ? ORDER BY slot`, c)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []paxos.SlotEntry
	for rows.Next() {
		var se paxos.SlotEntry
		var cmd []byte
		if err := rows.Scan(&se.Slot, &se.Entry.Noop, &se.Entry.ClientID, &se.Entry.Seq, &cmd); err != nil {
			return nil, err
		}
		se.Entry.Cmd = paxos.Value(cmd)
		out = append(out, se)
	}
	return out, rows.Err()
}

// AppendCommitted implements paxos.LogStorage. The entries and the new
// commit index are written in one transaction.
func (s *SQLite) AppendCommitted(es []paxos.SlotEntry) error {
	if len(es) == 0 {
		return nil
	}
	return s.write(func(tx *sql.Tx) error {
		c, err := s.commitIndex(tx)
		if err != nil {
			return err
		}
		if err := checkAppend(c, es); err != nil {
			return err
		}
		for _, se := range es {
			if _, err := tx.Exec(`INSERT INTO log_committed (slot, noop, client_id, seq, cmd) VALUES (?, ?, ?, ?, ?)`,
				se.Slot, se.Entry.Noop, se.Entry.ClientID, se.Entry.Seq, []byte(se.Entry.Cmd)); err != nil {
				return err
			}
		}
		_, err = tx.Exec(`INSERT INTO log_commit (id, commit_index) VALUES (1, ?)
			ON CONFLICT (id) DO UPDATE SET commit_index = excluded.commit_index`, es[len(es)-1].Slot)
		return err
	})
}
