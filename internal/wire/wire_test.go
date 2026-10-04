package wire

import (
	"reflect"
	"testing"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
)

// numKinds is the number of Paxos message types; build covers each one.
const numKinds = 15

// build makes a message of the given kind from fuzz inputs.
func build(kind uint8, a, b uint64, n int32, data []byte, flag bool, count uint8) paxos.Message {
	bal := paxos.Ballot{Round: a, Node: paxos.NodeID(n)}
	bal2 := paxos.Ballot{Round: b, Node: paxos.NodeID(-n)}
	val := paxos.Value(data)
	ent := paxos.Entry{Noop: flag, ClientID: a ^ b, Seq: b, Cmd: val}
	var slots []paxos.SlotEntry
	for i := uint8(0); i < count%5; i++ {
		slots = append(slots, paxos.SlotEntry{Slot: a + uint64(i), Ballot: bal2, Entry: ent})
	}
	var body paxos.Payload
	switch kind % numKinds {
	case 0:
		body = paxos.Prepare{Ballot: bal}
	case 1:
		body = paxos.Promise{Ballot: bal, Accepted: bal2, Value: val}
	case 2:
		body = paxos.Accept{Ballot: bal, Value: val}
	case 3:
		body = paxos.Accepted{Ballot: bal, Value: val}
	case 4:
		body = paxos.Nack{Ballot: bal, Promised: bal2}
	case 5:
		body = paxos.LogPrepare{Ballot: bal, Commit: b}
	case 6:
		body = paxos.LogPromise{Ballot: bal, Entries: slots}
	case 7:
		body = paxos.LogAccept{Ballot: bal, Slot: b, Entry: ent}
	case 8:
		body = paxos.LogAccepted{Ballot: bal, Slot: b, Entry: ent}
	case 9:
		body = paxos.LogNack{Ballot: bal, Promised: bal2}
	case 10:
		body = paxos.Heartbeat{Ballot: bal, Commit: b}
	case 11:
		body = paxos.CatchupRequest{From: a}
	case 12:
		body = paxos.CatchupReply{Entries: slots}
	case 13:
		body = paxos.ClientRequest{ClientID: a, Seq: b, Cmd: val}
	default:
		body = paxos.ClientReply{ClientID: a, Seq: b, OK: flag, Result: val, Leader: paxos.NodeID(n)}
	}
	return paxos.Message{From: paxos.NodeID(n), To: paxos.NodeID(n / 2), Body: body}
}

// FuzzRoundTrip checks that every message type survives Go struct ->
// protobuf -> bytes -> protobuf -> Go struct unchanged. "go test" runs the
// seed corpus (one seed per message type), so CI covers every type even
// without -fuzz.
func FuzzRoundTrip(f *testing.F) {
	for k := uint8(0); k < numKinds; k++ {
		f.Add(k, uint64(k)+1, uint64(k)*7, int32(k)+1, []byte("P\x00key\x00value"), k%2 == 0, k%5)
	}
	f.Add(uint8(6), ^uint64(0), uint64(0), int32(-2147483648), []byte{}, true, uint8(4))
	f.Fuzz(func(t *testing.T, kind uint8, a, b uint64, n int32, data []byte, flag bool, count uint8) {
		m := build(kind, a, b, n, data, flag, count)
		env, err := ToProto(m)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		var back paxosv1.Envelope
		if err := proto.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		got, err := FromProto(&back)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip changed the message:\nin  %#v\nout %#v", m, got)
		}
	})
}

// FuzzDecode feeds arbitrary bytes to the decoder. It must never panic;
// errors are fine.
func FuzzDecode(f *testing.F) {
	for k := uint8(0); k < numKinds; k++ {
		env, _ := ToProto(build(k, 3, 4, 5, []byte("x"), true, 2))
		raw, _ := proto.Marshal(env)
		f.Add(raw)
	}
	for k := uint8(0); k < numTxnKinds; k++ {
		env, _ := ToProto(buildTxn(k, 3, 4, 5, []byte("x"), true, 5))
		raw, _ := proto.Marshal(env)
		f.Add(raw)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff})
	f.Fuzz(func(_ *testing.T, raw []byte) {
		var env paxosv1.Envelope
		if proto.Unmarshal(raw, &env) != nil {
			return
		}
		m, err := FromProto(&env)
		if err != nil {
			return
		}
		if _, err := ToProto(m); err != nil {
			panic(err)
		}
	})
}

func TestEveryKindCovered(t *testing.T) {
	seen := map[reflect.Type]bool{}
	for k := uint8(0); k < numKinds; k++ {
		seen[reflect.TypeOf(build(k, 1, 2, 3, nil, false, 1).Body)] = true
	}
	if len(seen) != numKinds {
		t.Fatalf("build covers %d message types, want %d", len(seen), numKinds)
	}
	txnSeen := map[reflect.Type]bool{}
	for k := uint8(0); k < numTxnKinds; k++ {
		txnSeen[reflect.TypeOf(buildTxn(k, 1, 2, 3, []byte("k"), false, 1).Body.(paxos.Ext).Body)] = true
	}
	if len(txnSeen) != numTxnKinds {
		t.Fatalf("buildTxn covers %d message types, want %d", len(txnSeen), numTxnKinds)
	}
	// Every oneof case in the envelope must be reachable.
	cases := (&paxosv1.Envelope{}).ProtoReflect().Descriptor().Oneofs().ByName("body").Fields().Len()
	if cases != numKinds+numTxnKinds {
		t.Fatalf("Envelope has %d body cases, build and buildTxn cover %d", cases, numKinds+numTxnKinds)
	}
}

func TestErrors(t *testing.T) {
	if _, err := FromProto(&paxosv1.Envelope{}); err != ErrNoBody {
		t.Fatalf("empty envelope: %v", err)
	}
	type bogus struct{ paxos.Payload }
	if _, err := ToProto(paxos.Message{Body: bogus{}}); err == nil {
		t.Fatal("unknown type must fail")
	}
	// Client requests and replies are local only.
	if _, err := ToProto(paxos.Message{Body: paxos.Ext{Body: txn.ReadReq{Key: "k"}}}); err == nil {
		t.Fatal("ReadReq must not be sent over the wire")
	}
}

// numTxnKinds is the number of transaction message types that cross the
// network; buildTxn covers each one.
const numTxnKinds = 5

// buildTxn makes a transaction message of the given kind from fuzz inputs.
// Shards are uint32 on the wire, so a and n are folded into that range.
// Empty maps and slices are nil, which is how they decode.
func buildTxn(kind uint8, a, b uint64, n int32, data []byte, flag bool, count uint8) paxos.Message {
	meta := txn.Meta{ID: txn.ID(a), TS: b}
	sh := txn.ShardID(uint32(n))
	var parts []txn.ShardID
	for i := uint8(0); i < count%4; i++ {
		parts = append(parts, txn.ShardID(uint32(a)+uint32(i)))
	}
	part := txn.Part{Shard: sh}
	key := string(data)
	if count%3 != 0 {
		part.Reads = map[string]uint64{key: b, key + "x": a}
	}
	if count%2 != 0 {
		part.Writes = map[string]string{key: string(data) + "v"}
	}
	var body any
	switch kind % numTxnKinds {
	case 0:
		body = txn.PrepareReq{Txn: meta, Coord: txn.ShardID(uint32(b)), Participants: parts, Part: part}
	case 1:
		body = txn.Vote{Txn: meta.ID, Shard: sh, Yes: flag}
	case 2:
		body = txn.Decision{Txn: meta.ID, Commit: flag}
	case 3:
		body = txn.QueryOutcome{Txn: meta.ID, From: sh, Participants: parts}
	default:
		body = txn.WoundReq{Txn: meta.ID}
	}
	return paxos.Message{From: paxos.NodeID(n), To: paxos.NodeID(-n), Body: paxos.Ext{Body: body}}
}

// FuzzTxnRoundTrip is FuzzRoundTrip for the two-phase commit messages,
// including the envelope's shard field.
func FuzzTxnRoundTrip(f *testing.F) {
	for k := uint8(0); k < numTxnKinds; k++ {
		f.Add(k, uint64(k)+1, uint64(k)*7, int32(k)+1, []byte("acct\x001"), k%2 == 0, k%6, uint32(k))
	}
	f.Add(uint8(0), ^uint64(0), uint64(0), int32(-2147483648), []byte{}, true, uint8(5), ^uint32(0))
	f.Fuzz(func(t *testing.T, kind uint8, a, b uint64, n int32, data []byte, flag bool, count uint8, shard uint32) {
		if !utf8.Valid(data) {
			return // proto3 strings (map keys and values) must be UTF-8
		}
		m := buildTxn(kind, a, b, n, data, flag, count)
		env, err := ToProto(m)
		if err != nil {
			t.Fatal(err)
		}
		env.Shard = shard
		raw, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		var back paxosv1.Envelope
		if err := proto.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		if back.GetShard() != shard {
			t.Fatalf("shard %d came back as %d", shard, back.GetShard())
		}
		got, err := FromProto(&back)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip changed the message:\nin  %#v\nout %#v", m, got)
		}
	})
}
