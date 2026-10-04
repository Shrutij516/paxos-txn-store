package wire

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
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
	// Every oneof case in the envelope must be reachable.
	cases := (&paxosv1.Envelope{}).ProtoReflect().Descriptor().Oneofs().ByName("body").Fields().Len()
	if cases != numKinds {
		t.Fatalf("Envelope has %d body cases, build covers %d", cases, numKinds)
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
}
