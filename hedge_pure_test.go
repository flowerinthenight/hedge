package hedge

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"
)

func TestPureMethods(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	if op.HostPort() != "localhost:12345" {
		t.Errorf("expected localhost:12345, got %s", op.HostPort())
	}
	if op.Name() != "localhost:12345" {
		t.Errorf("expected localhost:12345, got %s", op.Name())
	}
	if op.IsRunning() {
		t.Errorf("expected IsRunning() to be false")
	}
}

func TestOptions(t *testing.T) {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	inCh := make(chan *StreamMessage)
	outCh := make(chan *StreamMessage)

	var leaderCbCalled bool
	leaderCb := func(data any, msg []byte) { leaderCbCalled = true }

	var handlerCbCalled bool
	handlerCb := func(data any, msg []byte) ([]byte, error) {
		handlerCbCalled = true
		return nil, nil
	}

	op := New(nil, "localhost:12345", "lock", "name", "log",
		WithDuration(10),
		WithGroupSyncInterval(5*time.Second),
		WithLogger(logger),
		WithGrpcHostPort("localhost:9090"),
		WithLeaderCallback("lcb", leaderCb),
		WithLeaderHandler("lhcb", handlerCb),
		WithBroadcastHandler("bhcb", handlerCb),
		WithMembersChangedHandler("mhcb", handlerCb),
		WithLeaderStreamChannels(inCh, outCh),
		WithBroadcastStreamChannels(inCh, outCh),
	)

	if op.lockTimeout != 10 { t.Errorf("expected lockTimeout=10, got %d", op.lockTimeout) }
	if op.syncInterval != 5*time.Second { t.Errorf("expected syncInterval=5s, got %v", op.syncInterval) }
	if op.logger != logger { t.Errorf("logger not set correctly") }
	if op.grpcHostPort != "localhost:9090" { t.Errorf("expected grpcHostPort=localhost:9090, got %s", op.grpcHostPort) }

	if op.cbLeaderData != "lcb" || op.cbLeader == nil { t.Errorf("leader callback not set correctly") } else {
		op.cbLeader(nil, nil)
		if !leaderCbCalled { t.Errorf("expected leader callback to be called") }
	}

	if op.fnLdrData != "lhcb" || op.fnLeader == nil { t.Errorf("leader handler not set correctly") } else {
		op.fnLeader(nil, nil)
		if !handlerCbCalled { t.Errorf("expected handler callback to be called") }
	}

	handlerCbCalled = false
	if op.fnBcData != "bhcb" || op.fnBroadcast == nil { t.Errorf("broadcast handler not set correctly") } else {
		op.fnBroadcast(nil, nil)
		if !handlerCbCalled { t.Errorf("expected broadcast handler to be called") }
	}

	handlerCbCalled = false
	if op.fnMemChangedData != "mhcb" || op.fnMemberChanged == nil { t.Errorf("members changed handler not set correctly") } else {
		op.fnMemberChanged(nil, nil)
		if !handlerCbCalled { t.Errorf("expected members changed handler to be called") }
	}

	if op.leaderStreamIn != inCh || op.leaderStreamOut != outCh { t.Errorf("leader stream channels not set correctly") }
	if op.broadcastStreamIn != inCh || op.broadcastStreamOut != outCh { t.Errorf("broadcast stream channels not set correctly") }
}

func TestMembersManagement(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	op.addMember("node1")
	op.addMember("node2")

	members := op.getMembers()
	if len(members) != 2 { t.Errorf("expected 2 members, got %d", len(members)) }
	if _, ok := members["node1"]; !ok { t.Errorf("node1 not found in members") }

	op.delMember("node1")
	members = op.getMembers()
	if len(members) != 1 { t.Errorf("expected 1 member, got %d", len(members)) }

	newMembers := map[string]struct{}{ "node3": {}, "node4": {} }
	op.setMembers(newMembers)
	members = op.getMembers()
	if len(members) != 2 { t.Errorf("expected 2 members, got %d", len(members)) }

	encoded := op.encodeMembers()
	dec, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil { t.Fatalf("failed to decode members: %v", err) }

	var decodedMembers map[string]struct{}
	if err := json.Unmarshal(dec, &decodedMembers); err != nil { t.Fatalf("failed to unmarshal members: %v", err) }
	if len(decodedMembers) != 2 { t.Errorf("expected 2 decoded members, got %d", len(decodedMembers)) }

	sliceMembers := op.Members()
	if len(sliceMembers) != 2 { t.Errorf("Members() mismatch, got %v", sliceMembers) }
}

func TestBuildAckReply(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	reply := op.buildAckReply(nil)
	if reply != CmdAck + "\n" { t.Errorf("expected ACK\\n, got %q", reply) }

	err := ErrNotRunning
	reply = op.buildAckReply(err)
	encodedErr := base64.StdEncoding.EncodeToString([]byte(err.Error()))
	expected := CmdAck + " " + encodedErr + "\n"
	if reply != expected { t.Errorf("expected %q, got %q", expected, reply) }
}
