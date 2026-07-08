package hedge

import (
	"context"
	"encoding/base64"
	"net"
	"strings"
	"testing"
	"time"
)

type mockConn struct {
	net.Conn
	written []byte
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestProtocol_DoHeartbeat(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	conn := &mockConn{}

	msg := CmdPing + " test-node"
	doHeartbeat(context.Background(), op, conn, msg)

	members := op.getMembers()
	if _, ok := members["test-node"]; !ok {
		t.Errorf("expected test-node to be added to members")
	}

	reply := strings.TrimSpace(string(conn.written))
	dec, err := base64.StdEncoding.DecodeString(reply)
	if err != nil {
		t.Fatalf("failed to decode reply: %v", err)
	}

	if !strings.Contains(string(dec), "test-node") {
		t.Errorf("expected encoded members to contain test-node, got %s", string(dec))
	}
}

func TestProtocol_DoMembers(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	conn := &mockConn{}

	// Encoded `{"node-a":{}, "node-b":{}}`
	encoded := base64.StdEncoding.EncodeToString([]byte(`{"node-a":{}, "node-b":{}}`))
	msg := CmdMembers + " " + encoded

	doMembers(context.Background(), op, conn, msg)

	members := op.getMembers()
	// node-a, node-b, and the local op.hostPort are expected
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}
	if _, ok := members["node-a"]; !ok {
		t.Errorf("expected node-a in members")
	}
	if _, ok := members["localhost:12345"]; !ok {
		t.Errorf("expected localhost:12345 in members")
	}
}

func TestProtocol_DoConfirmLeader(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	conn := &mockConn{}

	// Fake the leader state
	op.currentLeaderState.Store(LeaderState{Leader: true, Token: 123})

	doConfirmLeader(context.Background(), op, conn, CmdLeader)

	if strings.TrimSpace(string(conn.written)) != CmdAck {
		t.Errorf("expected %s, got %s", CmdAck, string(conn.written))
	}
}

func TestProtocol_DoSend(t *testing.T) {
	called := false
	handler := func(data any, msg []byte) ([]byte, error) {
		called = true
		if string(msg) != "test-payload" {
			t.Errorf("expected test-payload, got %s", string(msg))
		}
		return []byte("reply-payload"), nil
	}

	op := New(nil, "localhost:12345", "lock", "name", "log", WithLeaderHandler(nil, handler))
	op.currentLeaderState.Store(LeaderState{Leader: true, Token: 123})

	conn := &mockConn{}

	encoded := base64.StdEncoding.EncodeToString([]byte("test-payload"))
	msg := CmdSend + " " + encoded

	doSend(context.Background(), op, conn, msg)

	if !called {
		t.Errorf("expected leader handler to be called")
	}

	reply := strings.TrimSpace(string(conn.written))
	ss := strings.Split(reply, " ")
	dec, _ := base64.StdEncoding.DecodeString(ss[1])
	if string(dec) != "reply-payload" {
		t.Errorf("expected reply-payload, got %s", string(dec))
	}
}

func TestProtocol_DoBroadcast(t *testing.T) {
	called := false
	handler := func(data any, msg []byte) ([]byte, error) {
		called = true
		if string(msg) != "bc-payload" {
			t.Errorf("expected bc-payload, got %s", string(msg))
		}
		return []byte("bc-reply"), nil
	}

	op := New(nil, "localhost:12345", "lock", "name", "log", WithBroadcastHandler(nil, handler))

	conn := &mockConn{}

	encoded := base64.StdEncoding.EncodeToString([]byte("bc-payload"))
	msg := CmdBroadcast + " " + encoded

	doBroadcast(context.Background(), op, conn, msg)

	if !called {
		t.Errorf("expected broadcast handler to be called")
	}

	reply := strings.TrimSpace(string(conn.written))
	ss := strings.Split(reply, " ")
	dec, _ := base64.StdEncoding.DecodeString(ss[1])
	if string(dec) != "bc-reply" {
		t.Errorf("expected bc-reply, got %s", string(dec))
	}
}
