package hedge

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubPeer is a minimal TCP server speaking hedge's line request/reply protocol,
// used to exercise the Broadcast connection pool without Cloud Spanner. If reply
// is nil the peer accepts connections and reads requests but never replies
// ("accept then hang"). Otherwise it replies reply(request) + "\n" per request.
type stubPeer struct {
	ln      net.Listener
	quit    chan struct{}
	accepts atomic.Int32
	reply   func(req string) string
}

func newStubPeer(t *testing.T, reply func(string) string) *stubPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	sp := &stubPeer{ln: ln, quit: make(chan struct{}), reply: reply}
	go sp.serve()
	t.Cleanup(sp.close)
	return sp
}

func (sp *stubPeer) addr() string { return sp.ln.Addr().String() }

func (sp *stubPeer) close() {
	select {
	case <-sp.quit:
	default:
		close(sp.quit)
	}
	sp.ln.Close()
}

func (sp *stubPeer) serve() {
	for {
		conn, err := sp.ln.Accept()
		if err != nil {
			return
		}
		sp.accepts.Add(1)
		go sp.handle(conn)
	}
}

func (sp *stubPeer) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if sp.reply == nil {
			<-sp.quit // accept then never reply; hold the conn open
			return
		}
		c.Write([]byte(sp.reply(strings.TrimSuffix(line, "\n")) + "\n"))
	}
}

// newTestOp builds an Op with just the fields broadcastTo/sendCtx need.
func newTestOp(poolSize int, timeout time.Duration) *Op {
	op := &Op{broadcastTimeout: timeout}
	op.pool = newConnPool(poolSize)
	return op
}

// 4.1 A peer that accepts then never replies -> broadcastTo returns a timeout
// within the configured timeout, and the pooled conn is evicted (no slot leak).
func TestBroadcastPool_StalledPeerTimesOut(t *testing.T) {
	sp := newStubPeer(t, nil) // never replies
	op := newTestOp(2, 300*time.Millisecond)

	start := time.Now()
	_, err := op.broadcastTo(context.Background(), sp.addr(), "PING\n", op.broadcastTimeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took %v; deadline should bound it near %v", elapsed, op.broadcastTimeout)
	}
	if n := op.pool.idleLen(sp.addr()); n != 0 {
		t.Fatalf("expected the failed conn to be discarded, idle=%d", n)
	}
}

// 4.2 A context deadline sooner than the timeout bounds the round trip: broadcastTo
// returns at ~ctx deadline, well before the (much larger) per-node timeout.
func TestBroadcastPool_ContextDeadlineWins(t *testing.T) {
	sp := newStubPeer(t, nil) // never replies
	op := newTestOp(2, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := op.broadcastTo(ctx, sp.addr(), "PING\n", op.broadcastTimeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from ctx deadline")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("took %v; ctx deadline (250ms) should win over timeout (10s)", elapsed)
	}
}

// 4.3 Repeated calls to a stable peer reuse a single connection (no per-call dial).
func TestBroadcastPool_ReusesConnection(t *testing.T) {
	sp := newStubPeer(t, func(req string) string { return req })
	op := newTestOp(2, 2*time.Second)

	for i := 0; i < 5; i++ {
		reply, err := op.broadcastTo(context.Background(), sp.addr(), fmt.Sprintf("REQ-%d\n", i), op.broadcastTimeout)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if want := fmt.Sprintf("REQ-%d", i); reply != want {
			t.Fatalf("call %d: reply=%q want %q", i, reply, want)
		}
	}

	if n := sp.accepts.Load(); n != 1 {
		t.Fatalf("expected exactly 1 connection for 5 sequential calls, got %d", n)
	}
	if n := op.pool.idleLen(sp.addr()); n != 1 {
		t.Fatalf("expected 1 idle pooled conn, got %d", n)
	}
}

// remove() closes and drops a departed peer's pooled conns.
func TestBroadcastPool_RemoveClosesConns(t *testing.T) {
	sp := newStubPeer(t, func(req string) string { return req })
	op := newTestOp(2, 2*time.Second)

	if _, err := op.broadcastTo(context.Background(), sp.addr(), "HI\n", op.broadcastTimeout); err != nil {
		t.Fatal(err)
	}
	if op.pool.idleLen(sp.addr()) != 1 {
		t.Fatal("expected a pooled conn before remove")
	}
	op.pool.remove(sp.addr())
	if n := op.pool.idleLen(sp.addr()); n != 0 {
		t.Fatalf("expected 0 idle conns after remove, got %d", n)
	}
}

// 4.4 Concurrent calls to the same peer never cross replies (serialization).
func TestBroadcastPool_ConcurrentNoCrossedReplies(t *testing.T) {
	sp := newStubPeer(t, func(req string) string { return req }) // echo
	op := newTestOp(2, 3*time.Second)

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := fmt.Sprintf("TOKEN-%d", i)
			reply, err := op.broadcastTo(context.Background(), sp.addr(), req+"\n", op.broadcastTimeout)
			if err != nil {
				errs <- err
				return
			}
			if reply != req {
				errs <- fmt.Errorf("crossed reply: sent %q got %q", req, reply)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if idle := op.pool.idleLen(sp.addr()); idle > 2 {
		t.Fatalf("pool exceeded size: idle=%d > 2", idle)
	}
}

// 4.5 WithBroadcastPoolSize(0) reproduces dial-per-call behavior.
func TestBroadcastPool_DisabledDialsPerCall(t *testing.T) {
	sp := newStubPeer(t, func(req string) string { return req })
	op := newTestOp(0, 2*time.Second) // pooling disabled

	const calls = 4
	for i := 0; i < calls; i++ {
		if _, err := op.broadcastTo(context.Background(), sp.addr(), "X\n", op.broadcastTimeout); err != nil {
			t.Fatal(err)
		}
	}

	if n := sp.accepts.Load(); n != calls {
		t.Fatalf("pooling disabled: expected %d dials, got %d", calls, n)
	}
	if n := op.pool.idleLen(sp.addr()); n != 0 {
		t.Fatalf("pooling disabled: expected 0 idle conns, got %d", n)
	}
}
