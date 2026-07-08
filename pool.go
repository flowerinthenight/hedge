package hedge

import (
	"context"
	"net"
	"sync"
	"time"
)

const (
	defaultBroadcastPoolSize = 2
	defaultBroadcastTimeout  = 5 * time.Second
	broadcastKeepAlive       = 30 * time.Second
)

// peerPool holds up to connPool.size idle, ready-to-use connections to a single
// peer. The wire protocol is an untagged, '\n'-terminated line request/reply, so a
// connection carries exactly one in-flight request at a time; concurrency comes
// from handing different pooled connections to concurrent callers, not from
// sharing one. Hence a small pool per peer rather than a single shared conn.
type peerPool struct {
	mu   sync.Mutex
	idle []net.Conn
}

// connPool is the client-side, per-peer connection pool for the unary Broadcast
// API, keyed by member id ("ip:port"). A size of 0 disables pooling, restoring the
// previous dial-a-connection-per-call behavior.
type connPool struct {
	mu    sync.Mutex
	peers map[string]*peerPool
	size  int
}

func newConnPool(size int) *connPool {
	return &connPool{peers: make(map[string]*peerPool), size: size}
}

func (p *connPool) bucket(id string) *peerPool {
	p.mu.Lock()
	defer p.mu.Unlock()
	pp, ok := p.peers[id]
	if !ok {
		pp = &peerPool{}
		p.peers[id] = pp
	}
	return pp
}

// get checks out a connection to id, reusing an idle one if available (returning
// reused=true) or cold-dialing a new one. With pooling disabled it always dials.
func (p *connPool) get(ctx context.Context, id string, timeout time.Duration) (conn net.Conn, reused bool, err error) {
	if p.size > 0 {
		pp := p.bucket(id)
		pp.mu.Lock()
		if n := len(pp.idle); n > 0 {
			conn = pp.idle[n-1]
			pp.idle = pp.idle[:n-1]
			pp.mu.Unlock()
			return conn, true, nil
		}
		pp.mu.Unlock()
	}

	conn, err = dialPeer(ctx, id, timeout)
	return conn, false, err
}

// put returns a healthy connection to id's bucket if there is room, otherwise
// closes it. With pooling disabled it always closes (dial-per-call behavior).
func (p *connPool) put(id string, conn net.Conn) {
	if p.size <= 0 {
		conn.Close()
		return
	}

	p.mu.Lock()
	pp, ok := p.peers[id]
	p.mu.Unlock()
	if !ok { // peer removed (delMember) while we held the conn
		conn.Close()
		return
	}

	pp.mu.Lock()
	if len(pp.idle) < p.size {
		pp.idle = append(pp.idle, conn)
		pp.mu.Unlock()
		return
	}
	pp.mu.Unlock()
	conn.Close() // bucket full; don't hoard extras
}

// discard closes a broken connection without returning it to the pool.
func (p *connPool) discard(conn net.Conn) {
	if conn != nil {
		conn.Close()
	}
}

// remove closes and drops all connections to a departed peer.
func (p *connPool) remove(id string) {
	p.mu.Lock()
	pp, ok := p.peers[id]
	if ok {
		delete(p.peers, id)
	}
	p.mu.Unlock()
	if !ok {
		return
	}

	pp.mu.Lock()
	for _, c := range pp.idle {
		c.Close()
	}
	pp.idle = nil
	pp.mu.Unlock()
}

// closeAll closes every pooled connection. Called on shutdown.
func (p *connPool) closeAll() {
	p.mu.Lock()
	peers := p.peers
	p.peers = make(map[string]*peerPool)
	p.mu.Unlock()

	for _, pp := range peers {
		pp.mu.Lock()
		for _, c := range pp.idle {
			c.Close()
		}
		pp.idle = nil
		pp.mu.Unlock()
	}
}

// idleLen reports the number of idle connections held for id (test helper).
func (p *connPool) idleLen(id string) int {
	p.mu.Lock()
	pp, ok := p.peers[id]
	p.mu.Unlock()
	if !ok {
		return 0
	}
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return len(pp.idle)
}

// dialPeer opens a new TCP connection with keep-alive enabled, so silently-dead
// peers (killed pod, LB reset) are detected while the conn sits idle in the pool
// rather than only on the next write.
func dialPeer(ctx context.Context, id string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout, KeepAlive: broadcastKeepAlive}
	return d.DialContext(ctx, "tcp", id)
}
