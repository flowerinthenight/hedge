## 1. Options and pool scaffolding

- [x] 1.1 Add `WithBroadcastPoolSize(n int)` and `WithBroadcastTimeout(d time.Duration)` options with defaults (poolSize 2, timeout 5s)
- [x] 1.2 Add pool fields to `Op` (`pool` keyed by member id, plus its mutex)
- [x] 1.3 Implement `peerPool` (idle slice + size) and `connPool` with `get`/`put`/`discard`/`remove`/`closeAll` (in `pool.go`)
- [x] 1.4 Enable keep-alive on each pooled connection (via `net.Dialer{KeepAlive}` in `dialPeer`)

## 2. Bound per-node I/O (Change 2)

- [x] 2.1 Add `sendCtx(ctx, conn, msg, timeout)` setting `conn.SetDeadline` to `min(now+timeout, ctx.Deadline())`
- [x] 2.2 Evict (discard) the connection on write/read/deadline error (in `broadcastTo`)

## 3. Rewire Broadcast (Change 1 + Change 3)

- [x] 3.1 Replace per-member dial/close with `get` → `sendCtx` → `put`/`discard` + single retry on a reused conn (`broadcastTo`)
- [x] 3.2 Cold-dial via `net.Dialer{Timeout, KeepAlive}.DialContext(ctx, ...)`
- [x] 3.3 Make collection cancellable: `select` on a `w.Wait()` done-channel vs `ctx.Done()` (non-stream path)
- [x] 3.4 Hook `delMember` to `pool.remove(id)` (close + drop the peer's bucket)
- [x] 3.5 Close the pool on shutdown (TCP-listener teardown path)
- [x] 3.6 Update `BroadcastArgs.Timeout` doc: now bounds the full per-node round trip

## 4. Tests

- [x] 4.1 Stub peer that accepts then never replies → timeout within `Timeout`; pooled conn evicted (no slot leak)
- [x] 4.2 Context deadline sooner than timeout bounds the round trip (unit-level; full Broadcast-level ctx-cancel return needs the Spanner integration harness)
- [x] 4.3 Repeated calls to a stable peer open one conn (assert no per-call dial); `remove` closes its conns
- [x] 4.4 Concurrent calls to the same peer never cross replies (serialization)
- [x] 4.5 `WithBroadcastPoolSize(0)` reproduces dial-per-call behavior
