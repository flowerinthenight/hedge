## Context

`services/sapphired`, a leader-elected fleet, uses unary `Broadcast` for periodic
liveness heartbeats (`doBroadcastHeartbeat`, full fan-out every 2s per active workload)
and per-entity state queries. Timeouts were absent at 80–100 pods and appeared at ~150.

Investigation on a running follower pod:

| Signal | Value | Meaning |
|---|---|---|
| `nofile` limit / open fds | 1,048,576 / 330 | fd exhaustion ruled out |
| `ListenOverflows` / `TCPBacklogDrop` | 0 / 0 | accept-backlog overflow not observed |
| **`TIME_WAIT` → `:8081`** | **2371 → 3214 → 3371** | churning fast — the signal |
| `ESTABLISHED` → `:8081` | 0 | hedge conns are transient (dial→close), not pooled |
| `ESTABLISHED` → `:443` | 312 | only persistent conns are to Spanner/GCP |
| `net.ipv4.tcp_tw_reuse` | 0 | `TIME_WAIT` ports locked ~60s, not reusable |
| `ip_local_port_range` | 32768–60999 = 28,231 | the ceiling |

Each `Broadcast` opens a connection per member and the caller closes it, so `TIME_WAIT`
accrues on the caller's ephemeral ports. Outbound rate scales with fan-out width `N`;
cluster load with active broadcasters × `N`. 100→150 pods is ~2.25× cluster load. This
is churn, not a per-call timeout value — shortening the dial timeout does not reduce
connection count.

Current path (both versions): one goroutine per member doing
`net.DialTimeout` → `send` → `recv` → `defer conn.Close()`, then an unconditional
`w.Wait()`. Key enabler: the server already reuses connections — `handleMsg` is a
`for {}` loop and `doBroadcast` writes its reply and returns without closing. Only the
client discards the connection.

## Goals / Non-Goals

**Goals:**
- Eliminate per-broadcast connection churn transparently to callers.
- Bound per-node I/O so one stalled peer cannot stall a broadcast or leak a pool slot.
- Make `ctx` effective for cancellation.
- Keep the public API and wire protocol unchanged.

**Non-Goals:**
- Protocol-level multiplexing / request IDs (wire-format change; revisit separately).
- Changes to `StreamBroadcast` (separate channel-based API).
- Applying the new deadline to member-sync ping / `getLeaderConn` / `SendToLeader`
  (possible follow-up; not required here).

## Decisions

### Per-peer pool of connections, not one shared connection

The wire protocol is an **untagged** line request/reply: `recv` reads the next
`\n`-terminated line and assumes it is the reply to the last request. A pooled connection
must therefore carry **one request at a time**. Concurrency comes from handing *different*
pooled connections to concurrent callers, so each peer gets a small pool
(checkout/checkin), not a single mutex-guarded connection (which would head-of-line-block
concurrent broadcasters).

`get(ctx, id)` pops an idle conn or cold-dials if none; `put(id, conn)` returns a healthy
conn if the bucket has room, else closes it; `discard(conn)` closes a broken conn without
returning it. `delMember` closes and removes a peer's bucket; shutdown closes all.

**Buffered-reader caveat.** `recv` allocates a *fresh* `bufio.NewReader(conn)` per call
(v2 `hedge.go:1173`, v3 `hedge.go:1228`; the server reads the same way). A `bufio.Reader`
can buffer bytes past the `\n`; discarding it each call is safe **only** because one
request is on the wire at a time. Consequences: do not pipeline; and if `recv` is later
refactored to a persistent per-conn reader, attach that reader to the pooled conn and
reuse it across checkouts rather than re-creating it from the raw `net.Conn`.

*Alternative considered:* protocol multiplexing with request IDs would remove the pool
entirely but is a cross-version wire-format change — deferred.

### Bound I/O with a ctx/timeout-aware deadline

Add `sendCtx(ctx, conn, msg)` that sets `conn.SetDeadline` to
`min(now + WithBroadcastTimeout, ctx.Deadline())`, bounding both write and
`ReadString`. On expiry, evict the conn. `BroadcastArgs.Timeout` now bounds the full
per-node round trip (dial + send + recv), the more intuitive reading.

### Make ctx effective

Cold dial uses `net.Dialer{Timeout: timeout}.DialContext(ctx, ...)`. Collection becomes
cancellable via `select` on a `w.Wait()` done-channel vs `ctx.Done()`; pooled conns carry
deadlines so goroutines unwind on their own.

## Risks / Trade-offs

- **Stale idle connections** (killed pod, LB reset) → mitigation: `SetKeepAlive(true)` +
  keepalive period; failed request triggers `discard` + optional single retry on a fresh
  conn.
- **Wedged pooled connection permanently holds a slot** → mitigation: Change 2 deadline
  evicts it; the pool does not leak the slot.
- **Under-sized pool under bursty concurrency** → graceful: overflow borrowers cold-dial
  and close on check-in (bucket full); a little churn on the overflow only, never a stall
  or correctness issue. `WithBroadcastPoolSize` is the tuning knob. **Confirmed on a live
  10-pod cluster (see Validation):** with the default `poolSize=2`, driving 6 concurrent
  broadcasts per peer kept the pool pinned at its cap but produced steady `TIME_WAIT`
  growth from the ~4 overflow dials/peer; dropping to ≤2 concurrent broadcasts flattened
  it to the background floor. So the residual-churn risk is real whenever a caller's peak
  concurrent broadcasts per peer exceeds `poolSize`.
- **Reply-crossing on a shared conn** → prevented by the one-request-per-connection
  invariant (see serialization decision); a regression test asserts concurrent broadcasts
  never cross replies.

### Scaling characteristics

Pool size is driven by **concurrency**, not frequency or fleet size:

| Change | Effect |
|---|---|
| Broadcast interval (2s → 5s → 30s) | None on sizing; longer idle gaps lean on keepalive + discard/retry. Server's `recv` has no idle timeout, so it holds pooled conns open indefinitely between broadcasts. |
| Fleet size (150 → 200 → 500) | Total conns ≈ `peers × poolSize` — linear and tiny (200×2 = 400, 200×4 = 800). Removes the ~quadratic creation-rate cliff. |
| Concurrent broadcast loops ↑ | The only reason to raise `poolSize` (peak simultaneous broadcasts to one peer). |

Beyond these scales (thousands of peers, wildly bursty concurrency) add idle eviction
(close conns idle past a TTL) or an adaptive max-capped size; not needed here.

## Migration Plan

1. File one issue describing the churn (with the evidence table) plus the two I/O gaps.
2. Two PRs (v2 and v3 are separate module majors in the same repo), each adding Change 1
   (pool) + Change 2 (deadline) + Change 3 (ctx) behind the new options, pooling
   defaulted on.
3. After release, bump sapphired's `hedge/v2`; the fix applies with no sapphired code
   change. The downstream `BroadcastArgs{Timeout: 2s}` mitigation can stay or relax.
4. **Rollback:** `WithBroadcastPoolSize(0)` restores exact dial-per-call behavior without
   reverting the release.

## Validation

Verified end-to-end on the `hedgedemo` deployment (10 pods, hedge TCP port 8080), in
addition to the unit tests. Runtime validation is inherently point-in-time, so this
section records the **method and conclusions**; raw per-sample numbers live in the PR.

**Method.** The demo image builds the local package (same module, no `replace` needed).
The slim runtime image has no `ss`/`netstat`, so socket state is read from
`/proc/net/tcp` (port 8080 = hex `1F90`; state `01`=ESTABLISHED, `06`=TIME_WAIT; field
`$2`=local/server side, `$3`=remote/outbound side). Broadcast load is driven via the
demo's `/broadcast` HTTP endpoint; membership changes via `kubectl scale`.

**Confirmed:**
- **Reuse:** pooled `ESTABLISHED` conns to peers persist and are reused across calls,
  staying warm after load stops (no re-dial on the next broadcast).
- **Bounded, no leak:** pooled conns never exceed `peers × poolSize`; the count reflects
  peak concurrency per peer and is not reaped (no idle TTL), so steady-state fd usage
  tracks the busiest moment, not the current one.
- **Churn eliminated within capacity:** with concurrency ≤ `poolSize`, broadcast-driven
  `TIME_WAIT` stays flat at the background floor.
- **Graceful degradation:** concurrency > `poolSize` churns only on the overflow (pool
  stays capped) — see Risks / Trade-offs.
- **Eviction:** on scale-down, a surviving broadcaster's pooled conn count dropped to
  match the smaller member set (`delMember` → `pool.remove`); no stale conns to departed
  peers.
- **Correctness:** `/broadcast` returns one reply per member throughout.

**Out of scope, observed:** a small constant `TIME_WAIT` floor exists at zero broadcast
load, from the member-sync/ping path (`WithGroupSyncInterval`, leader pings) — not
addressed by this change.

## Open Questions

- ~~Default `poolSize`: 2 vs 4 for the initial release?~~ **Resolved:** keep the default
  at 2 for the library, but the value must match the caller's peak concurrent broadcasts
  per peer to fully avoid churn — validation showed 2 leaves overflow churn under heavier
  concurrency. sapphired should set `WithBroadcastPoolSize` to its measured peak rather
  than rely on the default.
- Should the single-retry-on-fresh-conn be on by default or opt-in?
- Extend Change 2's deadline to the ping / `SendToLeader` paths now or in a follow-up?
  (Validation showed this path is the residual `TIME_WAIT` floor at idle.)
