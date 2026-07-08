## Why

Under production load (~150-pod fleet) the unary `Op.Broadcast` dials a fresh TCP
connection to every member on every call and closes it, leaving thousands of
`TIME_WAIT` sockets per pod and approaching ephemeral-port exhaustion
(`connect: cannot assign requested address`, dial timeouts). Connection *creation rate*
scales ~`broadcasters × N` (quadratic in fleet size), which is why scaling 100→150 pods
crossed a threshold. Two secondary gaps let a single slow/dead peer stall a whole
broadcast: post-connect I/O has no deadline, and the `ctx` argument is ignored.

## What Changes

- Add an internal **per-peer connection pool** to `Op`; `Broadcast` checks a connection
  out, does its request/reply, and checks it back in instead of dialing and closing.
  Steady state drops from thousands of churning sockets to ~`peers × poolSize`
  persistent connections. Requires no protocol change (the server's `handleMsg` loop
  already reuses connections).
- **Bound the post-connect I/O** (`send`/`recv`) with a context/timeout-aware deadline so
  a stalled peer cannot block a broadcast or permanently occupy a pool slot.
- **Honor the `ctx` argument** in `Broadcast`: cancellation aborts in-flight dials and
  the reply-collection wait.
- Add two backward-compatible options: `WithBroadcastPoolSize(n)` (default 2–4; `n=0`
  disables pooling, restoring exact current behavior) and `WithBroadcastTimeout(d)`
  (per-node round-trip; default 5s).
- Applies identically to `hedge/v2` (v2.3.0) and `hedge/v3` (v3.2.0). No public API
  signature changes; `ctx` simply becomes effective.

## Capabilities

### New Capabilities
- `broadcast`: unary `Op.Broadcast` request/reply fan-out — connection lifecycle,
  per-node I/O bounding, context handling, and tuning options.

### Modified Capabilities
<!-- None: openspec/specs/ has no captured broadcast spec yet, so behavior is defined as new. -->

## Impact

- **Code (v2):** `Broadcast` (`hedge.go:933`), per-member dial (`hedge.go:974`),
  `w.Wait()` (`hedge.go:1006`), `send`/`recv` (`hedge.go:1155`/`1168`), `delMember`
  (`hedge.go:1381`), plus a new pool type and options.
- **Code (v3):** `Broadcast` (`hedge.go:993`), dial (`hedge.go:1034`), `w.Wait()`
  (`hedge.go:1066`), `send`/`recv` (`hedge.go:1215`/`1228`), `delMember`
  (`hedge.go:1441`).
- **Protocol:** none. `handleMsg`'s `for {}` loop and `doBroadcast` (write-and-return,
  no close) already support reuse.
- **Downstream (`services/sapphired`):** none required. Its shipped
  `BroadcastArgs{Timeout: 2s}` mitigation can stay or relax to defaults after upgrade.
- **Out of scope:** protocol-level multiplexing / request IDs, `StreamBroadcast`, and the
  member-sync ping / `getLeaderConn` / `SendToLeader` paths.
