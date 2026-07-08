# broadcast Specification

## Purpose
Defines the behavior of `Op.Broadcast` — hedge's send-to-all-members primitive — including
per-peer TCP connection pooling, connection eviction, bounded round trips, context handling,
and backward-compatible tuning options.
## Requirements
### Requirement: Connection reuse via per-peer pool

The unary `Op.Broadcast` SHALL reuse TCP connections to each member through an internal
per-peer connection pool keyed by member id (`"ip:port"`), rather than dialing a new
connection and closing it on every call. The pool MUST NOT require any wire-protocol
change; it relies on the server's existing connection-reuse loop.

#### Scenario: Repeated broadcasts reuse connections

- **WHEN** `Broadcast` is called repeatedly against a stable member set
- **THEN** at most `poolSize` connections are opened per member for the whole sequence
- **AND** subsequent calls perform no additional dial (assert no per-call dial)

#### Scenario: Cold start dials on demand

- **WHEN** a broadcast needs a connection to a peer whose pool has no idle connection
- **THEN** exactly one new connection is dialed for that peer and used for the request

#### Scenario: Concurrent broadcasts to one peer do not cross replies

- **WHEN** multiple broadcasts target the same peer concurrently
- **THEN** each request/reply is carried on its own connection (one in-flight request per
  connection) and no reply is delivered to the wrong request

#### Scenario: Concurrency above poolSize does not grow the pool

- **WHEN** more than `poolSize` broadcasts target the same peer concurrently
- **THEN** the connections beyond `poolSize` are dialed for their in-flight requests and
  closed on completion rather than retained
- **AND** the pool retains at most `poolSize` connections per peer, with no unbounded
  growth (excess concurrency degrades to per-call dialing for the overflow only)

### Requirement: Broken and departed connections are evicted

A pooled connection that fails a write or read, or whose deadline expires, SHALL be
closed and dropped from the pool rather than returned. When a member leaves the cluster,
its pooled connections SHALL be closed and its pool entry removed. All pooled connections
SHALL be closed on shutdown. Connections SHALL enable TCP keep-alive so silently-dead
peers are detected while idle.

#### Scenario: Failed request evicts the connection

- **WHEN** a request on a pooled connection fails or times out
- **THEN** that connection is closed and not returned to the pool (the pool slot is not
  leaked)
- **AND** an optional single retry MAY be attempted on a fresh connection

#### Scenario: Member removal closes its connections

- **WHEN** `delMember` removes a peer
- **THEN** that peer's pooled connections are closed and its pool entry is removed

### Requirement: Bounded per-node round trip

The `send`/`recv` path SHALL set a deadline covering both the write and the read so that a
peer that accepts the connection but never replies cannot block the broadcast
indefinitely. The deadline SHALL be the smaller of the configured broadcast timeout and
any deadline on the caller's `ctx`.

#### Scenario: Stalled peer times out within the timeout

- **WHEN** a peer accepts the connection but never replies
- **THEN** `Broadcast` returns a timeout error for that peer within the configured
  `Timeout`
- **AND** the pooled connection to that peer is evicted

### Requirement: Broadcast honors its context

`Broadcast(ctx, ...)` SHALL make `ctx` effective: cancellation MUST abort an in-flight
cold dial and MUST cause the reply-collection wait to return without blocking on
outstanding peers.

#### Scenario: Cancelled context returns early

- **WHEN** the caller's `ctx` is cancelled before all replies are collected
- **THEN** `Broadcast` returns before the configured `Timeout` elapses

### Requirement: Backward-compatible tuning options

The library SHALL provide `WithBroadcastPoolSize(n int)` and
`WithBroadcastTimeout(d time.Duration)`. Existing callers SHALL observe identical
behavior by default except that pooling is enabled. `WithBroadcastPoolSize(0)` SHALL
disable pooling and restore the exact dial-per-call behavior.

#### Scenario: Defaults preserve behavior

- **WHEN** a caller upgrades without setting any new option
- **THEN** `Broadcast` works unchanged, with pooling enabled (default size 2–4) and a
  default per-node timeout of 5s

#### Scenario: Pooling can be disabled

- **WHEN** `WithBroadcastPoolSize(0)` is configured
- **THEN** `Broadcast` dials a fresh connection per call and closes it, as before pooling

