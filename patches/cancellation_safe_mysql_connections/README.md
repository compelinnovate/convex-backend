# Cancellation-Safe MySQL Connections

Status: the maintained backend patch owns each direct MySQL operation and transaction through a
cancel-on-drop guard. Normal completion preserves the pooled connection. Dropping the owning
future always removes the interrupted connection from the reusable pool and force-closes its
transport. Server-side numeric cancellation is disabled by default. An explicit trusted-topology
setting, `MYSQL_SERVER_SIDE_CANCELLATION_TRUSTED_SINGLE_NAMESPACE=true`, enables a bounded worker
that keeps the target transport open through the `KILL CONNECTION` response before force-closing
it. The default untrusted-topology path never issues numeric `KILL CONNECTION`.

## Problem

The MySQL driver returns a dropped pooled connection to its recycler. If a query future is canceled
while the server is still producing or waiting to produce a response, recycler cleanup waits for
and drains that response before the connection can be reused. The application-level timeout that
dropped the query also drops the database timeout wrapper, so the normal error path does not get an
opportunity to replace and disconnect the connection.

The driver's normal `Conn::disconnect` is also graceful: it sends `COM_QUIT` through a command path
that first drains pending results. It therefore cannot cancel a statement whose response is still
blocked. Force-closing the client transport is not sufficient either: a server thread waiting for
a lock may not read the peer's close until the wait ends.

This can retain a server-side statement after its caller has already received a timeout. It can
also make later acquisition latency depend on an earlier canceled operation.

## Contract

- A non-streaming direct query or execute operation preserves its connection only after consuming
  its full response. Operational errors and database timeouts first remove and force-disconnect the
  failed connection, then install a replacement. A driver path that leaves the connection marked
  disconnected is handled the same way even when the returned error is not classified as
  operational. Ordinary server errors have consumed their response and may reuse the existing
  connection.
- A streamed collection preserves its connection after successful stream and row-mapping
  completion. A handled operational error, database timeout, or disconnected driver state
  preserves only the fresh replacement connection. Other stream or mapping errors may leave a
  response unread, so that connection is discarded.
- A completed transaction attempt preserves its connection unless transaction start or an inner
  operation reports an error that may leave the protocol incomplete. An ordinary body error leaves
  the driver transaction marked for rollback through normal pool cleanup.
- Dropping an owning future always moves the connection out of its reusable slot. In the default
  untrusted-topology mode, the owner immediately calls the driver's synchronous force-disconnect
  path and never dispatches a control request. That path marks the connection disconnected,
  detaches it from the pool, drops its transport without draining results, rolling back, or sending
  `COM_QUIT`, and releases both the in-use and total pool accounting. The interrupted connection
  cannot enter recycler cleanup or become reusable, although a blocked server statement may remain
  until the server observes the closed transport or another server-side limit ends it.
- In trusted-single-namespace mode, dropping an owner moves the connection into its reserved
  cancellation request. The worker keeps that physical transport open until the numeric kill
  reaches a terminal response. This prevents ordinary statement completion from removing the
  target session before the kill and prevents the target identifier from being reused while the
  worker acts on it. The worker then force-disconnects the transport before publishing completion.
  The queued connection has a force-disconnect owner, so worker abort, worker shutdown with queued
  work, a failed control operation, or a dropped completion receiver cannot return an interrupted
  connection to recycler cleanup.
- Trusted-single-namespace mode uses a dedicated one-connection control pool built from the same
  sanitized connection options and processes numeric `KILL CONNECTION` statements serially. The
  first operation reservation establishes its control connection before the first data connection
  is acquired, and the pool retains that physical connection for the lifetime of the cancellation
  lane. It does not acquire from the ordinary data pool, so a saturated data pool cannot prevent
  cancellation.
- In trusted-single-namespace mode, every direct database operation reserves one slot in the
  bounded cancellation lane before it issues SQL. A transaction holds one reservation for its full
  attempt. The request carries the protocol connection identifier, a process-local
  physical-connection generation, and its request time. A newer local generation for the same
  server identifier makes the request stale. Registry entries are removed when normal checked-out
  ownership ends or when a cancellation reaches a terminal outcome, and removal matches both
  identifier and generation so an older cancellation cannot erase a newer reused-identifier
  registration. A request that is already stale does not acquire control capacity, and the worker
  checks its generation again immediately before issuing the kill. The worker also verifies that
  the pool still returned the initialized physical control connection. A replacement control
  transport can belong to a restarted server or a different backend namespace where the same
  numeric identifier names an unrelated session, so replacement fails closed before issuing
  `KILL`. Each request has a short hard deadline and is never retried.
- `KILL CONNECTION` returning success means that the server accepted the kill request; it does not
  prove that server-side cleanup or waiter removal has completed. Integration verification must
  observe the target waiter independently. MySQL error 1094 is a control failure, not evidence that
  the target is gone: it cannot distinguish a target that disappeared from a control connection
  routed to a different connection-identifier namespace. Keeping an operational target transport
  open removes the ordinary completion-versus-kill source of 1094 without weakening this
  fail-closed rule.
- In trusted-single-namespace mode, the control account and target connection use the same
  configured credentials. MySQL permits an account to kill its own sessions without an
  administrative kill privilege. Initial control acquisition, control-transport replacement, or
  kill failure closes admission to the cancellation lane, so later operations fail before issuing
  SQL instead of creating an unbounded orphan population. A control connection whose kill fails or
  exceeds the hard deadline is force-disconnected rather than returned to the one-connection
  control pool with a possibly unread response.
- Dropping an individual transaction method future, receiving a database timeout or operational
  error from it, or observing that the driver left the connection disconnected marks that
  transaction connection unusable. A later method or commit rejects reuse, and the outer
  transaction owner force-disconnects it.
- An operational error, database timeout, or disconnected driver state first removes and
  force-disconnects the failed connection. Replacement acquisition starts only after the reusable
  slot is empty and the old transport is closed. A replacement is registered and installed only
  after it also reserves cancellation capacity. If acquisition or reservation fails, or if its
  caller is canceled, the slot remains empty and cannot return or disconnect the failed connection
  a second time.
- A retry starts only after a replacement connection is installed in the reusable slot.
- Pool shutdown first waits for checked-out data connections to return or be discarded, then stops
  the cancellation worker and disconnects its control pool. In trusted-single-namespace mode it
  drains queued cancellation requests before stopping. Explicit shutdown awaits that sequence.
  Dropping the wrapper schedules the same sequence on a best-effort background task, so the runtime
  must remain alive for that cleanup to finish.
- Errors propagate to the caller. The cancellation guard does not turn a failed operation into a
  success or retry transaction work.

## Scope

The patch applies at the generic MySQL connection wrapper, so it covers persistence reads and
lease-owned write transactions without depending on a particular query shape, function runtime,
or timeout source. The default mode does not establish a control connection or reserve bounded
worker capacity. Trusted-single-namespace mode has a cancellation queue with
`MYSQL_MAX_CONNECTIONS` slots and adds one separately bounded, long-lived control connection
without changing ordinary data pool capacity or MySQL statement timeout values. Neither mode
requires Performance Schema visibility.

## Activation and topology obligation

`MYSQL_SERVER_SIDE_CANCELLATION_TRUSTED_SINGLE_NAMESPACE` defaults to `false` and accepts only the
exact values `true` and `false`; any other value fails strict configuration parsing. With the
default `false`, canceled and incomplete connections are force-disconnected locally and numeric
`KILL CONNECTION` is never attempted. Set it to `true` only when the operator has verified that
every data connection and the dedicated control connection created from the configured endpoint
always share one numeric connection-identifier namespace.

This assertion must remain valid across endpoint routing, proxy multiplexing, failover, and the
full lifetime of the control transport. Do not enable the setting for a proxy or managed endpoint,
including Vitess-compatible endpoints, unless its documented contract guarantees this property.
The initial control connection and a data connection can expose plausible numeric identifiers even
when they belong to different namespaces; the client cannot detect that condition. The persistent
control-generation check only rejects replacement after initialization and does not prove the
initial namespace relationship. Enabling the setting without this guarantee can make an identifier
collision kill an unrelated same-account session.

The repository vendors `mysql_async` 0.37.1 based on
`https://github.com/blackbeam/mysql_async` at
`ce4b27698c50fb945d8c9ff8c40a2a646be50b12`. It is mechanically identical to that revision outside
the two changed driver files. `src/conn/mod.rs` adds `Conn::force_disconnect`, a process-local
physical-connection generation, and the live-transport regression test. `src/conn/pool/mod.rs` adds
the dedicated pool-accounting discard path, wakes the recycler when a direct discard removes the
last connection during concurrent shutdown, and adds test-only accounting support. Normal graceful
disconnect behavior is unchanged.

The allowlist-style root `.dockerignore` includes only `third_party/mysql_async` under
`third_party`, so the relative dependency is available to image builds without admitting unrelated
third-party source or local build output into the context.

## Observability

The patch exports:

- `convex_local_backend_mysql_cancellation_requested_total`, labeled by `cluster_name`;
- `convex_local_backend_mysql_cancellation_terminal_total`, labeled by `outcome` and
  `cluster_name`; and
- `convex_local_backend_mysql_cancellation_seconds`, with the same terminal labels.

Terminal outcomes are `client_disconnected`, `kill_accepted`, `stale_generation`, and
`control_failure`. `client_disconnected` is the default-mode local force-disconnect and means no
numeric kill was dispatched. `kill_accepted` means MySQL accepted the numeric command; it does not
prove that the target waiter has disappeared. `stale_generation` means the local physical
connection generation is no longer the registered target. `control_failure` closes later
cancellation admission. Correlate these series with data-pool capacity, database latency, caller
outcomes, and independent observation of the exact server session.

## Verification

Focused unit tests cover numeric statement construction, stale-generation rejection,
generation-matched registry removal, target-transport retention through the kill response,
force-discard on worker abort and queued shutdown, dedicated worker progress, fail-closed queue
behavior, untrusted-topology local disconnect without server-side dispatch,
trusted-single-namespace dispatch without early target disconnect, cancel-on-drop ownership
removal, replacement-installation ownership invariants, single-discard behavior, and transaction
poisoning. A driver-level regression test uses a live TCP transport to verify that force-disconnect
closes it promptly and releases both the in-use and total pool counts without entering recycler
cleanup. A second driver regression test
parks the recycler during pool shutdown and verifies that force-disconnecting the final in-use
connection wakes it and lets shutdown finish. Integration verification must use the configured
application account to hold a server-side statement, cancel its caller, and confirm independently
that the exact session and lock waiter disappear before the blocking owner is released. It must
also replace or break the initialized control transport and verify that the lane rejects the new
physical control connection without issuing a numeric kill through it.

## Rollback

Restore the upstream connection wrapper and the upstream registry dependency. No schema or data rollback
is required. The restored behavior may again leave canceled statements in recycler cleanup until
the server responds.
