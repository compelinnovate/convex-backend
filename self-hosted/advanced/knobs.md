# Advanced Configuration and Tuning

There is a large number of detailed configuration options in
[knobs.rs](/crates/common/src/knobs.rs). These options are configurable via
environment variables. In order to tune your Convex instance at scale for your
workload, you may need to adjust these knobs. You will have to set these
environment variables by adding them to your `docker-compose.yml` file. Commonly
overriden knobs are listed in the `env` section of the
[`docker-compose.yml`](../docker/docker-compose.yml)

## `APPLICATION_MAX_CONCURRENT_*` knobs

You can increase the max concurrency on your self-hosted instance with these
environment variables. Note that increasing concurrency will increase load on
your system and after a certain threshold, performance will degrade. You will
have to tune parameters based on your own hardware and workload.

Each nonzero application limit has dependency-only overflow carved out of its
configured total. The effective reserve is the smaller of
`ISOLATE_DEPENDENCY_WORKER_RESERVE` and one less than the application limit.
Every request class consumes shared base capacity first. At the query, mutation,
and default-runtime action limits, only a request that unblocks a function
retaining an isolate worker may raise occupancy above that base. Nested Node
actions receive the same treatment at the Node action limit because their
parent retains a permit from that limit.

## Isolate worker and queue knobs

`MAX_ISOLATE_WORKERS` is the total number of isolate workers assigned requests
at once. `ISOLATE_DEPENDENCY_WORKER_RESERVE` is dependency-only overflow carved
out of that total: all requests share
`MAX_ISOLATE_WORKERS - ISOLATE_DEPENDENCY_WORKER_RESERVE` base occupancy, and
only work that unblocks an isolate-holding ancestor can use the remainder. The
reserve must be smaller than the worker total, and the worker total must be
nonzero.

`MAX_ISOLATE_ACTION_WORKERS` separately caps independent default-runtime and
HTTP actions that retain workers. `0` derives this cap from shared base worker
capacity. An explicit value cannot exceed base capacity. Queries, mutations,
and child actions that unblock an ancestor are not subject to this cap.

`ISOLATE_QUEUE_SIZE` is the shared base capacity of the bounded CoDel queue and
must be nonzero. Dependencies can use
`ISOLATE_DEPENDENCY_WORKER_RESERVE` additional queue entries. Once both parts
are full, another enqueue fails immediately instead of waiting in an unbounded
queue.

A one-worker pool cannot run a function and a separately scheduled descendant
at the same time. Use at least two workers and a reserve of at least one for
applications with these call patterns. The reserve is finite; deep chains or
parallel fanout can still consume every worker and queue entry.

## `FUNRUN_ISOLATE_ACTIVE_THREADS`

This caps isolates actively executing JavaScript. `0` means unlimited. A
request can release this permit while waiting for asynchronous work, so this is
not the same as assigned isolate workers and does not provide dependency-only
overflow. Use it to control CPU oversubscription, and raise it only after
checking backend CPU headroom and throttling.
