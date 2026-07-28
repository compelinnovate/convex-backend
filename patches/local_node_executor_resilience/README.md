# Local Node Executor Resilience

This implementation restores local Node-action liveness when the shared Node
process stops responding and bounds healthy-generation lifetime when memory or
non-evictable module identity grows. The upstream local executor uses one Node
process for the complete deployment. A synchronous loop or event-loop stall in
one action can therefore stop every unrelated Node action while queries,
mutations, V8 actions, MySQL, and the backend health endpoint remain available.
Repeated deployment analysis and execution can also retain ESM module graphs
for the life of that process even after the corresponding disk-cache entry is
deleted.

## Failure mode

The Rust client enforces the Node action timeout independently of the Node event
loop. Before this patch, both timeout paths returned the typed timeout response
without removing the current Node executor generation:

- timeout before response headers from `POST /invoke`;
- timeout while reading the streaming response after headers.

Later actions continued to use the same unresponsive process. The first final
failures appeared after the ten-minute Node action timeout plus the backend's
five-second allowance, and recovery required replacing the backend or manually
terminating the child.

One absolute Rust deadline covers request send, response headers, and response
streaming. The request is not configured with reqwest's total-response timeout
because that timeout also wraps the body and would surface as an untyped chunk
error before the response-stream retirement path runs. Local response streaming
also has a 64 MiB aggregate byte limit and a 1,024-part decoded NDJSON limit,
both above the valid function-result and log budgets. A second result is
rejected immediately, and partial lines use one aggregate byte buffer instead
of retaining transport-chunk metadata. A malformed shared process therefore
cannot grow one unterminated protocol line or amplify many tiny protocol
objects until the backend runs out of memory.

## Generation retirement

The patch gives every local Node process a monotonic process-local generation
number. A request retains the exact generation that accepted it. Request
timeout, response-stream timeout, request transport failure, a response with
`exitingProcess=true`, or watchdog failure can retire that generation. Backend
shutdown retires the current generation with `explicit_shutdown` and rejects
later invocations. A healthy generation also begins graceful retirement when
its sampled Linux direct-child RSS, age, or lifetime-unique imported
source-package count reaches the configured threshold.

Healthy retirement first closes admission while holding the generation-state
mutex, then waits for active Rust requests to drain before using the same
identity-fenced retirement and child-reaping path. The current child remains
the only child until it has been reaped; a waiting request cannot start a
replacement while the old child is still resident. The RSS threshold is a
sampled graceful-retirement trigger and planning allowance, not a hard maximum.
The child can grow between samples and while active requests drain, and the
direct-child sample excludes descendants.

Retirement uses `Arc::ptr_eq` while holding the generation slot. A late timeout
or connection error from an old request cannot remove a replacement. Several
concurrent failures can request retirement, but only the first request whose
generation is still current changes the slot and increments the retirement
counter.

Retirement starts child termination immediately. Request-held references can
remain alive while old calls unwind, but they do not keep a blocked child
consuming a CPU core until the ten-minute request timeout. The next invocation
creates a replacement with a new process, socket, tempdir, module cache, timers,
listeners, and package state. Replacement remains lazy so an idle deployment
does not keep creating Node processes. Every spawned executor-server child
immediately enters a managed owner. If startup is canceled before publication,
that owner starts termination and transfers the wait to the runtime instead of
relying only on Tokio's best-effort orphan reaper. Retirement likewise transfers
termination to a spawned child owner before awaiting it, so canceling the request
that detected the failure does not cancel termination or reaping. While the
runtime remains available, child cleanup waits for the exit status, including
when an operator or the child itself wins the exit race. The replacement-duration
histogram measures successful process startup after the next invocation; it does
not include the intentional idle interval before that invocation.
Unexpected child-termination or wait errors contain only the process-local
generation and bounded operating-system error kind and propagate through
retirement instead of being hidden by the child owner. Request-driven retirement
returns the error; detached watchdog and shutdown boundaries emit a fixed
cleanup-failure log after the generation slot is already absent. Drop cleanup
retries a failed kill before waiting and falls back to kill-on-drop instead of
waiting indefinitely on a child whose termination never started. Cleanup
removes the tempdir only after it confirms direct-child reaping; cancellation,
runtime teardown, or a wait failure preserves the directory instead. A
confirmed-reaped tempdir moves to a detached native cleanup thread so recursive
removal does not occupy an asynchronous worker or keep Tokio runtime shutdown
waiting for a blocking task. Failure to start that thread preserves the
directory. A cleanup-thread start or filesystem removal failure emits only a
fixed cleanup message and bounded operating-system error kind.

Before supervisor termination, the child owner performs a nonblocking state
probe. It records whether the child was still running, had already exited, or
could not be inspected, then records whether the supervisor successfully
requested the terminating signal and the final reaped exit class. This
distinguishes a spontaneous child exit from a transport failure followed by
supervisor-initiated termination. The observation and its metric are emitted
inside the detached child owner, so cancellation of the request that initiated
retirement cannot remove the completed-termination evidence.

Generation selection, retirement state, and replacement-pending state share one
mutex. Retirement publishes the absent-generation gauges before releasing that
mutex, and replacement publishes the new-generation gauges before releasing it.
A replacement therefore cannot be followed by stale zeroes from the old
retirement. The active-request gauge is aggregate across current and draining
generations because old request guards can outlive replacement. It counts Rust
requests assigned to a generation, not Node HTTP handlers that may continue
after their Rust future is canceled. Potentially slow child startup uses a
separate single-flight lock, so late failures from the retired generation can
inspect the generation slot without waiting for replacement health checks to
finish. The Node version probe is also kill-on-drop, has a five-second deadline,
retains at most 1 KiB of standard output, terminates on the first excess chunk,
and discards standard error. A hung or noisy probe therefore cannot retain the
single-flight startup lock indefinitely or grow an unbounded output buffer. A
failed probe exit is rejected even if it wrote a supported-looking version
string.

`NodeExecutor::shutdown` is a synchronous trait operation. The local
implementation rejects later invocations immediately and schedules the
identity-fenced slot transition, child termination, and reaping on the runtime;
the trait call does not wait for that work to finish. Managed child drop and
process exit remain the fallbacks if runtime shutdown cancels the task.

## Event-loop watchdog

Each active generation has one backend-owned watchdog task. The watchdog:

- waits one second between checks;
- reads Linux direct-child RSS concurrently with `GET /health`;
- gives `GET /health` one second to complete and accepts at most 64 KiB of
  response data;
- clears the miss count after a valid `status="ok"` response;
- retires the selected generation after five consecutive misses;
- verifies generation identity again before publishing every response
  observation;
- treats an oversized or malformed health response, a negative stack-duration
  total, or a decreasing process-local event counter as a failed check;
- requires package and stack aggregates to be either both present or both
  absent, and treats a partial aggregate response as a failed check;
- exits when the executor, generation slot, or selected generation no longer
  exists.

The worst normal detection time is approximately ten seconds because each missed
check can consume its one-second timeout before the next interval. Asynchronous
provider requests do not block the health handler. Synchronous work that blocks
the event loop for approximately ten seconds is treated as a failed process and
must not be used as an application execution strategy.

The startup health check remains separate. Startup performs up to 50 checks at
100 ms intervals and uses the same one-second per-check timeout before
publishing a generation.

After every watchdog observation, proactive trigger precedence in the base patch is direct-child
RSS, lifetime imported source-package count, then generation age. When the
[`backend_memory_resilience`](../backend_memory_resilience/README.md) patch is also carried,
sustained cgroup pressure with a material direct-child RSS sample follows the ordinary RSS check
and precedes the package and age checks. These checks run even when the health endpoint is failing;
the consecutive-health-miss decision follows them. A failed Linux RSS read records `failure` and
skips only the RSS trigger for that iteration. Non-Linux builds record `unsupported` and do not
enforce an RSS trigger. A successful sample records `success`.

## Metrics and logs

The patch exports bounded backend metrics:

- `local_node_executor_generation_present_info`;
- `local_node_executor_generation_starts_total`;
- `local_node_executor_child_starts_total`;
- `local_node_executor_child_exits_total{class}`;
- `local_node_executor_generation_retirements_total{reason}`;
- `local_node_executor_retirement_diagnostics_total{reason,request_kind,phase,transport_error_kind}`;
- `local_node_executor_child_terminations_total{reason,state_before,supervisor_kill_requested,exit_class}`;
- `local_node_executor_replacement_outcomes_total{outcome}`;
- `local_node_executor_replacement_seconds`;
- `local_node_executor_generation_age_seconds`;
- `local_node_executor_health_check_seconds{phase,outcome}`;
- `local_node_executor_consecutive_health_misses`;
- `local_node_executor_waiting_requests`;
- `local_node_executor_request_starts_total`;
- `local_node_executor_request_completions_total{outcome}`;
- `local_node_executor_active_requests`;
- `local_node_executor_old_space_limit_bytes`;
- `local_node_executor_rss_retirement_threshold_bytes`;
- `local_node_executor_memory_pressure_rss_threshold_bytes`;
- `local_node_executor_memory_pressure_grace_seconds`;
- `local_node_executor_memory_pressure_active_info`;
- `local_node_executor_age_retirement_threshold_seconds`;
- `local_node_executor_package_retirement_threshold_info`;
- `local_node_executor_child_rss_bytes`;
- `local_node_executor_child_rss_telemetry_info`;
- `local_node_executor_child_rss_samples_total{outcome}`;
- `local_node_executor_generation_draining_info`;
- `local_node_executor_retirement_decisions_total{reason,decision}`;
- `local_node_executor_imported_source_packages_info`.

The base retirement reason is one of `request_timeout`, `response_stream_timeout`,
`connection_error`, `process_exiting`, `health_check_failed`, `rss_limit`, `package_limit`,
`age_limit`, or `explicit_shutdown`. Backend memory resilience additionally adds
`cgroup_pressure`. Child exit
class is one of `success`, `failure`, or `signal`. RSS sampling outcome is one
of `success`, `failure`, or `unsupported`. A proactive retirement decision is
`not_current`, `already_draining`, or `drain_started`. Request outcome is one of
`success`, `user_error`, `invalid_response`, `request_timeout`,
`response_stream_timeout`, `connection_error`, `transport_error`,
`response_stream_error`, `http_error`, `args_too_large`, or `internal_error`.
Neither label contains function names, module paths, package keys, request IDs,
raw errors, or deployment-specific values. Labelled counters are absent until
the corresponding outcome occurs and can be evicted after inactivity. Operator
queries treat an absent bounded label as zero only when the compatibility gauge
or that metric family has an opening sample at the report start. A contract
first observed inside the window has partial coverage, so its missing labels
remain unknown. A completely absent family on an older backend remains `not
emitted`, not zero.

`connection_error` is the bounded generation-retirement reason for local
request submission and response-body transport failures. Request outcomes keep
direct connect failures separate from other pre-header transport failures.

Retirement diagnostics identify request kind as `execute`, `analyze`, or
`build_deps`; watchdog and shutdown use `not_applicable`. Phase is one of
`before_response_headers`, `response_body`, `response_payload`, `health_check`,
`watchdog`, or `shutdown`. Transport category is one of `timeout`,
`connection_refused`, `connection_reset`, `connection_aborted`,
`not_connected`, `broken_pipe`, `unexpected_eof`, `other_io`, `connect`,
`body`, `request`, `other`, or `not_applicable`. Child state is `running`,
`already_exited`, or `probe_failed`; the supervisor-kill label is boolean and
final exit class retains the existing `success`, `failure`, or `signal`
contract. Replacement outcome is `ready`, `startup_failed`, or
`aborted_shutdown`. None of these metrics use generation as a label.

Interpret `local_node_executor_child_rss_bytes` only while
`local_node_executor_child_rss_telemetry_info` is one. A failed or unsupported
sample changes freshness to zero but retains the last byte value. Configuration
gauges are process configuration. Current generation age, RSS freshness,
draining state, imported package count, and package/cache state reset when the
generation is removed or replaced. Counters are process-local and require
reset-aware deltas.

The waiting gauge covers requests waiting for generation selection or child
startup. The active gauge covers assigned requests across the current and
draining generations; the assignment handoff can make one request appear in both
gauges for a brief interval. Executor-server child starts include failed startup
processes, while a generation start is recorded only after startup health
succeeds and the generation is published.

When the atomic source-package patch is also present, the Node health response
supplies aggregate package and stack state. The watchdog exports the aggregate
gauges and converts process-local event totals into backend counter deltas:

- `local_node_executor_retained_source_packages_info` and
  `local_node_executor_retained_source_package_bytes`;
- `local_node_executor_retained_external_packages_info` and
  `local_node_executor_retained_external_package_bytes`;
- `local_node_executor_active_source_package_owners_info` and
  `local_node_executor_registered_stack_roots_info`;
- `local_node_executor_imported_source_packages_info`, a monotonic
  generation-lifetime count of source-package roots submitted to Node's dynamic
  importer;
- source/external hit, publish, retire, and failed-publication events in
  `local_node_executor_package_events_total{package_kind,operation}`;
- `local_node_executor_stack_format_invocations_total`,
  `local_node_executor_stack_format_frames_total`, and duration since the prior
  successful health observation in `local_node_executor_stack_format_seconds`.
  The histogram records every supported successful-observation interval,
  including measured zero-work intervals, and its sum is aggregate stack-format
  seconds.

Without the atomic source-package patch, the health response omits both
aggregate objects and these package and stack metric families remain absent
(`not emitted`); unsupported telemetry is not reported as measured zero.

Generation start and retirement logs contain only the process-local generation
number, replacement flag and startup duration, bounded retirement reason,
generation age, active request count, and a boolean indicating whether the
companion runtime aggregates are supported. Retirement also includes the last
successfully observed aggregate source-package, external-package, and
registered-stack-root counts. They do not include the child command, request,
function, package key, URL, environment, or raw error object.

Retirement logs also include bounded request kind, phase, transport category,
and whether a replacement is expected. Completed child-termination logs include
generation, retirement reason, child state before termination, whether the
supervisor requested the terminating signal, and final exit class. Replacement
start, failure, and shutdown-abort logs include the process-local generation it
was intended to replace. Raw transport errors, requests, child standard output,
and child standard error remain excluded.

The local server child does not inherit backend standard input, output, or
error. Function console output continues through the bounded NDJSON response
protocol, while direct `process.stdout` and `process.stderr` writes cannot bypass
that protocol into backend infrastructure logs.

## Tests

Focused Rust tests cover:

- retiring the selected generation;
- terminating an indefinitely noisy Node version probe as soon as its bounded
  output is full;
- retaining an unpublished generation's private temporary directory until
  detached child cleanup confirms direct-child reaping after startup
  cancellation, and preserving it when reaping is unconfirmed;
- reaping a child after an operator wins the termination race;
- preserving a replacement when an old generation reports a late timeout;
- collapsing concurrent retirement requests into one slot transition;
- continuing child termination and reaping after the retiring caller is
  canceled;
- classifying transport failures into closed, sanitized categories;
- distinguishing supervisor termination of a running child from reaping a
  child that had already exited;
- resetting the watchdog miss count after a valid health response and retiring
  only after the later consecutive-failure threshold;
- distinguishing an upstream health response with no companion aggregates from
  a malformed partial or explicit-null aggregate response, and rejecting an
  invalid cumulative duration at startup;
- preserving a typed request timeout before response headers and retiring that
  generation;
- retiring a generation when the local server closes during request submission
  before response headers;
- retiring a generation when the local server closes its response body after
  sending headers;
- preserving a typed response-stream timeout after headers and retiring that
  generation;
- rejecting duplicate results and excess decoded response parts without
  retaining them until the stream ends;
- rejecting negative timing values and inconsistent syscall counts in executor
  responses instead of coercing them into valid metrics;
- shutdown retirement and child reaping;
- inclusive and ordered RSS/package/age retirement decisions, with cgroup-pressure ordering in the
  backend memory-resilience composition;
- strict old-space/RSS configuration validation and Linux RSS parsing;
- graceful admission closure and drain before proactive retirement;
- continuing drain and child ownership after the initiating caller is
  canceled;
- fencing replacement startup until the retiring direct child is reaped;
- lifetime imported-package counting that begins only at an actual dynamic
  import attempt and survives disk-cache retirement.

The package patch owns the Node-side health aggregate, package-lifetime, and
stack-root tests. The production rollout verifies watchdog health responses,
generated metrics, child replacement, and successful Node completions with
ordinary workload rather than a provider fixture.

## Adoption and rollback

Timeout and unhealthy-watchdog recovery are automatic after a backend image
containing the lifecycle implementation starts. Healthy proactive retirement
uses these startup knobs:

- `LOCAL_NODE_EXECUTOR_MAX_OLD_SPACE_SIZE_MIB`;
- `LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES`;
- `LOCAL_NODE_EXECUTOR_MAX_GENERATION_AGE_SECS`;
- `LOCAL_NODE_EXECUTOR_MAX_IMPORTED_SOURCE_PACKAGES`.

The backend memory-resilience composition additionally uses:

- `LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_MIN_RSS_BYTES`;
- `LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE_SECS`.

The backend validates positive values and requires the V8 old-space allowance to remain strictly
below the ordinary RSS retirement threshold. The memory-resilience composition also requires the
pressure RSS threshold to remain strictly below that ordinary threshold. It passes old space to
Node with `--max-old-space-size` before the script path. V8 old space excludes
Buffers, native modules, executable code, allocator retention, and descendant
processes. No process pool is required.

Rollback restores the previous backend image and its complete tracked capacity environment. When
the previous image already contains timeout and unhealthy generation retirement, rolling back the
local Node resilience patch removes only the newer healthy RSS/package/age controls and their
telemetry; it does not rewrite earlier deployed lifecycle history. Rolling back the separate
backend memory-resilience patch removes its cgroup-pressure input without changing the base Node
retirement mechanisms. Timeout recycling, unhealthy-watchdog retirement, and healthy proactive
retirement share the same generation guard and child-termination contract, so carrying an
unreviewed partial image is not supported.

The patch does not remove the one-process throughput ceiling or isolate
synchronous Node work across processes. A process pool remains a separate
measured design after generation recovery and package lifetime are stable. The
Rust termination boundary owns the direct Node executor child and retains its
temporary directory through confirmed direct-child reaping. If cleanup is
canceled or runtime teardown prevents Rust from confirming that reap, Rust
preserves the directory instead of removing files from under a possibly live
direct child. Rust does not create a process group or cgroup for descendants
and receives no descendant-exit acknowledgment.

During ordinary `build_deps` completion and timeout, the parent observes npm
supervisor close after signaling its owned group. After abrupt Node death, IPC
disconnect triggers a best-effort group kill. Neither path acknowledges
descendant exit. Rust can remove the generation temporary directory after
reaping Node without confirming that detached descendants have exited.
Subprocesses started by user code remain outside both contracts.
