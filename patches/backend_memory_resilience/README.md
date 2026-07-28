# Backend Memory Resilience

Status: the maintained backend patch accounts for configured and observed memory, reclaims optional
allocator and local Node state before external admission shedding, exports a shared pressure signal
for downstream owner-specific patches, and preserves the finite cgroup limit as the hard boundary.
All controls are disabled by default and require a readable, finite cgroup v2 memory limit when
enabled.

## Problem

A self-hosted backend can retain substantial memory in several independent owners: Rust and native
allocations, free allocator arena space, V8 isolate heaps and reusable contexts, the local Node
executor, caches, thread stacks, and cgroup kernel memory. Process RSS, allocator accounting, and
cgroup usage overlap rather than forming an additive breakdown. A hard cgroup limit alone therefore
cannot identify which optional owner should be reclaimed, and external request shedding should not
be the first response when warm state can be released safely.

This patch owns the startup feasibility checks, process/allocator/cgroup telemetry, hysteretic
reclamation and external HTTP shedding, allocator trim, and the shared pressure signal. It extends
the maintained local Node generation-retirement patch with a pressure response and exposes the
signal to later owner-specific patches.

The maintained commit is applied after
[`local_node_executor_resilience`](../local_node_executor_resilience/README.md), whose generation
fencing and graceful retirement it reuses, and
[`shared_base_http_admission`](../shared_base_http_admission/README.md), whose dependency-aware HTTP
gate it extends. It does not require a context-reuse patch.

## Pressure controller

`LOCAL_BACKEND_MEMORY_RECLAMATION_ENABLED` enables internal reclamation. It enters when finite cgroup
headroom is at or below `LOCAL_BACKEND_MEMORY_RECLAMATION_ENTER_HEADROOM_BYTES` and exits only when
headroom reaches `LOCAL_BACKEND_MEMORY_RECLAMATION_EXIT_HEADROOM_BYTES`. Defaults are 6 GiB and
8 GiB. The exit boundary must exceed the entry boundary and remain below `memory.max`.

The existing external shedding controller remains independently gated by
`LOCAL_BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED`, with default 3 GiB entry and 5 GiB exit headroom.
When both controllers are enabled, the reclamation entry and exit boundaries must each preserve
more headroom than the corresponding shedding boundary. Invalid relationships fail startup.

The controller samples cgroup headroom every second. On an eligible reclamation sample it first
evaluates allocator trim in bounded blocking work, then resamples the cgroup. It publishes the
shared pressure signal only if headroom remains below the reclamation exit condition. A slow or
failed trim cannot hide a later crossing of the external-shedding boundary. Losing the required
cgroup source or observing a runtime limit that invalidates configured thresholds triggers
controlled backend shutdown rather than silently disabling the safety dependency.

## Allocator reclamation and telemetry

`LOCAL_BACKEND_MALLOC_TRIM_ENABLED` enables explicit glibc `malloc_trim(0)` while reclamation is
active. It requires reclamation to be enabled. A trim is evaluated only when `mallinfo2` reports at
least `LOCAL_BACKEND_MALLOC_TRIM_MIN_FREE_BYTES` of logical free arena space, default 1 GiB, and no
evaluation has occurred within `LOCAL_BACKEND_MALLOC_TRIM_COOLDOWN_SECS`, default 300 seconds.

`mallinfo2` aggregates glibc arenas, but its `fordblks` value is logical allocator free space. It is
not proof that the same number of resident bytes can be returned. The Boolean `malloc_trim` result
also does not quantify released memory. Each completed trim therefore records immediate signed
changes in process RSS, process anonymous RSS, cgroup current usage, cgroup anonymous memory, and
allocator free bytes. It also records duration and process page faults across the bounded sample.
Unsupported allocators publish an explicit unsupported outcome and do nothing.

Arena-count telemetry uses `malloc_info` with a fixed 4 MiB `fmemopen` buffer and runs once every
five minutes. Oversized, malformed, or unsupported output is a telemetry failure; it cannot allocate
an unbounded diagnostic buffer. Allocator sampling, arena counting, full process/cgroup reporting,
and trim run in blocking work rather than occupying an asynchronous runtime worker.

Trim failure is an optional-recovery failure. It is counted and logged, but it does not suppress the
shared pressure signal or the Node consumer. A diagnostic or allocator failure must not disable the
remaining reclamation actions while cgroup headroom is low.

## Local Node reclamation

The local Node watchdog observes the same pressure signal. After continuous pressure for
`LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE_SECS`, default 60 seconds, it gracefully retires the
current generation only when a successful direct-child RSS sample is at or above
`LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_MIN_RSS_BYTES`, default 2 GiB. The pressure RSS floor must be
positive and strictly below the ordinary RSS retirement threshold.

The ordinary RSS limit remains the first proactive decision, followed by cgroup pressure, imported
package count, and generation age. Missing RSS telemetry, a smaller child, a shorter pressure
interval, or a cleared signal cannot trigger pressure retirement. The existing generation fencing,
admission close, active-request drain, health-failure preemption, direct-child termination, and
reaping contract is unchanged. The new bounded retirement reason is `cgroup_pressure`.

## Observability

The patch extends the existing bounded memory families with:

- `backend_allocator_arenas_info` and `backend_allocator_arena_telemetry_info`;
- `backend_memory_reclamation_enabled_info`, `backend_memory_reclamation_active_info`,
  `backend_memory_reclamation_headroom_threshold_bytes{boundary}`, and
  `backend_memory_reclamation_transitions_total{state}`;
- `backend_allocator_trim_enabled_info`, `backend_allocator_trim_active_info`,
  `backend_allocator_trim_configuration_info{component}`,
  `backend_allocator_trim_attempts_total{outcome}`, `backend_allocator_trim_seconds`,
  `backend_allocator_trim_memory_change_bytes{component}`, and
  `backend_allocator_trim_page_faults_total{kind}`;
- `local_node_executor_memory_pressure_active_info`,
  `local_node_executor_memory_pressure_rss_threshold_bytes`, and
  `local_node_executor_memory_pressure_grace_seconds`; and
- `cgroup_pressure` in the existing Node retirement and decision families.

Trim outcomes are `returned_true`, `returned_false`, `unsupported`, and `sample_failure`. Falling
below the logical-free threshold does not call `malloc_trim` and does not increment an attempt
outcome. Signed result gauges retain the latest completed sample, so consumers must qualify them
with a counter increment in the observation window. Arena and allocator availability gauges
distinguish measured zero from unsupported or failed telemetry.

## Activation and verification

The reclamation, allocator-trim, and external-shedding switches default to `false`. Enabling either
controller requires a finite cgroup v2 limit. Enabling trim additionally requires reclamation. All
byte settings use strict nonnegative decimal parsing; trim minimum free space, trim cooldown, Node
pressure RSS, and Node pressure grace must be positive. Changing the process environment requires a
backend restart.

Focused tests cover hysteresis, pressure-signal publication after trim evaluation, trim cooldown and
signed results, bounded live glibc arena counting, Node RSS and grace requirements, and ordinary-RSS
decision priority. Runtime verification must use emitted metrics to measure trim latency and memory
changes, owner reclamation, Node retirement, and cgroup recovery. No fixed reclaimed-byte or latency
benefit is assumed.

Removing the patch restores the previous backend memory behavior. No schema or data change is
involved, but an older image does not understand the new environment variables or emit the new
metric families.
