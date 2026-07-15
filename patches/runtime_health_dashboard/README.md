# Runtime health dashboard semantics

## Summary

This patch makes the self-hosted dashboard describe measured queueing and
scheduled-function lag without claiming stronger capacity failures than the
available metrics establish. It also separates scheduler-state sampling from
rate-limited log publication and records direct scheduled-job admission lag.

It is a generic self-hosting patch. It does not contain application function
names, route names, or deployment-specific limits.

## Motivation

The stock health view can misclassify two normal runtime transitions:

- Function concurrency increments outstanding work before an asynchronous permit
  acquisition. A one-sample queued value therefore does not prove that the
  configured concurrency limit was reached.
- Scheduled-function lag is queried in one-minute buckets, rounded to whole
  minutes, and refreshed once per minute. The backend extrapolates its last
  reported ready time across trailing buckets, but records that state through
  the same call that rate-limits structured scheduler logs. If the scheduler
  advances to another future ready time less than 30 seconds later, the updated
  state can be suppressed and the old timestamp can appear overdue even though
  the scheduler is waiting for the newer timestamp.

The second issue is especially misleading because the log stream stores whole
lag seconds while the health graph stores rounded minutes. An operator can see
`1` in the graph while scheduler logs and direct admission-lag telemetry show no
corresponding one-second backlog.

## Behavior

### Function concurrency

The health summary reports queueing as queueing. It distinguishes queueing in
the latest bucket from queueing earlier in the one-hour window and does not say
that a configured limit was reached. Actual capacity conclusions still require
the configured limit, permit timeout, scheduler rejection, and queue evidence.

### Scheduled-function lag

Every scheduler iteration that obtains a ready-time decision records that
observation in the in-memory app-metrics store before applying the
structured-log predicate.
Structured scheduler events retain their existing 30-second ready-time-change
and overdue-heartbeat coalescing. Suppressed log publication no longer leaves
stale state available to dashboard extrapolation.

An iteration that errors before obtaining a queue decision leaves the last
successful observation in place because no fresher queue state is available.
The endpoint seeds each requested window with the latest retained earlier
sample, so repeated scheduler errors cannot make a known overdue state disappear
when the one-hour window advances past that sample.

The executor refreshes the pending queue head even while all of its execution
slots are occupied. Cancellation or replacement can therefore move the head to
a later ready time without leaving the previous time available for
extrapolation until a running job finishes.

Each full-executor refresh initializes one index head per scheduled-job
namespace, then advances past at most the parallelism-bounded running-ID set
before returning the first non-running entry. The database scan completes
before the separate scheduler-lag store is locked for its in-memory update, so
the refresh neither holds that lock across asynchronous work nor introduces a
reverse lock order.

If a backward wall-clock step places an app-metrics sample before the store's
startup time or latest writable bucket, the dedicated store discards its
pre-jump timeline and restarts at the new observation. This removes every old
ready time from extrapolation while preserving the lag observed at the new
wall-clock time. Historical points before the reset become unavailable because
their timestamp ordering no longer represents observation ordering.

Scheduler lag uses a dedicated source store retaining at most 240 15-second
buckets, or one hour while sampling continues. The generic UDF metric store
remains at its existing one-minute resolution and memory shape. A sample is
stored as lag at the source bucket start rather than lag at the instant the
sample arrived. The endpoint can then reconstruct
`output_time - next_job_time` for each requested point without treating a
mid-bucket sample as if it occurred at the bucket start. This prevents a healthy
negative catch-up sample from turning into increasing positive lag during
resampling.

The latest observation in a 15-second source bucket replaces any earlier
observation in that bucket. This is state sampling, not event counting: it can
remove a shorter-lived earlier lag state, but it cannot double-count it. Direct
admission telemetry retains each admission attempt independently.

The backend also records `scheduled_job_admission_lag_seconds` once for each
ordinary scheduled-job admission attempt, immediately before background
dispatch. This unlabeled histogram measures the delay from that attempt's
target timestamp to scheduler admission. A retried job contributes one sample
for each admission attempt. Downstream action admission and user-code start have
separate timing boundaries. The scheduler anchors lag to the wall-clock
due-time decision and adds monotonic elapsed time until dispatch, so a wall-clock
step during the queue scan cannot erase or inflate already observed lag. The
extrapolated ready-time series remains useful for a scheduler that stops making
progress, while the direct histogram provides an independent execution-path
measurement.

The existing `scheduled_job_execution_lag_seconds` histogram remains a
rate-limited sample of ready-queue state. Despite its legacy name, it is not a
direct execution-start measurement.

The dashboard:

- requests 15-second buckets across the existing one-hour view;
- refreshes every 15 seconds;
- aligns the query window, chart timestamps, and deployment markers to those
  15-second buckets and includes the current bucket start;
- retains lag in seconds instead of rounding to whole minutes;
- leaves missing backend samples as gaps instead of rendering them as zero lag;
- formats sub-second, second, and minute values with explicit units;
- treats lag through 20 seconds as healthy, over 20 seconds as warning, and over
  five minutes as critical;
- reports recovery only after a materially delayed sample, not after any
  positive floating-point value.

These thresholds preserve the stock scheduler-status intent while making the
underlying values and units visible.

The dashboard makes four times as many requests and receives four times as many
points as the old minute view, or roughly sixteen times the transfer rate for
this one small metric response. The backend cost remains bounded to one
240-bucket gauge store per process and one unlabeled admission-lag histogram;
no application label or UDF metric family is multiplied. The scheduled-lag
store has its own lock, so updating it does not contend on the function-log and
generic UDF-metrics lock. Each in-order update overwrites at most one value in
one writable source bucket. A backward-time reset discards at most 240 buckets,
and neither path emits an external log event.

## Deployment

The backend and dashboard changes should normally be shipped together. The
backend change improves the app-metrics timeseries for every dashboard client.
The dashboard change is wire-compatible with an older backend because the
endpoint already returns seconds, but an older backend still has one-minute
source buckets and can temporarily extrapolate a stale ready time. The full
15-second contract therefore requires both halves.

Deploying the dashboard image does not require replacing the Convex backend.
Deploying the backend half follows the normal self-hosted backend image rollout
and should be grouped with another needed backend release rather than performed
solely for this display correction.

## Verification

Verify with a window containing both ordinary scheduled traffic and a short
executor-full period:

- the graph displays millisecond or second values without rounding them to one
  minute;
- adjacent points and deployment markers have distinct, aligned second-level
  timestamps;
- the status summary uses the current aligned 15-second bucket rather than the
  preceding bucket;
- a catch-up transition returns promptly to zero;
- moving the next future ready time by less than the structured-log interval
  cannot leave the previous timestamp available for extrapolation;
- structured scheduler logs retain their prior 30-second coalescing behavior;
- changing or canceling the pending queue head while the ordinary executor is
  full refreshes the extrapolated ready time;
- a catch-up sample arriving partway through a source bucket cannot become
  positive lag solely from resampling;
- a backward wall-clock step, including one before the metric store's startup
  time, does not suppress overdue-to-future recovery or render discarded
  pre-jump history as healthy samples;
- repeated scheduler errors do not make the last known overdue state disappear
  when it moves before the requested one-hour window;
- genuine sustained lag continues increasing and crosses warning and critical
  thresholds;
- `scheduled_job_admission_lag_seconds` records one observation per ordinary
  scheduler admission attempt, remains independent of ready-time extrapolation,
  and does not lose already observed lag on a wall-clock rollback;
- queueing warnings do not say that a concurrency limit was reached;
- direct admission-lag histograms, scheduler log events, and the dashboard
  timeseries agree on the order of magnitude.

## Rejected alternatives

### Keep minute buckets and only change the label

This leaves short but operationally useful lag invisible and does not correct
the false one-minute point.

### Remove backend extrapolation entirely

Extrapolation is useful when the oldest overdue job remains unchanged and the
scheduler itself is not advancing. Removing it would under-report a genuinely
stuck scheduler.

### Emit external logs on every scheduler loop

That would remove ambiguity but produce unnecessary log volume during busy
workloads. The app-metrics state is cheap and must reflect every scheduler
observation because it is extrapolated. Structured logs remain rate-limited, and
the direct admission histogram records only scheduler admission attempts.

### Infer saturation from queued work alone

Queueing can be a normal asynchronous handoff. Saturation requires
configured-limit and failure or wait evidence that the dashboard endpoint does
not currently carry.
