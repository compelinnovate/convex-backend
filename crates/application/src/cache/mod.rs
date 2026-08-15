use std::{
    cmp,
    collections::BTreeMap,
    iter,
    mem,
    sync::{
        atomic::{
            AtomicU32,
            AtomicUsize,
            Ordering,
        },
        Arc,
        LazyLock,
    },
    time::{
        Duration,
        SystemTime,
    },
};

use anyhow::Context;
use async_broadcast::{
    broadcast,
    Receiver,
    Sender,
};
use common::{
    audit_log_lines::AuditLogVars,
    components::PublicFunctionPath,
    execution_context::{
        ExecutionContext,
        RequestContext,
    },
    identity::IdentityCacheKey,
    knobs::{
        DATABASE_UDF_SYSTEM_TIMEOUT,
        DATABASE_UDF_USER_TIMEOUT,
    },
    query_analysis_admission::{
        QueryAnalysisAdmission,
        QueryAnalysisPermit,
    },
    query_journal::QueryJournal,
    runtime::Runtime,
    types::{
        ActiveJavascriptClass,
        AllowedVisibility,
        FunctionCaller,
        QueryInvocation,
        SchedulerDependencyClass,
        TableName,
        TableStats,
        Timestamp,
        UdfType,
    },
};
use database::{
    Database,
    Token,
};
use errors::{
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use futures::{
    select_biased,
    FutureExt,
};
use keybroker::Identity;
use lru::LruCache;
use metrics::{
    decrement_degradable_leader_permits_in_use,
    get_timer,
    increment_degradable_leader_permits_in_use,
    initialize_degradable_leader_metrics,
    log_cache_hit_visibility_rejected,
    log_cache_size,
    log_degradable_cache_recheck,
    log_degradable_cache_wait,
    log_degradable_leader_admission,
    log_drop_cache_result_too_old,
    log_perform_go,
    log_perform_wait_peer_timeout,
    log_perform_wait_self_timeout,
    log_plan_go,
    log_plan_peer_timeout,
    log_plan_ready,
    log_plan_wait,
    log_query_bandwidth_bytes,
    log_success,
    log_validate_refresh_failed,
    log_validate_system_time_in_the_future,
    log_validate_system_time_too_old,
    log_validate_ts_too_old,
    query_cache_log_eviction,
    succeed_get_timer,
    DegradableCacheLeaderClass,
    DegradableCacheRecheckOutcome,
    DegradableLeaderAdmissionOutcome,
    GoReason,
};
use parking_lot::Mutex;
use smallvec::{
    smallvec,
    SmallVec,
};
use sync_types::{
    types::SerializedArgs,
    QueryWorkloadClass,
};
use udf::{
    validation::{
        PendingArgsPolicy,
        ValidatedPathAndArgs,
        VisibilityInfo,
    },
    FunctionOutcome,
    UdfOutcome,
};
use usage_tracking::FunctionUsageTracker;
use value::{
    heap_size::HeapSize,
    ConvexValue,
    PendingValue,
};

use crate::{
    application_function_runner::FunctionRouter,
    audit_logging::AuditLogClient,
    function_log::FunctionExecutionLog,
    QueryReturn,
};

mod metrics;

static TOTAL_QUERY_TIMEOUT: LazyLock<Duration> =
    LazyLock::new(|| *DATABASE_UDF_USER_TIMEOUT + *DATABASE_UDF_SYSTEM_TIMEOUT);

/// Maximum age or future offset to tolerate for time-dependent results.
/// Future offsets account for clock skew between the backend and function
/// runner. This should be at least `TOTAL_QUERY_TIMEOUT` or else long queries
/// will loop.
static MAX_CACHE_AGE: LazyLock<Duration> =
    LazyLock::new(|| *TOTAL_QUERY_TIMEOUT + Duration::from_secs(1));

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum QueryExecutionClass {
    Dependency,
    Normal,
    Degradable,
}

impl QueryExecutionClass {
    fn new(
        scheduler_dependency: SchedulerDependencyClass,
        query_workload_class: Option<QueryWorkloadClass>,
        degradable_admission_enabled: bool,
    ) -> Self {
        if scheduler_dependency.unblocks_ancestor() {
            Self::Dependency
        } else if degradable_admission_enabled
            && query_workload_class == Some(QueryWorkloadClass::Degradable)
        {
            Self::Degradable
        } else {
            Self::Normal
        }
    }

    fn wait_decision(self, leader: Self) -> CacheWaitDecision {
        match (self, leader) {
            (Self::Dependency, Self::Dependency)
            | (Self::Normal, Self::Dependency | Self::Normal)
            | (Self::Degradable, _) => CacheWaitDecision::Wait,
            (Self::Dependency, Self::Normal) => {
                CacheWaitDecision::Bypass(GoReason::DependencyCannotWaitForIndependentPeer)
            },
            (Self::Dependency, Self::Degradable) => {
                CacheWaitDecision::Bypass(GoReason::DependencyCannotWaitForDegradablePeer)
            },
            (Self::Normal, Self::Degradable) => {
                CacheWaitDecision::Bypass(GoReason::NormalCannotWaitForDegradablePeer)
            },
        }
    }

    fn metric_leader_class(self) -> DegradableCacheLeaderClass {
        match self {
            Self::Dependency => DegradableCacheLeaderClass::Dependency,
            Self::Normal => DegradableCacheLeaderClass::Normal,
            Self::Degradable => DegradableCacheLeaderClass::Degradable,
        }
    }
}

enum CacheWaitDecision {
    Wait,
    Bypass(GoReason),
}

#[derive(Clone)]
struct DegradableQueryLeaderAdmission {
    inner: Arc<DegradableQueryLeaderAdmissionInner>,
}

struct DegradableQueryLeaderAdmissionInner {
    admission: QueryAnalysisAdmission,
    permits_in_use: AtomicUsize,
}

struct DegradableQueryLeaderPermit {
    permit: Option<QueryAnalysisPermit>,
    admission: Arc<DegradableQueryLeaderAdmissionInner>,
}

impl DegradableQueryLeaderAdmission {
    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::from_shared(QueryAnalysisAdmission::new(capacity))
    }

    fn from_shared(admission: QueryAnalysisAdmission) -> Self {
        initialize_degradable_leader_metrics(admission.capacity());
        Self {
            inner: Arc::new(DegradableQueryLeaderAdmissionInner {
                admission,
                permits_in_use: AtomicUsize::new(0),
            }),
        }
    }

    fn try_acquire(&self) -> anyhow::Result<DegradableQueryLeaderPermit> {
        let Some(permit) = self.inner.admission.try_acquire_degradable() else {
            log_degradable_leader_admission(DegradableLeaderAdmissionOutcome::Deferred);
            return Err(ErrorMetadata::degradable_query_capacity().into());
        };
        self.inner.permits_in_use.fetch_add(1, Ordering::SeqCst);
        increment_degradable_leader_permits_in_use();
        Ok(DegradableQueryLeaderPermit {
            permit: Some(permit),
            admission: self.inner.clone(),
        })
    }

    #[cfg(test)]
    fn has_available_permit(&self) -> bool {
        self.inner.admission.try_acquire_degradable().is_some()
    }
}

impl Drop for DegradableQueryLeaderPermit {
    fn drop(&mut self) {
        let permit = self
            .permit
            .take()
            .expect("degradable query leader permit released twice");
        // Account for this leader while the semaphore still excludes its
        // replacement, so concurrent acquisition cannot make occupancy exceed
        // the configured capacity.
        let previous = self.admission.permits_in_use.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0, "degradable query leader permit underflow");
        decrement_degradable_leader_permits_in_use();
        drop(permit);
    }
}

#[derive(Clone)]
pub struct CacheManager<RT: Runtime> {
    rt: RT,
    database: Database<RT>,
    function_router: FunctionRouter<RT>,
    udf_execution: FunctionExecutionLog<RT>,
    audit_log_client: AuditLogClient,

    tenant_id: QueryCacheTenantId,
    cache: QueryCache,
    degradable_query_leader_admission: Option<DegradableQueryLeaderAdmission>,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
struct QueryCacheTenantId(u32);
impl QueryCacheTenantId {
    fn allocate() -> Self {
        static NEXT_ID: AtomicU32 = AtomicU32::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        assert_ne!(id, u32::MAX, "tenant id overflow");
        QueryCacheTenantId(id)
    }
}

/// A cache key representing a specific query request, before it runs.
/// It may have more specific information than a `StoredCacheKey`, because
/// multiple query requests may be served by the same cache entry.
/// e.g. if a query does not check `ctx.auth`, then `RequestedCacheKey`
/// contains the identity, but `StoredCacheKey` does not.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct RequestedCacheKey {
    tenant_id: QueryCacheTenantId,
    path: PublicFunctionPath,
    args: SerializedArgs,
    identity: IdentityCacheKey,
    journal: QueryJournal,
    allowed_visibility: AllowedVisibility,
}

impl RequestedCacheKey {
    // In order from most specific to least specific.
    fn _possible_cache_keys(&self) -> Vec<StoredCacheKey> {
        vec![
            self.precise_cache_key(),
            StoredCacheKey {
                tenant_id: self.tenant_id,
                path: self.path.clone(),
                args: self.args.clone(),
                // Include queries that did not read `ctx.auth`.
                identity: None,
                journal: self.journal.clone(),
                allowed_visibility: self.allowed_visibility,
            },
        ]
    }

    fn precise_cache_key(&self) -> StoredCacheKey {
        StoredCacheKey {
            tenant_id: self.tenant_id,
            path: self.path.clone(),
            args: self.args.clone(),
            identity: Some(self.identity.clone()),
            journal: self.journal.clone(),
            allowed_visibility: self.allowed_visibility,
        }
    }

    fn get_cache_entry<'a>(
        &'a self,
        cache: &'a mut LruCache<StoredCacheKey, CacheEntry>,
        stored_key_hint: Option<&'_ StoredCacheKey>,
    ) -> (Option<&'a CacheEntry>, StoredCacheKey) {
        for key in self._possible_cache_keys() {
            if cache.contains(&key) {
                return (Some(cache.get(&key).unwrap()), key);
            }
        }
        if let Some(stored_key_hint) = stored_key_hint {
            assert!(self._possible_cache_keys().contains(stored_key_hint));
            (None, stored_key_hint.clone())
        } else {
            (None, self.precise_cache_key())
        }
    }

    fn cache_keys_after_execution(&self, outcome: &UdfOutcome) -> SmallVec<[StoredCacheKey; 2]> {
        let identity = if outcome.observed_identity {
            Some(self.identity.clone())
        } else {
            None
        };
        let key = StoredCacheKey {
            tenant_id: self.tenant_id,
            path: self.path.clone(),
            args: self.args.clone(),
            identity,
            journal: self.journal.clone(),
            allowed_visibility: self.allowed_visibility,
        };
        if self.journal != outcome.journal {
            // Record the result under *both* the original journal and the new
            // journal, as we expect re-executing the query with
            // `outcome.journal` to return the same result.
            //
            // This would typically happen if `self.journal` is empty (e.g. a
            // fresh caller starting a new paginated query), in which case
            // `outcome.journal` would contain a newly created journal. We want
            // to be able to cache requests from both the _same_ caller (who
            // will make their next request using `outcome.journal`) and from
            // new callers (who will use an empty journal).
            //
            // The journal could also change if the query starts paginating over
            // a different range of the table or a different table entirely.
            smallvec![
                StoredCacheKey {
                    journal: outcome.journal.clone(),
                    ..key.clone()
                },
                key
            ]
        } else {
            smallvec![key]
        }
    }
}

/// A cache key representing a persisted query result.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct StoredCacheKey {
    tenant_id: QueryCacheTenantId,
    path: PublicFunctionPath,
    args: SerializedArgs,
    // None means that the query did not read `ctx.auth`.
    identity: Option<IdentityCacheKey>,
    journal: QueryJournal,
    allowed_visibility: AllowedVisibility,
}

impl StoredCacheKey {
    /// Approximate size in-memory of the CacheEntry structure, including stack
    /// and heap allocated memory.
    fn size(&self) -> usize {
        mem::size_of::<Self>()
            + self.path.heap_size()
            + self.args.heap_size()
            + self.identity.heap_size()
            + self.journal.heap_size()
    }
}

enum CacheEntry {
    Ready(CacheResult),
    Waiting {
        id: u64,
        started: tokio::time::Instant,
        receiver: Receiver<CacheResult>,
        // The UDF is being executed at this timestamp.
        ts: Timestamp,
        execution_class: QueryExecutionClass,
    },
}

impl CacheEntry {
    /// Approximate size in-memory of the CacheEntry structure, including stack
    /// and heap allocated memory.
    fn size(&self) -> usize {
        mem::size_of::<Self>()
            + match self {
                CacheEntry::Ready(result) => result.heap_size(),
                // This is an under count since there might be something in the receiver.
                // However, this is kind of hard to measure, and we expect this to
                // be the exception, not the rule.
                CacheEntry::Waiting { .. } => 0,
            }
    }
}

#[derive(Clone)]
struct CacheResult {
    outcome: Arc<UdfOutcome>,
    original_ts: Timestamp,
    token: Token,
    visibility_info: Option<VisibilityInfo>,
}

impl HeapSize for CacheResult {
    fn heap_size(&self) -> usize {
        self.outcome.heap_size() + self.original_ts.heap_size() + self.token.heap_size()
    }
}

impl<RT: Runtime> CacheManager<RT> {
    pub fn new(
        rt: RT,
        database: Database<RT>,
        function_router: FunctionRouter<RT>,
        udf_execution: FunctionExecutionLog<RT>,
        audit_log_client: AuditLogClient,
        cache: QueryCache,
        query_analysis_admission: Option<QueryAnalysisAdmission>,
    ) -> Self {
        // each `CacheManager` (for a different deployment) gets its own tenant
        // ID within `Cache`, which has a _global_ size-limit
        let tenant_id = QueryCacheTenantId::allocate();
        Self {
            rt,
            database,
            function_router,
            udf_execution,
            audit_log_client,
            tenant_id,
            cache,
            degradable_query_leader_admission: query_analysis_admission
                .map(DegradableQueryLeaderAdmission::from_shared),
        }
    }

    /// Execute a UDF with the given arguments and identity at a particular
    /// timestamp. This function internally handles LRU caching these
    /// function executions and ensuring that served cache values are
    /// consistent as of the given timestamp.
    #[fastrace::trace]
    pub async fn get(
        &self,
        request_context: RequestContext,
        path: PublicFunctionPath,
        args: SerializedArgs,
        identity: Identity,
        ts: Timestamp,
        journal: Option<QueryJournal>,
        caller: FunctionCaller,
        usage_tracker: FunctionUsageTracker,
        query_invocation: QueryInvocation,
        scheduler_dependency: SchedulerDependencyClass,
        query_workload_class: Option<QueryWorkloadClass>,
    ) -> anyhow::Result<QueryReturn> {
        let timer = get_timer();
        let result = self
            ._get(
                request_context,
                path,
                args,
                identity,
                ts,
                journal,
                caller,
                usage_tracker,
                query_invocation,
                scheduler_dependency,
                query_workload_class,
            )
            .await;
        match &result {
            Ok((query_return, is_cache_hit)) => {
                succeed_get_timer(
                    timer,
                    *is_cache_hit,
                    query_return.journal.end_cursor.is_some(),
                );
            },
            Err(e) => {
                timer.finish_with(e.metric_status_label_value());
            },
        }
        Ok(result?.0)
    }

    async fn _get(
        &self,
        request_context: RequestContext,
        path: PublicFunctionPath,
        args: SerializedArgs,
        identity: Identity,
        ts: Timestamp,
        journal: Option<QueryJournal>,
        caller: FunctionCaller,
        usage_tracker: FunctionUsageTracker,
        query_invocation: QueryInvocation,
        scheduler_dependency: SchedulerDependencyClass,
        query_workload_class: Option<QueryWorkloadClass>,
    ) -> anyhow::Result<(QueryReturn, bool)> {
        let start = self.rt.monotonic_now();
        let identity_cache_key = identity.cache_key();
        let requested_key = RequestedCacheKey {
            tenant_id: self.tenant_id,
            path,
            args,
            identity: identity_cache_key,
            journal: journal.unwrap_or_else(QueryJournal::new),
            allowed_visibility: caller.allowed_visibility(),
        };
        let context = ExecutionContext::new(request_context, &caller);
        let execution_class = QueryExecutionClass::new(
            scheduler_dependency,
            query_workload_class,
            self.degradable_query_leader_admission.is_some(),
        );
        // If the query exists at some cache key, but the cached entry is invalid,
        // create a Waiting entry at that key, even if it's not the most precise for the
        // request. e.g. if the query was cached with identity:None, create a
        // Waiting entry with identity:None so other queries with other
        // identities will wait for us to finish. This requires keeping
        // `stored_key_hint` across iterations, since finding an invalid key
        // removes it from the cache and continues the loop.
        let mut stored_key_hint = None;

        let mut num_attempts = 0;
        let mut retry_description = vec![];
        'top: loop {
            num_attempts += 1;
            let now = self.rt.monotonic_now();

            // Bound the total time of the caching algorithm in cases we wait to
            // long before we start executing. If there is slowdown, we prefer to
            // fast fail instead of adding additional load to the system.
            let elapsed = now - start;
            anyhow::ensure!(
                elapsed <= *TOTAL_QUERY_TIMEOUT,
                "Query execution time out: {elapsed:?} after {num_attempts} attempts. Attempts: \
                 {retry_description:?}",
            );

            // Step 1: Decide what we're going to do this iteration: use a cached value,
            // wait on someone else to run a UDF, or run the UDF ourselves.
            let miss_behavior = if execution_class == QueryExecutionClass::Degradable {
                CacheMissBehavior::InspectDegradable
            } else {
                CacheMissBehavior::Publish
            };
            let maybe_plan = self.cache.plan_cache_op(
                &requested_key,
                stored_key_hint.as_ref(),
                start,
                now,
                &identity,
                ts,
                context.clone(),
                execution_class,
                miss_behavior,
            );
            let (op, stored_key) = match maybe_plan {
                Some(CachePlan::Operation(op, stored_key)) => (op, stored_key),
                Some(CachePlan::NeedsDegradableAdmission(stored_key)) => {
                    let admission = self
                        .degradable_query_leader_admission
                        .as_ref()
                        .expect("degradable cache miss planned while admission is disabled");
                    let permit = admission.try_acquire()?;
                    let recheck = self.cache.plan_cache_op(
                        &requested_key,
                        Some(&stored_key),
                        start,
                        now,
                        &identity,
                        ts,
                        context.clone(),
                        execution_class,
                        CacheMissBehavior::PublishDegradable(permit),
                    );
                    match recheck {
                        Some(CachePlan::Operation(op, stored_key)) => {
                            log_degradable_cache_recheck(op.degradable_recheck_outcome());
                            (op, stored_key)
                        },
                        Some(CachePlan::NeedsDegradableAdmission(_)) => {
                            panic!("degradable cache recheck requested admission twice")
                        },
                        None => {
                            log_degradable_cache_recheck(DegradableCacheRecheckOutcome::Retry);
                            retry_description
                                .push(format!("plan_cache_recheck_failed ({elapsed:?})"));
                            continue 'top;
                        },
                    }
                },
                None => {
                    retry_description.push(format!("plan_cache_op_failed ({elapsed:?})"));
                    continue 'top;
                },
            };
            stored_key_hint = Some(stored_key.clone());

            // Create a waiting entry in order to guarantee the waiting entry always
            // get cleaned up if the current future returns an error or gets dropped.
            let waiting_entry_id = match op {
                CacheOp::Go {
                    waiting_entry_id, ..
                } => waiting_entry_id,
                _ => None,
            };
            let mut waiting_entry_guard =
                WaitingEntryGuard::new(waiting_entry_id, &stored_key, self.cache.clone());

            // Step 2: Perform our cache operation, potentially running the UDF.
            let is_cache_hit = match op {
                // Serving from cache.
                CacheOp::Ready { .. } => true,
                // We either use a result from the cache or we continue from 'top
                // after Step 2 without logging.
                CacheOp::Wait { .. } => true,
                // We are executing ourselves.
                CacheOp::Go { .. } => false,
            };
            let op_type = op.to_string();
            let (result, table_stats) = match self
                .perform_cache_op(
                    &requested_key,
                    &stored_key,
                    op,
                    usage_tracker.clone(),
                    &identity,
                    scheduler_dependency,
                )
                .await?
            {
                Some(r) => r,
                None => {
                    retry_description.push(format!("perform_op_{op_type}_failed ({elapsed:?})"));
                    continue 'top;
                },
            };

            // Step 3: Validate that the cache result we got is good enough. Is our desired
            // timestamp in its validity interval? If it looked at system time, is it not
            // too old?
            let cache_result = match self.validate_cache_result(&stored_key, ts, result).await? {
                Some(r) => r,
                None => {
                    retry_description.push(format!("validate_cache_result_failed ({elapsed:?})"));
                    continue 'top;
                },
            };

            // Step 4: Rewrite the value into the cache. If this was a cache hit, this will
            // bump the cache result's token. This method will discard the new value if the
            // UDF failed or if a newer (i.e. higher `original_ts`) value is in the cache.
            if cache_result.outcome.result.is_ok() {
                // We do not cache JSErrors
                let actual_stored_keys =
                    requested_key.cache_keys_after_execution(&cache_result.outcome);
                waiting_entry_guard.complete(actual_stored_keys, cache_result.clone());
            } else {
                drop(waiting_entry_guard);
            }

            // Step 5: Log some stuff and return.
            let vars =
                AuditLogVars::from_context(context.clone(), identity.convex_actor_var(), &self.rt)?;
            self.audit_log_client
                .send_logs(
                    cache_result.outcome.audit_log_lines.resolve_bodies(&vars)?,
                    &usage_tracker,
                )
                .await?;
            log_success(num_attempts);
            let usage_stats = usage_tracker.clone().gather_user_stats();
            let database_bandwidth_bytes = usage_stats.database_egress.values().sum();
            log_query_bandwidth_bytes(
                cache_result.outcome.journal.end_cursor.is_some(),
                database_bandwidth_bytes,
            );
            self.udf_execution
                .log_query(
                    &cache_result.outcome,
                    table_stats,
                    is_cache_hit,
                    start.elapsed(),
                    caller,
                    usage_tracker,
                    context.clone(),
                    query_invocation,
                )
                .await;
            let result = QueryReturn {
                // A top-level query's result never contains unresolved commit
                // timestamps.
                result: match cache_result.outcome.result.clone() {
                    Ok(value) => Ok(value.try_into()?),
                    Err(e) => Err(e),
                },
                log_lines: cache_result.outcome.log_lines.clone(),
                token: cache_result.token,
                journal: cache_result.outcome.journal.clone(),
            };
            return Ok((result, is_cache_hit));
        }
    }

    fn authorize_cache_hit(
        &self,
        requested_key: &RequestedCacheKey,
        identity: &Identity,
        result: CacheResult,
    ) -> anyhow::Result<CacheResult> {
        // Assumes invariant: only `JsError` results lack visibility info, and those
        // are not cached, so we fail closed rather than serve a result we cannot
        // authorize.
        let visibility_info = result
            .visibility_info
            .as_ref()
            .context("Cached query result is missing visibility info")?;
        let access = visibility_info.check_access(
            requested_key.allowed_visibility,
            identity,
            UdfType::Query,
            requested_key.path.clone(),
        )?;
        let Err(js_error) = access else {
            return Ok(result);
        };
        tracing::warn!(
            "Refusing to serve cached result for {} to a caller that may not run it: {js_error}",
            requested_key
                .path
                .clone()
                .debug_into_component_path()
                .debug_str(),
        );
        log_cache_hit_visibility_rejected(identity.tag());
        Ok(CacheResult {
            outcome: Arc::new(UdfOutcome::from_error(
                js_error,
                requested_key.path.clone().debug_into_component_path(),
                requested_key.args.clone(),
                identity.clone().into(),
                self.rt.clone(),
                None,
            )?),
            original_ts: result.original_ts,
            // The caller may not observe the read set of a result they aren't
            // allowed to read, so give them nothing to subscribe to.
            token: Token::empty(result.original_ts),
            visibility_info: None,
        })
    }

    #[fastrace::trace]
    async fn perform_cache_op(
        &self,
        requested_key: &RequestedCacheKey,
        key: &StoredCacheKey,
        op: CacheOp<'_>,
        usage_tracker: FunctionUsageTracker,
        requester_identity: &Identity,
        scheduler_dependency: SchedulerDependencyClass,
    ) -> anyhow::Result<Option<(CacheResult, BTreeMap<TableName, TableStats>)>> {
        let pause_client = self.rt.pause_client();
        pause_client.wait("perform_cache_op").await;
        let r = match op {
            CacheOp::Ready { result } => {
                if result.outcome.result.is_err() {
                    panic!("Developer error: Cache contained failed execution for {key:?}")
                }
                (
                    self.authorize_cache_hit(requested_key, requester_identity, result)?,
                    BTreeMap::new(),
                )
            },
            CacheOp::Wait {
                waiting_entry_id,
                mut receiver,
                remaining,
            } => {
                let result = select_biased! {
                    maybe_result = receiver.recv().fuse() => match maybe_result {
                        Err(..) => {
                            // The peer working on the cache entry went away (perhaps due to an
                            // error), so remove its entry and retry.
                            self.cache.remove_waiting(key, waiting_entry_id);
                            log_perform_wait_peer_timeout();
                            return Ok(None)
                        },
                        Ok(r) => r,
                    },
                    // We checked in `plan_cache_op` that the peer hadn't timed out with respect to
                    // its start time, so timing out here means that we're on a retry and ran out of
                    // our time ourselves. Therefore, don't remove the cache entry in this
                    // condition.
                    _ = self.rt.wait(remaining).fuse() => {
                        log_perform_wait_self_timeout();
                        return Ok(None)
                    },
                };
                if result.outcome.result.is_err() {
                    panic!("Developer error: CacheOp::Go sent failed execution for {key:?}")
                }
                (
                    self.authorize_cache_hit(requested_key, requester_identity, result)?,
                    BTreeMap::new(),
                )
            },
            CacheOp::Go {
                waiting_entry_id: _,
                degradable_leader_permit,
                sender,
                path,
                args,
                identity,
                ts,
                journal,
                allowed_visibility,
                context,
            } => {
                // Only a cache operation that still owns the admitted leader
                // permit receives degradable active-JavaScript service. A
                // cache hit, follower, normal bypass, or dependency must not
                // inherit a client declaration by accident.
                let active_javascript_class = if degradable_leader_permit.is_some() {
                    ActiveJavascriptClass::Degradable
                } else {
                    ActiveJavascriptClass::Protected
                };
                let mut tx = self
                    .database
                    .begin_with_ts(identity.clone(), ts, usage_tracker)
                    .await?;
                // We are validating UDF visibility here as opposed to earlier so the validation
                // checks are transactional with running the query. This is safe because we will
                // never serve a result based of a stale visibility check since the data read as
                // part of the visibility check is part of the ReadSet for this query.
                let validate_result = ValidatedPathAndArgs::new_with_returns_validator(
                    allowed_visibility,
                    &mut tx,
                    path.clone(),
                    args.clone(),
                    UdfType::Query,
                    PendingArgsPolicy::Reject,
                )
                .await?;

                let (mut tx, query_outcome, visibility_info) = match validate_result {
                    Err(js_err) => {
                        let query_outcome = UdfOutcome::from_error(
                            js_err,
                            path.clone().debug_into_component_path(),
                            args.clone(),
                            identity.clone().into(),
                            self.rt.clone(),
                            None,
                        )?;
                        (tx, query_outcome, None)
                    },
                    Ok((path_and_args, returns_validator, visibility_info)) => {
                        let component = path_and_args.path().component;
                        let (mut tx, outcome) = self
                            .function_router
                            .execute_query_or_mutation(
                                tx,
                                path_and_args,
                                UdfType::Query,
                                journal.clone(),
                                context,
                                scheduler_dependency,
                                active_javascript_class,
                            )
                            .await?;
                        let FunctionOutcome::Query(mut query_outcome) = outcome else {
                            anyhow::bail!("Received non-query outcome when executing a query")
                        };
                        if let Ok(json_packed_value) = &query_outcome.result
                            && returns_validator.needs_validation()
                        {
                            let output: ConvexValue = json_packed_value
                                .unpack()
                                .and_then(PendingValue::try_into_concrete)
                                .map_err(|e| {
                                    e.wrap_error_message(|msg| {
                                        format!(
                                            "Function {} return value invalid: {msg}",
                                            query_outcome.path.debug_str(),
                                        )
                                    })
                                })?;
                            let table_mapping = tx.table_mapping().namespace(component.into());
                            let virtual_system_mapping = tx.virtual_system_mapping();
                            let returns_validation_error = returns_validator.check_output(
                                &output,
                                &table_mapping,
                                virtual_system_mapping,
                            );
                            if let Some(js_err) = returns_validation_error {
                                query_outcome.result = Err(js_err);
                            }
                        }
                        (tx, query_outcome, Some(visibility_info))
                    },
                };
                let ts = tx.begin_timestamp();
                let table_stats = tx.take_stats();
                let token = tx.into_token()?;
                let result = CacheResult {
                    outcome: Arc::new(query_outcome),
                    original_ts: *ts,
                    token,
                    visibility_info,
                };
                if result.outcome.result.is_ok()
                    && requested_key
                        .cache_keys_after_execution(&result.outcome)
                        .contains(key)
                {
                    let _: Result<_, _> = sender.try_broadcast(result.clone());
                } else {
                    // Send an error to receivers so any waiting peers will retry.
                    drop(sender);
                }
                log_perform_go(result.outcome.result.is_ok());
                (result, table_stats)
            },
        };
        Ok(Some(r))
    }

    #[fastrace::trace]
    async fn validate_cache_result(
        &self,
        key: &StoredCacheKey,
        ts: Timestamp,
        mut result: CacheResult,
    ) -> anyhow::Result<Option<CacheResult>> {
        if ts < result.original_ts {
            // If the cached value is newer than the requested timestamp,
            // we have to re-execute the UDF.
            log_validate_ts_too_old();
            return Ok(None);
        }
        result.token = match self.database.refresh_token(result.token, ts).await? {
            Ok(t) => t,
            Err(_invalid_ts) => {
                tracing::debug!(
                    "Couldn't refresh cache entry from {} to {}, retrying...",
                    result.original_ts,
                    ts
                );
                self.cache.remove_ready(key, result.original_ts);
                log_validate_refresh_failed();
                return Ok(None);
            },
        };
        if result.outcome.observed_time {
            let sys_now = self.rt.unix_timestamp();
            let cached_time = result.outcome.unix_timestamp;
            match sys_now.checked_sub(cached_time) {
                Some(entry_age) if entry_age > *MAX_CACHE_AGE => {
                    tracing::debug!(
                        "Log entry for {:?} used system time and is too old ({:?}), retrying...",
                        key,
                        entry_age
                    );
                    self.cache.remove_ready(key, result.original_ts);
                    log_validate_system_time_too_old();
                    return Ok(None);
                },
                None if cached_time - sys_now > *MAX_CACHE_AGE => {
                    tracing::warn!(
                        "Cached value's timestamp {:?} is too far in the future (current wall \
                         time: {:?})",
                        cached_time,
                        sys_now,
                    );
                    self.cache.remove_ready(key, result.original_ts);
                    log_validate_system_time_in_the_future();
                    return Ok(None);
                },
                Some(..) | None => (),
            }
        }
        Ok(Some(result))
    }
}

// A wrapper struct that makes sure that the waiting entry always gets removed
// when the performing operation is dropped, even if the caller future gets
// canceled.
struct WaitingEntryGuard<'a> {
    entry_id: Option<u64>,
    key: &'a StoredCacheKey,
    cache: QueryCache,
}

impl<'a> WaitingEntryGuard<'a> {
    fn new(entry_id: Option<u64>, key: &'a StoredCacheKey, cache: QueryCache) -> Self {
        Self {
            entry_id,
            key,
            cache,
        }
    }

    // Marks the waiting entry as removed, so we don't have to remove it on Drop
    fn complete(&mut self, actual_stored_keys: SmallVec<[StoredCacheKey; 2]>, result: CacheResult) {
        if let Some(entry_id) = self.entry_id.take() {
            self.cache.remove_waiting(self.key, entry_id);
            self.cache.put_ready(actual_stored_keys, result);
        }
    }
}

impl Drop for WaitingEntryGuard<'_> {
    fn drop(&mut self) {
        // Remove the cache entry from the cache if still present.
        if let Some(entry_id) = self.entry_id {
            self.cache.remove_waiting(self.key, entry_id)
        }
    }
}

struct Inner {
    cache: LruCache<StoredCacheKey, CacheEntry>,
    size: usize,
    size_limit: usize,

    next_waiting_id: u64,
}

#[derive(Clone)]
pub struct QueryCache {
    inner: Arc<Mutex<Inner>>,
}

impl QueryCache {
    pub fn new(size_limit: usize) -> Self {
        let inner = Inner {
            cache: LruCache::unbounded(),
            size: 0,
            next_waiting_id: 0,
            size_limit,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    fn plan_cache_op<'a>(
        &self,
        key: &'a RequestedCacheKey,
        stored_key_hint: Option<&'_ StoredCacheKey>,
        start: tokio::time::Instant,
        now: tokio::time::Instant,
        identity: &'a Identity,
        ts: Timestamp,
        context: ExecutionContext,
        execution_class: QueryExecutionClass,
        miss_behavior: CacheMissBehavior,
    ) -> Option<CachePlan<'a>> {
        let go = |sender: Option<(Sender<_>, u64)>, degradable_leader_permit| {
            let (sender, waiting_entry_id) = match sender {
                Some((sender, waiting_entry_id)) => (sender, Some(waiting_entry_id)),
                None => {
                    // No one should wait for this, so it's okay to drop the
                    // receiver. And the sender ignores errors.
                    let (sender, _) = broadcast(1);
                    (sender, None)
                },
            };
            CacheOp::Go {
                waiting_entry_id,
                degradable_leader_permit,
                sender,
                path: &key.path,
                args: &key.args,
                identity,
                ts,
                journal: &key.journal,
                allowed_visibility: key.allowed_visibility,
                context,
            }
        };
        let mut inner = self.inner.lock();
        let (entry, stored_key) = key.get_cache_entry(&mut inner.cache, stored_key_hint);
        let op = match entry {
            Some(CacheEntry::Ready(r)) => {
                if ts < r.original_ts {
                    // If another request has already executed this UDF at a
                    // newer timestamp, we can't use the cache. Re-execute
                    // in this case.
                    let degradable_leader_permit = match miss_behavior {
                        CacheMissBehavior::Publish => None,
                        CacheMissBehavior::InspectDegradable => {
                            return Some(CachePlan::NeedsDegradableAdmission(stored_key));
                        },
                        CacheMissBehavior::PublishDegradable(permit) => Some(permit),
                    };
                    tracing::debug!("Cache value too new for {:?}", stored_key);
                    if degradable_leader_permit.is_some() {
                        log_degradable_leader_admission(DegradableLeaderAdmissionOutcome::Admitted);
                    }
                    log_plan_go(GoReason::PeerTimestampTooNew);
                    go(None, degradable_leader_permit)
                } else {
                    tracing::debug!("Cache value ready for {:?}", stored_key);
                    log_plan_ready();
                    CacheOp::Ready { result: r.clone() }
                }
            },
            Some(CacheEntry::Waiting {
                id,
                started: peer_started,
                receiver,
                ts: peer_ts,
                execution_class: peer_execution_class,
            }) => {
                let entry_id = *id;
                if *peer_ts > ts {
                    let degradable_leader_permit = match miss_behavior {
                        CacheMissBehavior::Publish => None,
                        CacheMissBehavior::InspectDegradable => {
                            return Some(CachePlan::NeedsDegradableAdmission(stored_key));
                        },
                        CacheMissBehavior::PublishDegradable(permit) => Some(permit),
                    };
                    if degradable_leader_permit.is_some() {
                        log_degradable_leader_admission(DegradableLeaderAdmissionOutcome::Admitted);
                    }
                    log_plan_go(GoReason::PeerTimestampTooNew);
                    return Some(CachePlan::Operation(
                        go(None, degradable_leader_permit),
                        stored_key,
                    ));
                }
                if let CacheWaitDecision::Bypass(reason) =
                    execution_class.wait_decision(*peer_execution_class)
                {
                    // The higher-class caller must retain its existing
                    // application and dependency scheduling contract. Queries
                    // are side-effect-free, so this duplicate does not replace
                    // or cancel the waiting leader.
                    log_plan_go(reason);
                    return Some(CachePlan::Operation(go(None, None), stored_key));
                }
                // We don't serialize sampling `now` under the cache lock, and since it can
                // occur on different threads, we're not guaranteed that
                // `peer_started < now`. So, if the peer started *after* us,
                // consider its `peer_elapsed` time to be zero.
                let peer_started = cmp::min(now, *peer_started);
                let peer_elapsed = now - peer_started;
                if peer_elapsed >= *TOTAL_QUERY_TIMEOUT {
                    tracing::debug!(
                        "Peer timed out ({:?}), removing cache entry and retrying",
                        peer_elapsed
                    );
                    inner.remove_waiting(&stored_key, entry_id);
                    log_plan_peer_timeout();
                    return None;
                }
                let get_elapsed = now - start;
                let remaining = *TOTAL_QUERY_TIMEOUT - cmp::max(peer_elapsed, get_elapsed);
                tracing::debug!("Waiting for peer to compute {:?}", stored_key);
                log_plan_wait();
                if execution_class == QueryExecutionClass::Degradable {
                    log_degradable_cache_wait(peer_execution_class.metric_leader_class());
                }
                CacheOp::Wait {
                    waiting_entry_id: *id,
                    receiver: receiver.clone(),
                    remaining,
                }
            },
            None => {
                let degradable_leader_permit = match miss_behavior {
                    CacheMissBehavior::Publish => None,
                    CacheMissBehavior::InspectDegradable => {
                        return Some(CachePlan::NeedsDegradableAdmission(stored_key));
                    },
                    CacheMissBehavior::PublishDegradable(permit) => Some(permit),
                };
                tracing::debug!("No cache value for {:?}, executing UDF...", stored_key);
                let (sender, executor_id) =
                    inner.put_waiting(stored_key.clone(), now, ts, execution_class);
                if degradable_leader_permit.is_some() {
                    log_degradable_leader_admission(DegradableLeaderAdmissionOutcome::Admitted);
                }
                log_plan_go(GoReason::NoCacheResult);
                go(Some((sender, executor_id)), degradable_leader_permit)
            },
        };
        Some(CachePlan::Operation(op, stored_key))
    }

    fn remove_waiting(&self, key: &StoredCacheKey, entry_id: u64) {
        self.inner.lock().remove_waiting(key, entry_id)
    }

    fn remove_ready(&self, key: &StoredCacheKey, original_ts: Timestamp) {
        self.inner.lock().remove_ready(key, original_ts)
    }

    fn put_ready(&self, keys: SmallVec<[StoredCacheKey; 2]>, result: CacheResult) {
        let mut inner = self.inner.lock();
        for (result, key) in iter::repeat_n(result, keys.len()).zip(keys) {
            inner.put_ready(key, result);
        }
    }
}

impl Inner {
    // Remove only a `CacheEntry::Ready` from the cache, predicated on its
    // `executor_id` matching.
    fn remove_waiting(&mut self, key: &StoredCacheKey, entry_id: u64) {
        match self.cache.get(key) {
            Some(CacheEntry::Waiting { id, .. }) if *id == entry_id => {
                let (actual_key, entry) = self.cache.pop_entry(key).unwrap();
                self.size -= actual_key.size() + entry.size();
            },
            _ => (),
        }
        log_cache_size(self.size)
    }

    // Remove only a `CacheEntry::Ready` from the cache, predicated on its
    // `original_ts` matching.
    fn remove_ready(&mut self, key: &StoredCacheKey, original_ts: Timestamp) {
        match self.cache.get(key) {
            Some(CacheEntry::Ready(result)) if result.original_ts == original_ts => {
                let (actual_key, entry) = self.cache.pop_entry(key).unwrap();
                self.size -= actual_key.size() + entry.size();
            },
            _ => (),
        }
        log_cache_size(self.size);
    }

    fn put_waiting(
        &mut self,
        key: StoredCacheKey,
        now: tokio::time::Instant,
        ts: Timestamp,
        execution_class: QueryExecutionClass,
    ) -> (Sender<CacheResult>, u64) {
        let id = self.next_waiting_id;
        self.next_waiting_id += 1;

        let (sender, receiver) = broadcast(1);

        let new_entry = CacheEntry::Waiting {
            id,
            receiver,
            started: now,
            ts,
            execution_class,
        };
        let new_size = key.size() + new_entry.size();
        let old_size = self
            .cache
            .push(key, new_entry)
            .map(|(old_key, old_value)| old_key.size() + old_value.size())
            .unwrap_or(0);

        // N.B.: `self.size - old_size` could be _negative_ if `key.size()` was larger
        // than the size of the preexisting key; therefore add before subtracting
        self.size = self.size + new_size - old_size;

        self.enforce_size_limit();
        (sender, id)
    }

    // Put a `CacheEntry::Ready` into the cache, potentially dropping it if there's
    // already a value with a higher `original_ts`.
    fn put_ready(&mut self, key: StoredCacheKey, result: CacheResult) {
        match self.cache.get_mut(&key) {
            Some(entry @ CacheEntry::Waiting { .. }) => {
                let new_entry = CacheEntry::Ready(result);
                self.size -= entry.size();
                self.size += new_entry.size();
                *entry = new_entry;
            },
            Some(CacheEntry::Ready(existing_result)) => {
                if existing_result.original_ts < result.original_ts
                    || (existing_result.original_ts == result.original_ts
                        && existing_result.token.ts() < result.token.ts())
                {
                    self.size -= existing_result.heap_size();
                    self.size += result.heap_size();
                    *existing_result = result;
                } else {
                    tracing::debug!(
                        "dropping cache result for {key:?} because result timestamp {} <= cached \
                         timestamp {}",
                        result.original_ts,
                        existing_result.original_ts
                    );
                    log_drop_cache_result_too_old();
                }
            },
            None => {
                let new_entry = CacheEntry::Ready(result);
                self.size += key.size() + new_entry.size();
                self.cache.put(key, new_entry);
            },
        }
        self.enforce_size_limit();
    }

    /// Pop records until the cache is under the given size.
    fn enforce_size_limit(&mut self) {
        while self.size > self.size_limit {
            let (popped_key, popped_entry) = self
                .cache
                .pop_lru()
                .expect("Cache is too large without any items?");
            self.size -= popped_key.size() + popped_entry.size();
            if let CacheEntry::Ready(r) = popped_entry {
                let system_time: SystemTime = r.token.ts().into();
                if let Ok(t) = system_time.elapsed() {
                    query_cache_log_eviction(t);
                }
            }
        }
        log_cache_size(self.size)
    }
}

enum CacheMissBehavior {
    Publish,
    InspectDegradable,
    PublishDegradable(DegradableQueryLeaderPermit),
}

enum CachePlan<'a> {
    Operation(CacheOp<'a>, StoredCacheKey),
    NeedsDegradableAdmission(StoredCacheKey),
}

#[derive(strum::Display)]
enum CacheOp<'a> {
    Ready {
        result: CacheResult,
    },
    Wait {
        waiting_entry_id: u64,
        receiver: Receiver<CacheResult>,
        remaining: Duration,
    },
    Go {
        waiting_entry_id: Option<u64>,
        degradable_leader_permit: Option<DegradableQueryLeaderPermit>,
        sender: Sender<CacheResult>,
        path: &'a PublicFunctionPath,
        args: &'a SerializedArgs,
        identity: &'a Identity,
        ts: Timestamp,
        journal: &'a QueryJournal,
        allowed_visibility: AllowedVisibility,
        context: ExecutionContext,
    },
}

impl CacheOp<'_> {
    fn degradable_recheck_outcome(&self) -> DegradableCacheRecheckOutcome {
        match self {
            Self::Ready { .. } => DegradableCacheRecheckOutcome::Ready,
            Self::Wait { .. } => DegradableCacheRecheckOutcome::Wait,
            Self::Go {
                waiting_entry_id: Some(_),
                ..
            } => DegradableCacheRecheckOutcome::Published,
            Self::Go {
                waiting_entry_id: None,
                ..
            } => DegradableCacheRecheckOutcome::DirectExecution,
        }
    }
}

#[cfg(test)]
mod tests {
    use common::{
        components::ExportPath,
        identity::InertIdentity,
        runtime::UnixTimestamp,
        types::FunctionCaller,
        RequestId,
    };
    use value::{
        JsonPackedValue,
        PendingValue,
    };

    use super::*;

    fn requested_key() -> RequestedCacheKey {
        RequestedCacheKey {
            tenant_id: QueryCacheTenantId::allocate(),
            path: PublicFunctionPath::RootExport("test:default".parse::<ExportPath>().unwrap()),
            args: SerializedArgs::from_args(vec![]).unwrap(),
            identity: IdentityCacheKey::Unknown(None),
            journal: QueryJournal::new(),
            allowed_visibility: AllowedVisibility::PublicOnly,
        }
    }

    fn context() -> ExecutionContext {
        ExecutionContext::new(
            RequestContext::new_for_system_request(RequestId::new()),
            &FunctionCaller::Cron,
        )
    }

    fn plan<'a>(
        cache: &QueryCache,
        key: &'a RequestedCacheKey,
        stored_key_hint: Option<&StoredCacheKey>,
        identity: &'a Identity,
        execution_class: QueryExecutionClass,
        miss_behavior: CacheMissBehavior,
    ) -> Option<CachePlan<'a>> {
        plan_at_ts(
            cache,
            key,
            stored_key_hint,
            identity,
            Timestamp::MIN,
            execution_class,
            miss_behavior,
        )
    }

    fn plan_at_ts<'a>(
        cache: &QueryCache,
        key: &'a RequestedCacheKey,
        stored_key_hint: Option<&StoredCacheKey>,
        identity: &'a Identity,
        ts: Timestamp,
        execution_class: QueryExecutionClass,
        miss_behavior: CacheMissBehavior,
    ) -> Option<CachePlan<'a>> {
        let now = tokio::time::Instant::now();
        cache.plan_cache_op(
            key,
            stored_key_hint,
            now,
            now,
            identity,
            ts,
            context(),
            execution_class,
            miss_behavior,
        )
    }

    fn cache_result(key: &RequestedCacheKey) -> CacheResult {
        cache_result_at(key, Timestamp::MIN)
    }

    fn cache_result_at(key: &RequestedCacheKey, original_ts: Timestamp) -> CacheResult {
        let outcome = UdfOutcome {
            path: key.path.clone().debug_into_component_path(),
            arguments: key.args.clone(),
            identity: InertIdentity::Unknown,
            observed_identity: false,
            rng_seed: [0; 32],
            observed_rng: false,
            unix_timestamp: UnixTimestamp::from_nanos(0),
            observed_time: false,
            log_lines: vec![].into(),
            journal: QueryJournal::new(),
            audit_log_lines: vec![].into(),
            result: Ok(JsonPackedValue::pack(PendingValue::Concrete(
                ConvexValue::Null,
            ))),
            syscall_trace: udf::SyscallTrace::new(),
            udf_server_version: None,
            memory_in_mb: 0,
            user_execution_time: Some(Duration::ZERO),
        };
        CacheResult {
            outcome: Arc::new(outcome),
            original_ts,
            token: Token::empty(original_ts),
            visibility_info: None,
        }
    }

    fn put_ready(cache: &QueryCache, key: &RequestedCacheKey) {
        put_ready_at(cache, key, Timestamp::MIN)
    }

    fn put_ready_at(cache: &QueryCache, key: &RequestedCacheKey, original_ts: Timestamp) {
        cache
            .inner
            .lock()
            .put_ready(key.precise_cache_key(), cache_result_at(key, original_ts));
    }

    #[test]
    fn disabled_classification_and_wait_bypass_matrix_are_closed() {
        assert_eq!(
            QueryExecutionClass::new(
                SchedulerDependencyClass::Independent,
                Some(QueryWorkloadClass::Degradable),
                false,
            ),
            QueryExecutionClass::Normal
        );
        assert_eq!(
            QueryExecutionClass::new(
                SchedulerDependencyClass::Independent,
                Some(QueryWorkloadClass::Degradable),
                true,
            ),
            QueryExecutionClass::Degradable
        );
        assert_eq!(
            QueryExecutionClass::new(
                SchedulerDependencyClass::UnblocksAncestor,
                Some(QueryWorkloadClass::Degradable),
                true,
            ),
            QueryExecutionClass::Dependency
        );

        use CacheWaitDecision::{
            Bypass,
            Wait,
        };
        use QueryExecutionClass::{
            Degradable,
            Dependency,
            Normal,
        };
        assert!(matches!(Dependency.wait_decision(Dependency), Wait));
        assert!(matches!(
            Dependency.wait_decision(Normal),
            Bypass(GoReason::DependencyCannotWaitForIndependentPeer)
        ));
        assert!(matches!(
            Dependency.wait_decision(Degradable),
            Bypass(GoReason::DependencyCannotWaitForDegradablePeer)
        ));
        assert!(matches!(Normal.wait_decision(Dependency), Wait));
        assert!(matches!(Normal.wait_decision(Normal), Wait));
        assert!(matches!(
            Normal.wait_decision(Degradable),
            Bypass(GoReason::NormalCannotWaitForDegradablePeer)
        ));
        assert!(matches!(Degradable.wait_decision(Dependency), Wait));
        assert!(matches!(Degradable.wait_decision(Normal), Wait));
        assert!(matches!(Degradable.wait_decision(Degradable), Wait));
    }

    #[test]
    fn ready_and_wait_plans_do_not_acquire_degradable_capacity() {
        let key = requested_key();
        let identity = Identity::Unknown(None);
        let admission = DegradableQueryLeaderAdmission::new(1);

        let ready_cache = QueryCache::new(usize::MAX);
        put_ready(&ready_cache, &key);
        assert!(matches!(
            plan(
                &ready_cache,
                &key,
                None,
                &identity,
                QueryExecutionClass::Degradable,
                CacheMissBehavior::InspectDegradable,
            ),
            Some(CachePlan::Operation(CacheOp::Ready { .. }, _))
        ));
        assert!(admission.has_available_permit());

        let waiting_cache = QueryCache::new(usize::MAX);
        let normal_leader = plan(
            &waiting_cache,
            &key,
            None,
            &identity,
            QueryExecutionClass::Normal,
            CacheMissBehavior::Publish,
        )
        .unwrap();
        assert!(matches!(
            plan(
                &waiting_cache,
                &key,
                None,
                &identity,
                QueryExecutionClass::Degradable,
                CacheMissBehavior::InspectDegradable,
            ),
            Some(CachePlan::Operation(CacheOp::Wait { .. }, _))
        ));
        assert!(admission.has_available_permit());
        let CachePlan::Operation(
            CacheOp::Go {
                waiting_entry_id: Some(waiting_entry_id),
                ..
            },
            stored_key,
        ) = normal_leader
        else {
            panic!("normal miss did not publish a leader")
        };
        waiting_cache.remove_waiting(&stored_key, waiting_entry_id);
    }

    #[test]
    fn degradable_miss_admission_is_immediate_and_recheck_releases_unused_permit() {
        let cache = QueryCache::new(usize::MAX);
        let key = requested_key();
        let identity = Identity::Unknown(None);
        let admission = DegradableQueryLeaderAdmission::new(1);

        let CachePlan::NeedsDegradableAdmission(stored_key) = plan(
            &cache,
            &key,
            None,
            &identity,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::InspectDegradable,
        )
        .unwrap() else {
            panic!("empty degradable cache did not request admission")
        };
        assert!(cache.inner.lock().cache.is_empty());

        let held = admission.try_acquire().unwrap();
        let Err(error) = admission.try_acquire() else {
            panic!("saturated admission unexpectedly returned a permit")
        };
        assert!(error.is_degradable_query_capacity());
        assert!(cache.inner.lock().cache.is_empty());
        drop(held);

        let permit = admission.try_acquire().unwrap();
        let normal_leader = plan(
            &cache,
            &key,
            Some(&stored_key),
            &identity,
            QueryExecutionClass::Normal,
            CacheMissBehavior::Publish,
        )
        .unwrap();
        assert!(matches!(
            plan(
                &cache,
                &key,
                Some(&stored_key),
                &identity,
                QueryExecutionClass::Degradable,
                CacheMissBehavior::PublishDegradable(permit),
            ),
            Some(CachePlan::Operation(CacheOp::Wait { .. }, _))
        ));
        assert!(admission.has_available_permit());

        let CachePlan::Operation(
            CacheOp::Go {
                waiting_entry_id: Some(waiting_entry_id),
                ..
            },
            stored_key,
        ) = normal_leader
        else {
            panic!("normal miss did not publish during recheck race")
        };
        cache.remove_waiting(&stored_key, waiting_entry_id);
    }

    #[tokio::test]
    async fn analysis_reservations_reduce_degradable_admission() {
        let shared_admission = QueryAnalysisAdmission::new(2);
        let degradable_admission =
            DegradableQueryLeaderAdmission::from_shared(shared_admission.clone());
        let analysis = shared_admission.acquire_analysis().await;
        let degradable = degradable_admission.try_acquire().unwrap();

        let Err(error) = degradable_admission.try_acquire() else {
            panic!("analysis did not borrow from degradable capacity")
        };
        assert!(error.is_degradable_query_capacity());

        drop(analysis);
        assert!(degradable_admission.try_acquire().is_ok());
        drop(degradable);
    }

    #[test]
    fn degradable_timestamp_duplicates_require_and_retain_admission() {
        let key = requested_key();
        let identity = Identity::Unknown(None);
        let newer_ts = Timestamp::try_from(1_u64).unwrap();

        let ready_cache = QueryCache::new(usize::MAX);
        put_ready_at(&ready_cache, &key, newer_ts);
        let Some(CachePlan::NeedsDegradableAdmission(ready_key)) = plan_at_ts(
            &ready_cache,
            &key,
            None,
            &identity,
            Timestamp::MIN,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::InspectDegradable,
        ) else {
            panic!("a too-new ready result bypassed degradable admission")
        };
        let ready_admission = DegradableQueryLeaderAdmission::new(1);
        let ready_permit = ready_admission.try_acquire().unwrap();
        let Some(CachePlan::Operation(ready_duplicate, _)) = plan_at_ts(
            &ready_cache,
            &key,
            Some(&ready_key),
            &identity,
            Timestamp::MIN,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::PublishDegradable(ready_permit),
        ) else {
            panic!("an admitted too-new ready result did not execute directly")
        };
        assert!(matches!(
            &ready_duplicate,
            CacheOp::Go {
                waiting_entry_id: None,
                degradable_leader_permit: Some(_),
                ..
            }
        ));
        assert!(!ready_admission.has_available_permit());
        drop(ready_duplicate);
        assert!(ready_admission.has_available_permit());

        let waiting_cache = QueryCache::new(usize::MAX);
        let normal_leader = plan_at_ts(
            &waiting_cache,
            &key,
            None,
            &identity,
            newer_ts,
            QueryExecutionClass::Normal,
            CacheMissBehavior::Publish,
        )
        .unwrap();
        let Some(CachePlan::NeedsDegradableAdmission(waiting_key)) = plan_at_ts(
            &waiting_cache,
            &key,
            None,
            &identity,
            Timestamp::MIN,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::InspectDegradable,
        ) else {
            panic!("a too-new waiting result bypassed degradable admission")
        };
        let waiting_admission = DegradableQueryLeaderAdmission::new(1);
        let held = waiting_admission.try_acquire().unwrap();
        let Err(error) = waiting_admission.try_acquire() else {
            panic!("a timestamp duplicate ignored saturated admission")
        };
        assert!(error.is_degradable_query_capacity());
        drop(held);

        let waiting_permit = waiting_admission.try_acquire().unwrap();
        let Some(CachePlan::Operation(waiting_duplicate, _)) = plan_at_ts(
            &waiting_cache,
            &key,
            Some(&waiting_key),
            &identity,
            Timestamp::MIN,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::PublishDegradable(waiting_permit),
        ) else {
            panic!("an admitted too-new waiting result did not execute directly")
        };
        assert!(matches!(
            &waiting_duplicate,
            CacheOp::Go {
                waiting_entry_id: None,
                degradable_leader_permit: Some(_),
                ..
            }
        ));
        assert!(!waiting_admission.has_available_permit());
        drop(waiting_duplicate);
        assert!(waiting_admission.has_available_permit());

        let CachePlan::Operation(
            CacheOp::Go {
                waiting_entry_id: Some(waiting_entry_id),
                ..
            },
            stored_key,
        ) = normal_leader
        else {
            panic!("normal newer query did not publish a waiting leader")
        };
        waiting_cache.remove_waiting(&stored_key, waiting_entry_id);
    }

    #[test]
    fn published_degradable_leader_owns_permit_until_operation_drop() {
        let cache = QueryCache::new(usize::MAX);
        let key = requested_key();
        let identity = Identity::Unknown(None);
        let admission = DegradableQueryLeaderAdmission::new(1);
        let CachePlan::NeedsDegradableAdmission(stored_key) = plan(
            &cache,
            &key,
            None,
            &identity,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::InspectDegradable,
        )
        .unwrap() else {
            panic!("empty degradable cache did not request admission")
        };
        let permit = admission.try_acquire().unwrap();
        let CachePlan::Operation(op, stored_key) = plan(
            &cache,
            &key,
            Some(&stored_key),
            &identity,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::PublishDegradable(permit),
        )
        .unwrap() else {
            panic!("admitted degradable miss did not publish")
        };
        let CacheOp::Go {
            waiting_entry_id: Some(waiting_entry_id),
            ..
        } = &op
        else {
            panic!("admitted degradable miss was not a cache leader")
        };
        assert!(!admission.has_available_permit());

        let guard = WaitingEntryGuard::new(Some(*waiting_entry_id), &stored_key, cache.clone());
        drop(op);
        assert!(admission.has_available_permit());
        drop(guard);
        assert!(cache.inner.lock().cache.is_empty());
    }

    #[test]
    fn successful_degradable_leader_completion_replaces_waiting_entry() {
        let cache = QueryCache::new(usize::MAX);
        let key = requested_key();
        let identity = Identity::Unknown(None);
        let admission = DegradableQueryLeaderAdmission::new(1);
        let CachePlan::NeedsDegradableAdmission(stored_key) = plan(
            &cache,
            &key,
            None,
            &identity,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::InspectDegradable,
        )
        .unwrap() else {
            panic!("empty degradable cache did not request admission")
        };
        let permit = admission.try_acquire().unwrap();
        let CachePlan::Operation(op, stored_key) = plan(
            &cache,
            &key,
            Some(&stored_key),
            &identity,
            QueryExecutionClass::Degradable,
            CacheMissBehavior::PublishDegradable(permit),
        )
        .unwrap() else {
            panic!("admitted degradable miss did not publish")
        };
        let CacheOp::Go {
            waiting_entry_id: Some(waiting_entry_id),
            ..
        } = &op
        else {
            panic!("admitted degradable miss was not a cache leader")
        };
        let mut guard = WaitingEntryGuard::new(Some(*waiting_entry_id), &stored_key, cache.clone());

        drop(op);
        guard.complete(smallvec![stored_key.clone()], cache_result(&key));
        assert!(admission.has_available_permit());
        assert!(matches!(
            cache.inner.lock().cache.get(&stored_key),
            Some(CacheEntry::Ready(_))
        ));
    }
}
