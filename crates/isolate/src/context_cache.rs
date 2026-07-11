use std::{
    collections::HashMap,
    hash::Hash,
    sync::{
        atomic::{
            AtomicUsize,
            Ordering,
        },
        Arc,
        LazyLock,
    },
};

use ::metrics::IntoLabel;
use common::{
    components::{
        CanonicalizedComponentModulePath,
        ResolvedComponentFunctionPath,
    },
    interval::IntervalSet,
    knobs::ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE,
    memory_pressure::MemoryPressureSignal,
    runtime::Runtime,
    types::TabletIndexName,
};
use database::{
    Transaction,
    TransactionReadSet,
};
use deno_core::v8::{
    self,
    scope,
};
use fastrace::local::LocalSpan;
use parking_lot::Mutex;
use value::{
    sha256::Sha256Digest,
    TableName,
    TableNamespace,
};

use crate::{
    client::Request,
    metrics::{
        create_context_timer,
        log_context_cache_cleared,
        log_context_cache_entry_added,
        log_context_cache_entry_removed,
        log_context_cache_operation,
        ContextCacheOperation,
    },
    module_map::ModuleMap,
};

pub(crate) static MAX_REUSABLE_CONTEXTS_PER_ISOLATE: LazyLock<usize> = LazyLock::new(|| {
    ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE
        .checked_add(1)
        .expect("per-isolate context cache capacity overflow")
});
pub(crate) const CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE: usize = 2;

struct SavedContext {
    context: v8::Global<v8::Context>,
    module_map: ModuleMap,
    read_set: ContextReadSet,
}

pub struct ContextCache {
    fresh_context: Option<v8::Global<v8::Context>>,
    reusable_contexts: TinyLfuCache<ReusableContextKey, SavedContext>,
    resident_permits: Vec<ContextCachePermit>,
    budget: Arc<ContextCacheBudget>,
    cached_contexts: Arc<CachedContexts>,
    memory_pressure: MemoryPressureSignal,
    cgroup_memory_pressure_active: bool,
    low_memory_notification_pending: bool,
}

pub(crate) struct ContextCacheBudget {
    capacity: usize,
    owned_contexts: AtomicUsize,
}

struct ContextCachePermit {
    budget: Arc<ContextCacheBudget>,
}

pub(crate) struct TakenContext {
    context: v8::Global<v8::Context>,
    module_map: ModuleMap,
    read_set: ContextReadSet,
    // Rust drops struct fields in declaration order. Keep the permit last so
    // validation failures destroy every V8 root before returning pool capacity.
    token: ReusableContextToken,
}

pub(crate) struct ReusableContextToken {
    key: ReusableContextKey,
    segment: CacheSegment,
    permit: ContextCachePermit,
}

/// A mirror of the cache keys present in a `ContextCache`.
/// This struct is `Send + Sync` so that it can be used by the isolate
/// scheduler.
pub struct CachedContexts {
    inner: Mutex<CachedContextsInner>,
}

struct CachedContextsInner {
    saved_contexts: Vec<ReusableContextKey>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReusableContextKey {
    kind: ReusableContextKind,
    module_path: CanonicalizedComponentModulePath,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheSegment {
    Window,
    Main,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MainAdmissionMode {
    GrowIfSpace,
    ReplaceResident,
}

struct CacheEntry<K, V> {
    key: K,
    value: V,
    last_access: u64,
}

struct TakenCacheEntry<K, V> {
    key: K,
    value: V,
    segment: CacheSegment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheEvictionReason {
    DuplicateReplacement,
    FrequencyAdmission,
    FrequencyRejection,
    PoolCapacityReplacement,
    MemoryPressure,
}

struct EvictedCacheEntry<K, V> {
    entry: CacheEntry<K, V>,
    reason: CacheEvictionReason,
    was_cached: bool,
}

struct CacheInsertionResult<K, V> {
    candidate_admitted: bool,
    evicted: Option<EvictedCacheEntry<K, V>>,
}

struct TinyLfuCache<K, V> {
    window: Option<CacheEntry<K, V>>,
    main: Vec<CacheEntry<K, V>>,
    main_capacity: usize,
    frequencies: HashMap<K, u16>,
    frequency_aging_interval: usize,
    observations_since_aging: usize,
    access_clock: u64,
}

impl<K: Clone + Eq + Hash, V> TinyLfuCache<K, V> {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_protected_capacity(*ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE)
    }

    fn with_protected_capacity(main_capacity: usize) -> Self {
        assert!(
            main_capacity > 0,
            "protected cache capacity must be positive"
        );
        let frequency_aging_interval = main_capacity
            .checked_add(1)
            .and_then(|capacity| capacity.checked_mul(16))
            .expect("context cache frequency-aging interval overflow");
        // The scheduler-wide budget can be smaller than this structural bound.
        // Grow with actual residents instead of reserving the configured maximum
        // in every isolate worker.
        Self {
            window: None,
            main: Vec::new(),
            main_capacity,
            frequencies: HashMap::new(),
            frequency_aging_interval,
            observations_since_aging: 0,
            access_clock: 0,
        }
    }

    fn len(&self) -> usize {
        usize::from(self.window.is_some()) + self.main.len()
    }

    fn contains_key(&self, key: &K) -> bool {
        self.window.as_ref().is_some_and(|entry| &entry.key == key)
            || self.main.iter().any(|entry| &entry.key == key)
    }

    fn can_insert_without_growth(&self, key: &K) -> bool {
        self.window.is_some() && !self.contains_key(key)
    }

    fn keys(&self) -> impl Iterator<Item = &K> {
        self.window
            .iter()
            .map(|entry| &entry.key)
            .chain(self.main.iter().map(|entry| &entry.key))
    }

    fn observe(&mut self, key: &K) {
        // Count misses as well as hits so a rejected new key can eventually prove
        // that it is hotter than a resident. Aging prevents an old winner from
        // becoming permanent after the request distribution changes.
        let count = self.frequencies.entry(key.clone()).or_default();
        *count = count.saturating_add(1);
        self.observations_since_aging += 1;
        if self.observations_since_aging == self.frequency_aging_interval {
            self.frequencies.retain(|_, count| {
                *count /= 2;
                *count > 0
            });
            self.observations_since_aging = 0;
        }
    }

    fn frequency(&self, key: &K) -> u16 {
        self.frequencies.get(key).copied().unwrap_or_default()
    }

    fn take(&mut self, key: &K) -> Option<TakenCacheEntry<K, V>> {
        self.observe(key);
        if self.window.as_ref().is_some_and(|entry| &entry.key == key) {
            let entry = self
                .window
                .take()
                .expect("matched window entry disappeared");
            return Some(TakenCacheEntry {
                key: entry.key,
                value: entry.value,
                segment: CacheSegment::Window,
            });
        }
        let position = self.main.iter().position(|entry| &entry.key == key)?;
        let entry = self.main.remove(position);
        Some(TakenCacheEntry {
            key: entry.key,
            value: entry.value,
            segment: CacheSegment::Main,
        })
    }

    fn insert_new(&mut self, key: K, value: V) -> CacheInsertionResult<K, V> {
        self.insert(key, value, CacheSegment::Window, false, self.main_capacity)
    }

    fn insert_new_without_growth(&mut self, key: K, value: V) -> CacheInsertionResult<K, V> {
        assert!(
            self.can_insert_without_growth(&key),
            "replacement admission requires a distinct probationary resident"
        );
        let previous_len = self.len();
        self.access_clock = self
            .access_clock
            .checked_add(1)
            .expect("context cache access clock overflow");
        let candidate = CacheEntry {
            key,
            value,
            last_access: self.access_clock,
        };
        let displaced_window = self
            .window
            .replace(candidate)
            .expect("replacement admission lost its probationary resident");
        let evicted = if self.main.is_empty() {
            Some(EvictedCacheEntry {
                entry: displaced_window,
                reason: CacheEvictionReason::PoolCapacityReplacement,
                was_cached: true,
            })
        } else {
            // The pool has no token with which to grow a partially populated
            // main segment. Let the displaced probationary context compete with
            // an existing protected resident instead; this preserves both the
            // global count and the normal strict-frequency admission rule.
            let (_, evicted) = self.admit_to_main(
                displaced_window,
                false,
                true,
                MainAdmissionMode::ReplaceResident,
                self.main_capacity,
            );
            evicted
        };
        assert_eq!(
            self.len(),
            previous_len,
            "replacement admission grew the context cache"
        );
        CacheInsertionResult {
            candidate_admitted: true,
            evicted,
        }
    }

    fn reinsert(&mut self, entry: TakenCacheEntry<K, V>) -> CacheInsertionResult<K, V> {
        self.insert(
            entry.key,
            entry.value,
            entry.segment,
            true,
            self.main_capacity,
        )
    }

    fn reinsert_protected_with_capacity(
        &mut self,
        entry: TakenCacheEntry<K, V>,
        main_capacity: usize,
    ) -> CacheInsertionResult<K, V> {
        assert_eq!(entry.segment, CacheSegment::Main);
        assert!(
            main_capacity > 0,
            "protected cache capacity must be positive"
        );
        self.insert(
            entry.key,
            entry.value,
            CacheSegment::Main,
            true,
            main_capacity,
        )
    }

    fn insert(
        &mut self,
        key: K,
        value: V,
        segment: CacheSegment,
        returning_resident: bool,
        main_capacity: usize,
    ) -> CacheInsertionResult<K, V> {
        self.access_clock = self
            .access_clock
            .checked_add(1)
            .expect("context cache access clock overflow");
        let candidate = CacheEntry {
            key,
            value,
            last_access: self.access_clock,
        };
        // Two contexts with the same key are one logical resident. Replace a
        // nested duplicate in place so the returning context reuses its slot and
        // permit instead of competing with unrelated protected entries.
        if self
            .window
            .as_ref()
            .is_some_and(|entry| entry.key == candidate.key)
        {
            let duplicate = std::mem::replace(
                self.window
                    .as_mut()
                    .expect("matched window entry disappeared"),
                candidate,
            );
            return CacheInsertionResult {
                candidate_admitted: true,
                evicted: Some(EvictedCacheEntry {
                    entry: duplicate,
                    reason: CacheEvictionReason::DuplicateReplacement,
                    was_cached: true,
                }),
            };
        }
        if let Some(position) = self
            .main
            .iter()
            .position(|entry| entry.key == candidate.key)
        {
            let duplicate = std::mem::replace(&mut self.main[position], candidate);
            return CacheInsertionResult {
                candidate_admitted: true,
                evicted: Some(EvictedCacheEntry {
                    entry: duplicate,
                    reason: CacheEvictionReason::DuplicateReplacement,
                    was_cached: true,
                }),
            };
        }

        match segment {
            CacheSegment::Main => {
                let (candidate_admitted, evicted) = self.admit_to_main(
                    candidate,
                    returning_resident,
                    false,
                    MainAdmissionMode::GrowIfSpace,
                    main_capacity,
                );
                return CacheInsertionResult {
                    candidate_admitted,
                    evicted,
                };
            },
            CacheSegment::Window => {},
        }

        let evicted = self.window.replace(candidate).and_then(|displaced_window| {
            let (_, evicted) = self.admit_to_main(
                displaced_window,
                false,
                true,
                MainAdmissionMode::GrowIfSpace,
                main_capacity,
            );
            evicted
        });
        CacheInsertionResult {
            candidate_admitted: true,
            evicted,
        }
    }

    fn admit_to_main(
        &mut self,
        candidate: CacheEntry<K, V>,
        returning_resident: bool,
        candidate_was_cached: bool,
        mode: MainAdmissionMode,
        main_capacity: usize,
    ) -> (bool, Option<EvictedCacheEntry<K, V>>) {
        match mode {
            MainAdmissionMode::GrowIfSpace => {
                if self.main.len() < main_capacity {
                    self.main.push(candidate);
                    return (true, None);
                }
            },
            MainAdmissionMode::ReplaceResident => {},
        }
        let victim_position = self.weakest_main_position();
        let victim = &self.main[victim_position];
        let candidate_frequency = self.frequency(&candidate.key);
        let victim_frequency = self.frequency(&victim.key);
        // A probationary candidate must be strictly hotter; the protected
        // resident wins ties. A returning protected context has just completed a
        // successful access, so recency resolves a protected/protected tie that
        // can arise when nested execution filled its former slot.
        let candidate_wins = candidate_frequency > victim_frequency
            || (returning_resident
                && candidate_frequency == victim_frequency
                && candidate.last_access > victim.last_access);
        if candidate_wins {
            let victim = std::mem::replace(&mut self.main[victim_position], candidate);
            (
                true,
                Some(EvictedCacheEntry {
                    entry: victim,
                    reason: CacheEvictionReason::FrequencyAdmission,
                    was_cached: true,
                }),
            )
        } else {
            (
                false,
                Some(EvictedCacheEntry {
                    entry: candidate,
                    reason: CacheEvictionReason::FrequencyRejection,
                    was_cached: candidate_was_cached,
                }),
            )
        }
    }

    fn weakest_main_position(&self) -> usize {
        self.main
            .iter()
            .enumerate()
            .min_by_key(|(_, entry)| (self.frequency(&entry.key), entry.last_access))
            .map(|(position, _)| position)
            .expect("weakest main entry requires a nonempty main cache")
    }

    fn evict_for_memory_pressure(&mut self) -> Option<EvictedCacheEntry<K, V>> {
        let entry = self.window.take().or_else(|| {
            (!self.main.is_empty()).then(|| self.main.remove(self.weakest_main_position()))
        })?;
        Some(EvictedCacheEntry {
            entry,
            reason: CacheEvictionReason::MemoryPressure,
            was_cached: true,
        })
    }

    fn shrink_for_cgroup_memory_pressure(
        &mut self,
        protected_capacity: usize,
    ) -> Vec<EvictedCacheEntry<K, V>> {
        assert!(
            protected_capacity > 0,
            "cgroup-pressure protected capacity must be positive"
        );
        let mut evicted = Vec::new();
        // A partially populated cache can be within the numeric pressure cap
        // while still containing a probationary resident. Remove the window
        // unconditionally, then enforce the protected-only capacity.
        while self.window.is_some() || self.main.len() > protected_capacity {
            evicted.push(
                self.evict_for_memory_pressure()
                    .expect("cgroup-pressure cache shrink lost its victim"),
            );
        }
        evicted
    }

    fn clear(&mut self) -> Vec<CacheEntry<K, V>> {
        let evicted = self
            .window
            .take()
            .into_iter()
            .chain(self.main.drain(..))
            .collect();
        self.frequencies.clear();
        self.observations_since_aging = 0;
        self.access_clock = 0;
        evicted
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ReusableContextKind {
    DatabaseUdf,
    Action,
    HttpAction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextCacheClearReason {
    AdmissionReplacement,
    PoolCapacityReplacement,
    DuplicateReplacement,
    MemoryPressure,
    CgroupMemoryPressure,
    AppDefinitionEvaluation,
    CacheDrop,
}

pub struct ContextReadSet {
    pub read_set: TransactionReadSet,
    pub range_hashes: Vec<(
        TableNamespace,
        TabletIndexName,
        TableName,
        IntervalSet,
        Sha256Digest,
    )>,
}

pub(crate) fn context_cache_key(
    function_path: &ResolvedComponentFunctionPath,
) -> CanonicalizedComponentModulePath {
    CanonicalizedComponentModulePath {
        component: function_path.component,
        module_path: function_path.udf_path.module().clone(),
    }
}

impl ContextCacheBudget {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "context cache budget must be positive");
        Self {
            capacity,
            owned_contexts: AtomicUsize::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<ContextCachePermit> {
        self.owned_contexts
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |owned| {
                (owned < self.capacity).then_some(owned + 1)
            })
            .ok()?;
        Some(ContextCachePermit {
            budget: self.clone(),
        })
    }

    pub(crate) fn owned_contexts(&self) -> usize {
        self.owned_contexts.load(Ordering::Acquire)
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for ContextCachePermit {
    fn drop(&mut self) {
        self.budget
            .owned_contexts
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |owned| {
                owned.checked_sub(1)
            })
            .expect("context cache budget underflow");
    }
}

impl TakenContext {
    pub(crate) fn read_set(&self) -> &ContextReadSet {
        &self.read_set
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        v8::Global<v8::Context>,
        ModuleMap,
        ContextReadSet,
        ReusableContextToken,
    ) {
        (self.context, self.module_map, self.read_set, self.token)
    }
}

impl ContextCache {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_budget(
            Arc::new(ContextCacheBudget::new(*MAX_REUSABLE_CONTEXTS_PER_ISOLATE)),
            MemoryPressureSignal::default(),
        )
    }

    pub(crate) fn with_budget(
        budget: Arc<ContextCacheBudget>,
        memory_pressure: MemoryPressureSignal,
    ) -> Self {
        let cgroup_memory_pressure_active = memory_pressure.is_active();
        Self {
            fresh_context: None,
            reusable_contexts: TinyLfuCache::with_protected_capacity(
                *ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE,
            ),
            resident_permits: Vec::new(),
            budget,
            cached_contexts: Arc::new(CachedContexts {
                inner: Mutex::new(CachedContextsInner {
                    saved_contexts: Vec::new(),
                }),
            }),
            memory_pressure,
            cgroup_memory_pressure_active,
            low_memory_notification_pending: false,
        }
    }

    pub(crate) fn prepare(&mut self, isolate: &mut v8::Isolate) {
        if !self.cgroup_memory_pressure_active && self.fresh_context.is_none() {
            scope!(let scope, isolate);
            let context = make_context(scope);
            self.fresh_context = Some(v8::Global::new(scope, context));
        }
    }

    pub(crate) fn clear_fresh_context(&mut self) {
        self.fresh_context = None;
    }

    pub(crate) fn clear(&mut self, reason: ContextCacheClearReason) {
        let removed_fresh_root = self.fresh_context.take().is_some();
        let evicted = {
            let mut cached_contexts = self.cached_contexts.inner.lock();
            self.assert_mirror(&cached_contexts);
            assert_eq!(
                self.resident_permits.len(),
                self.reusable_contexts.len(),
                "resident context permits drifted before cache clear"
            );
            let evicted = self.reusable_contexts.clear();
            cached_contexts.saved_contexts.clear();
            evicted
        };
        if self.cgroup_memory_pressure_active && (removed_fresh_root || !evicted.is_empty()) {
            self.low_memory_notification_pending = true;
        }
        for entry in evicted {
            log_context_cache_cleared(entry.key.kind, reason);
            log_context_cache_entry_removed(entry.key.kind);
        }
        // Keep the pool ownership until all removed V8 roots have been dropped on
        // this isolate thread.
        self.resident_permits.clear();
    }

    pub(crate) fn get_or_create_fresh_context<'s>(
        &mut self,
        scope: &v8::PinScope<'s, '_, ()>,
    ) -> v8::Local<'s, v8::Context> {
        if let Some(context) = self.fresh_context.take() {
            v8::Local::new(scope, context)
        } else {
            make_context(scope)
        }
    }

    pub(crate) fn save_context(
        &mut self,
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
        token: Option<ReusableContextToken>,
    ) {
        self.save_reusable_context(
            ReusableContextKey {
                kind: ReusableContextKind::DatabaseUdf,
                module_path,
            },
            SavedContext {
                context,
                module_map,
                read_set,
            },
            token,
        );
    }

    pub(crate) fn save_http_action_context(
        &mut self,
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
        token: Option<ReusableContextToken>,
    ) {
        self.save_reusable_context(
            ReusableContextKey {
                kind: ReusableContextKind::HttpAction,
                module_path,
            },
            SavedContext {
                context,
                module_map,
                read_set,
            },
            token,
        );
    }

    pub(crate) fn save_action_context(
        &mut self,
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
        token: Option<ReusableContextToken>,
    ) {
        self.save_reusable_context(
            ReusableContextKey {
                kind: ReusableContextKind::Action,
                module_path,
            },
            SavedContext {
                context,
                module_map,
                read_set,
            },
            token,
        );
    }

    fn save_reusable_context(
        &mut self,
        key: ReusableContextKey,
        saved_context: SavedContext,
        token: Option<ReusableContextToken>,
    ) {
        // The worker cannot poll its watch receiver while it is serving a
        // request. Keep controller transitions ordered before this guard or
        // after the complete synchronous publication, including rejected-root
        // destruction on early returns.
        let memory_pressure = self.memory_pressure.clone();
        let pressure_state = memory_pressure.lock_state();
        self.save_reusable_context_under_pressure_guard(
            key,
            saved_context,
            token,
            pressure_state.is_active(),
        );
        drop(pressure_state);
    }

    fn save_reusable_context_under_pressure_guard(
        &mut self,
        key: ReusableContextKey,
        saved_context: SavedContext,
        token: Option<ReusableContextToken>,
        pressure_active: bool,
    ) {
        if self.cgroup_memory_pressure_active != pressure_active {
            let removed_root = self.set_cgroup_memory_pressure(pressure_active);
            // V8 collection requires the isolate and remains a worker-loop action.
            // Preserve the collection request until the removed roots leave this
            // save path.
            self.low_memory_notification_pending |= removed_root;
        }
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted before cache save"
        );
        let context_kind = key.kind;
        let mut candidate_permit = None;
        let insertion = {
            let mut cached_contexts = self.cached_contexts.inner.lock();
            self.assert_mirror(&cached_contexts);
            let insertion = if self.cgroup_memory_pressure_active {
                let Some(token) = token else {
                    self.low_memory_notification_pending = true;
                    log_context_cache_operation(
                        context_kind,
                        ContextCacheOperation::RejectMemoryPressure,
                    );
                    return;
                };
                assert_eq!(token.key, key, "reused context returned under another key");
                if token.segment == CacheSegment::Window {
                    self.low_memory_notification_pending = true;
                    log_context_cache_operation(
                        context_kind,
                        ContextCacheOperation::RejectMemoryPressure,
                    );
                    // Destroy the rejected V8 roots before returning their
                    // shared ownership to another worker.
                    drop(saved_context);
                    drop(token);
                    return;
                }
                candidate_permit = Some(token.permit);
                self.reusable_contexts.reinsert_protected_with_capacity(
                    TakenCacheEntry {
                        key,
                        value: saved_context,
                        segment: token.segment,
                    },
                    CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE,
                )
            } else if let Some(token) = token {
                assert_eq!(token.key, key, "reused context returned under another key");
                candidate_permit = Some(token.permit);
                self.reusable_contexts.reinsert(TakenCacheEntry {
                    key,
                    value: saved_context,
                    segment: token.segment,
                })
            } else if self.reusable_contexts.contains_key(&key) {
                // The existing duplicate already owns a fungible local permit.
                self.reusable_contexts.insert_new(key, saved_context)
            } else if let Some(permit) = self.budget.try_acquire() {
                candidate_permit = Some(permit);
                self.reusable_contexts.insert_new(key, saved_context)
            } else if self.reusable_contexts.can_insert_without_growth(&key) {
                // At the shared resident cap, preserve adaptation by exchanging one
                // local probationary/weak resident without transiently owning a new
                // pool token.
                self.reusable_contexts
                    .insert_new_without_growth(key, saved_context)
            } else {
                log_context_cache_operation(
                    context_kind,
                    ContextCacheOperation::RejectPoolCapacity,
                );
                return;
            };
            let capacity = if self.cgroup_memory_pressure_active {
                CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE
            } else {
                *MAX_REUSABLE_CONTEXTS_PER_ISOLATE
            };
            assert!(
                self.reusable_contexts.len() <= capacity,
                "context cache exceeded its active capacity"
            );
            self.replace_mirror(&mut cached_contexts);
            insertion
        };

        if self.cgroup_memory_pressure_active && insertion.evicted.is_some() {
            // The next worker-loop iteration requests collection after the
            // rejected or replaced Global has been dropped by this save path.
            self.low_memory_notification_pending = true;
        }

        let candidate_admitted = insertion.candidate_admitted;
        if candidate_admitted && let Some(permit) = candidate_permit.take() {
            self.resident_permits.push(permit);
        }
        if let Some(evicted) = insertion.evicted
            && evicted.was_cached
        {
            let clear_reason = match evicted.reason {
                CacheEvictionReason::DuplicateReplacement => {
                    ContextCacheClearReason::DuplicateReplacement
                },
                CacheEvictionReason::FrequencyAdmission
                | CacheEvictionReason::FrequencyRejection => {
                    ContextCacheClearReason::AdmissionReplacement
                },
                CacheEvictionReason::PoolCapacityReplacement => {
                    ContextCacheClearReason::PoolCapacityReplacement
                },
                CacheEvictionReason::MemoryPressure => {
                    unreachable!("save path cannot report memory-pressure eviction")
                },
            };
            self.log_resident_eviction(evicted, clear_reason);
        }
        // Drop removed V8 roots before releasing excess fungible permits, so
        // another worker cannot reuse pool ownership while an evicted Global is
        // still alive on this isolate thread.
        drop(candidate_permit);
        // Permits are deliberately fungible inside one isolate cache. Reconcile
        // after every result because duplicate replacement and frequency admission
        // can change which residents remain independently of which permit arrived
        // with the candidate. This avoids binding permits to module keys.
        while self.resident_permits.len() > self.reusable_contexts.len() {
            self.resident_permits.pop();
        }
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted"
        );

        if candidate_admitted {
            log_context_cache_entry_added(context_kind);
            log_context_cache_operation(context_kind, ContextCacheOperation::Save);
        } else if self.cgroup_memory_pressure_active {
            log_context_cache_operation(context_kind, ContextCacheOperation::RejectMemoryPressure);
        } else {
            log_context_cache_operation(context_kind, ContextCacheOperation::RejectFrequency);
        }
    }

    pub(crate) fn take_reused_context(
        &mut self,
        module_path: &CanonicalizedComponentModulePath,
    ) -> Option<TakenContext> {
        self.take_reusable_context(ReusableContextKey {
            kind: ReusableContextKind::DatabaseUdf,
            module_path: module_path.clone(),
        })
    }

    pub(crate) fn take_http_action_context(
        &mut self,
        module_path: &CanonicalizedComponentModulePath,
    ) -> Option<TakenContext> {
        self.take_reusable_context(ReusableContextKey {
            kind: ReusableContextKind::HttpAction,
            module_path: module_path.clone(),
        })
    }

    pub(crate) fn take_action_context(
        &mut self,
        module_path: &CanonicalizedComponentModulePath,
    ) -> Option<TakenContext> {
        self.take_reusable_context(ReusableContextKey {
            kind: ReusableContextKind::Action,
            module_path: module_path.clone(),
        })
    }

    fn take_reusable_context(&mut self, key: ReusableContextKey) -> Option<TakenContext> {
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted before cache take"
        );
        let taken = {
            let mut cached_contexts = self.cached_contexts.inner.lock();
            self.assert_mirror(&cached_contexts);
            let taken = self.reusable_contexts.take(&key);
            if taken.is_some() {
                self.replace_mirror(&mut cached_contexts);
            }
            taken
        }?;
        let permit = self
            .resident_permits
            .pop()
            .expect("cached context missing resident permit");
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted after cache take"
        );
        let context_kind = taken.key.kind;
        log_context_cache_entry_removed(context_kind);
        log_context_cache_operation(context_kind, ContextCacheOperation::Take);
        Some(TakenContext {
            context: taken.value.context,
            module_map: taken.value.module_map,
            read_set: taken.value.read_set,
            token: ReusableContextToken {
                key: taken.key,
                segment: taken.segment,
                permit,
            },
        })
    }

    pub(crate) fn evict_for_memory_pressure(&mut self) -> bool {
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted before pressure eviction"
        );
        let evicted = {
            let mut cached_contexts = self.cached_contexts.inner.lock();
            self.assert_mirror(&cached_contexts);
            let evicted = self.reusable_contexts.evict_for_memory_pressure();
            self.replace_mirror(&mut cached_contexts);
            evicted
        };
        let Some(evicted) = evicted else {
            return false;
        };
        self.log_resident_eviction(evicted, ContextCacheClearReason::MemoryPressure);
        self.resident_permits
            .pop()
            .expect("evicted context missing resident permit");
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted after pressure eviction"
        );
        true
    }

    /// Applies the process-wide cgroup pressure state on the isolate thread.
    /// Returns whether at least one V8 root was removed since the previous
    /// application or by this transition and collection should be requested
    /// after those roots have been dropped.
    pub(crate) fn set_cgroup_memory_pressure(&mut self, active: bool) -> bool {
        let mut removed_root = std::mem::take(&mut self.low_memory_notification_pending);
        if self.cgroup_memory_pressure_active == active {
            return removed_root;
        }
        self.cgroup_memory_pressure_active = active;
        if !active {
            return removed_root;
        }

        removed_root |= self.fresh_context.take().is_some();
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted before cgroup-pressure eviction"
        );
        let evicted = {
            let mut cached_contexts = self.cached_contexts.inner.lock();
            self.assert_mirror(&cached_contexts);
            let evicted = self
                .reusable_contexts
                .shrink_for_cgroup_memory_pressure(CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE);
            self.replace_mirror(&mut cached_contexts);
            evicted
        };
        removed_root |= !evicted.is_empty();
        for evicted in evicted {
            self.log_resident_eviction(evicted, ContextCacheClearReason::CgroupMemoryPressure);
            self.resident_permits
                .pop()
                .expect("cgroup-pressure victim missing resident permit");
        }
        assert_eq!(
            self.resident_permits.len(),
            self.reusable_contexts.len(),
            "resident context permits drifted after cgroup-pressure eviction"
        );
        removed_root
    }

    fn assert_mirror(&self, cached_contexts: &CachedContextsInner) {
        assert!(
            self.reusable_contexts
                .keys()
                .eq(cached_contexts.saved_contexts.iter()),
            "context cache mirror keys drifted"
        );
    }

    fn replace_mirror(&self, cached_contexts: &mut CachedContextsInner) {
        // The scheduler may read this Arc after the isolate is recreated. Update
        // every key while holding its one lock, after local insertion and before
        // releasing removed V8 roots, so it never advertises a destroyed context.
        cached_contexts.saved_contexts.clear();
        cached_contexts
            .saved_contexts
            .extend(self.reusable_contexts.keys().cloned());
    }

    fn log_resident_eviction(
        &self,
        evicted: EvictedCacheEntry<ReusableContextKey, SavedContext>,
        reason: ContextCacheClearReason,
    ) {
        assert!(
            evicted.was_cached,
            "only residents can be logged as evicted"
        );
        log_context_cache_cleared(evicted.entry.key.kind, reason);
        log_context_cache_entry_removed(evicted.entry.key.kind);
    }

    // Isolate-thread callers use `with_release_permit`: hashing that blocks
    // releases the active-JavaScript permit and records the blocked interval as
    // initialization system work, while a synchronously ready cache path keeps
    // the permit and does not create a pause.
    #[fastrace::trace]
    pub(crate) async fn validate_and_apply_context_read_set<RT: Runtime>(
        tx: &mut Transaction<RT>,
        read_set: &ContextReadSet,
    ) -> anyhow::Result<bool> {
        let mut reusable = scopeguard::guard(false, |reusable| {
            LocalSpan::add_property(|| ("reuse_success", reusable.as_label()));
        });
        for (namespace, tablet_index_name, table_name, intervals, hash) in &read_set.range_hashes {
            let tablet = *tablet_index_name.table();
            if !tx.table_mapping().tablet_id_exists(tablet) {
                return Ok(false);
            }
            let (new_namespace, _, new_table_name) =
                tx.table_mapping().get_table_metadata(tablet)?;
            anyhow::ensure!(namespace == new_namespace, "{tablet} changed namespace?");
            anyhow::ensure!(table_name == new_table_name, "{tablet} changed name?");
            let Some(new_hash) = tx
                .hash_index_interval_no_deps(tablet_index_name, table_name, intervals)
                .await?
            else {
                return Ok(false);
            };
            if new_hash != *hash {
                return Ok(false);
            }
        }
        *reusable = true;
        // All hashes match, so merge the saved reads into the request transaction
        // before running code compiled from the cached context.
        tx.apply_reads(read_set.read_set.clone());
        Ok(true)
    }

    // The caller owns timeout accounting because HTTP capture already runs inside
    // the consolidated initialization pause while database capture does not.
    #[fastrace::trace]
    pub(crate) async fn capture_context_read_set<RT: Runtime>(
        read_set: TransactionReadSet,
        tx: &mut Transaction<RT>,
    ) -> anyhow::Result<Option<ContextReadSet>> {
        anyhow::ensure!(
            read_set.read_set().iter_search().count() == 0,
            "searches can't be done during init"
        );
        let mut range_hashes = vec![];
        for (tablet_index_name, reads) in read_set.read_set().iter_indexed() {
            let &(namespace, _table_number, ref table_name) = tx
                .table_mapping()
                .get_table_metadata(*tablet_index_name.table())?;
            anyhow::ensure!(
                table_name.is_system(),
                "context init read non-system table {table_name}?"
            );
            let table_name = table_name.clone();
            let Some(hash) = tx
                .hash_index_interval_no_deps(tablet_index_name, &table_name, &reads.intervals)
                .await?
            else {
                return Ok(None);
            };
            range_hashes.push((
                namespace,
                tablet_index_name.clone(),
                table_name,
                reads.intervals.clone(),
                hash,
            ));
        }
        Ok(Some(ContextReadSet {
            read_set,
            range_hashes,
        }))
    }

    pub fn cached_contexts(&self) -> &Arc<CachedContexts> {
        &self.cached_contexts
    }
}

impl CachedContexts {
    pub fn can_serve_request<RT: Runtime>(&self, request: &Request<RT>) -> bool {
        let this = self.inner.lock();
        let Some(kind) = request.reusable_context_kind() else {
            return false;
        };
        request.module().is_some_and(|module_path| {
            this.saved_contexts
                .contains(&ReusableContextKey { kind, module_path })
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        contexts: impl IntoIterator<Item = (ReusableContextKind, CanonicalizedComponentModulePath)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(CachedContextsInner {
                saved_contexts: contexts
                    .into_iter()
                    .map(|(kind, module_path)| ReusableContextKey { kind, module_path })
                    .collect(),
            }),
        })
    }
}

impl Drop for ContextCache {
    fn drop(&mut self) {
        // The scheduler holds a clone of `CachedContexts` while a worker is idle, so
        // the mirror can outlive this cache during isolate recreation. Clear it here
        // to avoid advertising contexts that were destroyed with the old isolate.
        self.clear(ContextCacheClearReason::CacheDrop);
    }
}

fn make_context<'s>(scope: &v8::PinScope<'s, '_, ()>) -> v8::Local<'s, v8::Context> {
    let _create_context_timer = create_context_timer();
    v8::Context::new(scope, v8::ContextOptions::default())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        CacheEvictionReason,
        CacheSegment,
        ContextCacheBudget,
        TinyLfuCache,
        CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE,
        MAX_REUSABLE_CONTEXTS_PER_ISOLATE,
    };

    fn miss_and_save(
        cache: &mut TinyLfuCache<u16, u16>,
        key: u16,
    ) -> super::CacheInsertionResult<u16, u16> {
        assert!(cache.take(&key).is_none());
        cache.insert_new(key, key)
    }

    fn hit_and_return(cache: &mut TinyLfuCache<u16, u16>, key: u16) {
        let entry = cache.take(&key).expect("expected cache hit");
        assert!(cache.reinsert(entry).candidate_admitted);
    }

    fn fill_cache() -> TinyLfuCache<u16, u16> {
        let mut cache = TinyLfuCache::new();
        for key in 1..=*MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16 {
            assert!(miss_and_save(&mut cache, key).candidate_admitted);
        }
        cache
    }

    #[test]
    fn configured_protected_capacity_changes_the_cache_shape() {
        let protected_capacity = 7;
        let reusable_capacity = protected_capacity + 1;
        let mut cache = TinyLfuCache::with_protected_capacity(protected_capacity);
        for key in 1..=reusable_capacity as u16 {
            assert!(miss_and_save(&mut cache, key).candidate_admitted);
        }

        assert_eq!(cache.main.len(), protected_capacity);
        assert!(cache.window.is_some());
        assert_eq!(cache.len(), reusable_capacity);
        assert_eq!(cache.frequency_aging_interval, reusable_capacity * 16);

        assert!(miss_and_save(&mut cache, reusable_capacity as u16 + 1).candidate_admitted);
        assert_eq!(cache.len(), reusable_capacity);
    }

    #[test]
    fn one_hit_outlier_does_not_evict_protected_resident() {
        let mut cache = fill_cache();
        let window_key = *MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16;
        let new_key = window_key + 1;
        let insertion = miss_and_save(&mut cache, new_key);

        assert!(insertion.candidate_admitted);
        assert_eq!(cache.len(), *MAX_REUSABLE_CONTEXTS_PER_ISOLATE);
        assert!(cache.contains_key(&new_key));
        assert!(!cache.contains_key(&window_key));
        assert!((1..window_key).all(|key| cache.contains_key(&key)));
        assert!(insertion.evicted.iter().any(|entry| {
            entry.entry.key == window_key
                && entry.reason == CacheEvictionReason::FrequencyRejection
                && entry.was_cached
        }));
    }

    #[test]
    fn hot_probationary_entry_replaces_weakest_protected_resident() {
        let mut cache = fill_cache();
        let window_key = *MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16;
        let new_key = window_key + 1;
        hit_and_return(&mut cache, window_key);
        hit_and_return(&mut cache, window_key);

        let insertion = miss_and_save(&mut cache, new_key);
        assert!(cache.contains_key(&window_key));
        assert!(!cache.contains_key(&1));
        assert!(insertion.evicted.iter().any(|entry| {
            entry.entry.key == 1 && entry.reason == CacheEvictionReason::FrequencyAdmission
        }));
    }

    #[test]
    fn frequency_aging_forgets_old_popularity() {
        let mut cache = TinyLfuCache::<u16, u16>::new();
        for _ in 0..8 {
            cache.observe(&1);
        }
        for offset in 0..(cache.frequency_aging_interval as u16 - 8) {
            cache.observe(&(1000 + offset));
        }
        assert_eq!(cache.frequency(&1), 4);

        for round in 0..4u16 {
            for offset in 0..cache.frequency_aging_interval as u16 {
                cache.observe(&(2000 + round * cache.frequency_aging_interval as u16 + offset));
            }
        }
        assert_eq!(cache.frequency(&1), 0);
    }

    #[test]
    fn returning_protected_entry_stays_bounded_after_nested_save() {
        let mut cache = fill_cache();
        let outer = cache.take(&1).expect("expected protected outer hit");
        assert_eq!(outer.segment, CacheSegment::Main);

        let new_key = *MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16 + 1;
        assert!(miss_and_save(&mut cache, new_key).candidate_admitted);
        let insertion = cache.reinsert(outer);

        assert!(insertion.candidate_admitted);
        assert_eq!(cache.len(), *MAX_REUSABLE_CONTEXTS_PER_ISOLATE);
        assert!(cache.contains_key(&1));
        assert!(!cache.contains_key(&2));
    }

    #[test]
    fn duplicate_nested_key_is_replaced_without_duplicate_residents() {
        let mut cache = fill_cache();
        let outer = cache.take(&1).expect("expected protected outer hit");
        assert!(miss_and_save(&mut cache, 1).candidate_admitted);

        let insertion = cache.reinsert(outer);
        assert!(insertion.candidate_admitted);
        assert!(cache.contains_key(&1));
        assert_eq!(cache.keys().filter(|key| **key == 1).count(), 1);
        assert!(cache.len() <= *MAX_REUSABLE_CONTEXTS_PER_ISOLATE);
        assert!(insertion.evicted.iter().any(|entry| {
            entry.entry.key == 1
                && entry.reason == CacheEvictionReason::DuplicateReplacement
                && entry.was_cached
        }));
    }

    #[test]
    fn returning_protected_context_replaces_nested_duplicate_in_place() {
        let mut cache = fill_cache();
        for key in 2..=*MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16 {
            for _ in 0..4 {
                hit_and_return(&mut cache, key);
            }
        }
        let outer = cache.take(&1).expect("expected protected outer hit");
        assert!(miss_and_save(&mut cache, 1).candidate_admitted);

        let insertion = cache.reinsert(outer);

        assert!(insertion.candidate_admitted);
        assert!(cache.contains_key(&1));
        assert_eq!(cache.len(), *MAX_REUSABLE_CONTEXTS_PER_ISOLATE);
        assert!(insertion.evicted.iter().any(|entry| {
            entry.entry.key == 1
                && entry.reason == CacheEvictionReason::DuplicateReplacement
                && entry.was_cached
        }));
        assert!(!insertion
            .evicted
            .iter()
            .any(|entry| entry.reason == CacheEvictionReason::FrequencyRejection));
        assert_eq!(
            cache.take(&1).expect("replacement disappeared").segment,
            CacheSegment::Window
        );
    }

    #[test]
    fn recency_breaks_returning_protected_frequency_tie() {
        let mut cache = fill_cache();
        let outer = cache.take(&1).expect("expected protected outer hit");
        let new_key = *MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16 + 1;
        assert!(miss_and_save(&mut cache, new_key).candidate_admitted);
        for key in 2..=*MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16 {
            cache.observe(&key);
        }

        let insertion = cache.reinsert(outer);

        assert!(insertion.candidate_admitted);
        assert!(cache.contains_key(&1));
        assert!(!cache.contains_key(&2));
        assert_eq!(cache.len(), *MAX_REUSABLE_CONTEXTS_PER_ISOLATE);
    }

    #[test]
    fn memory_pressure_evicts_window_then_weakest_protected_entry() {
        let mut cache = fill_cache();
        hit_and_return(&mut cache, 1);
        let window_key = *MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16;

        let first = cache
            .evict_for_memory_pressure()
            .expect("expected probationary eviction");
        let second = cache
            .evict_for_memory_pressure()
            .expect("expected protected eviction");

        assert_eq!(first.entry.key, window_key);
        assert_eq!(second.entry.key, 2);
        assert_eq!(first.reason, CacheEvictionReason::MemoryPressure);
        assert_eq!(second.reason, CacheEvictionReason::MemoryPressure);
    }

    #[test]
    fn cgroup_pressure_keeps_only_the_two_strongest_protected_entries() {
        let mut cache = fill_cache();
        for _ in 0..3 {
            hit_and_return(&mut cache, 1);
        }
        for _ in 0..2 {
            hit_and_return(&mut cache, 2);
        }

        let evicted = cache
            .shrink_for_cgroup_memory_pressure(CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE)
            .into_iter()
            .map(|evicted| evicted.entry.key)
            .collect::<Vec<_>>();

        assert_eq!(
            evicted.first().copied(),
            Some(*MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16)
        );
        assert_eq!(
            evicted.len(),
            *MAX_REUSABLE_CONTEXTS_PER_ISOLATE - CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE
        );
        assert!(evicted[1..].iter().all(|key| *key >= 3));
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
    }

    #[test]
    fn cgroup_pressure_removes_a_partial_cache_probationary_resident() {
        let mut cache = TinyLfuCache::new();
        assert!(miss_and_save(&mut cache, 1).candidate_admitted);
        assert!(miss_and_save(&mut cache, 2).candidate_admitted);
        assert_eq!(cache.len(), CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE);

        let evicted =
            cache.shrink_for_cgroup_memory_pressure(CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE);

        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].entry.key, 2);
        assert!(cache.window.is_none());
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&1));
    }

    #[test]
    fn hot_protected_context_returning_during_pressure_competes_for_two_slots() {
        let mut cache = fill_cache();
        for _ in 0..3 {
            hit_and_return(&mut cache, 1);
        }
        for _ in 0..2 {
            hit_and_return(&mut cache, 2);
        }
        let returning = cache
            .take(&1)
            .expect("expected protected context in flight");
        drop(
            cache.shrink_for_cgroup_memory_pressure(CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE),
        );

        let insertion = cache.reinsert_protected_with_capacity(
            returning,
            CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE,
        );

        assert!(insertion.candidate_admitted);
        assert_eq!(cache.len(), CGROUP_PRESSURE_REUSABLE_CONTEXTS_PER_ISOLATE);
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
    }

    #[test]
    fn no_growth_admission_preserves_the_hard_entry_bound() {
        let mut cache = TinyLfuCache::new();
        assert!(miss_and_save(&mut cache, 1).candidate_admitted);

        let insertion = cache.insert_new_without_growth(2, 2);
        assert!(insertion.candidate_admitted);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&2));
        assert!(!cache.contains_key(&1));
        assert!(insertion.evicted.iter().any(|entry| {
            entry.entry.key == 1 && entry.reason == CacheEvictionReason::PoolCapacityReplacement
        }));
    }

    #[test]
    fn no_growth_admission_does_not_fill_an_empty_protected_slot() {
        let mut cache = TinyLfuCache::new();
        for key in 1..=3 {
            assert!(miss_and_save(&mut cache, key).candidate_admitted);
        }

        let insertion = cache.insert_new_without_growth(4, 4);

        assert!(insertion.candidate_admitted);
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&4));
        assert!((1..=2).all(|key| cache.contains_key(&key)));
        assert!(!cache.contains_key(&3));
        assert!(insertion.evicted.iter().any(|entry| {
            entry.entry.key == 3
                && entry.reason == CacheEvictionReason::FrequencyRejection
                && entry.was_cached
        }));
    }

    #[test]
    fn pool_full_miss_cannot_evict_main_while_window_is_in_flight() {
        let mut cache = fill_cache();
        let window_key = *MAX_REUSABLE_CONTEXTS_PER_ISOLATE as u16;
        let new_key = window_key + 1;
        let window = cache.take(&window_key).expect("expected probationary hit");

        assert!(!cache.can_insert_without_growth(&new_key));
        assert!((1..window_key).all(|key| cache.contains_key(&key)));

        assert!(cache.reinsert(window).candidate_admitted);
        assert_eq!(cache.len(), *MAX_REUSABLE_CONTEXTS_PER_ISOLATE);
    }

    #[test]
    fn shared_budget_token_is_retained_until_drop() {
        let budget = Arc::new(ContextCacheBudget::new(2));
        let first = budget.try_acquire().expect("first permit rejected");
        let second = budget.try_acquire().expect("second permit rejected");
        assert!(budget.try_acquire().is_none());
        assert_eq!(budget.owned_contexts(), 2);

        drop(first);
        let replacement = budget.try_acquire().expect("released permit not reusable");
        assert_eq!(budget.owned_contexts(), 2);
        drop((second, replacement));
        assert_eq!(budget.owned_contexts(), 0);
    }
}
