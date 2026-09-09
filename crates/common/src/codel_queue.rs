use std::{
    cmp::{
        max,
        Ordering as CmpOrdering,
    },
    collections::VecDeque,
    future::poll_fn,
    pin::Pin,
    sync::Arc,
    task::{
        Context,
        Poll,
    },
    time::Duration,
};

use event_listener::Event;
use futures::{
    future::FusedFuture,
    Future,
    Stream,
};
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::{
    knobs::{
        CODEL_QUEUE_CONGESTED_EXPIRATION_MILLIS,
        CODEL_QUEUE_IDLE_EXPIRATION_MILLIS,
    },
    metrics::{
        log_codel_queue_overloaded,
        log_codel_queue_size,
        log_codel_queue_time_since_empty,
    },
    runtime::Runtime,
};

#[derive(thiserror::Error, Debug)]
#[error("Queue full")]
pub struct QueueFull;

/// Exact reason an asynchronous CoDel queue send was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoDelQueueSendError {
    /// The applicable base or base-plus-reserve capacity is occupied.
    Full,
    /// No receiver remains to consume the item.
    Closed,
}

/// Instead of simply dropping items from the queue,
/// we return expired items so the caller can dispose of them.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("Item expired in queue")]
pub struct ExpiredInQueue;

/// Queue for buffering requests while avoiding consistently large latency.
/// Following the algorithm described at https://queue.acm.org/detail.cfm?id=2839461
///
/// There's an alternate C++
/// implementation at https://github.com/facebook/folly/blob/main/folly/executors/Codel.cpp
/// which was not used in the making of this implementation.
pub struct CoDelQueue<RT: Runtime, T> {
    rt: RT,
    /// (item, expiration)
    buffer: VecDeque<(T, Instant)>,
    capacity: usize,
    capacity_with_reserve: usize,
    last_time_empty: Instant,
    idle_expiration: Duration,
    congested_expiration: Duration,
}

impl<RT: Runtime, T> CoDelQueue<RT, T> {
    pub fn new_with_defaults(rt: RT, capacity: usize) -> Self {
        Self::new(
            rt,
            capacity,
            *CODEL_QUEUE_IDLE_EXPIRATION_MILLIS,
            *CODEL_QUEUE_CONGESTED_EXPIRATION_MILLIS,
        )
    }

    pub fn new(
        rt: RT,
        capacity: usize,
        idle_expiration: Duration,
        congested_expiration: Duration,
    ) -> Self {
        let last_time_empty = rt.monotonic_now();
        Self {
            rt,
            buffer: VecDeque::new(),
            capacity,
            capacity_with_reserve: capacity,
            last_time_empty,
            idle_expiration,
            congested_expiration,
        }
    }

    pub fn new_with_reserved_capacity(rt: RT, capacity: usize, reserved_capacity: usize) -> Self {
        let mut queue = Self::new_with_defaults(rt, capacity);
        queue.capacity_with_reserve = capacity
            .checked_add(reserved_capacity)
            .expect("CoDel queue capacity overflow");
        queue
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn _is_idle(&self, now: Instant) -> bool {
        (self.last_time_empty + self.idle_expiration) > now
    }

    fn update_last_time_empty(&mut self, now: Instant) {
        if self.is_empty() {
            self.last_time_empty = now;
        }
        self.log_metrics(now);
    }

    fn log_metrics(&self, now: Instant) {
        log_codel_queue_size(self.len());
        log_codel_queue_overloaded(!self._is_idle(now));
        log_codel_queue_time_since_empty(now - self.last_time_empty)
    }

    pub fn push(&mut self, item: T) -> Result<(), (QueueFull, T)> {
        self.push_with_limit(item, self.capacity)
    }

    /// Pushes an item using capacity that ordinary callers cannot consume.
    /// Returns whether this item occupied the reserved portion of the queue.
    pub fn push_with_reserved_capacity(&mut self, item: T) -> Result<bool, (QueueFull, T)> {
        let used_reserved_capacity = self.len() >= self.capacity;
        self.push_with_limit(item, self.capacity_with_reserve)?;
        Ok(used_reserved_capacity)
    }

    fn push_with_limit(&mut self, item: T, capacity: usize) -> Result<(), (QueueFull, T)> {
        self.try_push_with_limit(item, capacity)
            .map_err(|item| (QueueFull, item))
    }

    fn try_push_with_limit(&mut self, item: T, capacity: usize) -> Result<(), T> {
        if self.len() >= capacity {
            return Err(item);
        }
        let now = self.rt.monotonic_now();
        self.update_last_time_empty(now);
        // the time at which we would transition from idle to congested;
        // this may be in the past
        // `self.last_time_empty` can't change during the lifetime of an item,
        // so this is fine to calculate now.
        // N.B.: `now + self.idle_expiration >= congested_time` always holds, so
        // we will always be in the congested regime by the time any item
        // expires
        let congested_time = self.last_time_empty + self.idle_expiration;
        let expiration = max(congested_time, now + self.congested_expiration);
        self.buffer.push_back((item, expiration));
        Ok(())
    }

    fn pop_front(&mut self, now: Instant) -> Option<(T, Instant)> {
        let result = self.buffer.pop_front();
        // If the queue is newly empty, update last_empty_time=now().
        // This is redundant since it will remain empty and that will only
        // matter if we check is_idle, which will also update last_empty_time.
        // But it doesn't hurt to keep it updated.
        self.update_last_time_empty(now);
        result
    }

    fn pop_back(&mut self, now: Instant) -> Option<(T, Instant)> {
        let result = self.buffer.pop_back();
        self.update_last_time_empty(now);
        result
    }

    fn pop_expired(&mut self) -> Option<(T, ExpiredInQueue)> {
        let now = self.rt.monotonic_now();
        if let Some((item, _)) = self
            .buffer
            .pop_front_if(|(_, expiration)| *expiration <= now)
        {
            self.update_last_time_empty(now);
            Some((item, ExpiredInQueue))
        } else {
            None
        }
    }

    pub fn pop_with_expiration(&mut self) -> Option<(T, Result<Instant, ExpiredInQueue>)> {
        let now = self.rt.monotonic_now();
        self.update_last_time_empty(now);
        if let Some((_, oldest_expiry_time)) = self.buffer.front()
            && *oldest_expiry_time <= now
        {
            // Drain expired item.
            self.pop_front(now)
                .map(|(item, _)| (item, Err(ExpiredInQueue)))
        } else {
            if self._is_idle(now) {
                // FIFO
                self.pop_front(now)
            } else {
                // LIFO
                self.pop_back(now)
            }
            .map(|(item, expiration)| (item, Ok(expiration)))
        }
    }

    pub fn pop_selecting<K: Ord>(
        &mut self,
        mut select_key: impl FnMut(&T) -> Option<K>,
    ) -> Option<(T, Result<Instant, ExpiredInQueue>)> {
        let now = self.rt.monotonic_now();
        self.update_last_time_empty(now);
        if let Some((_, oldest_expiry_time)) = self.buffer.front()
            && *oldest_expiry_time <= now
        {
            return self
                .pop_front(now)
                .map(|(item, _)| (item, Err(ExpiredInQueue)));
        }

        let is_idle = self._is_idle(now);
        let mut selected: Option<(usize, K)> = None;
        for (idx, (item, _)) in self.buffer.iter().enumerate() {
            let Some(key) = select_key(item) else {
                continue;
            };
            let should_replace = match &selected {
                None => true,
                Some((selected_idx, selected_key)) => match key.cmp(selected_key) {
                    CmpOrdering::Less => true,
                    CmpOrdering::Equal if is_idle => idx < *selected_idx,
                    CmpOrdering::Equal => idx > *selected_idx,
                    CmpOrdering::Greater => false,
                },
            };
            if should_replace {
                selected = Some((idx, key));
            }
        }
        let (idx, _) = selected?;
        let (item, expiration) = self
            .buffer
            .remove(idx)
            .expect("selected queue entry must still exist");
        self.update_last_time_empty(now);
        Some((item, Ok(expiration)))
    }

    pub fn pop(&mut self) -> Option<(T, Option<ExpiredInQueue>)> {
        self.pop_with_expiration()
            .map(|(item, expiration)| (item, expiration.err()))
    }

    pub fn into_sender_and_receiver(self) -> (CoDelQueueSender<RT, T>, CoDelQueueReceiver<RT, T>) {
        let inner = Arc::new(Mutex::new(Inner {
            queue: self,
            event: Event::new(),
            expired_event: Event::new(),
            senders: 1,
            receivers: 1,
        }));
        (
            CoDelQueueSender {
                inner: inner.clone(),
            },
            CoDelQueueReceiver {
                inner,
                listener: None,
                expiration_wait: None,
            },
        )
    }
}
#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
            Weak,
        },
        task::{
            Context,
            Poll,
        },
        time::{
            Duration,
            SystemTime,
        },
    };

    use futures::{
        future::FusedFuture,
        FutureExt as _,
        StreamExt as _,
    };
    use parking_lot::Mutex;
    use rand::SeedableRng;
    use rand_chacha::ChaCha12Rng;

    use super::{
        CoDelQueue,
        CoDelQueueSendError,
    };
    use crate::{
        knobs::CODEL_QUEUE_IDLE_EXPIRATION_MILLIS,
        pause::PauseClient,
        runtime::{
            Runtime,
            SpawnHandle,
        },
    };

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Clone)]
    struct QueueTestRuntime {
        now: Arc<Mutex<tokio::time::Instant>>,
        timer_drop_probe: Arc<Mutex<Option<Arc<dyn Fn() + Send + Sync>>>>,
    }

    impl QueueTestRuntime {
        fn new() -> Self {
            Self {
                now: Arc::new(Mutex::new(tokio::time::Instant::now())),
                timer_drop_probe: Arc::new(Mutex::new(None)),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock();
            *now += duration;
        }

        fn set_timer_drop_probe(&self, probe: impl Fn() + Send + Sync + 'static) {
            *self.timer_drop_probe.lock() = Some(Arc::new(probe));
        }
    }

    struct QueueTestWait {
        inner: Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
        drop_probe: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl Future for QueueTestWait {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.inner.as_mut().poll(cx)
        }
    }

    impl FusedFuture for QueueTestWait {
        fn is_terminated(&self) -> bool {
            self.inner.is_terminated()
        }
    }

    impl Drop for QueueTestWait {
        fn drop(&mut self) {
            if let Some(probe) = self.drop_probe.take() {
                probe();
            }
        }
    }

    impl Runtime for QueueTestRuntime {
        fn wait(
            &self,
            duration: Duration,
        ) -> Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>> {
            let now = self.now.clone();
            let inner = Box::pin(
                async move {
                    tokio::time::sleep(duration).await;
                    let mut now = now.lock();
                    *now += duration;
                }
                .fuse(),
            );
            let drop_probe = self.timer_drop_probe.lock().clone();
            Box::pin(QueueTestWait { inner, drop_probe })
        }

        fn spawn(
            &self,
            _name: &'static str,
            _f: impl Future<Output = ()> + Send + 'static,
        ) -> Box<dyn SpawnHandle> {
            panic!("QueueTestRuntime::spawn is not used by these tests")
        }

        fn spawn_thread<Fut: Future<Output = ()>, F: FnOnce() -> Fut + Send + 'static>(
            &self,
            _name: &str,
            _f: F,
        ) -> Box<dyn SpawnHandle> {
            panic!("QueueTestRuntime::spawn_thread is not used by these tests")
        }

        fn system_time(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }

        fn monotonic_now(&self) -> tokio::time::Instant {
            *self.now.lock()
        }

        fn rng(&self) -> Box<dyn rand::RngCore> {
            Box::new(ChaCha12Rng::seed_from_u64(0))
        }

        fn pause_client(&self) -> PauseClient {
            PauseClient::new()
        }
    }

    struct LockingDropProbe {
        inner: Weak<Mutex<super::Inner<QueueTestRuntime, LockingDropProbe>>>,
        unlocked_drops: Arc<AtomicUsize>,
    }

    impl Drop for LockingDropProbe {
        fn drop(&mut self) {
            let inner = self.inner.upgrade().expect("queue must still exist");
            if inner.try_lock().is_some() {
                self.unlocked_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn pop_selecting_keeps_ineligible_items_queued() {
        let rt = QueueTestRuntime::new();
        let mut queue = CoDelQueue::new_with_defaults(rt, 4);
        queue.push("parent").unwrap();

        assert!(queue.pop_selecting(|_| None::<u8>).is_none());

        let (item, expired) = queue.pop_selecting(|_| Some(0)).unwrap();
        assert_eq!(item, "parent");
        assert!(expired.is_ok());
    }

    #[test]
    fn dropping_last_receiver_closes_and_drains_queue() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new_with_defaults(rt, 1);
        let (sender, receiver) = queue.into_sender_and_receiver();
        let drops = Arc::new(AtomicUsize::new(0));
        sender.try_send(DropCounter(drops.clone())).unwrap();

        drop(receiver);
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        assert_eq!(
            sender.try_send_detailed(DropCounter(drops.clone())),
            Err(CoDelQueueSendError::Closed)
        );
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn sender_rejections_drop_items_outside_queue_lock() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new_with_reserved_capacity(rt, 1, 1);
        let (sender, receiver) = queue.into_sender_and_receiver();
        let inner = Arc::downgrade(&sender.inner);
        let unlocked_drops = Arc::new(AtomicUsize::new(0));

        sender
            .try_send(LockingDropProbe {
                inner: inner.clone(),
                unlocked_drops: unlocked_drops.clone(),
            })
            .unwrap();
        assert!(sender
            .try_send(LockingDropProbe {
                inner: inner.clone(),
                unlocked_drops: unlocked_drops.clone(),
            })
            .is_err());
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 1);

        assert!(sender
            .try_send_with_reserved_capacity(LockingDropProbe {
                inner: inner.clone(),
                unlocked_drops: unlocked_drops.clone(),
            })
            .unwrap());
        assert!(sender
            .try_send_with_reserved_capacity(LockingDropProbe {
                inner: inner.clone(),
                unlocked_drops: unlocked_drops.clone(),
            })
            .is_err());
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 2);

        drop(receiver);
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 4);

        assert!(sender
            .try_send(LockingDropProbe {
                inner: inner.clone(),
                unlocked_drops: unlocked_drops.clone(),
            })
            .is_err());
        assert!(sender
            .try_send_with_reserved_capacity(LockingDropProbe {
                inner,
                unlocked_drops: unlocked_drops.clone(),
            })
            .is_err());
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn recoverable_sender_retains_rejected_items_until_caller_drops_them() {
        let queue = CoDelQueue::new_with_reserved_capacity(QueueTestRuntime::new(), 1, 1);
        let (sender, receiver) = queue.into_sender_and_receiver();
        let inner = Arc::downgrade(&sender.inner);
        let unlocked_drops = Arc::new(AtomicUsize::new(0));
        let probe = || LockingDropProbe {
            inner: inner.clone(),
            unlocked_drops: unlocked_drops.clone(),
        };

        assert!(sender.try_send_or_recover(probe()).is_ok());
        // Recovery uses ordinary admission, even when dependency reserve is free.
        let Err((super::QueueFull, full_item)) = sender.try_send_or_recover(probe()) else {
            panic!("Ordinary recovery API consumed dependency reserve");
        };
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 0);
        drop(receiver);
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 1);
        drop(full_item);
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 2);

        let Err((super::QueueFull, closed_item)) = sender.try_send_or_recover(probe()) else {
            panic!("Recovery API admitted work after receiver closure");
        };
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 2);
        drop(closed_item);
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn detailed_sender_errors_distinguish_full_from_closed() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new_with_reserved_capacity(rt, 1, 1);
        let (sender, receiver) = queue.into_sender_and_receiver();

        sender.try_send_detailed("ordinary").unwrap();
        assert_eq!(
            sender.try_send_detailed("ordinary full"),
            Err(CoDelQueueSendError::Full)
        );
        assert!(sender
            .try_send_with_reserved_capacity_detailed("reserved")
            .unwrap());
        assert_eq!(
            sender.try_send_with_reserved_capacity_detailed("reserved full"),
            Err(CoDelQueueSendError::Full)
        );

        drop(receiver);
        assert_eq!(
            sender.try_send_detailed("ordinary closed"),
            Err(CoDelQueueSendError::Closed)
        );
        assert_eq!(
            sender.try_send_with_reserved_capacity_detailed("reserved closed"),
            Err(CoDelQueueSendError::Closed)
        );
    }

    #[test]
    fn reserved_capacity_is_unavailable_to_ordinary_senders() {
        let rt = QueueTestRuntime::new();
        let mut queue = CoDelQueue::new_with_reserved_capacity(rt, 2, 1);
        queue.push("normal_a").unwrap();
        queue.push("normal_b").unwrap();

        assert!(queue.push("normal_c").is_err());
        assert!(queue.push_with_reserved_capacity("dependency").unwrap());
        assert!(queue
            .push_with_reserved_capacity("dependency_overflow")
            .is_err());

        assert_eq!(queue.pop().unwrap().0, "normal_a");
        assert_eq!(queue.pop().unwrap().0, "normal_b");
        assert!(!queue
            .push_with_reserved_capacity("dependency_in_base_capacity")
            .unwrap());
    }

    #[test]
    fn pop_selecting_uses_priority_then_fifo_while_idle() {
        let rt = QueueTestRuntime::new();
        let mut queue = CoDelQueue::new_with_defaults(rt, 4);
        queue.push("parent").unwrap();
        queue.push("dependency_a").unwrap();
        queue.push("dependency_b").unwrap();
        queue.push("normal").unwrap();

        let (item, expired) = queue
            .pop_selecting(|item| match *item {
                "dependency_a" | "dependency_b" => Some(0),
                "normal" => Some(1),
                "parent" => Some(2),
                _ => None,
            })
            .unwrap();
        assert_eq!(item, "dependency_a");
        assert!(expired.is_ok());
    }

    #[test]
    fn pop_selecting_drains_expired_head_before_selector() {
        let rt = QueueTestRuntime::new();
        let mut queue = CoDelQueue::new_with_defaults(rt.clone(), 4);
        queue.push("expired_parent").unwrap();
        rt.advance(*CODEL_QUEUE_IDLE_EXPIRATION_MILLIS + Duration::from_millis(1));
        queue.push("dependency").unwrap();

        let (item, expired) = queue
            .pop_selecting(|item| if *item == "dependency" { Some(0) } else { None })
            .unwrap();
        assert_eq!(item, "expired_parent");
        assert!(expired.is_err());
    }

    #[test]
    fn pop_drains_expired_entries_before_newer_lifo_work() {
        let rt = QueueTestRuntime::new();
        let mut queue = CoDelQueue::new_with_defaults(rt.clone(), 4);
        queue.push("expired_congested").unwrap();
        rt.advance(*CODEL_QUEUE_IDLE_EXPIRATION_MILLIS + Duration::from_millis(1));
        queue.push("fresh_congested").unwrap();

        let (item, expired) = queue.pop().unwrap();
        assert_eq!(item, "expired_congested");
        assert!(expired.is_some());
    }

    #[test]
    fn pop_selecting_uses_lifo_within_priority_while_congested() {
        let rt = QueueTestRuntime::new();
        let mut queue = CoDelQueue::new_with_defaults(rt.clone(), 4);
        queue.push("expired_sentinel").unwrap();
        rt.advance(*CODEL_QUEUE_IDLE_EXPIRATION_MILLIS + Duration::from_millis(1));
        queue.push("first_dependency").unwrap();
        queue.push("second_dependency").unwrap();

        let (item, expired) = queue.pop_selecting(|_| Some(0)).unwrap();
        assert_eq!(item, "expired_sentinel");
        assert!(expired.is_err());

        let (item, expired) = queue.pop_selecting(|_| Some(0)).unwrap();
        assert_eq!(item, "second_dependency");
        assert!(expired.is_ok());
    }

    #[tokio::test]
    async fn expired_receiver_wakes_when_queued_head_expires() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new(rt, 1, Duration::from_millis(1), Duration::from_millis(1));
        let (sender, receiver) = queue.into_sender_and_receiver();
        let mut expired_receiver = receiver.expired_receiver();
        sender.try_send("ineligible").unwrap();

        let (item, _) = tokio::time::timeout(Duration::from_secs(1), expired_receiver.next())
            .await
            .expect("expiration receiver did not wake at the CoDel deadline")
            .expect("sender is still open");

        assert_eq!(item, "ineligible");

        sender.try_send("all_workers_busy").unwrap();
        let (item, _) = tokio::time::timeout(Duration::from_secs(1), expired_receiver.next())
            .await
            .expect("expiration receiver did not wake at the CoDel deadline")
            .expect("sender is still open");
        assert_eq!(item, "all_workers_busy");
    }

    #[tokio::test]
    async fn selecting_receiver_wakes_when_ineligible_head_expires() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new(rt, 1, Duration::from_millis(1), Duration::from_millis(1));
        let (sender, mut receiver) = queue.into_sender_and_receiver();
        sender.try_send("ineligible").unwrap();

        let (item, expiration) = tokio::time::timeout(
            Duration::from_secs(1),
            receiver.recv_next_selecting(|_| None::<u8>),
        )
        .await
        .expect("selecting receiver did not wake at the CoDel deadline")
        .expect("sender is still open");

        assert_eq!(item, "ineligible");
        assert!(expiration.is_err());
    }

    #[tokio::test]
    async fn selecting_receiver_keeps_ineligible_item_after_last_sender_closes() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new(rt, 1, Duration::from_millis(1), Duration::from_millis(1));
        let (sender, mut receiver) = queue.into_sender_and_receiver();
        sender.try_send("ineligible").unwrap();
        drop(sender);

        let (item, expiration) = tokio::time::timeout(
            Duration::from_secs(1),
            receiver.recv_next_selecting(|_| None::<u8>),
        )
        .await
        .expect("closed queue did not retain its item until expiration")
        .expect("queued item was lost when the last sender closed");

        assert_eq!(item, "ineligible");
        assert!(expiration.is_err());
        assert!(receiver.recv_next_selecting(|_| Some(0)).await.is_none());
    }

    #[tokio::test]
    async fn expired_receiver_drains_item_after_last_sender_closes() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new(rt, 1, Duration::from_millis(1), Duration::from_millis(1));
        let (sender, receiver) = queue.into_sender_and_receiver();
        let mut expired_receiver = receiver.expired_receiver();
        sender.try_send("ineligible").unwrap();
        drop(sender);

        let (item, _) = tokio::time::timeout(Duration::from_secs(1), expired_receiver.next())
            .await
            .expect("closed expiration receiver did not wait for the queued deadline")
            .expect("queued item was lost when the last sender closed");
        assert_eq!(item, "ineligible");
        assert!(expired_receiver.next().await.is_none());
    }

    #[tokio::test]
    async fn expired_receiver_closes_when_last_main_receiver_is_dropped() {
        let rt = QueueTestRuntime::new();
        let queue: CoDelQueue<_, &'static str> =
            CoDelQueue::new(rt, 1, Duration::from_secs(60), Duration::from_secs(60));
        let (sender, receiver) = queue.into_sender_and_receiver();
        let mut expired_receiver = receiver.expired_receiver();
        drop(receiver);

        assert_eq!(
            sender.try_send_detailed("closed"),
            Err(CoDelQueueSendError::Closed)
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), expired_receiver.next())
                .await
                .expect("expiration receiver did not observe main receiver closure")
                .is_none()
        );
    }

    #[tokio::test]
    async fn expired_receiver_replaces_timer_outside_queue_lock() {
        let rt = QueueTestRuntime::new();
        let queue = CoDelQueue::new(
            rt.clone(),
            1,
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        let (sender, mut receiver) = queue.into_sender_and_receiver();
        let mut expired_receiver = receiver.expired_receiver();
        sender.try_send("first").unwrap();

        let inner = Arc::downgrade(&sender.inner);
        let unlocked_drops = Arc::new(AtomicUsize::new(0));
        let unlocked_drops_for_probe = unlocked_drops.clone();
        rt.set_timer_drop_probe(move || {
            let inner = inner.upgrade().expect("queue must still exist");
            if inner.try_lock().is_some() {
                unlocked_drops_for_probe.fetch_add(1, Ordering::Relaxed);
            }
        });

        let mut pending = Box::pin(expired_receiver.next());
        assert!(futures::poll!(&mut pending).is_pending());

        let (item, expiration) = receiver
            .recv_with_expiration()
            .await
            .expect("sender is still open");
        assert_eq!(item, "first");
        assert!(expiration.is_ok());
        rt.advance(Duration::from_millis(1));
        sender.try_send("replacement").unwrap();

        assert!(futures::poll!(&mut pending).is_pending());
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 1);
    }
}

/// Wrapper around CoDelQueue that makes it async.
pub fn new_codel_queue_async<RT: Runtime, T>(
    rt: RT,
    capacity: usize,
) -> (CoDelQueueSender<RT, T>, CoDelQueueReceiver<RT, T>) {
    CoDelQueue::new_with_defaults(rt, capacity).into_sender_and_receiver()
}

pub fn new_codel_queue_async_with_reserved_capacity<RT: Runtime, T>(
    rt: RT,
    capacity: usize,
    reserved_capacity: usize,
) -> (CoDelQueueSender<RT, T>, CoDelQueueReceiver<RT, T>) {
    CoDelQueue::new_with_reserved_capacity(rt, capacity, reserved_capacity)
        .into_sender_and_receiver()
}

struct Inner<RT: Runtime, T> {
    queue: CoDelQueue<RT, T>,
    event: Event,
    expired_event: Event,
    senders: usize,
    receivers: usize,
}

enum SendCapacity {
    Base,
    WithReserve,
}

pub struct CoDelQueueReceiver<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
    listener: Option<event_listener::EventListener>,
    expiration_wait: Option<(
        Instant,
        Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
    )>,
}

impl<RT: Runtime, T> Clone for CoDelQueueReceiver<RT, T> {
    fn clone(&self) -> Self {
        self.inner.lock().receivers += 1;
        Self {
            inner: self.inner.clone(),
            listener: None,
            expiration_wait: None,
        }
    }
}

impl<RT: Runtime, T> Drop for CoDelQueueReceiver<RT, T> {
    fn drop(&mut self) {
        let queued = {
            let mut inner = self.inner.lock();
            inner.receivers = inner
                .receivers
                .checked_sub(1)
                .expect("CoDel receiver count underflow");
            if inner.receivers == 0 {
                // A sender cannot produce a response after the last consumer
                // has gone away. Drain retained items and reject later sends.
                let queued = std::mem::take(&mut inner.queue.buffer);
                let now = inner.queue.rt.monotonic_now();
                inner.queue.update_last_time_empty(now);
                inner.event.notify(usize::MAX);
                inner.expired_event.notify(usize::MAX);
                Some(queued)
            } else {
                None
            }
        };
        // Queued items may own arbitrary Drop implementations.
        drop(queued);
    }
}

pub struct CoDelQueueExpiredReceiver<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
    listener: Option<event_listener::EventListener>,
    next_expiry_timer: Option<(
        Instant,
        Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
    )>,
}

pub struct CoDelQueueSender<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
}

impl<RT: Runtime, T> Clone for CoDelQueueSender<RT, T> {
    fn clone(&self) -> Self {
        self.inner.lock().senders += 1;
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<RT: Runtime, T> Drop for CoDelQueueSender<RT, T> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock();
        inner.senders -= 1;
        if inner.senders == 0 {
            // Queue is closed. Wake receivers to drain retained work or close.
            inner.event.notify(usize::MAX);
            inner.expired_event.notify(usize::MAX);
        }
    }
}

impl<RT: Runtime, T> CoDelQueueSender<RT, T> {
    pub fn try_send(&self, item: T) -> Result<(), QueueFull> {
        self.try_send_detailed(item).map_err(|_| QueueFull)
    }

    pub fn try_send_or_recover(&self, item: T) -> Result<(), (QueueFull, T)> {
        self.try_send_with_limit(item, SendCapacity::Base)
            .map(|_| ())
            .map_err(|(_, item)| (QueueFull, item))
    }

    pub fn try_send_detailed(&self, item: T) -> Result<(), CoDelQueueSendError> {
        self.try_send_with_limit(item, SendCapacity::Base)
            .map(|_| ())
            .map_err(|(error, _)| error)
    }

    pub fn try_send_with_reserved_capacity(&self, item: T) -> Result<bool, QueueFull> {
        self.try_send_with_reserved_capacity_detailed(item)
            .map_err(|_| QueueFull)
    }

    pub fn try_send_with_reserved_capacity_detailed(
        &self,
        item: T,
    ) -> Result<bool, CoDelQueueSendError> {
        self.try_send_with_limit(item, SendCapacity::WithReserve)
            .map_err(|(error, _)| error)
    }

    fn try_send_with_limit(
        &self,
        item: T,
        send_capacity: SendCapacity,
    ) -> Result<bool, (CoDelQueueSendError, T)> {
        let mut inner = self.inner.lock();
        if inner.receivers == 0 {
            return Err((CoDelQueueSendError::Closed, item));
        }
        let used_reserved_capacity = inner.queue.len() >= inner.queue.capacity;
        let capacity = match send_capacity {
            SendCapacity::Base => inner.queue.capacity,
            SendCapacity::WithReserve => inner.queue.capacity_with_reserve,
        };
        if let Err(item) = inner.queue.try_push_with_limit(item, capacity) {
            // Return ownership so callers recover or drop the item after the lock releases.
            return Err((CoDelQueueSendError::Full, item));
        }
        inner.event.notify_additional(1);
        // All `CoDelQueueExpiredReceiver`s need to be woken since they don't consume
        // the queue item
        inner.expired_event.notify(usize::MAX);
        Ok(used_reserved_capacity)
    }
}

impl<RT: Runtime, T> CoDelQueueReceiver<RT, T> {
    pub fn poll_next_with_expiration(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(T, Result<Instant, ExpiredInQueue>)>> {
        let mut inner = self.inner.lock();
        // If there is an item in the queue, pop it.
        // If the queue is closed, return None.
        if let Some(result) = inner.queue.pop_with_expiration() {
            return Poll::Ready(Some(result));
        } else if inner.senders == 0 {
            return Poll::Ready(None);
        }

        loop {
            match Pin::new(self.listener.get_or_insert_with(|| inner.event.listen())).poll(cx) {
                // The queue is still empty. The listener is stored for the next
                // poll, and it has registered with cx.waker to be woken when
                // it is notified of the queue becoming nonempty.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    // This should not happen, because the listener is only notified
                    // when the queue state changes, which is impossible while we are
                    // holding self.inner.lock(). But we can be defensive in case of
                    // spurious wakeups, by dropping the listener and looping.
                    self.listener.take();
                    continue;
                },
            }
        }
    }

    /// Like `.next()`, but additionally returns the expiration time for
    /// non-expired requests.
    pub async fn recv_with_expiration(&mut self) -> Option<(T, Result<Instant, ExpiredInQueue>)> {
        poll_fn(|cx| self.poll_next_with_expiration(cx)).await
    }

    fn poll_next_selecting_with_expiration<K: Ord>(
        &mut self,
        cx: &mut Context<'_>,
        select_key: &mut impl FnMut(&T) -> Option<K>,
    ) -> Poll<Option<(T, Result<Instant, ExpiredInQueue>)>> {
        loop {
            let mut inner = self.inner.lock();
            if let Some(result) = inner.queue.pop_selecting(&mut *select_key) {
                let expiration_wait = self.expiration_wait.take();
                drop(inner);
                drop(expiration_wait);
                return Poll::Ready(Some(result));
            } else if inner.senders == 0 && inner.queue.is_empty() {
                let expiration_wait = self.expiration_wait.take();
                drop(inner);
                drop(expiration_wait);
                return Poll::Ready(None);
            }

            let next_expiry_time = inner
                .queue
                .buffer
                .front()
                .map(|(_, expiration)| *expiration);
            let rt = inner.queue.rt.clone();
            let queue_changed = Pin::new(self.listener.get_or_insert_with(|| inner.event.listen()))
                .poll(cx)
                .is_ready();
            drop(inner);

            if queue_changed {
                self.listener.take();
                continue;
            }
            if self
                .expiration_wait
                .as_ref()
                .map(|(expiration, _)| *expiration)
                != next_expiry_time
            {
                let previous = self.expiration_wait.take();
                self.expiration_wait = next_expiry_time.map(|expiration| {
                    let wait = rt.wait(expiration.saturating_duration_since(rt.monotonic_now()));
                    (expiration, wait)
                });
                drop(previous);
            }
            if self
                .expiration_wait
                .as_mut()
                .is_some_and(|(_, timer)| timer.as_mut().poll(cx).is_ready())
            {
                let completed = self.expiration_wait.take();
                drop(completed);
                continue;
            }
            return Poll::Pending;
        }
    }

    pub async fn recv_next_selecting<K: Ord>(
        &mut self,
        mut select_key: impl FnMut(&T) -> Option<K>,
    ) -> Option<(T, Result<Instant, ExpiredInQueue>)> {
        poll_fn(|cx| self.poll_next_selecting_with_expiration(cx, &mut select_key)).await
    }

    /// Returns a stream that yields only expired entries from the queue. If
    /// nothing has expired, it blocks until the next expiry.
    pub fn expired_receiver(&self) -> CoDelQueueExpiredReceiver<RT, T> {
        CoDelQueueExpiredReceiver {
            inner: self.inner.clone(),
            listener: None,
            next_expiry_timer: None,
        }
    }
}

impl<RT: Runtime, T> Stream for CoDelQueueReceiver<RT, T> {
    type Item = (T, Option<ExpiredInQueue>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.poll_next_with_expiration(cx) {
            Poll::Ready(Some((item, expiration))) => Poll::Ready(Some((item, expiration.err()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<RT: Runtime, T> Stream for CoDelQueueExpiredReceiver<RT, T> {
    type Item = (T, ExpiredInQueue);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(T, ExpiredInQueue)>> {
        let this = &mut *self;
        loop {
            let mut inner = this.inner.lock();
            if let Some(result) = inner.queue.pop_expired() {
                let listener = this.listener.take();
                let next_expiry_timer = this.next_expiry_timer.take();
                drop(inner);
                drop(listener);
                drop(next_expiry_timer);
                return Poll::Ready(Some(result));
            } else if inner.receivers == 0 || (inner.senders == 0 && inner.queue.is_empty()) {
                let listener = this.listener.take();
                let next_expiry_timer = this.next_expiry_timer.take();
                drop(inner);
                drop(listener);
                drop(next_expiry_timer);
                return Poll::Ready(None);
            }

            let next_expiry_time = inner
                .queue
                .buffer
                .front()
                .map(|(_, expiration)| *expiration);
            let rt = inner.queue.rt.clone();
            // Register for queue changes as well as the deadline. A new head
            // can replace the one associated with the current timer.
            let queue_changed = Pin::new(
                this.listener
                    .get_or_insert_with(|| inner.expired_event.listen()),
            )
            .poll(cx)
            .is_ready();
            drop(inner);

            if queue_changed {
                this.listener.take();
                continue;
            }
            if this
                .next_expiry_timer
                .as_ref()
                .map(|(expiration, _)| *expiration)
                != next_expiry_time
            {
                let previous = this.next_expiry_timer.take();
                this.next_expiry_timer = next_expiry_time.map(|expiration| {
                    let wait = rt.wait(expiration.saturating_duration_since(rt.monotonic_now()));
                    (expiration, wait)
                });
                drop(previous);
            }
            let deadline_elapsed = this
                .next_expiry_timer
                .as_mut()
                .is_some_and(|(_, timer)| timer.as_mut().poll(cx).is_ready());
            if deadline_elapsed {
                let completed = this.next_expiry_timer.take();
                drop(completed);
                continue;
            }
            return Poll::Pending;
        }
    }
}
