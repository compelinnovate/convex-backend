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
        if self.len() >= capacity {
            return Err((QueueFull, item));
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

    use super::CoDelQueue;
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
    }

    impl QueueTestRuntime {
        fn new() -> Self {
            Self {
                now: Arc::new(Mutex::new(tokio::time::Instant::now())),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock();
            *now += duration;
        }
    }

    impl Runtime for QueueTestRuntime {
        fn wait(
            &self,
            duration: Duration,
        ) -> Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>> {
            let now = self.now.clone();
            Box::pin(
                async move {
                    tokio::time::sleep(duration).await;
                    let mut now = now.lock();
                    *now += duration;
                }
                .fuse(),
            )
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
        assert!(sender.is_closed());
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        assert!(sender.try_send(DropCounter(drops.clone())).is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 2);
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

pub struct CoDelQueueReceiver<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
    listener: Option<event_listener::EventListener>,
}

impl<RT: Runtime, T> Clone for CoDelQueueReceiver<RT, T> {
    fn clone(&self) -> Self {
        self.inner.lock().receivers += 1;
        Self {
            inner: self.inner.clone(),
            listener: None,
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
            // Queue is closed. Wake up all receivers so they return None.
            inner.event.notify(usize::MAX);
            inner.expired_event.notify(usize::MAX);
        }
    }
}

impl<RT: Runtime, T> CoDelQueueSender<RT, T> {
    pub fn try_send(&self, item: T) -> Result<(), QueueFull> {
        // N.B.: on error, only drops `item` after releasing the lock
        self.try_send_or_recover(item)
            .map_err(|(QueueFull, _)| QueueFull)
    }

    pub fn try_send_or_recover(&self, item: T) -> Result<(), (QueueFull, T)> {
        let mut inner = self.inner.lock();
        if inner.receivers == 0 {
            return Err((QueueFull, item));
        }
        inner.queue.push(item)?;
        inner.event.notify_additional(1);
        // All `CoDelQueueExpiredReceiver`s need to be woken since they don't consume
        // the queue item
        inner.expired_event.notify(usize::MAX);
        Ok(())
    }

    pub fn try_send_with_reserved_capacity(&self, item: T) -> Result<bool, QueueFull> {
        let result = {
            let mut inner = self.inner.lock();
            if inner.receivers == 0 {
                Err((QueueFull, item))
            } else {
                inner.queue.push_with_reserved_capacity(item).inspect(|_| {
                    inner.event.notify_additional(1);
                    inner.expired_event.notify(usize::MAX);
                })
            }
        };
        // Rejected items can own queue handles; drop them only after releasing the lock.
        result.map_err(|(QueueFull, _)| QueueFull)
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().receivers == 0
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

    pub fn poll_next_selecting_with_expiration<K: Ord>(
        &mut self,
        cx: &mut Context<'_>,
        select_key: &mut impl FnMut(&T) -> Option<K>,
    ) -> Poll<Option<(T, Result<Instant, ExpiredInQueue>)>> {
        let mut inner = self.inner.lock();
        if let Some(result) = inner.queue.pop_selecting(&mut *select_key) {
            return Poll::Ready(Some(result));
        } else if inner.senders == 0 {
            return Poll::Ready(None);
        }

        loop {
            match Pin::new(self.listener.get_or_insert_with(|| inner.event.listen())).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    self.listener.take();
                    continue;
                },
            }
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
                this.listener.take();
                this.next_expiry_timer.take();
                return Poll::Ready(Some(result));
            } else if inner.senders == 0 {
                this.listener.take();
                this.next_expiry_timer.take();
                return Poll::Ready(None);
            }

            let next_expiry_time = inner
                .queue
                .buffer
                .front()
                .map(|(_, expiration)| *expiration);
            if this
                .next_expiry_timer
                .as_ref()
                .map(|(expiration, _)| *expiration)
                != next_expiry_time
            {
                this.next_expiry_timer = next_expiry_time.map(|expiration| {
                    let wait = inner
                        .queue
                        .rt
                        .wait(expiration.saturating_duration_since(inner.queue.rt.monotonic_now()));
                    (expiration, wait)
                });
            }

            // Register for queue changes as well as the deadline. A new head
            // can replace the one associated with the current timer.
            let queue_changed = Pin::new(
                this.listener
                    .get_or_insert_with(|| inner.expired_event.listen()),
            )
            .poll(cx)
            .is_ready();
            let deadline_elapsed = this
                .next_expiry_timer
                .as_mut()
                .is_some_and(|(_, timer)| timer.as_mut().poll(cx).is_ready());

            if queue_changed {
                this.listener.take();
            }
            if deadline_elapsed {
                this.next_expiry_timer.take();
            }
            if queue_changed || deadline_elapsed {
                drop(inner);
                continue;
            }
            return Poll::Pending;
        }
    }
}
