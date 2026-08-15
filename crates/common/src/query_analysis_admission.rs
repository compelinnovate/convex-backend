use std::sync::Arc;

use tokio::sync::{
    OwnedSemaphorePermit,
    Semaphore,
    TryAcquireError,
};

/// One application-scoped capacity gate shared by degradable query leaders
/// and isolate module analysis.
///
/// Degradable queries require an immediate decision so the sync protocol can
/// retain stale results instead of adding a waiter. Analysis waits fairly for
/// the same permits. Consequently, analysis borrows elastic capacity instead
/// of adding work above the configured degradable-query ceiling.
#[derive(Clone)]
pub struct QueryAnalysisAdmission {
    capacity: usize,
    semaphore: Arc<Semaphore>,
}

/// A permit from [`QueryAnalysisAdmission`].
pub struct QueryAnalysisPermit {
    _permit: OwnedSemaphorePermit,
}

impl QueryAnalysisAdmission {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "query-analysis capacity must be positive");
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Attempts immediate admission for a degradable query leader.
    ///
    /// A queued analysis waiter receives a released permit before a later
    /// immediate acquisition can take it because the underlying semaphore is
    /// fair.
    pub fn try_acquire_degradable(&self) -> Option<QueryAnalysisPermit> {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return None,
            Err(TryAcquireError::Closed) => {
                panic!("query-analysis admission semaphore unexpectedly closed")
            },
        };
        Some(QueryAnalysisPermit { _permit: permit })
    }

    /// Waits fairly for capacity for one isolate module analysis attempt.
    pub async fn acquire_analysis(&self) -> QueryAnalysisPermit {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("query-analysis admission semaphore unexpectedly closed");
        QueryAnalysisPermit { _permit: permit }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::QueryAnalysisAdmission;

    #[tokio::test]
    async fn analysis_borrows_from_degradable_capacity() {
        let admission = QueryAnalysisAdmission::new(2);
        let degradable = admission
            .try_acquire_degradable()
            .expect("first degradable query was not admitted");
        let analysis = admission.acquire_analysis().await;

        assert!(admission.try_acquire_degradable().is_none());

        drop(analysis);
        let replacement = admission
            .try_acquire_degradable()
            .expect("released analysis reservation was not returned");
        drop(replacement);
        drop(degradable);
    }

    #[tokio::test]
    async fn queued_analysis_precedes_new_degradable_admission() {
        let admission = QueryAnalysisAdmission::new(1);
        let degradable = admission
            .try_acquire_degradable()
            .expect("degradable query was not admitted");
        let mut analysis = std::pin::pin!(admission.acquire_analysis());
        assert!(futures::poll!(&mut analysis).is_pending());

        drop(degradable);
        // The fair semaphore assigns the released permit to the queued
        // analysis future before that future is polled again.
        assert!(admission.try_acquire_degradable().is_none());
        let analysis = tokio::time::timeout(Duration::from_secs(1), &mut analysis)
            .await
            .expect("queued analysis did not receive released capacity");
        drop(analysis);

        assert!(admission.try_acquire_degradable().is_some());
    }
}
