// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper types for performing automatic transaction retries

use chrono::Utc;
use oximeter::{types::Sample, Metric, MetricsError, Target};
use rand::{thread_rng, Rng};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Identifies "which" transaction is retrying
#[derive(Debug, Clone, Target)]
struct DatabaseTransaction {
    name: String,
}

// Identifies that a retry has occurred, and track how long
// the transaction took (either since starting, or since the last
// retry failure was recorded).
#[derive(Debug, Clone, Metric)]
struct RetryData {
    #[datum]
    latency: f64,
    attempt: u32,
}

// Collects all transaction retry samples
#[derive(Debug, Default, Clone)]
pub(crate) struct Producer {
    samples: Arc<Mutex<Vec<Sample>>>,
}

impl Producer {
    pub(crate) fn new() -> Self {
        Self { samples: Arc::new(Mutex::new(vec![])) }
    }

    fn append(
        &self,
        transaction: &DatabaseTransaction,
        data: &RetryData,
    ) -> Result<(), MetricsError> {
        let sample = Sample::new_with_timestamp(Utc::now(), transaction, data)?;
        self.samples.lock().unwrap().push(sample);
        Ok(())
    }
}

struct RetryHelperInner {
    start: chrono::DateTime<Utc>,
    attempts: u32,
}

impl RetryHelperInner {
    fn new() -> Self {
        Self { start: Utc::now(), attempts: 1 }
    }

    fn tick(&mut self) -> Self {
        let start = self.start;
        let attempts = self.attempts;

        self.start = Utc::now();
        self.attempts += 1;

        Self { start, attempts }
    }
}

/// Helper utility for tracking retry attempts and latency.
/// Intended to be used from within "transaction_async_with_retry".
pub(crate) struct RetryHelper {
    producer: Producer,
    name: &'static str,
    inner: Mutex<RetryHelperInner>,
}

const MIN_RETRY_BACKOFF: Duration = Duration::from_millis(0);
const MAX_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const MAX_RETRY_ATTEMPTS: u32 = 10;

impl RetryHelper {
    /// Creates a new RetryHelper, and starts a timer tracking the transaction
    /// duration.
    pub(crate) fn new(producer: &Producer, name: &'static str) -> Self {
        Self {
            producer: producer.clone(),
            name,
            inner: Mutex::new(RetryHelperInner::new()),
        }
    }

    // Called upon retryable transaction failure.
    //
    // This function:
    // - Appends a metric identifying the duration of the transaction operation
    // - Performs a random (uniform) backoff (limited to less than 50 ms)
    // - Returns "true" if the transaction should be restarted
    async fn retry_callback(&self) -> bool {
        // Look at the current attempt and start time so we can log this
        // information before we start sleeping.
        let (start, attempt) = {
            let inner = self.inner.lock().unwrap();
            (inner.start, inner.attempts)
        };

        let latency = (Utc::now() - start)
            .to_std()
            .unwrap_or(Duration::ZERO)
            .as_secs_f64();

        let _ = self.producer.append(
            &DatabaseTransaction { name: self.name.into() },
            &RetryData { latency, attempt },
        );

        // This backoff is not exponential, but I'm not sure we actually want
        // that much backoff here. If we're repeatedly failing, it would
        // probably be better to fail the operation, at which point Oximeter
        // will keep track of the failing transaction and identify that it's a
        // high-priority target for CTE conversion.
        let duration = {
            let mut rng = thread_rng();
            rng.gen_range(MIN_RETRY_BACKOFF..MAX_RETRY_BACKOFF)
        };
        tokio::time::sleep(duration).await;

        // Now that we've finished sleeping, reset the timer and bump the number
        // of attempts we've tried.
        let inner = self.inner.lock().unwrap().tick();
        return inner.attempts < MAX_RETRY_ATTEMPTS;
    }

    /// Converts this function to a retryable callback that can be used from
    /// "transaction_async_with_retry".
    pub(crate) fn as_callback(
        self,
    ) -> impl Fn() -> futures::future::BoxFuture<'static, bool> {
        let r = Arc::new(self);
        move || {
            let r = r.clone();
            Box::pin(async move { r.retry_callback().await })
        }
    }
}

impl oximeter::Producer for Producer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, MetricsError> {
        let samples = std::mem::take(&mut *self.samples.lock().unwrap());
        Ok(Box::new(samples.into_iter()))
    }
}
