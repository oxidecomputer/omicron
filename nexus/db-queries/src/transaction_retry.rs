// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper types for performing automatic transaction retries

use chrono::Utc;
use oximeter::{types::Sample, Metric, MetricsError, Target};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Identifies "which" transaction is retrying
#[derive(Debug, Clone, Target)]
struct Transaction {
    name: String,
}

// Identifies that a retry has occurred, and track how long
// the transaction took (either since starting, or since the last
// retry failure was recorded).
#[derive(Debug, Clone, Metric)]
struct RetryData {
    #[datum]
    latency: f64,
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
        transaction: &Transaction,
        latency: Duration,
    ) -> Result<(), MetricsError> {
        let sample = Sample::new_with_timestamp(
            Utc::now(),
            transaction,
            &RetryData { latency: latency.as_secs_f64() },
        )?;
        self.samples.lock().unwrap().push(sample);
        Ok(())
    }
}

struct RetryHelperInner {
    start: chrono::DateTime<Utc>,
    attempts: usize,
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

const MAX_RETRY_ATTEMPTS: usize = 10;

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
    // - Performs a randomly generated backoff (limited to less than 50 ms)
    // - Returns "true" if the transaction should be restarted
    async fn retry_callback(&self) -> bool {
        let inner = self.inner.lock().unwrap().tick();

        let _ = self.producer.append(
            &Transaction { name: self.name.into() },
            (Utc::now() - inner.start).to_std().unwrap_or(Duration::ZERO),
        );

        // This backoff is not exponential, but I'm not sure we actually want
        // that much backoff here. If we're repeatedly failing, it would
        // probably be better to fail the operation, at which point Oximeter
        // will keep track of the failing transaction and identify that it's a
        // high-priority target for CTE conversion.
        tokio::time::sleep(Duration::from_millis(rand::random::<u64>() % 50))
            .await;

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
        let samples =
            std::mem::replace(&mut *self.samples.lock().unwrap(), vec![]);
        Ok(Box::new(samples.into_iter()))
    }
}
