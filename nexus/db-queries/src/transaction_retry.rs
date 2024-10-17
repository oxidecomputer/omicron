// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper types for performing automatic transaction retries

use async_bb8_diesel::AsyncConnection;
use chrono::Utc;
use diesel::result::Error as DieselError;
use oximeter::{types::Sample, MetricsError};
use rand::{thread_rng, Rng};
use slog::{info, warn, Logger};
use std::sync::{Arc, Mutex};
use std::time::Duration;

oximeter::use_timeseries!("database-transaction.toml");
use database_transaction::DatabaseTransaction;
use database_transaction::RetryData;

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

    fn has_retried(&self) -> bool {
        self.attempts > 1
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
pub struct RetryHelper {
    log: Logger,
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
    pub(crate) fn new(
        log: &Logger,
        producer: &Producer,
        name: &'static str,
    ) -> Self {
        Self {
            log: log.new(o!("transaction" => name)),
            producer: producer.clone(),
            name,
            inner: Mutex::new(RetryHelperInner::new()),
        }
    }

    /// Calls the function "f" in an asynchronous, retryable transaction.
    pub async fn transaction<R, Func, Fut>(
        self,
        conn: &async_bb8_diesel::Connection<crate::db::DbConnection>,
        f: Func,
    ) -> Result<R, DieselError>
    where
        R: Send + 'static,
        Fut: std::future::Future<Output = Result<R, DieselError>> + Send,
        Func: Fn(async_bb8_diesel::Connection<crate::db::DbConnection>) -> Fut
            + Send
            + Sync,
    {
        let slef = Arc::new(self);
        let result = conn
            .transaction_async_with_retry(f, slef.clone().as_callback())
            .await;

        let retry_info = slef.inner.lock().unwrap();
        if retry_info.has_retried() {
            info!(
                slef.log,
                "transaction completed";
                "attempts" => retry_info.attempts,
            );
        }

        result
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
            &RetryData { datum: latency, attempt },
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

        warn!(
            self.log,
            "Retryable transaction failure";
            "retry_after (ms)" => duration.as_millis(),
        );
        tokio::time::sleep(duration).await;

        // Now that we've finished sleeping, reset the timer and bump the number
        // of attempts we've tried.
        let inner = self.inner.lock().unwrap().tick();
        return inner.attempts < MAX_RETRY_ATTEMPTS;
    }

    // Converts this function to a retryable callback that can be used from
    // "transaction_async_with_retry".
    fn as_callback(
        self: Arc<Self>,
    ) -> impl Fn() -> futures::future::BoxFuture<'static, bool> {
        move || {
            let r = self.clone();
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

/// Helper utility for passing non-retryable errors out-of-band from
/// transactions.
///
/// Transactions prefer to act on the `diesel::result::Error` type,
/// but transaction users may want more meaningful error types.
/// This utility helps callers safely propagate back Diesel errors while
/// retaining auxiliary error info.
pub struct OptionalError<E>(Arc<Mutex<Option<E>>>);

impl<E> Clone for OptionalError<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<E: std::fmt::Debug> OptionalError<E> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// Sets "Self" to the value of `error` and returns `DieselError::RollbackTransaction`.
    pub fn bail(&self, err: E) -> DieselError {
        (*self.0.lock().unwrap()).replace(err);
        DieselError::RollbackTransaction
    }

    /// If `diesel_error` is retryable, returns it without setting Self.
    ///
    /// Otherwise, sets "Self" to the value of `err`, and returns
    /// `DieselError::RollbackTransaction`.
    pub fn bail_retryable_or(
        &self,
        diesel_error: DieselError,
        err: E,
    ) -> DieselError {
        self.bail_retryable_or_else(diesel_error, |_diesel_error| err)
    }

    /// If `diesel_error` is retryable, returns it without setting Self.
    ///
    /// Otherwise, sets "Self" to the value of `f` applied to `diesel_err`, and
    /// returns `DieselError::RollbackTransaction`.
    pub fn bail_retryable_or_else<F>(
        &self,
        diesel_error: DieselError,
        f: F,
    ) -> DieselError
    where
        F: FnOnce(DieselError) -> E,
    {
        if crate::db::error::retryable(&diesel_error) {
            return diesel_error;
        } else {
            self.bail(f(diesel_error))
        }
    }

    /// If "Self" was previously set to a non-retryable error, return it.
    pub fn take(self) -> Option<E> {
        (*self.0.lock().unwrap()).take()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use oximeter::types::FieldValue;

    // If a transaction is explicitly rolled back, we should not expect any
    // samples to be taken. With no retries, this is just a normal operation
    // failure.
    #[tokio::test]
    async fn test_transaction_rollback_produces_no_samples() {
        let logctx = dev::test_setup_log(
            "test_transaction_rollback_produces_no_samples",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .transaction_retry_wrapper(
                "test_transaction_rollback_produces_no_samples",
            )
            .transaction(&conn, |_conn| async move {
                Err::<(), _>(diesel::result::Error::RollbackTransaction)
            })
            .await
            .expect_err("Should have failed");

        let samples = datastore
            .transaction_retry_producer()
            .samples
            .lock()
            .unwrap()
            .clone();
        assert_eq!(samples, vec![]);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // If a transaction fails with a retryable error, we record samples,
    // providing oximeter-level information about the attempts.
    #[tokio::test]
    async fn test_transaction_retry_produces_samples() {
        let logctx =
            dev::test_setup_log("test_transaction_retry_produces_samples");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        datastore
            .transaction_retry_wrapper(
                "test_transaction_retry_produces_samples",
            )
            .transaction(&conn, |_conn| async move {
                Err::<(), _>(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::SerializationFailure,
                    Box::new("restart transaction: Retry forever!".to_string()),
                ))
            })
            .await
            .expect_err("Should have failed");

        let samples = datastore
            .transaction_retry_producer()
            .samples
            .lock()
            .unwrap()
            .clone();
        assert_eq!(samples.len(), MAX_RETRY_ATTEMPTS as usize);

        for i in 0..samples.len() {
            let sample = &samples[i];

            assert_eq!(
                sample.timeseries_name,
                "database_transaction:retry_data"
            );

            let target_fields = sample.sorted_target_fields();
            assert_eq!(
                target_fields["name"].value,
                FieldValue::String(
                    "test_transaction_retry_produces_samples"
                        .to_string()
                        .into()
                )
            );

            // Attempts are one-indexed
            let metric_fields = sample.sorted_metric_fields();
            assert_eq!(
                metric_fields["attempt"].value,
                FieldValue::U32(u32::try_from(i).unwrap() + 1),
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
