// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use futures::Future;
use slog::warn;
use slog::Logger;

//use crate::api::external::Error;
use crate::backoff::retry_notify;
use crate::backoff::retry_policy_internal_service;
use crate::backoff::BackoffError;

#[derive(Debug)]
pub enum ProgenitorOperationRetryError<E, GE: std::fmt::Debug> {
    /// Nexus determined that the operation will never return a known result
    /// because the remote server is gone.
    Gone,

    /// Attempting to check if the retry loop should be stopped failed
    GoneCheckError(GE),

    /// The retry loop progenitor operation saw a permanent client error
    ProgenitorError(progenitor_client::Error<E>),
}

impl<E, GE: std::fmt::Debug> ProgenitorOperationRetryError<E, GE> {
    pub fn is_not_found(&self) -> bool {
        match &self {
            ProgenitorOperationRetryError::ProgenitorError(e) => match e {
                progenitor_client::Error::ErrorResponse(rv) => {
                    match rv.status() {
                        reqwest::StatusCode::NOT_FOUND => true,
                        _ => false,
                    }
                }

                _ => false,
            },

            _ => false,
        }
    }

    pub fn is_gone(&self) -> bool {
        matches!(&self, ProgenitorOperationRetryError::Gone)
    }
}

/// Retry a progenitor client operation until a known result is returned, or
/// until something tells us that we should stop trying.
///
/// Saga execution relies on the outcome of an external call being known: since
/// they are idempotent, reissue the external call until a known result comes
/// back. Retry if a communication error is seen, or if another retryable error
/// is seen.
///
/// During the retry loop, call the supplied `gone_check` function to see if the
/// retry loop should be aborted: in the cases where Nexus can _know_ that a
/// request will never complete, the retry loop must be aborted. Otherwise,
/// Nexus will indefinitely retry until some known result is returned.
///
/// Note that retrying is only valid if the `operation` itself is idempotent.
pub struct ProgenitorOperationRetry<
    T,
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, progenitor_client::Error<E>>>,
    BF: FnMut() -> BFut,
    BErr: std::fmt::Debug,
    BFut: Future<Output = Result<bool, BErr>>,
> {
    operation: F,

    /// If Nexus knows that the supplied operation will never successfully
    /// complete, then `gone_check` should return true.
    gone_check: BF,
}

impl<T, E, F, Fut, BF, BErr, BFut>
    ProgenitorOperationRetry<T, E, F, Fut, BF, BErr, BFut>
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, progenitor_client::Error<E>>>,
    BF: FnMut() -> BFut,
    BErr: std::fmt::Debug,
    BFut: Future<Output = Result<bool, BErr>>,
{
    pub fn new(operation: F, gone_check: BF) -> Self {
        Self { operation, gone_check }
    }

    pub async fn run(
        mut self,
        log: &Logger,
    ) -> Result<T, ProgenitorOperationRetryError<E, BErr>> {
        retry_notify(
            retry_policy_internal_service(),
            move || {
                let gone_check = (self.gone_check)();
                let f = (self.operation)();

                async move {
                    match gone_check.await {
                        Ok(dest_is_gone) => {
                            if dest_is_gone {
                                return Err(BackoffError::Permanent(
                                    ProgenitorOperationRetryError::Gone
                                ));
                            }
                        }

                        Err(e) => {
                            return Err(BackoffError::Permanent(
                                ProgenitorOperationRetryError::GoneCheckError(e)
                            ));
                        }
                    }

                    match f.await {
                        Err(progenitor_client::Error::CommunicationError(e)) => {
                            warn!(
                                log,
                                "saw transient communication error {}, retrying...",
                                e,
                            );

                            Err(BackoffError::transient(
                                ProgenitorOperationRetryError::ProgenitorError(
                                    progenitor_client::Error::CommunicationError(e)
                                )
                            ))
                        }

                        Err(progenitor_client::Error::ErrorResponse(
                            response_value,
                        )) => {
                            match response_value.status() {
                                // Retry on 503 or 429
                                reqwest::StatusCode::SERVICE_UNAVAILABLE
                                | reqwest::StatusCode::TOO_MANY_REQUESTS => {
                                    Err(BackoffError::transient(
                                        ProgenitorOperationRetryError::ProgenitorError(
                                            progenitor_client::Error::ErrorResponse(
                                                response_value
                                            )
                                        )
                                    ))
                                }

                                // Anything else is a permanent error
                                _ => Err(BackoffError::Permanent(
                                    ProgenitorOperationRetryError::ProgenitorError(
                                        progenitor_client::Error::ErrorResponse(
                                            response_value
                                        )
                                    )
                                ))
                            }
                        }

                        Err(e) => {
                            warn!(log, "saw permanent error {}, aborting", e,);

                            Err(BackoffError::Permanent(
                                ProgenitorOperationRetryError::ProgenitorError(e)
                            ))
                        }

                        Ok(v) => Ok(v),
                    }
                }
            },
            |error: ProgenitorOperationRetryError<E, BErr>, delay| {
                warn!(
                    log,
                    "failed external call ({:?}), will retry in {:?}", error, delay,
                );
            },
        )
        .await
    }
}

/// Retry a progenitor client operation until a known result is returned.
///
/// See [`ProgenitorOperationRetry`] for more information.
// TODO mark this deprecated, `never_bail` is a bad idea
pub async fn retry_until_known_result<F, T, E, Fut>(
    log: &slog::Logger,
    f: F,
) -> Result<T, progenitor_client::Error<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, progenitor_client::Error<E>>>,
    E: std::fmt::Debug,
{
    match ProgenitorOperationRetry::new(f, never_bail).run(log).await {
        Ok(v) => Ok(v),

        Err(e) => match e {
            ProgenitorOperationRetryError::ProgenitorError(e) => Err(e),

            ProgenitorOperationRetryError::Gone
            | ProgenitorOperationRetryError::GoneCheckError(_) => {
                // ProgenitorOperationRetry::new called with `never_bail` as the
                // bail check should never return these variants!
                unreachable!();
            }
        },
    }
}

async fn never_bail() -> Result<bool, ()> {
    Ok(false)
}
