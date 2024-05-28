// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Oxide Control Plane
//!
//! The overall architecture for the Oxide Control Plane is described in [RFD
//! 61](https://61.rfd.oxide.computer/).  This crate implements common facilities
//! used in the control plane.  Other top-level crates implement pieces of the
//! control plane (e.g., `omicron_nexus`).
//!
//! The best documentation for the control plane is RFD 61 and the rustdoc in
//! this crate.  Since this crate doesn't provide externally-consumable
//! interfaces, the rustdoc (generated with `--document-private-items`) is
//! intended primarily for engineers working on this crate.

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]
// TODO(#32): Remove this exception once resolved.
#![allow(clippy::field_reassign_with_default)]

pub mod address;
pub mod api;
pub mod backoff;
pub mod cmd;
pub mod disk;
pub mod ledger;
pub mod update;
pub mod vlan;

pub use update::hex_schema;

#[macro_export]
macro_rules! generate_logging_api {
    ($path:literal) => {
        progenitor::generate_api!(
            spec = $path,
            inner_type = slog::Logger,
            pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
                slog::debug!(log, "client request";
                    "method" => %request.method(),
                    "uri" => %request.url(),
                    "body" => ?&request.body(),
                );
            }),
            post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
                slog::debug!(log, "client response"; "result" => ?result);
            }),
        );
    };
}

/// A type that allows adding file and line numbers to log messages
/// automatically. It should be instantiated at the root logger of each
/// executable that desires this functionality, as in the following example.
/// ```ignore
///     slog::Logger::root(drain, o!(FileKv))
/// ```
pub struct FileKv;

impl slog::KV for FileKv {
    fn serialize(
        &self,
        record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        // Only log file information when severity is at least info level
        if record.level() > slog::Level::Info {
            return Ok(());
        }
        serializer.emit_arguments(
            "file".into(),
            &format_args!("{}:{}", record.file(), record.line()),
        )
    }
}

pub const OMICRON_DPD_TAG: &str = "omicron";

use futures::Future;
use slog::warn;

/// Retry a progenitor client operation until a known result is returned.
///
/// Saga execution relies on the outcome of an external call being known: since
/// they are idempotent, reissue the external call until a known result comes
/// back. Retry if a communication error is seen, or if another retryable error
/// is seen.
///
/// Note that retrying is only valid if the call itself is idempotent.
pub async fn retry_until_known_result<F, T, E, Fut>(
    log: &slog::Logger,
    mut f: F,
) -> Result<T, progenitor_client::Error<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, progenitor_client::Error<E>>>,
    E: std::fmt::Debug,
{
    backoff::retry_notify(
        backoff::retry_policy_internal_service(),
        move || {
            let fut = f();
            async move {
                match fut.await {
                    Err(progenitor_client::Error::CommunicationError(e)) => {
                        warn!(
                            log,
                            "saw transient communication error {}, retrying...",
                            e,
                        );

                        Err(backoff::BackoffError::transient(
                            progenitor_client::Error::CommunicationError(e),
                        ))
                    }

                    Err(progenitor_client::Error::ErrorResponse(
                        response_value,
                    )) => {
                        match response_value.status() {
                            // Retry on 503 or 429
                            http::StatusCode::SERVICE_UNAVAILABLE
                            | http::StatusCode::TOO_MANY_REQUESTS => {
                                Err(backoff::BackoffError::transient(
                                    progenitor_client::Error::ErrorResponse(
                                        response_value,
                                    ),
                                ))
                            }

                            // Anything else is a permanent error
                            _ => Err(backoff::BackoffError::Permanent(
                                progenitor_client::Error::ErrorResponse(
                                    response_value,
                                ),
                            )),
                        }
                    }

                    Err(e) => {
                        warn!(log, "saw permanent error {}, aborting", e,);

                        Err(backoff::BackoffError::Permanent(e))
                    }

                    Ok(v) => Ok(v),
                }
            }
        },
        |error: progenitor_client::Error<_>, delay| {
            warn!(
                log,
                "failed external call ({:?}), will retry in {:?}", error, delay,
            );
        },
    )
    .await
}
