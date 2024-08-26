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
pub mod policy;
pub mod progenitor_operation_retry;
pub mod update;
pub mod vlan;
pub mod zpool_name;

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

use crate::api::external::Error;
use crate::progenitor_operation_retry::ProgenitorOperationRetry;
use crate::progenitor_operation_retry::ProgenitorOperationRetryError;
use std::future::Future;

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

async fn never_bail() -> Result<bool, Error> {
    Ok(false)
}

/// A wrapper struct that does nothing other than elide the inner value from
/// [`std::debug::Debug`] output.
///
/// If you want to use this for secrets, consider that it might not do
/// everything you expect (it does not zeroize memory on drop, nor get in the
/// way of you removing the inner value from this wrapper struct).
#[derive(
    Clone, Copy, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct NoDebug<T>(pub T);

impl<T> std::fmt::Debug for NoDebug<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "..")
    }
}
