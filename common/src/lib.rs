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
pub mod nexus_config;
pub mod postgres_config;
pub mod update;

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

/// Location on internal storage where sled-specific information is stored.
///
/// This is mostly private to the `omicron-sled-agent` crate, but exists in
/// common so it may be cleared by the installation tools.
///
/// NOTE: Be careful when modifying this path - the installation tools will
/// **remove the entire directory** to re-install/uninstall the system.
pub const OMICRON_CONFIG_PATH: &'static str = "/var/oxide";
