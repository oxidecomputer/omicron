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
pub mod cmd;
pub mod disk;
pub mod ledger;
pub mod policy;
pub mod update;
pub mod vlan;
pub mod zpool_name;

pub use omicron_common_external::backoff;
pub use omicron_common_external::progenitor as progenitor_operation_retry;
pub use omicron_common_external::progenitor::retry_until_known_result;
pub use omicron_common_external::FileKv;
pub use update::hex_schema;

pub const OMICRON_DPD_TAG: &str = "omicron";

/// A wrapper struct that does nothing other than elide the inner value from
/// [`std::fmt::Debug`] output.
///
/// We define this within Omicron instead of using one of the many available
/// crates that do the same thing because it's trivial to do so, and we want the
/// flexibility to add traits to this type without needing to wait on upstream
/// to add an optional dependency.
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
