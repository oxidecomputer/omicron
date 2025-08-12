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
pub mod snake_case_result;
pub mod update;
pub mod vlan;
pub mod zone_images;
pub mod zpool_name;

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

pub fn hex_schema<const N: usize>(
    gen: &mut schemars::SchemaGenerator,
) -> schemars::schema::Schema {
    use schemars::JsonSchema;

    let mut schema: schemars::schema::SchemaObject =
        <String>::json_schema(gen).into();
    schema.format = Some(format!("hex string ({N} bytes)"));
    schema.into()
}
