/*!
 * # Oxide Control Plane
 *
 * The overall architecture for the Oxide Control Plane is described in [RFD
 * 61](https://61.rfd.oxide.computer/).  This crate implements common facilities
 * used in the control plane.  Other top-level crates implement pieces of the
 * control plane (e.g., `omicron_nexus`).
 *
 * The best documentation for the control plane is RFD 61 and the rustdoc in
 * this crate.  Since this crate doesn't provide externally-consumable
 * interfaces, the rustdoc (generated with `--document-private-items`) is
 * intended primarily for engineers working on this crate.
 */

/*
 * We only use rustdoc for internal documentation, including private items, so
 * it's expected that we'll have links to private items in the docs.
 */
#![allow(private_intra_doc_links)]
/* TODO(#32): Remove this exception once resolved. */
#![allow(clippy::field_reassign_with_default)]
/* TODO(#40): Remove this exception once resolved. */
#![allow(clippy::unnecessary_wraps)]
/* Clippy's style lints are useful, but not worth running automatically. */
#![allow(clippy::style)]

pub mod backoff;
pub mod cmd;
pub mod config;
pub mod db;
pub mod dev;
pub mod error;
pub mod http_client;
pub mod http_pagination;
pub mod model;
pub mod model_db;
pub mod packaging;

// XXX naming here needs work
mod sled_agent_client;
pub use sled_agent_client::Client as SledAgentClient;
pub use sled_agent_client::TestInterfaces as SledAgentTestInterfaces;
mod nexus_client;
pub use nexus_client::Client as NexusClient;

#[macro_use]
extern crate slog;
