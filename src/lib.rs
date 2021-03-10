/*!
 * # Oxide Control Plane prototype
 *
 * The overall architecture for the Oxide Control Plane is described in [RFD
 * 61](https://61.rfd.oxide.computer/).  This crate implements a prototype
 * control plane.  This crate does not provide a useful library interface; the
 * interfaces here are intended for use only by the executables (binaries) in
 * this crate and the test suite.
 *
 * The best documentation for the control plane is RFD 61 and the rustdoc in
 * this crate.  Since this crate doesn't provide externally-consumable
 * interfaces, the rustdoc (generated with `--document-private-items`) is
 * intended primarily for engineers working on this crate.
 *
 * There are two major components here with parallel sets of interfaces:
 *
 * * [`nexus`] is the heart of the control plane.
 *   It provides the user-facing external API as well as an internal-facing API
 *   for use by other control plane components.
 *   * [`nexus::run_server`] is used by the executable binary that you can
 *     use to run a Nexus instance.
 *   * [`nexus::run_openapi_external`] is used by the same binary to
 *     generate an OpenAPI spec for the external API.
 *   * [`nexus::Config`] represents the configuration of a Nexus instance.
 *   * [`nexus::Client`] provides a client interface to the internal Nexus API.
 *   * [`nexus::Server`] provides an interface for starting a Nexus instance.
 *   While this component is a prototype, the intent is to evolve this into the
 *   final production service.
 *
 * * [`sled_agent`] is the part of the control plane residing on each individual
 *   compute server (sled).  This agent provides interfaces used by Nexus to
 *   manage resources on the sled.  The implementation here is completely
 *   simulated.
 *   * [`sled_agent::run_server`] is used by the executable binary that you can use to
 *     run a simulated sled agent.
 *   * [`sled_agent::Config`] represents the configuration of a sled agent.
 *   * [`sled_agent::Client`] provides a client interface to the sled agent's API
 *   * [`sled_agent::Server`] provides an interface for starting a sled_agent
 *     instance.
 *   This implementation will not wind up as part of a production Oxide system,
 *   but the intent is to make it available to developers to test their own
 *   software or to get a feel for working with an Oxide system.
 * * [`bootstrap_agent`] is the part of the control plane bundled with the
 *   base OS, responsible for establishing a trust quorum and launching
 *   the oither parts of the control plane (sled_agent and Nexus).
 *   * [`bootstrap_agent::Config`] represents the configuration of a bootstrap agent.
 *   * [`bootstrap_agent::Server`] provides an interface for starting a bootstrap
 *     instance.
 *
 * There's other common code at the top level, with the most important being:
 *
 * * [`api_error`], used to represent most errors within both top-level
 *   components
 * * [`api_model`], which contains types used in both the external and internal
 *   APIs
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

pub mod api_model;
pub mod bootstrap_agent;
pub mod cmd;
pub mod dev_db;
pub mod nexus;
pub mod packaging;
pub mod sled_agent;
pub mod test_util;

mod api_error;
mod backoff;
mod http_client;
mod http_pagination;

#[macro_use]
extern crate slog;
