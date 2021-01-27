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
 * * [`oxide_controller`] (also called OXC) is the heart of the control plane.
 *   It provides the user-facing external API as well as an internal-facing API
 *   for use by other control plane components.
 *   * [`controller_run_server`] is used by the executable binary that you can
 *     use to run an OXC instance.
 *   * [`controller_run_openapi_external`] is used by the same binary to
 *     generate an OpenAPI spec for the external API.
 *   * [`ConfigController`] represents the configuration of an OXC instance.
 *   * [`ControllerClient`] provides a client interface to the internal OXC API.
 *   * [`OxideControllerServer`] provides an interface for starting an OXC
 *     instance.
 *   While this component is a prototype, the intent is to evolve this into the
 *   final production service.
 *
 * * [`sled_agent`] is the part of the control plane residing on each individual
 *   compute server (sled).  This agent provides interfaces used by OXC to
 *   manage resources on the sled.  The implementation here is completely
 *   simulated.
 *   * [`sa_run_server`] is used by the executable binary that you can use to
 *     run a simulated `SledAgent`.
 *   * [`ConfigSledAgent`] represents the configuration of a sled agent.
 *   * [`SledAgentClient`] provides a client interface to the sled agent's API
 *   * [`SledAgentServer`] provides an interface for starting an OXC
 *     instance.
 *   This implementation will not wind up as part of a production Oxide system,
 *   but the intent is to make it available to developers to test their own
 *   software or to get a feel for working with an Oxide system.
 *
 * There's other common code at the top level, with the most important being:
 *
 * * [`api_error`], used to represent most errors within both top-level
 *   components
 * * [`api_model`], which contains types used in both the external and internal
 *   APIs
 * * [`datastore`], an in-memory store for control plane data.  This is
 *   instantiated (from scratch) with each OXC instance.
 */

/*
 * We only use rustdoc for internal documentation, including private items, so
 * it's expected that we'll have links to private items in the docs.
 */
#![allow(private_intra_doc_links)]

mod api_error;
pub mod api_model;
mod cmd;
mod datastore;
mod http_client;
mod http_pagination;
mod oxide_controller;
mod sled_agent;
mod test_util;

pub use oxide_controller::controller_run_openapi_external;
pub use oxide_controller::controller_run_server;
pub use oxide_controller::ConfigController;
pub use oxide_controller::ControllerClient;
pub use oxide_controller::ControllerServerContext;
pub use oxide_controller::OxideController;
pub use oxide_controller::OxideControllerServer;
pub use oxide_controller::OxideControllerTestInterfaces;

pub use sled_agent::sa_run_server;
pub use sled_agent::ConfigSledAgent;
pub use sled_agent::SimMode;
pub use sled_agent::SledAgentClient;
pub use sled_agent::SledAgentServer;
pub use sled_agent::SledAgentTestInterfaces;

pub use cmd::fatal;
pub use cmd::CmdError;

#[macro_use]
extern crate slog;
