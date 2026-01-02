// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]

// Module for executing the simulated sled agent.
pub mod sim;

// Modules shared by both simulated and non-simulated sled agents.
pub mod common;

// Modules for the non-simulated sled agent.
pub mod artifact_store;
mod backing_fs;
pub mod bootstrap;
pub mod config;
mod ddm_reconciler;
pub(crate) mod hardware_monitor;
mod http_entrypoints;
mod instance;
mod instance_manager;
mod long_running_tasks;
mod metrics;
mod nexus;
mod probe_manager;
mod profile;
pub mod rack_setup;
pub mod server;
pub mod services;
mod sled_agent;
mod support_bundle;
mod swap_device;
mod updates;
mod vmm_reservoir;
mod zone_bundle;

#[cfg(test)]
mod fakes;

#[macro_use]
extern crate slog;

#[cfg(feature = "image-trampoline")]
compile_error!("Sled Agent should not be built with `-i trampoline`");
