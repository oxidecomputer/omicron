// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]
// Clippy's style lints are useful, but not worth running automatically.
#![allow(clippy::style)]
// assert_matches! is pretty useful for tests so just enable it conditionally.
#![cfg_attr(test, feature(assert_matches))]

// Module for executing the simulated sled agent.
pub mod sim;

// Modules shared by both simulated and non-simulated sled agents.
pub mod bootstrap;
pub mod common;
mod http_entrypoints;
mod nexus;
mod params;
mod updates;

// Modules for the non-simulated sled agent.
cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        pub mod config;
        mod illumos;
        mod instance;
        mod instance_manager;
        pub mod rack_setup;
        pub mod server;
        mod services;
        mod sled_agent;
        mod storage_manager;
    }
}

#[cfg(test)]
mod mocks;

#[macro_use]
extern crate slog;

/// Location on internal storage where sled-specific information is stored.
#[cfg(target_os = "illumos")]
pub(crate) const OMICRON_CONFIG_PATH: &'static str = "/var/tmp/oxide";
