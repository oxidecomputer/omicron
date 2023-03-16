// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service

/// Configuration files which automate input to RSS.
pub mod config;
mod plan;
/// The main implementation of the RSS service.
pub mod service;

// Interfaces to help testing access to external services
mod dns_interface;
mod nexus_interface;
mod sled_interface;
