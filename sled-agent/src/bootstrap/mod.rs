// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related utilities

pub mod agent;
mod client;
pub mod config;
mod discovery;
mod http_entrypoints;
mod multicast;
mod params;
pub mod server;
mod spdm;
pub mod trust_quorum;
mod views;

pub fn run_openapi() -> Result<(), String> {
    http_entrypoints::ba_api()
        .openapi("Oxide Bootstrap Agent API", "0.0.1")
        .description("API for interacting with bootstrapping agents")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}
