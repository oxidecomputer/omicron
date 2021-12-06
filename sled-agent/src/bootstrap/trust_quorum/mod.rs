// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entry point for the trust quorum code
//!
//! The Trust quorum relies on IPv6 multicast discovery, rack secret handling,
//! and the SPDM protocol.
//!
//! Below is the trust quorum protocol for share retrieval over TCP.
//!
//! The following protocol is shown between two sleds only, but multicast
//! discovery and share requests will continue to run until enough shares
//! have been received to recreate the rack secret.
//!
//! Sled1                                      Sled2
//! =====                                      =====
//!  ||  ------- Multicast Discovery ---------  ||
//!  ||                                         ||
//!  ||  ---- Connect to TrustQuorum port --->  ||
//!  ||                                         ||
//!  ||  --------- SPDM Requests ------------>  ||
//!  ||                                         ||
//!  ||  <-------- SPDM Responses ------------  ||
//!  ||                                         ||
//!  ||  ----- SPDM Channel Established ------  ||
//!  ||                                         ||
//!  ||  --------- Request Share ------------>  ||
//!  ||                                         ||
//!  ||  <----------- Share ------------------  ||
//!

mod client;
mod msgs;
mod rack_secret;
mod server;

// TODO: Get rid of this security hole!
mod config;

pub use client::Client;
pub use config::Config;
pub use rack_secret::RackSecret;
pub use server::{Server, PORT};
