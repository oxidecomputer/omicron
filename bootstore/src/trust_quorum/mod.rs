// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entry point for the trust quorum code
//!
//! This module only provides the trust quorum primitives: the rack secret and
//! its associated machinery (splitting into shares, verification, etc.).
//! Distribution and retrieval of shares is the responsibility of the
//! bootstrap-agent, which uses sprockets to secure communications between
//! sleds.

mod error;
mod rack_secret;

pub use error::TrustQuorumError;
pub use rack_secret::RackSecret;
