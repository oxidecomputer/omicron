// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error type for trust quorum code

use super::super::spdm::SpdmError;

use std::net::SocketAddr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TrustQuorumError {
    #[error("Error running SPDM protocol: {0}")]
    Spdm(#[from] SpdmError),

    #[error("Not enough peers to unlock storage")]
    NotEnoughPeers,

    #[error("Bincode (de)serialization error: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),

    #[error("JSON (de)serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid secret share received from {0}")]
    InvalidShare(SocketAddr),

    #[error("Rack secret construction failed: {0:?}")]
    RackSecretConstructionFailed(vsss_rs::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
