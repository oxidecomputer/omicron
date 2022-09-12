// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error type for trust quorum code

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TrustQuorumError {
    // Temporary until the using code is written
    #[allow(dead_code)]
    #[error("Not enough peers to unlock storage")]
    NotEnoughPeers,

    // Temporary until the using code is written
    #[allow(dead_code)]
    #[error("Rack secret construction failed: {0:?}")]
    RackSecretConstructionFailed(vsss_rs::Error),
}
