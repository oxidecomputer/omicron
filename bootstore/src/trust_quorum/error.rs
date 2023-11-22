// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error type for trust quorum code

use derive_more::From;
use thiserror::Error;

#[derive(From, Debug, Error, PartialEq, Eq, Clone)]
pub enum TrustQuorumError {
    // Temporary until the using code is written
    #[error("Rack secret construction failed: {0:?}")]
    Vsssrs(vsss_rs::Error),

    // Failed to encrypt something
    #[error("Failed to encrypt")]
    FailedToEncrypt,

    // Failed to decrypt something
    #[error("Failed to decrypt")]
    FailedToDecrypt,
}
