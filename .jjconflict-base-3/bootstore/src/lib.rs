// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod schemes;
pub mod trust_quorum;

use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Sha3_256Digest([u8; 32]);

impl Sha3_256Digest {
    pub fn new(bytes: [u8; 32]) -> Self {
        Sha3_256Digest(bytes)
    }
}
