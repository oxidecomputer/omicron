// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use rand::{TryRngCore as _, rngs::OsRng};

use crate::v1::crypto::Salt;

impl Salt {
    /// Generate a new random salt.
    pub fn new() -> Salt {
        let mut rng = OsRng;
        let mut salt = [0u8; 32];
        rng.try_fill_bytes(&mut salt).expect("fetched random bytes from OsRng");
        Salt(salt)
    }
}
