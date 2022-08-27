// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database layer for the bootstore

/// The ID of the database used to store blobs.
///
/// We separate them because they are encrypted and accessed differently.
pub enum DbId {
    /// Used pre-rack unlock: Contains key shares and membership data
    TrustQuorum,

    /// Used post-rack unlock: Contains information necessary for setting
    /// up NTP.
    NetworkConfig,
}
