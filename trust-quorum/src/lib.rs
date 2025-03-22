// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol
//!
//! This protocol is written as a
//! [no-IO](https://sans-io.readthedocs.io/how-to-sans-io.html) implementation.
//! All persistent state and all networking is managed outside of this
//! implementation.

use derive_more::Display;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod configuration;
pub(crate) mod crypto;
mod error;
mod messages;
pub use configuration::Configuration;
pub use error::Error;
pub use messages::*;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
)]
pub struct RackId(Uuid);

impl RackId {
    pub fn new(uuid: Uuid) -> RackId {
        RackId(uuid)
    }

    pub fn random() -> RackId {
        RackId(Uuid::new_v4())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
)]
pub struct Epoch(u64);

/// The number of shares required to reconstruct the rack secret
///
/// Typically referred to as `k` in the docs
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
)]
pub struct Threshold(pub u8);

/// A unique identifier for a given trust quorum member.
//
/// This data is derived from the subject common name in the platform identity
/// certificate that makes up part of the certificate chain used to establish
/// [sprockets](https://github.com/oxidecomputer/sprockets) connections.
///
/// See RFDs 303 and 308 for more details.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PlatformId {
    pub part_number: String,
    pub serial_number: String,
}

/// A container to make messages between trust quorum nodes routable
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Envelope {
    to: PlatformId,
    from: PlatformId,
    msg: PeerMsg,
}
