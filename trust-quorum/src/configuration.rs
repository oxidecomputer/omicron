// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A configuration of a trust quroum

use crate::{
    BaseboardId, EncryptedRackSecret, Epoch, RackId, ShareDigest, Threshold,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// The configuration for a given epoch
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Configuration {
    pub rack_uuid: RackId,
    pub epoch: Epoch,
    pub last_committed_epoch: Epoch,

    /// We pick the first member of epoch 0 as coordinator when initializing from
    /// lrtq so we don't have to use an option
    pub coordinator: BaseboardId,
    pub members: BTreeMap<BaseboardId, ShareDigest>,
    pub threshold: Threshold,

    // There is no encrypted data for epoch 0
    pub encrypted: Option<EncryptedData>,

    // This is only possibly set for epoch 0
    pub lrtq_upgrade_id: Option<Uuid>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct EncryptedData {
    /// The encrypted rack secret for the last committed epoch
    pub encrypted_last_committed_rack_secret: EncryptedRackSecret,

    /// A random value used to derive the key to encrypt the rack secret from
    /// the last committed epoch
    ///
    /// We only encrypt the shares once and so we use a nonce of all zeros
    pub encrypted_last_committed_rack_secret_salt: [u8; 32],
}
