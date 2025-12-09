// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types describing the state of trust quorum in Nexus

use std::collections::BTreeMap;

use omicron_uuid_kinds::RackUuid;
use trust_quorum_protocol::{
    BaseboardId, EncryptedRackSecrets, Epoch, Sha3_256Digest, Threshold,
};

#[derive(Debug, Clone)]
pub enum TrustQuorumConfigState {
    Preparing,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub enum TrustQuorumMemberState {
    Unacked,
    Prepared,
    Committed,
}

#[derive(Debug, Clone)]
pub struct TrustQuorumMemberData {
    pub state: TrustQuorumMemberState,

    // Only filled in once the coordinator state is succesfully polled by nexus
    // after it has created the configuration.
    pub digest: Option<Sha3_256Digest>,
}

#[derive(Debug, Clone)]
pub struct TrustQuorumConfig {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub state: TrustQuorumConfigState,
    pub threshold: Threshold,
    pub commit_crash_tolerance: u8,
    pub coordinator: BaseboardId,
    pub encrypted_rack_secrets: Option<EncryptedRackSecrets>,
    pub members: BTreeMap<BaseboardId, TrustQuorumMemberData>,
}
