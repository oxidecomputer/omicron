// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack types.

pub use nexus_types_versions::latest::rack::*;

use crate::trust_quorum::{TrustQuorumConfig, TrustQuorumMemberState};
use omicron_uuid_kinds::GenericUuid;

impl From<TrustQuorumConfig> for RackMembershipStatus {
    fn from(value: TrustQuorumConfig) -> Self {
        // `Unacked` means that a member has not received and acked a `Prepare`
        // yet. `Prepared` means that a member has acknowledged the prepare but
        // not the commit. `Committed` is when the member starts participating
        // in the new group.
        //
        // Since we don't want to expose trust quorum specific knowledge to
        // the operator, and they really only want to know when the membership
        // change has started to take effect, we say that any member that hasn't
        // yet committed is unacknowledged.
        let unacknowledged_members = value
            .members
            .iter()
            .filter_map(|(id, data)| match data.state {
                TrustQuorumMemberState::Unacked
                | TrustQuorumMemberState::Prepared => Some(id.clone()),
                TrustQuorumMemberState::Committed => None,
            })
            .collect();
        let state = if value.state.is_committed() {
            RackMembershipChangeState::Committed
        } else if value.state.is_aborted() {
            RackMembershipChangeState::Aborted
        } else {
            RackMembershipChangeState::InProgress
        };

        Self {
            rack_id: value.rack_id.into_untyped_uuid(),
            version: RackMembershipVersion(value.epoch.0),
            state,
            members: value.members.keys().cloned().collect(),
            unacknowledged_members,
            time_created: value.time_created,
            time_committed: value.time_committed,
            time_aborted: value.time_aborted,
        }
    }
}
