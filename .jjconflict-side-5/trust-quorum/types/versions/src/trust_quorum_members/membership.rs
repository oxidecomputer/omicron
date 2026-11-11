// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: module doc: the members of a trust quorum and the bounds on their number.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::configuration::BaseboardId;

/// The minimum number of members in a trust quorum.
pub const TRUST_QUORUM_MIN_MEMBERS: usize = 3;

/// The maximum number of members in a trust quorum.
pub const TRUST_QUORUM_MAX_MEMBERS: usize = 32;

// TODO-RAINCLAUDE: members of a trust quorum; invariant is TRUST_QUORUM_MIN_MEMBERS..=TRUST_QUORUM_MAX_MEMBERS sleds, enforced on construction and on deserialization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "BTreeSet<BaseboardId>", into = "BTreeSet<BaseboardId>")]
pub struct TrustQuorumMembers(
    #[schemars(length(
        min = "TRUST_QUORUM_MIN_MEMBERS",
        max = "TRUST_QUORUM_MAX_MEMBERS"
    ))]
    pub(crate) BTreeSet<BaseboardId>,
);

impl TrustQuorumMembers {
    // TODO-RAINCLAUDE: the only constructor; refuses sets outside the size bounds.
    pub fn new(
        members: BTreeSet<BaseboardId>,
    ) -> Result<Self, TrustQuorumMembersError> {
        let num_members = members.len();
        if num_members < TRUST_QUORUM_MIN_MEMBERS {
            Err(TrustQuorumMembersError::TooFew { num_members })
        } else if num_members > TRUST_QUORUM_MAX_MEMBERS {
            Err(TrustQuorumMembersError::TooMany { num_members })
        } else {
            Ok(Self(members))
        }
    }
}

impl TryFrom<BTreeSet<BaseboardId>> for TrustQuorumMembers {
    type Error = TrustQuorumMembersError;

    fn try_from(members: BTreeSet<BaseboardId>) -> Result<Self, Self::Error> {
        Self::new(members)
    }
}

impl From<TrustQuorumMembers> for BTreeSet<BaseboardId> {
    fn from(members: TrustQuorumMembers) -> Self {
        members.0
    }
}

// TODO-RAINCLAUDE: why a set of sleds can't be trust quorum members; both messages match the datastore check this replaces.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TrustQuorumMembersError {
    #[error(
        "Invalid Trust Quorum membership size ({num_members}): must be \
         between {TRUST_QUORUM_MIN_MEMBERS} and {TRUST_QUORUM_MAX_MEMBERS} \
         nodes"
    )]
    TooFew { num_members: usize },
    #[error(
        "Invalid Trust Quorum membership size ({num_members}): must be \
         between {TRUST_QUORUM_MIN_MEMBERS} and {TRUST_QUORUM_MAX_MEMBERS} \
         nodes"
    )]
    TooMany { num_members: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members_of_size(num_members: usize) -> BTreeSet<BaseboardId> {
        (0..num_members)
            .map(|i| BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: format!("member-{i}"),
            })
            .collect()
    }

    #[test]
    fn test_trust_quorum_members_size_bounds() {
        for num_members in [0, 1, 2] {
            assert_eq!(
                TrustQuorumMembers::new(members_of_size(num_members)),
                Err(TrustQuorumMembersError::TooFew { num_members }),
            );
        }
        for num_members in [3, 4, 31, 32] {
            let members = TrustQuorumMembers::new(members_of_size(num_members))
                .expect("member count within the size bounds is accepted");
            assert_eq!(usize::from(members.num_members()), num_members);
            assert_eq!(members.into_set(), members_of_size(num_members));
        }
        for num_members in [33, 64] {
            assert_eq!(
                TrustQuorumMembers::new(members_of_size(num_members)),
                Err(TrustQuorumMembersError::TooMany { num_members }),
            );
        }
    }

    #[test]
    fn test_trust_quorum_members_deserialize_enforces_size_bounds() {
        for num_members in [2, 33] {
            let json = serde_json::to_string(&members_of_size(num_members))
                .expect("serialized member set");
            let error = serde_json::from_str::<TrustQuorumMembers>(&json)
                .expect_err("out-of-bounds member set is rejected");
            assert!(
                error.to_string().starts_with(&format!(
                    "Invalid Trust Quorum membership size ({num_members})"
                )),
                "unexpected error: {error}",
            );
        }

        let members = TrustQuorumMembers::new(members_of_size(3))
            .expect("three members are within the size bounds");
        let json = serde_json::to_string(&members).expect("serialized members");
        assert_eq!(
            json,
            serde_json::to_string(&members_of_size(3))
                .expect("serialized member set"),
        );
        assert_eq!(
            serde_json::from_str::<TrustQuorumMembers>(&json)
                .expect("deserialized members"),
            members,
        );
    }
}
