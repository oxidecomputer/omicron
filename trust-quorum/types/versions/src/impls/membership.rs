// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for TrustQuorumMembers.

use std::collections::{BTreeSet, btree_set};

use crate::latest::configuration::BaseboardId;
use crate::latest::membership::TrustQuorumMembers;

impl TrustQuorumMembers {
    pub fn num_members(&self) -> u8 {
        u8::try_from(self.0.len())
            .expect("TRUST_QUORUM_MAX_MEMBERS fits in a u8")
    }

    pub fn contains(&self, member: &BaseboardId) -> bool {
        self.0.contains(member)
    }

    pub fn iter(&self) -> btree_set::Iter<'_, BaseboardId> {
        self.0.iter()
    }

    pub fn as_set(&self) -> &BTreeSet<BaseboardId> {
        &self.0
    }

    pub fn into_set(self) -> BTreeSet<BaseboardId> {
        self.0
    }
}

impl<'a> IntoIterator for &'a TrustQuorumMembers {
    type Item = &'a BaseboardId;
    type IntoIter = btree_set::Iter<'a, BaseboardId>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
