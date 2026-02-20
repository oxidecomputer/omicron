// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for sled types.

use crate::latest::sled::{SledPolicy, SledProvisionPolicy, SledState};
use std::fmt;
use strum::IntoEnumIterator;

impl SledProvisionPolicy {
    /// Returns the opposite of the current provision state.
    pub const fn invert(self) -> Self {
        match self {
            Self::Provisionable => Self::NonProvisionable,
            Self::NonProvisionable => Self::Provisionable,
        }
    }
}

// Can't automatically derive strum::EnumIter because that doesn't provide a
// way to iterate over nested enums.
impl IntoEnumIterator for SledPolicy {
    type Iterator = std::array::IntoIter<Self, 3>;

    fn iter() -> Self::Iterator {
        [
            Self::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
            Self::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
            Self::Expunged,
        ]
        .into_iter()
    }
}

impl SledPolicy {
    /// Creates a new `SledPolicy` that is in-service and provisionable.
    pub fn provisionable() -> Self {
        Self::InService { provision_policy: SledProvisionPolicy::Provisionable }
    }

    /// Returns the provision policy, if the sled is in service.
    pub fn provision_policy(&self) -> Option<SledProvisionPolicy> {
        match self {
            Self::InService { provision_policy } => Some(*provision_policy),
            Self::Expunged => None,
        }
    }

    /// Returns true if the sled can be decommissioned with this policy
    ///
    /// This is a method here, rather than being a variant on `SledFilter`,
    /// because the "decommissionable" condition only has meaning for policies,
    /// not states.
    pub fn is_decommissionable(&self) -> bool {
        // This should be kept in sync with `all_decommissionable` below.
        match self {
            Self::InService { .. } => false,
            Self::Expunged => true,
        }
    }

    /// Returns all the possible policies a sled can have for it to be
    /// decommissioned.
    ///
    /// This is a method here, rather than being a variant on `SledFilter`,
    /// because the "decommissionable" condition only has meaning for policies,
    /// not states.
    pub fn all_decommissionable() -> &'static [Self] {
        &[Self::Expunged]
    }
}

impl fmt::Display for SledPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            } => write!(f, "in service"),
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            } => write!(f, "not provisionable"),
            SledPolicy::Expunged => write!(f, "expunged"),
        }
    }
}

impl fmt::Display for SledState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledState::Active => write!(f, "active"),
            SledState::Decommissioned => write!(f, "decommissioned"),
        }
    }
}
