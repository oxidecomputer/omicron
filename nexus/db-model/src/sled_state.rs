// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a sled's state as understood by Nexus.
//!
//! This is related to, but different from `SledState`: a sled's **policy** is
//! its disposition as specified by the operator, while its **state** refers to
//! what's currently on it, as determined by Nexus.
//!
//! For example, a sled might be in the `Active` state, but have a policy of
//! `Expunged` -- this would mean that Nexus knows about resources currently
//! provisioned on the sled, but the operator has said that it should be marked
//! as gone.

use super::impl_enum_type;
use nexus_types::external_api::sled;
use serde::{Deserialize, Serialize};
use std::fmt;
use strum::EnumIter;

impl_enum_type!(
    SledStateEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq, Eq, EnumIter)]
    pub enum SledState;

    // Enum values
    Active => b"active"
    Decommissioned => b"decommissioned"
);

impl fmt::Display for SledState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Forward to the canonical implementation in nexus-types.
        sled::SledState::from(*self).fmt(f)
    }
}

impl From<SledState> for sled::SledState {
    fn from(state: SledState) -> Self {
        match state {
            SledState::Active => sled::SledState::Active,
            SledState::Decommissioned => sled::SledState::Decommissioned,
        }
    }
}

impl From<sled::SledState> for SledState {
    fn from(state: sled::SledState) -> Self {
        match state {
            sled::SledState::Active => SledState::Active,
            sled::SledState::Decommissioned => SledState::Decommissioned,
        }
    }
}
