// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a physical disk's state as understood by Nexus.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::{Deserialize, Serialize};
use std::fmt;
use strum::EnumIter;

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "physical_disk_state", schema = "public"))]
    pub struct PhysicalDiskStateEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq, Eq, EnumIter)]
    #[diesel(sql_type = PhysicalDiskStateEnum)]
    pub enum PhysicalDiskState;

    // Enum values
    Active => b"active"
    Decommissioned => b"decommissioned"
);

impl fmt::Display for PhysicalDiskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Forward to the canonical implementation in nexus-types.
        views::PhysicalDiskState::from(*self).fmt(f)
    }
}

impl From<PhysicalDiskState> for views::PhysicalDiskState {
    fn from(state: PhysicalDiskState) -> Self {
        match state {
            PhysicalDiskState::Active => views::PhysicalDiskState::Active,
            PhysicalDiskState::Decommissioned => {
                views::PhysicalDiskState::Decommissioned
            }
        }
    }
}

impl From<views::PhysicalDiskState> for PhysicalDiskState {
    fn from(state: views::PhysicalDiskState) -> Self {
        match state {
            views::PhysicalDiskState::Active => PhysicalDiskState::Active,
            views::PhysicalDiskState::Decommissioned => {
                PhysicalDiskState::Decommissioned
            }
        }
    }
}
