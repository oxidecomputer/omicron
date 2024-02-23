// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "physical_disk_state"))]
    pub struct PhysicalDiskStateEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = PhysicalDiskStateEnum)]
    pub enum PhysicalDiskState;

    // Enum values
    Active => b"active"
    Draining => b"draining"
    Inactive => b"inactive"
);

impl From<PhysicalDiskState> for views::PhysicalDiskState {
    fn from(state: PhysicalDiskState) -> Self {
        use views::PhysicalDiskState as api;
        use PhysicalDiskState as db;
        match state {
            db::Active => api::Active,
            db::Draining => api::Draining,
            db::Inactive => api::Inactive,
        }
    }
}
