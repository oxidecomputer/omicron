// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sled_state", schema = "public"))]
    pub struct SledStateEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq, Eq, EnumIter)]
    #[diesel(sql_type = SledStateEnum)]
    pub enum SledState;

    // Enum values
    Active => b"active"
    Decommissioned => b"decommissioned"
);

impl From<SledState> for views::SledState {
    fn from(state: SledState) -> Self {
        match state {
            SledState::Active => views::SledState::Active,
            SledState::Decommissioned => views::SledState::Decommissioned,
        }
    }
}

impl From<views::SledState> for SledState {
    fn from(state: views::SledState) -> Self {
        match state {
            views::SledState::Active => SledState::Active,
            views::SledState::Decommissioned => SledState::Decommissioned,
        }
    }
}
