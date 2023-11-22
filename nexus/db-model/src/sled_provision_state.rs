// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::{Deserialize, Serialize};
use thiserror::Error;

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sled_provision_state"))]
    pub struct SledProvisionStateEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SledProvisionStateEnum)]
    pub enum SledProvisionState;

    // Enum values
    Provisionable => b"provisionable"
    NotProvisionable => b"not_provisionable"
);

impl From<SledProvisionState> for views::SledProvisionState {
    fn from(state: SledProvisionState) -> Self {
        match state {
            SledProvisionState::Provisionable => {
                views::SledProvisionState::Provisionable
            }
            SledProvisionState::NotProvisionable => {
                views::SledProvisionState::NonProvisionable
            }
        }
    }
}

impl TryFrom<views::SledProvisionState> for SledProvisionState {
    type Error = UnknownSledProvisionState;

    fn try_from(state: views::SledProvisionState) -> Result<Self, Self::Error> {
        match state {
            views::SledProvisionState::Provisionable => {
                Ok(SledProvisionState::Provisionable)
            }
            views::SledProvisionState::NonProvisionable => {
                Ok(SledProvisionState::NotProvisionable)
            }
            views::SledProvisionState::Unknown => {
                Err(UnknownSledProvisionState)
            }
        }
    }
}

/// An unknown [`views::SledProvisionState`] was encountered.
#[derive(Clone, Debug, Error)]
#[error("Unknown SledProvisionState")]
pub struct UnknownSledProvisionState;
