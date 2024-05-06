// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{collections::BTreeSet, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// TODO: unify this with the one in gateway http_entrypoints.rs.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct SpIdentifier {
    #[serde(rename = "type")]
    pub type_: SpType,
    pub slot: u32,
}

impl From<SpIdentifier> for gateway_client::types::SpIdentifier {
    fn from(value: SpIdentifier) -> Self {
        Self { type_: value.type_.into(), slot: value.slot }
    }
}

impl From<gateway_client::types::SpIdentifier> for SpIdentifier {
    fn from(value: gateway_client::types::SpIdentifier) -> Self {
        Self { type_: value.type_.into(), slot: value.slot }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum SpType {
    Switch,
    Sled,
    Power,
}

impl From<SpType> for gateway_client::types::SpType {
    fn from(value: SpType) -> Self {
        match value {
            SpType::Switch => Self::Switch,
            SpType::Sled => Self::Sled,
            SpType::Power => Self::Power,
        }
    }
}

impl From<gateway_client::types::SpType> for SpType {
    fn from(value: gateway_client::types::SpType) -> Self {
        match value {
            gateway_client::types::SpType::Switch => Self::Switch,
            gateway_client::types::SpType::Sled => Self::Sled,
            gateway_client::types::SpType::Power => Self::Power,
        }
    }
}

impl fmt::Display for SpType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpType::Switch => write!(f, "switch"),
            SpType::Sled => write!(f, "sled"),
            SpType::Power => write!(f, "power"),
        }
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
pub struct ClearUpdateStateResponse {
    /// The SPs for which update data was cleared.
    pub cleared: BTreeSet<SpIdentifier>,

    /// The SPs that had no update state to clear.
    pub no_update_data: BTreeSet<SpIdentifier>,
}
