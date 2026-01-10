// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for version POOL_SELECTION_ENUMS.
//!
//! This version changes probe creation to use `PoolSelector` instead of a flat
//! `ip_pool` field.

use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::ip_pool::PoolSelector;
use crate::v2026010300;

/// Create time parameters for probes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProbeCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
    /// Pool to allocate from.
    #[serde(default)]
    pub pool_selector: PoolSelector,
}

impl From<v2026010300::probe::ProbeCreate> for ProbeCreate {
    fn from(old: v2026010300::probe::ProbeCreate) -> ProbeCreate {
        let pool_selector = match old.ip_pool {
            Some(pool) => PoolSelector::Explicit { pool },
            None => PoolSelector::Auto { ip_version: None },
        };
        ProbeCreate { identity: old.identity, sled: old.sled, pool_selector }
    }
}
