// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Status information about the multirack join service

use std::net::Ipv6Addr;

use iddqd::{BiHashItem, BiHashMap, bi_upcast};
use omicron_common::address::{Ipv6Subnet, RACK_PREFIX_LENGTH};
use omicron_uuid_kinds::{RackUuid, SledUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct MultirackJoinServiceStatus {
    pub sled_id: SledUuid,
    pub rack_id: RackUuid,
    pub rack_subnet: Ipv6Subnet<RACK_PREFIX_LENGTH>,
    pub sled_agents: BiHashMap<SledAgentInfo>,
}

/// Status information for a given sled agent
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SledAgentInfo {
    pub baseboard_id: BaseboardId,
    pub sled_id: SledUuid,
    pub address: Ipv6Addr,
    pub start_state: SledAgentStartState,
}

impl BiHashItem for SledAgentInfo {
    type K1<'a> = &'a BaseboardId;
    type K2<'a> = &'a SledUuid;

    fn key1(&self) -> Self::K1<'_> {
        &self.baseboard_id
    }

    fn key2(&self) -> Self::K2<'_> {
        &self.sled_id
    }

    bi_upcast!();
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SledAgentStartState {
    InProgress { last_error: Option<String> },
    Started,
}
