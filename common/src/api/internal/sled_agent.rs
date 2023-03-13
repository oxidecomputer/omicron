// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for APIs exposed by Sled Agent.

use crate::api::external;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Information required to construct a virtual network interface for
/// a guest instance or service zone.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterface {
    pub name: external::Name,
    pub ip: IpAddr,
    pub mac: external::MacAddr,
    pub subnet: external::IpNet,
    pub vni: external::Vni,
    pub primary: bool,
    pub slot: u8,
}
