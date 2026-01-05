// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network interface types version 1

use std::net::IpAddr;

use crate::api::external;
use crate::api::external::Name;
use crate::api::external::Vni;
use crate::api::internal::shared::NetworkInterfaceKind;
use daft::Diffable;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Information required to construct a virtual network interface
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
pub struct NetworkInterface {
    pub id: Uuid,
    pub kind: NetworkInterfaceKind,
    pub name: Name,
    pub ip: IpAddr,
    pub mac: external::MacAddr,
    pub subnet: IpNet,
    pub vni: Vni,
    pub primary: bool,
    pub slot: u8,
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
}
