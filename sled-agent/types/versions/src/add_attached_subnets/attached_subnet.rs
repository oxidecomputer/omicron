// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::IdOrdMap;
use illumos_utils::opte::cidr_to_net;
use illumos_utils::opte::net_to_cidr;
use omicron_uuid_kinds::PropolisUuid;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Subnets attached to a single instance.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AttachedSubnets {
    pub subnets: IdOrdMap<AttachedSubnet>,
}

/// A subnet attached to a single instance.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct AttachedSubnet {
    /// The IP subnet.
    pub subnet: IpNet,
    /// The kind of subnet that is attached.
    pub kind: AttachedSubnetKind,
}

impl iddqd::IdOrdItem for AttachedSubnet {
    type Key<'a> = &'a IpNet;

    fn key(&self) -> Self::Key<'_> {
        &self.subnet
    }

    iddqd::id_upcast!();
}

impl From<AttachedSubnet> for illumos_utils::opte::AttachedSubnet {
    fn from(value: AttachedSubnet) -> Self {
        Self { cidr: net_to_cidr(value.subnet), kind: value.kind.into() }
    }
}

impl From<illumos_utils::opte::AttachedSubnet> for AttachedSubnet {
    fn from(value: illumos_utils::opte::AttachedSubnet) -> Self {
        Self { subnet: cidr_to_net(value.cidr), kind: value.kind.into() }
    }
}

/// The kind of attached subnet.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachedSubnetKind {
    /// This is a VPC subnet.
    Vpc,
    /// This is an external subnet.
    External,
}

impl From<AttachedSubnetKind> for illumos_utils::opte::AttachedSubnetKind {
    fn from(value: AttachedSubnetKind) -> Self {
        match value {
            AttachedSubnetKind::Vpc => Self::Vpc,
            AttachedSubnetKind::External => Self::External,
        }
    }
}

impl From<illumos_utils::opte::AttachedSubnetKind> for AttachedSubnetKind {
    fn from(value: illumos_utils::opte::AttachedSubnetKind) -> Self {
        match value {
            illumos_utils::opte::AttachedSubnetKind::Vpc => Self::Vpc,
            illumos_utils::opte::AttachedSubnetKind::External => Self::External,
        }
    }
}

/// Path parameters for referring to a single subnet attached to an instance.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct VmmSubnetPathParam {
    pub propolis_id: PropolisUuid,
    pub subnet: IpNet,
}
