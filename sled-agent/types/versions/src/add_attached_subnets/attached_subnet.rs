// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::IdOrdMap;
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
    /// Is this is a subnet in the external customer network.
    ///
    /// If false, this is a VPC Subnet attached to the instance.
    pub is_external: bool,
}

impl iddqd::IdOrdItem for AttachedSubnet {
    type Key<'a> = &'a IpNet;

    fn key(&self) -> Self::Key<'_> {
        &self.subnet
    }

    iddqd::id_upcast!();
}

/// Path parameters for referring to a single subnet attached to an instance.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct VmmSubnetPathParam {
    pub propolis_id: PropolisUuid,
    pub subnet: IpNet,
}
