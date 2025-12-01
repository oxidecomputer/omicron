// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use propolis_client::instance_spec::{
    ComponentV0, CrucibleStorageBackend, FileStorageBackend, InstanceSpecV0,
    SpecKey, VirtioNetworkBackend,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/// Metadata used to track statistics about an instance.
///
// NOTE: The instance ID is not here, since it's already provided in other
// pieces of the instance-related requests. It is pulled from there when
// publishing metrics for the instance.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InstanceMetadata {
    pub silo_id: Uuid,
    pub project_id: Uuid,
}

/// Represents a multicast group membership for an instance.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct InstanceMulticastMembership {
    pub group_ip: IpAddr,
    // For Source-Specific Multicast (SSM)
    pub sources: Vec<IpAddr>,
}

/// Specifies the virtual hardware configuration of a new Propolis VMM in the
/// form of a Propolis instance specification.
///
/// Sled-agent expects that when an instance spec is provided alongside an
/// `InstanceSledLocalConfig` to initialize a new instance, the NIC IDs in that
/// config's network interface list will match the IDs of the virtio network
/// backends in the instance spec.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VmmSpec(pub InstanceSpecV0);

impl VmmSpec {
    pub fn crucible_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &CrucibleStorageBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                ComponentV0::CrucibleStorageBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    pub fn viona_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &VirtioNetworkBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                ComponentV0::VirtioNetworkBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    pub fn file_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &FileStorageBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                ComponentV0::FileStorageBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }
}
