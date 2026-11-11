// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for Sled Agent API versions 43.

use std::net::SocketAddr;

use omicron_uuid_kinds::InstanceUuid;

use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::VmmRuntimeState;
use crate::v29;
use crate::v41;
use crate::v41::instance::InstanceSledLocalConfig;
use propolis_api_types_versions::v6::instance_spec::InstanceSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Specifies the virtual hardware configuration of a new Propolis VMM in the
/// form of a Propolis instance specification.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VmmSpec(pub InstanceSpec);

impl From<v29::instance::VmmSpec> for VmmSpec {
    fn from(other: v29::instance::VmmSpec) -> VmmSpec {
        let v6_spec: InstanceSpec = other.0.into();
        VmmSpec(v6_spec)
    }
}

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// The virtual hardware configuration this virtual machine should have when
    /// it is started.
    pub vmm_spec: VmmSpec,

    /// Information about the sled-local configuration that needs to be
    /// established to make the VM's virtual hardware fully functional.
    pub local_config: InstanceSledLocalConfig,

    /// The initial VMM runtime state for the VMM being registered.
    pub vmm_runtime: VmmRuntimeState,

    /// The ID of the instance for which this VMM is being created.
    pub instance_id: InstanceUuid,

    /// The ID of the migration in to this VMM, if this VMM is being
    /// ensured is part of a migration in. If this is `None`, the VMM is not
    /// being created due to a migration.
    pub migration_id: Option<Uuid>,

    /// The address at which this VMM should serve a Propolis server API.
    pub propolis_addr: SocketAddr,

    /// Metadata used to track instance statistics.
    pub metadata: InstanceMetadata,
}

impl From<v41::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(v41: v41::instance::InstanceEnsureBody) -> InstanceEnsureBody {
        InstanceEnsureBody {
            vmm_spec: v41.vmm_spec.into(),
            local_config: v41.local_config,
            vmm_runtime: v41.vmm_runtime,
            instance_id: v41.instance_id,
            migration_id: v41.migration_id,
            propolis_addr: v41.propolis_addr,
            metadata: v41.metadata,
        }
    }
}
