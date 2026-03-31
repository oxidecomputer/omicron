// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddr;

use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_uuid_kinds::InstanceUuid;
use propolis_api_types::instance_spec::InstanceSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v18;
use crate::v18::instance::InstanceSledLocalConfig;

/// Specifies the virtual hardware configuration of a new Propolis VMM in the
/// form of a Propolis instance specification.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VmmSpec(pub InstanceSpec);

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

impl From<v18::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(old: v18::instance::InstanceEnsureBody) -> InstanceEnsureBody {
        // Conversion goes v1 -> v2 -> v3 (latest) through propolis's
        // versioned From impls.
        let v2_spec: propolis_api_types_versions::v2::instance_spec::InstanceSpec =
            old.vmm_spec.0.into();
        let v3_spec: InstanceSpec = v2_spec.into();
        InstanceEnsureBody {
            vmm_spec: VmmSpec(v3_spec),
            local_config: old.local_config,
            vmm_runtime: old.vmm_runtime,
            instance_id: old.instance_id,
            migration_id: old.migration_id,
            propolis_addr: old.propolis_addr,
            metadata: old.metadata,
        }
    }
}
