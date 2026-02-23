// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo types for version PHYSICAL_STORAGE_QUOTAS.
//!
//! Adds `physical_storage` to quota types and physical provisioning fields
//! to utilization types.

use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Name, Nullable,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use uuid::Uuid;

use crate::v2025_11_20_00::certificate::CertificateCreate;
use crate::v2025_11_20_00::policy::{FleetRole, SiloRole};
use crate::v2025_11_20_00::silo::{SiloIdentityMode, VirtualResourceCounts};

/// A collection of resource counts used to set the virtual capacity of a silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotas {
    pub silo_id: Uuid,
    #[serde(flatten)]
    pub limits: VirtualResourceCounts,
    /// The amount of physical storage (in bytes) allocated for this silo.
    /// `None` means no limit is set.
    pub physical_storage: Option<i64>,
}

/// View of the current silo's resource utilization and capacity
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Utilization {
    /// Accounts for resources allocated to running instances or storage
    /// allocated via disks or snapshots.
    ///
    /// Note that CPU and memory resources associated with stopped instances
    /// are not counted here, whereas associated disks will still be counted.
    pub provisioned: VirtualResourceCounts,
    /// The total amount of resources that can be provisioned in this silo.
    /// Actions that would exceed this limit will fail.
    pub capacity: VirtualResourceCounts,
    /// Total physical disk bytes provisioned (including replication overhead)
    pub physical_disk_bytes_provisioned: i64,
    /// Physical storage capacity allocated via quota. `None` means no limit.
    pub physical_storage_allocated: Option<i64>,
}

/// View of a silo's resource utilization and capacity
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloUtilization {
    pub silo_id: Uuid,
    pub silo_name: Name,
    /// Accounts for the total resources allocated by the silo, including CPU
    /// and memory for running instances and storage for disks and snapshots.
    ///
    /// Note that CPU and memory resources associated with stopped instances
    /// are not counted here.
    pub provisioned: VirtualResourceCounts,
    /// Accounts for the total amount of resources reserved for silos via
    /// their quotas.
    pub allocated: VirtualResourceCounts,
    /// Total physical disk bytes provisioned (including replication overhead)
    pub physical_disk_bytes_provisioned: i64,
    /// Physical storage capacity allocated via quota. `None` means no limit.
    pub physical_storage_allocated: Option<i64>,
}

/// The amount of provisionable resources for a Silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasCreate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: i64,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: ByteCount,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: ByteCount,
    /// The amount of physical storage (in bytes) available for the Silo.
    /// `None` means no limit is enforced.
    #[serde(default)]
    pub physical_storage: Option<i64>,
}

/// Updateable properties of a Silo's resource limits.
/// If a value is omitted it will not be updated.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasUpdate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: Option<i64>,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: Option<ByteCount>,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: Option<ByteCount>,
    /// The amount of physical storage (in bytes) available for the Silo.
    /// Set to `null` to remove the limit.
    #[serde(default)]
    pub physical_storage: Option<Option<i64>>,
}

/// Create-time parameters for a `Silo`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub discoverable: bool,

    pub identity_mode: SiloIdentityMode,

    /// If set, this group will be created during Silo creation and granted the
    /// "Silo Admin" role. Identity providers can assert that users belong to
    /// this group and those users can log in and further initialize the Silo.
    ///
    /// Note that if configuring a SAML based identity provider,
    /// group_attribute_name must be set for users to be considered part of a
    /// group. See `SamlIdentityProviderCreate` for more information.
    pub admin_group_name: Option<String>,

    /// Initial TLS certificates to be used for the new Silo's console and API
    /// endpoints.  These should be valid for the Silo's DNS name(s).
    pub tls_certificates: Vec<CertificateCreate>,

    /// Limits the amount of provisionable CPU, memory, and storage in the Silo.
    /// CPU and memory are only consumed by running instances, while storage is
    /// consumed by any disk or snapshot. A value of 0 means that resource is
    /// *not* provisionable.
    pub quotas: SiloQuotasCreate,

    /// Mapping of which Fleet roles are conferred by each Silo role
    ///
    /// The default is that no Fleet roles are conferred by any Silo roles
    /// unless there's a corresponding entry in this map.
    #[serde(default)]
    pub mapped_fleet_roles: BTreeMap<SiloRole, BTreeSet<FleetRole>>,
}

/// Updateable properties of a silo's settings.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SiloAuthSettingsUpdate {
    /// Maximum lifetime of a device token in seconds. If set to null, users
    /// will be able to create tokens that do not expire.
    pub device_token_max_ttl_seconds: Nullable<NonZeroU32>,
}

// -- Conversions from the previous version (v2025_11_20_00) --

impl From<crate::v2025_11_20_00::silo::SiloQuotasCreate> for SiloQuotasCreate {
    fn from(old: crate::v2025_11_20_00::silo::SiloQuotasCreate) -> Self {
        let crate::v2025_11_20_00::silo::SiloQuotasCreate {
            cpus,
            memory,
            storage,
        } = old;
        Self { cpus, memory, storage, physical_storage: None }
    }
}

impl From<crate::v2025_11_20_00::silo::SiloQuotasUpdate> for SiloQuotasUpdate {
    fn from(old: crate::v2025_11_20_00::silo::SiloQuotasUpdate) -> Self {
        let crate::v2025_11_20_00::silo::SiloQuotasUpdate {
            cpus,
            memory,
            storage,
        } = old;
        Self { cpus, memory, storage, physical_storage: None }
    }
}

impl From<crate::v2025_11_20_00::silo::SiloCreate> for SiloCreate {
    fn from(old: crate::v2025_11_20_00::silo::SiloCreate) -> Self {
        let crate::v2025_11_20_00::silo::SiloCreate {
            identity,
            discoverable,
            identity_mode,
            admin_group_name,
            tls_certificates,
            quotas,
            mapped_fleet_roles,
        } = old;
        Self {
            identity,
            discoverable,
            identity_mode,
            admin_group_name,
            tls_certificates,
            quotas: quotas.into(),
            mapped_fleet_roles,
        }
    }
}

// -- Conversions from new → old (for backward-compat responses) --

impl From<SiloQuotas> for crate::v2025_11_20_00::silo::SiloQuotas {
    fn from(new: SiloQuotas) -> Self {
        let SiloQuotas { silo_id, limits, physical_storage: _ } = new;
        Self { silo_id, limits }
    }
}

impl From<Utilization> for crate::v2025_11_20_00::silo::Utilization {
    fn from(new: Utilization) -> Self {
        let Utilization {
            provisioned,
            capacity,
            physical_disk_bytes_provisioned: _,
            physical_storage_allocated: _,
        } = new;
        Self { provisioned, capacity }
    }
}

impl From<SiloUtilization> for crate::v2025_11_20_00::silo::SiloUtilization {
    fn from(new: SiloUtilization) -> Self {
        let SiloUtilization {
            silo_id,
            silo_name,
            provisioned,
            allocated,
            physical_disk_bytes_provisioned: _,
            physical_storage_allocated: _,
        } = new;
        Self { silo_id, silo_name, provisioned, allocated }
    }
}
