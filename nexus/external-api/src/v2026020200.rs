// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026020200 (`TRUST_QUORUM_ABORT_CONFIG`) that changed
//! in version 2026021100 (`PHYSICAL_STORAGE_QUOTAS`).
//!
//! The new version adds physical storage fields to quota and utilization types.

use nexus_types::external_api::{params, shared, views};
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Name, SimpleIdentityOrName,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// A collection of resource counts used to set the virtual capacity of a silo
///
/// Pre-`PHYSICAL_STORAGE_QUOTAS`: no `physical_storage` field.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotas {
    pub silo_id: Uuid,
    #[serde(flatten)]
    pub limits: views::VirtualResourceCounts,
}

impl From<views::SiloQuotas> for SiloQuotas {
    fn from(new: views::SiloQuotas) -> Self {
        let views::SiloQuotas { silo_id, limits, physical_storage: _ } = new;
        Self { silo_id, limits }
    }
}

/// View of the current silo's resource utilization and capacity
///
/// Pre-`PHYSICAL_STORAGE_QUOTAS`: no physical provisioning fields.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Utilization {
    pub provisioned: views::VirtualResourceCounts,
    pub capacity: views::VirtualResourceCounts,
}

impl From<views::Utilization> for Utilization {
    fn from(new: views::Utilization) -> Self {
        let views::Utilization {
            provisioned,
            capacity,
            physical_disk_bytes_provisioned: _,
            physical_storage_allocated: _,
        } = new;
        Self { provisioned, capacity }
    }
}

/// View of a silo's resource utilization and capacity
///
/// Pre-`PHYSICAL_STORAGE_QUOTAS`: no physical provisioning fields.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloUtilization {
    pub silo_id: Uuid,
    pub silo_name: Name,
    pub provisioned: views::VirtualResourceCounts,
    pub allocated: views::VirtualResourceCounts,
}

impl SimpleIdentityOrName for SiloUtilization {
    fn id(&self) -> Uuid {
        self.silo_id
    }
    fn name(&self) -> &Name {
        &self.silo_name
    }
}

impl From<views::SiloUtilization> for SiloUtilization {
    fn from(new: views::SiloUtilization) -> Self {
        let views::SiloUtilization {
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

/// The amount of provisionable resources for a Silo
///
/// Pre-`PHYSICAL_STORAGE_QUOTAS`: no `physical_storage` field.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasCreate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: i64,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: ByteCount,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: ByteCount,
}

impl From<SiloQuotasCreate> for params::SiloQuotasCreate {
    fn from(old: SiloQuotasCreate) -> Self {
        let SiloQuotasCreate { cpus, memory, storage } = old;
        params::SiloQuotasCreate {
            cpus,
            memory,
            storage,
            physical_storage: None,
        }
    }
}

/// Updateable properties of a Silo's resource limits.
/// If a value is omitted it will not be updated.
///
/// Pre-`PHYSICAL_STORAGE_QUOTAS`: no `physical_storage` field.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasUpdate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: Option<i64>,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: Option<ByteCount>,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: Option<ByteCount>,
}

impl From<SiloQuotasUpdate> for params::SiloQuotasUpdate {
    fn from(old: SiloQuotasUpdate) -> Self {
        let SiloQuotasUpdate { cpus, memory, storage } = old;
        params::SiloQuotasUpdate {
            cpus,
            memory,
            storage,
            physical_storage: None,
        }
    }
}

/// Create-time parameters for a `Silo`
///
/// Pre-`PHYSICAL_STORAGE_QUOTAS`: uses old `SiloQuotasCreate` without
/// `physical_storage`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub discoverable: bool,

    pub identity_mode: shared::SiloIdentityMode,

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
    pub tls_certificates: Vec<params::CertificateCreate>,

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
    pub mapped_fleet_roles:
        BTreeMap<shared::SiloRole, BTreeSet<shared::FleetRole>>,
}

impl From<SiloCreate> for params::SiloCreate {
    fn from(old: SiloCreate) -> Self {
        let SiloCreate {
            identity,
            discoverable,
            identity_mode,
            admin_group_name,
            tls_certificates,
            quotas,
            mapped_fleet_roles,
        } = old;
        params::SiloCreate {
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
