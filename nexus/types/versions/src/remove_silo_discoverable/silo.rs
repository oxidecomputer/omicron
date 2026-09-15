// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::certificate::CertificateCreate;
use crate::v2025_11_20_00::policy::{FleetRole, SiloRole};
use crate::v2025_11_20_00::silo::{SiloIdentityMode, SiloQuotasCreate};
use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Create-time parameters for a `Silo`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

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

impl From<v2025_11_20_00::silo::SiloCreate> for SiloCreate {
    fn from(old: v2025_11_20_00::silo::SiloCreate) -> Self {
        // Intentionally ignore old.discoverable: silos created through any
        // external API version are now always discoverable.
        Self {
            identity: old.identity,
            identity_mode: old.identity_mode,
            admin_group_name: old.admin_group_name,
            tls_certificates: old.tls_certificates,
            quotas: old.quotas,
            mapped_fleet_roles: old.mapped_fleet_roles,
        }
    }
}
