// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer-maintained API metadata

use crate::ClientPackageName;
use crate::DeploymentUnit;
use crate::ServerComponent;
use crate::ServerPackageName;
use anyhow::bail;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(try_from = "RawApiMetadata")]
pub struct AllApiMetadata {
    apis: BTreeMap<ClientPackageName, ApiMetadata>,
    deployment_units: BTreeMap<DeploymentUnit, DeploymentUnitInfo>,
}

impl AllApiMetadata {
    pub fn apis(&self) -> impl Iterator<Item = &ApiMetadata> {
        self.apis.values()
    }

    pub fn deployment_units(
        &self,
    ) -> impl Iterator<Item = (&DeploymentUnit, &DeploymentUnitInfo)> {
        self.deployment_units.iter()
    }

    pub fn client_pkgnames(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.apis.keys()
    }

    pub fn server_components(&self) -> impl Iterator<Item = &ServerComponent> {
        self.deployment_units.values().flat_map(|d| d.packages.iter())
    }

    pub fn client_pkgname_lookup(
        &self,
        pkgname: &ClientPackageName,
    ) -> Option<&ApiMetadata> {
        self.apis.get(pkgname)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawApiMetadata {
    apis: Vec<ApiMetadata>,
    deployment_units: Vec<DeploymentUnitInfo>,
}

impl TryFrom<RawApiMetadata> for AllApiMetadata {
    type Error = anyhow::Error;

    fn try_from(raw: RawApiMetadata) -> anyhow::Result<AllApiMetadata> {
        let mut apis = BTreeMap::new();

        for api in raw.apis {
            if let Some(previous) =
                apis.insert(api.client_package_name.clone(), api)
            {
                bail!(
                    "duplicate client package name in API metadata: {}",
                    &previous.client_package_name,
                );
            }
        }

        let mut deployment_units = BTreeMap::new();
        for info in raw.deployment_units {
            if let Some(previous) =
                deployment_units.insert(info.label.clone(), info)
            {
                bail!(
                    "duplicate deployment unit in API metadata: {}",
                    &previous.label,
                );
            }
        }

        Ok(AllApiMetadata { apis, deployment_units })
    }
}

#[derive(Deserialize)]
pub struct ApiMetadata {
    /// primary key for APIs is the client package name
    pub client_package_name: ClientPackageName,
    /// human-readable label for the API
    pub label: String,
    /// package name that the corresponding API lives in
    pub server_package_name: ServerPackageName,
    /// human-readable notes about this API
    pub notes: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentUnitInfo {
    /// human-readable label, also used as primary key
    pub label: DeploymentUnit,
    /// list of Rust packages that are shipped in this unit
    pub packages: Vec<ServerComponent>,
}
