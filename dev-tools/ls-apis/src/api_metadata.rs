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
use std::collections::BTreeSet;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(try_from = "RawApiMetadata")]
pub struct AllApiMetadata {
    apis: BTreeMap<ClientPackageName, ApiMetadata>,
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

        Ok(AllApiMetadata { apis })
    }
}

impl AllApiMetadata {
    pub fn apis(&self) -> impl Iterator<Item = &ApiMetadata> {
        self.apis.values()
    }

    pub fn client_pkgnames(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.apis.keys()
    }

    pub fn server_components(&self) -> impl Iterator<Item = &ServerComponent> {
        // There may be duplicates here.  That's not a mistake.  But we don't
        // want to report duplicates here.
        self.apis()
            .map(|api| &api.server_component)
            .collect::<BTreeSet<_>>()
            .into_iter()
    }

    pub fn client_pkgname_lookup(
        &self,
        pkgname: &ClientPackageName,
    ) -> Option<&ApiMetadata> {
        self.apis.get(pkgname)
    }
}

#[derive(Deserialize)]
pub struct ApiMetadata {
    /// primary key for APIs is the client package name
    pub client_package_name: ClientPackageName,
    /// human-readable label for the API
    pub label: String,
    /// package name that the corresponding API lives in
    // XXX-dap unused right now
    pub server_package_name: ServerPackageName,
    /// package name that the corresponding server lives in
    pub server_component: ServerComponent,
    /// name of the unit of deployment
    deployment_unit: Option<DeploymentUnit>,
    /// human-readable notes about this API
    pub notes: Option<String>,
}

impl ApiMetadata {
    pub fn deployment_unit(&self) -> DeploymentUnit {
        self.deployment_unit
            .clone()
            .unwrap_or_else(|| (*self.server_component).clone().into())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawApiMetadata {
    apis: Vec<ApiMetadata>,
}
