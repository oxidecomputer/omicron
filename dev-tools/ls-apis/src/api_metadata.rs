// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer-maintained API metadata

use crate::cargo::DepPath;
use crate::workspaces::Workspaces;
use crate::ClientPackageName;
use crate::DeploymentUnitName;
use crate::ServerComponentName;
use crate::ServerPackageName;
use anyhow::{bail, Result};
use serde::Deserialize;
use std::borrow::Borrow;
use std::collections::BTreeMap;

/// Describes the APIs in the system
///
/// This is the programmatic interface to the `api-manifest.toml` file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(try_from = "RawApiMetadata")]
pub struct AllApiMetadata {
    apis: BTreeMap<ClientPackageName, ApiMetadata>,
    deployment_units: BTreeMap<DeploymentUnitName, DeploymentUnitInfo>,
    dependency_rules: BTreeMap<ClientPackageName, Vec<DependencyFilterRule>>,
}

impl AllApiMetadata {
    /// Iterate over the distinct APIs described by the metadata
    pub fn apis(&self) -> impl Iterator<Item = &ApiMetadata> {
        self.apis.values()
    }

    /// Iterate over the deployment units defined in the metadata
    pub fn deployment_units(
        &self,
    ) -> impl Iterator<Item = (&DeploymentUnitName, &DeploymentUnitInfo)> {
        self.deployment_units.iter()
    }

    /// Iterate over the package names for all the APIs' clients
    pub fn client_pkgnames(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.apis.keys()
    }

    /// Iterate over the package names for all the APIs' servers
    pub fn server_components(
        &self,
    ) -> impl Iterator<Item = &ServerComponentName> {
        self.deployment_units.values().flat_map(|d| d.packages.iter())
    }

    /// Look up details about an API based on its client package name
    pub fn client_pkgname_lookup<P>(&self, pkgname: &P) -> Option<&ApiMetadata>
    where
        ClientPackageName: Borrow<P>,
        P: Ord,
        P: ?Sized,
    {
        self.apis.get(pkgname)
    }

    /// Returns how we should filter the given dependency
    pub(crate) fn evaluate_dependency(
        &self,
        workspaces: &Workspaces,
        client_pkgname: &ClientPackageName,
        dep_path: &DepPath,
    ) -> Result<Evaluation> {
        let Some(rules) = self.dependency_rules.get(client_pkgname) else {
            return Ok(Evaluation::default());
        };

        let which_rules: Vec<_> = rules
            .iter()
            .filter(|r| {
                assert_eq!(r.client, *client_pkgname);
                let pkgids = workspaces.workspace_pkgids(&r.ancestor);
                dep_path.contains_any(&pkgids)
            })
            .collect();

        if which_rules.is_empty() {
            return Ok(Evaluation::default());
        }

        if which_rules.len() > 1 {
            bail!(
                "client package {:?}: dependency matched multiple filters: {}",
                client_pkgname,
                which_rules
                    .into_iter()
                    .map(|r| r.ancestor.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        Ok(which_rules[0].evaluation)
    }
}

/// Format of the `api-manifest.toml` file
///
/// This is not exposed outside this module.  It's processed and validated in
/// the transformation to `AllApiMetadata`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawApiMetadata {
    apis: Vec<ApiMetadata>,
    deployment_units: Vec<DeploymentUnitInfo>,
    dependency_filter_rules: Vec<DependencyFilterRule>,
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

        let mut dependency_rules = BTreeMap::new();
        for rule in raw.dependency_filter_rules {
            if !apis.contains_key(&rule.client) {
                bail!(
                    "dependency rule references unknown client: {:?}",
                    rule.client
                );
            }

            dependency_rules
                .entry(rule.client.clone())
                .or_insert_with(Vec::new)
                .push(rule);
        }

        Ok(AllApiMetadata { apis, deployment_units, dependency_rules })
    }
}

/// Describes one API in the system
#[derive(Deserialize)]
pub struct ApiMetadata {
    /// the package name of the Progenitor client for this API
    ///
    /// This is used as the primary key for APIs.
    pub client_package_name: ClientPackageName,
    /// human-readable label for the API
    pub label: String,
    /// package name of the server that provides the corresponding API
    pub server_package_name: ServerPackageName,
    /// human-readable notes about this API
    pub notes: Option<String>,
}

/// Describes a unit that combines one or more servers that get deployed
/// together
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentUnitInfo {
    /// human-readable label, also used as primary key
    pub label: DeploymentUnitName,
    /// list of Rust packages that are shipped in this unit
    pub packages: Vec<ServerComponentName>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyFilterRule {
    pub ancestor: String,
    pub client: ClientPackageName,
    #[serde(default)]
    pub evaluation: Evaluation,
    // XXX-dap should these just be comments?
    #[allow(dead_code)]
    pub note: String,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Evaluation {
    /// This dependency has not been evaluated
    #[default]
    Unknown,
    /// This dependency should be ignored because it's not a real dependency --
    /// i.e., it's a false positive resulting from our methodology
    Bogus,
    /// This dependency should be ignored because it's not used in deployed
    /// systems
    NotDeployed,
    /// This dependency should not be part of the update DAG
    NonDag,
    /// This dependency should be part of the update DAG
    Dag,
}
