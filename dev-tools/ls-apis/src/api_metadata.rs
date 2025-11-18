// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer-maintained API metadata

use crate::ClientPackageName;
use crate::DeploymentUnitName;
use crate::ServerComponentName;
use crate::ServerPackageName;
use crate::cargo::DepPath;
use crate::workspaces::Workspaces;
use anyhow::{Result, bail};
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

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
    ignored_non_clients: BTreeSet<ClientPackageName>,
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

    /// Returns the set of packages that should *not* be treated as
    /// Progenitor-based clients
    pub fn ignored_non_clients(&self) -> &BTreeSet<ClientPackageName> {
        &self.ignored_non_clients
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

    /// Returns the list of APIs that have non-DAG dependency rules
    pub(crate) fn non_dag_apis(&self) -> impl Iterator<Item = &ApiMetadata> {
        self.dependency_rules.iter().filter_map(|(client_pkgname, rules)| {
            rules.iter().any(|r| r.evaluation == Evaluation::NonDag).then(
                || {
                    // unwrap(): we previously verified that the "client" for
                    // all dependency rules corresponds to an API that we have
                    // metadata for.
                    self.apis.get(client_pkgname).unwrap()
                },
            )
        })
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
    ignored_non_clients: Vec<ClientPackageName>,
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

        let mut ignored_non_clients = BTreeSet::new();
        for client_pkg in raw.ignored_non_clients {
            if !ignored_non_clients.insert(client_pkg.clone()) {
                bail!(
                    "entry in ignored_non_clients appearead twice: {:?}",
                    &client_pkg
                );
            }
        }

        Ok(AllApiMetadata {
            apis,
            deployment_units,
            dependency_rules,
            ignored_non_clients,
        })
    }
}

/// Describes how an API in the system manages drift between client and server
#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "versioned_how", content = "versioned_how_reason")]
pub enum VersionedHow {
    /// We have not yet determined how this API will be versioned.
    Unknown,

    /// This API will be versioned solely on the server.  (The update system
    /// will ensure that servers are always updated before clients.)
    Server,

    /// This API will be versioned on the client.  (The update system cannot
    /// ensure that servers are always updated before clients.)
    Client(String),
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
    /// expected consumers (Rust packages) that use this API
    ///
    /// By default, we don't make any assertions about expected consumers. But
    /// some APIs must have a fixed list of consumers, and we assert on that
    /// via this array.
    #[serde(default)]
    pub consumers: ApiExpectedConsumers,
    /// human-readable notes about this API
    pub notes: Option<String>,
    /// describes how we've decided this API will be versioned
    #[serde(default, flatten)]
    pub versioned_how: VersionedHow,
    /// If `dev_only` is true, then this API's server is not deployed in a
    /// production system.  It's only used in development environments.  The
    /// default is that APIs *are* deployed.
    #[serde(default)]
    dev_only: bool,
}

impl ApiMetadata {
    /// Returns whether this API's server component gets deployed on real
    /// systems
    pub fn deployed(&self) -> bool {
        !self.dev_only
    }
}

/// Expected consumers (Rust packages) for an API.
#[derive(Debug, Default)]
pub enum ApiExpectedConsumers {
    /// No assertions are made about consumers.
    #[default]
    Any,
    /// Exactly these consumers are allowed.
    Exactly(IdOrdMap<ApiExpectedConsumer>),
}

impl ApiExpectedConsumers {
    pub fn status(
        &self,
        server_pkgname: &ServerComponentName,
    ) -> ApiConsumerStatus {
        match self {
            ApiExpectedConsumers::Any => ApiConsumerStatus::NoAssertion,
            ApiExpectedConsumers::Exactly(consumers) => {
                if let Some(consumer) =
                    consumers.iter().find(|c| c.name == *server_pkgname)
                {
                    ApiConsumerStatus::Expected {
                        reason: consumer.reason.clone(),
                    }
                } else {
                    ApiConsumerStatus::Unexpected
                }
            }
        }
    }
}

impl<'de> Deserialize<'de> for ApiExpectedConsumers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        struct ApiExpectedConsumersVisitor;

        impl<'de> serde::de::Visitor<'de> for ApiExpectedConsumersVisitor {
            type Value = ApiExpectedConsumers;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter.write_str(
                    "null (for no assertions) or a list of Rust package names",
                )
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ApiExpectedConsumers::Any)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ApiExpectedConsumers::Any)
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                // Note IdOrdMap deserializes as a sequence by default.
                let consumers = IdOrdMap::<ApiExpectedConsumer>::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(seq),
                )?;
                Ok(ApiExpectedConsumers::Exactly(consumers))
            }
        }

        deserializer.deserialize_any(ApiExpectedConsumersVisitor)
    }
}

/// Describes a single allowed consumer for an API.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiExpectedConsumer {
    /// The name of the Rust package.
    pub name: ServerComponentName,
    /// The reason this consumer is allowed.
    pub reason: String,
}

impl IdOrdItem for ApiExpectedConsumer {
    type Key<'a> = &'a ServerComponentName;
    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
    id_upcast!();
}

/// The status of an API consumer that was discovered by walking the Cargo
/// metadata graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApiConsumerStatus {
    /// No assertions were made about this API consumer.
    NoAssertion,
    /// The API consumer is expected to be used.
    Expected { reason: String },
    /// The API consumer was not expected. This is an error case.
    Unexpected,
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
    // These notes are not currently used, but they are required.  They could as
    // well just be TOML comments.  But it seems nice to enforce that they're
    // present.  And this would let us include this explanation in output in the
    // future (e.g., to explain why some dependency was filtered out).
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
