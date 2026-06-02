// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer-maintained API metadata

use crate::ClientPackageName;
use crate::ServerComponentName;
use crate::ServerPackageName;
use crate::cargo::DepPath;
use crate::workspaces::Workspaces;
use anyhow::{Result, bail};
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_deployment_graph::DeploymentUnitName;
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
    deployment_units: IdOrdMap<DeploymentUnitInfo>,
    /// every server component across all deployment units, keyed by name
    ///
    /// This is the single source of truth for server components.
    /// `DeploymentUnitInfo` stores only component *names*; the components
    /// themselves (with their lifecycle and kind) live here.
    server_components: IdOrdMap<ServerComponent>,
    dependency_rules: BTreeMap<ClientPackageName, Vec<DependencyFilterRule>>,
    ignored_non_clients: BTreeSet<ClientPackageName>,
    intra_deployment_unit_only_edges: Vec<IntraDeploymentUnitOnlyEdge>,
}

impl AllApiMetadata {
    /// Iterate over the distinct APIs described by the metadata
    pub fn apis(&self) -> impl Iterator<Item = &ApiMetadata> {
        self.apis.values()
    }

    /// Iterate over the deployment units defined in the metadata
    pub fn deployment_units(
        &self,
    ) -> impl Iterator<Item = &DeploymentUnitInfo> {
        self.deployment_units.iter()
    }

    /// Look up a deployment unit's info by its name
    pub fn deployment_unit_info(
        &self,
        name: &DeploymentUnitName,
    ) -> Option<&DeploymentUnitInfo> {
        self.deployment_units.get(name)
    }

    /// Iterate over the package names for all the APIs' clients
    pub fn client_pkgnames(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.apis.keys()
    }

    /// Iterate over all the server components across all deployment units
    pub fn server_components(&self) -> impl Iterator<Item = &ServerComponent> {
        self.server_components.iter()
    }

    /// Look up a server component by name, or `None` if no component with that
    /// name is registered in any deployment unit
    pub fn server_component(
        &self,
        name: &ServerComponentName,
    ) -> Option<&ServerComponent> {
        self.server_components.get(name)
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

    /// Returns the list of intra-deployment-unit-only edges
    pub fn intra_deployment_unit_only_edges(
        &self,
    ) -> &[IntraDeploymentUnitOnlyEdge] {
        &self.intra_deployment_unit_only_edges
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
    deployment_units: Vec<RawDeploymentUnitInfo>,
    dependency_filter_rules: Vec<DependencyFilterRule>,
    ignored_non_clients: Vec<ClientPackageName>,
    intra_deployment_unit_only_edges: Vec<IntraDeploymentUnitOnlyEdge>,
}

/// Registers a server component, failing if a component with the same name has
/// already been registered.
///
/// `server_components` is keyed by component name and spans every deployment
/// unit, so this enforces that each component name appears exactly once across
/// all units' `packages` and `embedded_components`.  Without this check,
/// duplicates would surface later as a confusing "in multiple deployment
/// units" error, or as ambiguous component lookups.
fn register_server_component(
    server_components: &mut IdOrdMap<ServerComponent>,
    component: ServerComponent,
) -> Result<()> {
    if let Err(error) = server_components.insert_unique(component) {
        let new = error.new_item();
        // `IdOrdMap` is keyed by component name alone, so the new component
        // conflicts with exactly one previously-registered component.
        let previous = error.duplicates().first().expect(
            "a duplicate-key conflict has exactly one conflicting item",
        );
        if previous.deployment_unit == new.deployment_unit {
            bail!(
                "server component {:?} appears more than once in \
                 deployment unit {}; each component must appear exactly \
                 once across `packages` and `embedded_components`",
                new.name,
                new.deployment_unit,
            );
        }
        bail!(
            "server component {:?} appears in multiple deployment unit \
             entries ({} and {}); each component must appear exactly once \
             across `packages` and `embedded_components`",
            new.name,
            previous.deployment_unit,
            new.deployment_unit,
        );
    }
    Ok(())
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

        let mut deployment_units = IdOrdMap::new();
        let mut server_components: IdOrdMap<ServerComponent> = IdOrdMap::new();
        for raw_unit in raw.deployment_units {
            // Build this unit's list of component names (packages first, then
            // embedded components) while registering each component in
            // `server_components`.
            let mut component_names = Vec::new();

            for pkg in &raw_unit.packages {
                component_names.push(pkg.clone());
                register_server_component(
                    &mut server_components,
                    ServerComponent {
                        name: pkg.clone(),
                        deployment_unit: raw_unit.name.clone(),
                        lifecycle: Lifecycle::SteadyState,
                        kind: ServerComponentKind::TopLevel,
                    },
                )?;
            }

            for embedded in &raw_unit.embedded_components {
                if !raw_unit.packages.contains(&embedded.inside) {
                    bail!(
                        "embedded component {:?} has `inside = {:?}`, but \
                         no such package exists in deployment unit {:?}'s \
                         `packages` list",
                        embedded.name,
                        embedded.inside,
                        raw_unit.name,
                    );
                }
                component_names.push(embedded.name.clone());
                register_server_component(
                    &mut server_components,
                    ServerComponent {
                        name: embedded.name.clone(),
                        deployment_unit: raw_unit.name.clone(),
                        lifecycle: embedded.lifecycle,
                        kind: ServerComponentKind::Embedded {
                            inside: embedded.inside.clone(),
                        },
                    },
                )?;
            }

            let info =
                DeploymentUnitInfo { name: raw_unit.name, component_names };
            if let Err(e) = deployment_units.insert_unique(info) {
                bail!(
                    "duplicate deployment unit name in API metadata: {}",
                    e.new_item().name,
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
                    "entry in ignored_non_clients appeared twice: {:?}",
                    &client_pkg
                );
            }
        }

        // Validate that IDU-only edges reference only known server components
        // and APIs.
        for edge in &raw.intra_deployment_unit_only_edges {
            if server_components.get(&edge.server).is_none() {
                bail!(
                    "intra_deployment_unit_only_edges: \
                     unknown server component {:?}",
                    edge.server
                );
            }

            if !apis.contains_key(&edge.client) {
                bail!(
                    "intra_deployment_unit_only_edges: \
                     unknown client {:?}",
                    edge.client,
                );
            }
        }

        Ok(AllApiMetadata {
            apis,
            deployment_units,
            server_components,
            dependency_rules,
            ignored_non_clients,
            intra_deployment_unit_only_edges: raw
                .intra_deployment_unit_only_edges,
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
    pub restricted_to_consumers: ApiExpectedConsumers,
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
    /// This API has no configured restrictions on which consumers can use it.
    #[default]
    Unrestricted,
    /// This API is restricted to exactly these consumers.
    Restricted(IdOrdMap<ApiExpectedConsumer>),
}

impl ApiExpectedConsumers {
    pub fn status(
        &self,
        server_pkgname: &ServerComponentName,
    ) -> ApiConsumerStatus {
        match self {
            ApiExpectedConsumers::Unrestricted => {
                ApiConsumerStatus::NoAssertion
            }
            ApiExpectedConsumers::Restricted(consumers) => {
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
                Ok(ApiExpectedConsumers::Unrestricted)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ApiExpectedConsumers::Unrestricted)
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                // Note IdOrdMap deserializes as a sequence by default.
                let consumers = IdOrdMap::<ApiExpectedConsumer>::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(seq),
                )?;
                Ok(ApiExpectedConsumers::Restricted(consumers))
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
///
/// This is the validated form of a `[[deployment_units]]` entry; see
/// `RawDeploymentUnitInfo` for the on-disk format.
#[derive(Debug)]
pub struct DeploymentUnitInfo {
    /// human-readable name (e.g. "Nexus", "DNS Server"), also used as primary
    /// key
    pub name: DeploymentUnitName,
    /// names of the server components in this unit (`packages` first, then
    /// `embedded_components`)
    ///
    /// Components can be looked up using [`AllApiMetadata::server_component`].
    component_names: Vec<ServerComponentName>,
}

impl DeploymentUnitInfo {
    /// Iterates over the names of the server components in this unit
    pub fn component_names(
        &self,
    ) -> impl Iterator<Item = &ServerComponentName> {
        self.component_names.iter()
    }
}

impl IdOrdItem for DeploymentUnitInfo {
    type Key<'a> = &'a DeploymentUnitName;
    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
    id_upcast!();
}

/// A server component within a deployment unit
///
/// This is the validated form of a `packages` or `embedded_components` entry.
#[derive(Debug)]
pub struct ServerComponent {
    /// the Rust package name for this component, also used as primary key
    name: ServerComponentName,
    /// the deployment unit that this component is part of
    deployment_unit: DeploymentUnitName,
    /// when this component's code runs
    lifecycle: Lifecycle,
    /// whether this is a top-level package or an embedded component
    kind: ServerComponentKind,
}

impl ServerComponent {
    /// Returns the Rust package name for this component
    pub fn name(&self) -> &ServerComponentName {
        &self.name
    }

    /// Returns the deployment unit that this component is part of
    pub fn deployment_unit(&self) -> &DeploymentUnitName {
        &self.deployment_unit
    }

    /// Returns whether this is a top-level package or an embedded component
    pub fn kind(&self) -> &ServerComponentKind {
        &self.kind
    }

    /// Whether this component's consumed-API edges participate in the upgrade
    /// DAG
    ///
    /// Components whose code doesn't run during steady-state operation (e.g.,
    /// code that only runs at rack initialization) can't be affected by
    /// version skew during an online upgrade, so their API dependencies are
    /// excluded from the upgrade DAG and from cycle checks.
    pub fn in_upgrade_dag(&self) -> bool {
        self.lifecycle.in_upgrade_dag()
    }

    /// Returns a short, human-readable note describing how this component
    /// differs from an ordinary steady-state, top-level component, or `None`
    /// if it is one.
    pub fn display_note(&self) -> Option<String> {
        let mut notes = Vec::new();
        match &self.kind {
            ServerComponentKind::TopLevel => {}
            ServerComponentKind::Embedded { inside } => {
                notes.push(format!("embedded in {inside}"));
            }
        }
        match self.lifecycle {
            Lifecycle::SteadyState => {}
            Lifecycle::RackInit => {
                notes.push(String::from("rack-init only"));
            }
        }
        (!notes.is_empty()).then(|| notes.join("; "))
    }
}

impl IdOrdItem for ServerComponent {
    type Key<'a> = &'a ServerComponentName;
    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
    id_upcast!();
}

/// Distinguishes a deployment unit's top-level packages from its embedded
/// components
#[derive(Debug)]
pub enum ServerComponentKind {
    /// A package shipped directly in the deployment unit
    ///
    /// The package's Cargo dependencies are walked to discover both the APIs
    /// they produce and the APIs they consume.
    TopLevel,
    /// A library linked inside a `TopLevel` package of the *same* deployment
    /// unit, but tracked as a separate consumer of APIs
    ///
    /// Dependencies reachable only through an embedded component are
    /// attributed to the embedded component rather than to its parent package.
    /// Unlike top-level packages, embedded components do not themselves host
    /// Dropshot APIs, so they are not walked as API producers.
    Embedded {
        /// the package that links this library
        ///
        /// Guaranteed to name a `TopLevel` component in the same deployment
        /// unit.  When ls-apis walks `inside`'s Cargo dependencies, the
        /// embedded component's own package is treated as absent, so
        /// dependencies reachable *only* through the embedded component are
        /// attributed to it rather than to `inside`.  The embedded component
        /// is walked separately as its own consumer; a dependency that
        /// `inside` also reaches through another path stays attributed to
        /// `inside` as well.
        inside: ServerComponentName,
    },
}

/// Describes when a server component's code runs
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Lifecycle {
    /// The component runs during normal operation.  Its API dependencies are
    /// part of the upgrade DAG.
    #[default]
    SteadyState,
    /// The component runs only at rack initialization.  The rack is at a
    /// consistent version when this code runs, so its API dependencies are
    /// excluded from the upgrade DAG.
    RackInit,
}

impl Lifecycle {
    /// Whether components with this lifecycle participate in the upgrade DAG
    fn in_upgrade_dag(self) -> bool {
        match self {
            Lifecycle::SteadyState => true,
            Lifecycle::RackInit => false,
        }
    }
}

/// Format of a `[[deployment_units]]` entry in the `api-manifest.toml` file
///
/// This is processed and validated in the transformation to `AllApiMetadata`.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct RawDeploymentUnitInfo {
    /// human-readable name, also used as primary key
    name: DeploymentUnitName,
    /// the set of Rust packages that are shipped in this unit
    packages: BTreeSet<ServerComponentName>,
    /// list of embedded components: libraries inside one of `packages` that
    /// are tracked as separate consumers of APIs
    #[serde(default)]
    embedded_components: Vec<RawEmbeddedComponentInfo>,
    // Note: unlike for embedded components, we do not currently accept
    // `lifecycle` for deployment units because all deployment units are
    // treated as being steady-state. This is not inherent, though, and we can
    // add a `lifecycle` field here if it ever becomes necessary.
}

/// Format of an `embedded_components` entry in the `api-manifest.toml` file
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct RawEmbeddedComponentInfo {
    /// the Rust package name for this embedded component
    name: ServerComponentName,
    /// the deployment-unit package that contains this embedded component
    /// (i.e., the binary that links this library)
    ///
    /// Must name an entry in the same deployment unit's `packages` list.
    inside: ServerComponentName,
    /// when this embedded component's code runs
    #[serde(default)]
    lifecycle: Lifecycle,
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

/// An edge that should be excluded from the deployment unit dependency graph
/// because it represents communication that only happens locally within a
/// single instance of a single deployment unit.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntraDeploymentUnitOnlyEdge {
    /// The server component that consumes the API.
    pub server: ServerComponentName,
    /// The client package consumed.
    pub client: ClientPackageName,
    /// Explanation of why this edge is intra-deployment-unit-only.
    pub note: String,
    /// Permalinks to source code referenced by `note`
    pub permalinks: Vec<String>,
}

impl IntraDeploymentUnitOnlyEdge {
    /// Returns true if this rule matches the given (server, client) pair.
    pub fn matches(
        &self,
        server: &ServerComponentName,
        client: &ClientPackageName,
    ) -> bool {
        self.server == *server && self.client == *client
    }
}
