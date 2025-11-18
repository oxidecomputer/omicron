// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Query information about the Dropshot/OpenAPI/Progenitor-based APIs within
//! the Oxide system

use crate::ClientPackageName;
use crate::DeploymentUnitName;
use crate::LoadArgs;
use crate::ServerComponentName;
use crate::ServerPackageName;
use crate::api_metadata::AllApiMetadata;
use crate::api_metadata::ApiConsumerStatus;
use crate::api_metadata::ApiExpectedConsumer;
use crate::api_metadata::ApiExpectedConsumers;
use crate::api_metadata::ApiMetadata;
use crate::api_metadata::Evaluation;
use crate::api_metadata::VersionedHow;
use crate::cargo::DepPath;
use crate::parse_toml_file;
use crate::workspaces::Workspaces;
use anyhow::Result;
use anyhow::{Context, anyhow, bail};
use camino::Utf8PathBuf;
use cargo_metadata::Package;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use parse_display::{Display, FromStr};
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Query information about the Dropshot/OpenAPI/Progenitor-based APIs within
/// the Oxide system
pub struct SystemApis {
    /// maps a deployment unit to its list of service components
    unit_server_components:
        BTreeMap<DeploymentUnitName, BTreeSet<ServerComponentName>>,
    /// maps a server component to the deployment unit that it's part of
    /// (reverse of `unit_server_components`)
    server_component_units: BTreeMap<ServerComponentName, DeploymentUnitName>,

    /// maps a server component to the list of APIs it uses (using the client
    /// package name as a primary key for the API)
    apis_consumed: BTreeMap<
        ServerComponentName,
        BTreeMap<ClientPackageName, Vec<DepPath>>,
    >,
    /// maps an API name (using the client package name as primary key) to the
    /// list of server components that use it
    /// (reverse of `apis_consumed`)
    api_consumers: BTreeMap<ClientPackageName, IdOrdMap<ApiConsumer>>,

    /// maps an API name to the server component(s) that expose that API
    api_producers: BTreeMap<ClientPackageName, ApiProducerMap>,

    /// source of developer-maintained API metadata
    api_metadata: AllApiMetadata,
    /// source of Cargo package metadata
    workspaces: Workspaces,
}

type ApiProducerMap = BTreeMap<ServerComponentName, Vec<DepPath>>;

#[derive(Debug)]
struct ApiConsumer {
    server_pkgname: ServerComponentName,
    dep_paths: Vec<DepPath>,
    status: ApiConsumerStatus,
}

impl ApiConsumer {
    fn new(name: ServerComponentName, status: ApiConsumerStatus) -> Self {
        Self { server_pkgname: name, dep_paths: Vec::new(), status }
    }

    fn add_path(&mut self, path: DepPath) {
        self.dep_paths.push(path);
    }
}

impl IdOrdItem for ApiConsumer {
    type Key<'a> = &'a ServerComponentName;
    fn key(&self) -> Self::Key<'_> {
        &self.server_pkgname
    }
    id_upcast!();
}

/// Public form of an API consumer.
#[derive(Debug)]
pub struct ApiConsumerRef<'a> {
    /// The name of the consumer.
    pub server_pkgname: &'a ServerComponentName,
    /// The list of paths through which this consumer depends on the client,
    /// after filters have been applied.
    pub dep_paths: Vec<&'a DepPath>,
    /// The status of the consumer, such as whether it is expected to be present.
    pub status: &'a ApiConsumerStatus,
}

impl<'a> IdOrdItem for ApiConsumerRef<'a> {
    type Key<'b>
        = &'b ServerComponentName
    where
        Self: 'b;
    fn key(&self) -> Self::Key<'_> {
        self.server_pkgname
    }
    id_upcast!();
}

impl SystemApis {
    /// Load information about APIs in the system based on both developer-
    /// maintained metadata and Cargo-provided metadata
    pub fn load(args: LoadArgs) -> Result<SystemApis> {
        // Load the API manifest.
        let api_metadata: AllApiMetadata =
            parse_toml_file(&args.api_manifest_path)?;
        // Load Cargo metadata and validate it against the manifest.
        let (workspaces, warnings) = Workspaces::load(&api_metadata)?;
        if !warnings.is_empty() {
            // We treat these warnings as fatal here.
            for e in warnings {
                eprintln!("error: {:#}", e);
            }

            bail!(
                "found inconsistency between API manifest ({}) and \
                 information found from the Cargo dependency tree \
                 (see above)",
                &args.api_manifest_path
            );
        }

        // Create an index of server package names, mapping each one to the list
        // of APIs that it produces.
        let mut server_packages = BTreeMap::new();
        for api in api_metadata.apis() {
            server_packages
                .entry(api.server_package_name.clone())
                .or_insert_with(Vec::new)
                .push(api);
        }

        // Walk the deployment units, then walk each one's list of packages, and
        // then walk all of its dependencies.  Along the way, record whenever we
        // find a package whose name matches a known server package.  If we find
        // this, we've found which deployment unit (and which top-level package)
        // contains that server.  The result of this process is a set of data
        // structures that allow us to look up the components in a deployment
        // unit, the deployment unit for any component, the servers in each
        // component, etc.
        let mut tracker = ServerComponentsTracker::new(&server_packages);
        for (deployment_unit, dunit_info) in api_metadata.deployment_units() {
            for dunit_pkg in &dunit_info.packages {
                tracker.found_deployment_unit_package(
                    deployment_unit,
                    dunit_pkg,
                )?;
                let (workspace, server_pkg) =
                    workspaces.find_package_workspace(dunit_pkg)?;
                let dep_path = DepPath::for_pkg(server_pkg.id.clone());
                tracker.found_package(dunit_pkg, dunit_pkg, &dep_path);

                workspace.walk_required_deps_recursively(
                    server_pkg,
                    &mut |p: &Package, dep_path: &DepPath| {
                        tracker.found_package(dunit_pkg, &p.name, dep_path);
                    },
                )?;
            }
        }

        let (server_component_units, unit_server_components, api_producers) = (
            tracker.server_component_units,
            tracker.unit_server_components,
            tracker.api_producers,
        );

        // Ensure that if allowed_consumers is defined, all consumers listed are
        // specified by at least one deployment unit.
        for api in api_metadata.apis() {
            match &api.consumers {
                ApiExpectedConsumers::Any => {}
                ApiExpectedConsumers::Exactly(consumers) => {
                    for consumer in consumers {
                        if !server_component_units.contains_key(&consumer.name)
                        {
                            bail!(
                                "api {} specifies unknown consumer: {} \
                                 (with expected reason: {})",
                                api.client_package_name,
                                consumer.name,
                                consumer.reason,
                            );
                        }
                    }
                }
            }
        }

        // Now that we've figured out what servers are where, walk dependencies
        // of each server component and assemble structures to find which APIs
        // are produced and consumed by which components.
        let mut deps_tracker = ClientDependenciesTracker::new(&api_metadata);
        for server_pkgname in server_component_units.keys() {
            let (workspace, pkg) =
                workspaces.find_package_workspace(server_pkgname)?;
            workspace
                .walk_required_deps_recursively(
                    pkg,
                    &mut |p: &Package, dep_path: &DepPath| {
                        deps_tracker.found_dependency(
                            server_pkgname,
                            &p.name,
                            dep_path,
                        );
                    },
                )
                .with_context(|| {
                    format!(
                        "iterating dependencies of workspace {:?} package {:?}",
                        workspace.name(),
                        server_pkgname
                    )
                })?;
        }

        let (apis_consumed, api_consumers) =
            (deps_tracker.apis_consumed, deps_tracker.api_consumers);

        // Make sure that each API is produced by at least one producer.
        for api in api_metadata.apis() {
            let found_producer = api_producers.get(&api.client_package_name);
            if api.deployed() {
                if found_producer.is_none() {
                    bail!(
                        "error: found no producer for API with client package \
                         name {:?} in any deployment unit (should have been \
                         one that contains server package {:?})",
                        api.client_package_name,
                        api.server_package_name,
                    );
                }
            } else if let Some(found) = found_producer {
                bail!(
                    "error: metadata says there should be no deployed \
                     producer for API with client package name {:?}, but found \
                     one: {:?}",
                    api.client_package_name,
                    found
                );
            }
        }

        Ok(SystemApis {
            server_component_units,
            unit_server_components,
            apis_consumed,
            api_consumers,
            api_producers,
            api_metadata,
            workspaces,
        })
    }

    /// Iterate over the deployment units
    pub fn deployment_units(
        &self,
    ) -> impl Iterator<Item = &DeploymentUnitName> {
        self.unit_server_components.keys()
    }

    /// Get the deployment unit associated with a server component
    pub fn server_component_unit(
        &self,
        server_component: &ServerComponentName,
    ) -> Option<&DeploymentUnitName> {
        self.server_component_units.get(server_component)
    }

    /// For one deployment unit, iterate over the servers contained in it
    pub fn deployment_unit_servers(
        &self,
        unit: &DeploymentUnitName,
    ) -> Result<impl Iterator<Item = &ServerComponentName> + use<'_>> {
        Ok(self
            .unit_server_components
            .get(unit)
            .ok_or_else(|| anyhow!("unknown deployment unit: {}", unit))?
            .iter())
    }

    /// Returns the developer-maintained API metadata
    pub fn api_metadata(&self) -> &AllApiMetadata {
        &self.api_metadata
    }

    /// Given a server component, return the APIs consumed by this component
    pub fn component_apis_consumed(
        &self,
        server_component: &ServerComponentName,
        filter: ApiDependencyFilter,
    ) -> Result<
        impl Iterator<Item = (&ClientPackageName, &DepPath)> + '_ + use<'_>,
    > {
        let mut rv = Vec::new();
        let Some(apis_consumed) = self.apis_consumed.get(server_component)
        else {
            return Ok(rv.into_iter());
        };

        for (client_pkgname, dep_paths) in apis_consumed {
            let mut include = None;
            for p in dep_paths {
                if filter.should_include(
                    &self.api_metadata,
                    &self.workspaces,
                    client_pkgname,
                    p,
                )? {
                    include = Some(p);
                    break;
                };
            }
            if let Some(p) = include {
                rv.push((client_pkgname, p));
            }
        }

        Ok(rv.into_iter())
    }

    /// Given the client package name for an API, return the name of the server
    /// component(s) that provide it
    pub fn api_producers<'apis>(
        &'apis self,
        client: &ClientPackageName,
    ) -> impl Iterator<Item = &'apis ServerComponentName> + 'apis + use<'apis>
    {
        self.api_producers
            .get(client)
            .into_iter()
            .flat_map(|producers| producers.keys())
    }

    /// Given the client package name for an API, return the list of server
    /// components that consume it, along with the Cargo dependency path that
    /// connects each server to the client package
    pub fn api_consumers(
        &self,
        client: &ClientPackageName,
        filter: ApiDependencyFilter,
    ) -> Result<IdOrdMap<ApiConsumerRef<'_>>> {
        let mut rv = IdOrdMap::new();

        let Some(api_consumers) = self.api_consumers.get(client) else {
            return Ok(rv);
        };

        for api_consumer in api_consumers {
            let mut include = Vec::new();
            for p in &api_consumer.dep_paths {
                if filter.should_include(
                    &self.api_metadata,
                    &self.workspaces,
                    &client,
                    p,
                )? {
                    include.push(p);
                }
            }

            if !include.is_empty() {
                rv.insert_unique(ApiConsumerRef {
                    server_pkgname: &api_consumer.server_pkgname,
                    dep_paths: include,
                    status: &api_consumer.status,
                })
                .expect("api_consumers is uniquely indexed by server_pkgname");
            }
        }

        Ok(rv)
    }

    /// Given the client package name for an API and the name of a server
    /// component, returns `true` if the server is a producer of that API, or
    /// `false` if it is not.
    pub fn is_producer_of(
        &self,
        server: &ServerComponentName,
        client: &ClientPackageName,
    ) -> bool {
        self.api_producers
            .get(client)
            .map(|producers| producers.contains_key(server))
            .unwrap_or(false)
    }

    /// Given the name of any package defined in one of our workspaces, return
    /// information used to construct a label
    ///
    /// Returns `(name, rel_path)`, where `name` is the name of the workspace
    /// containing the package and `rel_path` is the relative path of the
    /// package within that workspace.
    pub fn package_label(&self, pkgname: &str) -> Result<(&str, Utf8PathBuf)> {
        let (workspace, _) = self.workspaces.find_package_workspace(pkgname)?;
        let pkgpath = workspace.find_workspace_package_path(pkgname)?;
        Ok((workspace.name(), pkgpath))
    }

    /// Given the name of any package defined in one of our workspaces, return
    /// an Asciidoc snippet that's usable to render the name of the package.
    /// This just uses `package_label()` but may in the future create links,
    /// too.
    pub fn adoc_label(&self, pkgname: &str) -> Result<String> {
        let (workspace, _) = self.workspaces.find_package_workspace(pkgname)?;
        let pkgpath = workspace.find_workspace_package_path(pkgname)?;
        Ok(format!(
            "https://github.com/oxidecomputer/{}/tree/main/{}[{}:{}]",
            workspace.name(),
            pkgpath,
            workspace.name(),
            pkgpath
        ))
    }

    /// Returns a string that can be passed to `dot(1)` to render a graph of
    /// API dependencies among deployment units
    pub fn dot_by_unit(&self, filter: ApiDependencyFilter) -> Result<String> {
        let mut graph = petgraph::graph::Graph::new();
        let nodes: BTreeMap<_, _> = self
            .deployment_units()
            .map(|name| (name, graph.add_node(name)))
            .collect();

        // Now walk through the deployment units, walk through each one's server
        // packages, walk through each one of the clients used by those, and
        // create a corresponding edge.
        for deployment_unit in self.deployment_units() {
            let server_components =
                self.deployment_unit_servers(deployment_unit).unwrap();
            let my_node = nodes.get(deployment_unit).unwrap();
            for server_pkg in server_components {
                for (client_pkg, _) in
                    self.component_apis_consumed(server_pkg, filter)?
                {
                    // Multiple server components may produce an API. However,
                    // if an API is produced by multiple server components
                    // within the same deployment unit, we would like to only
                    // create one edge per unit.  Thus, use a BTreeSet here to
                    // de-duplicate the producing units.
                    let other_units: BTreeSet<_> = self
                        .api_producers(client_pkg)
                        .map(|other_component| {
                            self.server_component_units
                                .get(other_component)
                                .unwrap()
                        })
                        .collect();
                    for other_unit in other_units {
                        let other_node = nodes.get(other_unit).unwrap();
                        graph.update_edge(
                            *my_node,
                            *other_node,
                            client_pkg.clone(),
                        );
                    }
                }
            }
        }

        Ok(Dot::new(&graph).to_string())
    }

    /// Returns a string that can be passed to `dot(1)` to render a graph of
    /// API dependencies among server components
    pub fn dot_by_server_component(
        &self,
        filter: ApiDependencyFilter,
    ) -> Result<String> {
        let (graph, _nodes) = self.make_component_graph(filter, false)?;
        Ok(Dot::new(&graph).to_string())
    }

    // The complex type below is only used in this one place: the return value
    // of an internal helper function.  A type alias doesn't seem better.
    #[allow(clippy::type_complexity)]
    fn make_component_graph(
        &self,
        dependency_filter: ApiDependencyFilter,
        versioned_on_server_only: bool,
    ) -> Result<(
        petgraph::graph::Graph<&ServerComponentName, &ClientPackageName>,
        BTreeMap<&ServerComponentName, NodeIndex>,
    )> {
        let mut graph = petgraph::graph::Graph::new();
        let nodes: BTreeMap<_, _> = self
            .server_component_units
            .keys()
            .map(|server_component| {
                (server_component, graph.add_node(server_component))
            })
            .collect();

        // Now walk through the server components, walk through each one of the
        // clients used by those, and create a corresponding edge.
        for server_component in self.apis_consumed.keys() {
            // unwrap(): we created a node for each server component above.
            let my_node = nodes.get(server_component).unwrap();
            let consumed_apis = self
                .component_apis_consumed(server_component, dependency_filter)?;
            for (client_pkg, _) in consumed_apis {
                if versioned_on_server_only {
                    let api = self
                        .api_metadata
                        .client_pkgname_lookup(client_pkg)
                        .unwrap();
                    if api.versioned_how != VersionedHow::Server {
                        continue;
                    }
                }

                for other_component in self.api_producers(client_pkg) {
                    let other_node = nodes.get(other_component).unwrap();
                    graph.add_edge(*my_node, *other_node, client_pkg);
                }
            }
        }

        Ok((graph, nodes))
    }

    /// Verifies various important properties about the assignment of which APIs
    /// are server-managed vs. client-managed.
    ///
    /// Returns a structure with proposals for how to assign APIs that are
    /// currently unassigned.
    pub fn dag_check(&self) -> Result<DagCheck<'_>> {
        // In this function, we'll use the following ApiDependencyFilter a bunch
        // when walking the component dependency graph.  "Default" is the
        // correct filter to use here.  This excludes relationships that are
        // totally bogus, only affect components that are never actually
        // deployed, or are part of an edge that we've already determined will
        // be "non-DAG".
        //
        // This last case might be a little confusing.  The whole point of this
        // function is to help developers figure out which edges should be part
        // of the DAG or not.  Why would we ignore edges based on whether
        // they're already in the DAG or not?
        //
        // Recall that there are two ways that the metadata can specify that a
        // particular API is "not part of the update DAG" (which is equivalent
        // to client-side-managed):
        //
        // - a specific class of Cargo dependencies can be marked "non-DAG" via
        //   a dependency filter rule.  The only case of this today is where we
        //   say that Cargo dependencies from "oximeter-producer" to
        //   "nexus-client" are "non-DAG".  This means we promise to make the
        //   Nexus internal API client-managed (i.e., not part of the update
        //   DAG).  We verify this promise below.
        // - a specific API can be marked as server-managed (meaning it's part
        //   of the update DAG) or not.  That's most of what this function deals
        //   with and proposes changes to.
        //
        // In the long term, it might be nice to combine these.  But that's more
        // work than it sounds like: we'd probably want to convert everything
        // to filter rules, but that requires (tediously) writing out every
        // single edge that we care about.  An alternative would be to eliminate
        // the non-DAG dependency filter rules and only use the property at the
        // API level.  However right now it seems quite possible that we do want
        // this on a per-edge basis, rather than a per-API basis (i.e., there
        // are some client-side-versioned APIs that have consumers that could
        // treat them as server-side-versioned).
        //
        // Anyway, what we're talking about here is ignoring the first category
        // of information and looking only at the second.  This is *safe* (i.e.,
        // correct) because we verify below that the second category (the
        // API-level `versioned_for` property) contains the same information
        // provided by the first category (the non-DAG dependency filter rules).
        // We *choose* to do this because it makes the heuristics below more
        // useful.  For example, excluding the non-DAG edges makes it easy for
        // the heuristic below to tell that crucible-pantry ought to be
        // server-side-managed because it has no (other) dependencies and so
        // can't be part of a cycle.
        let filter = ApiDependencyFilter::Default;

        // Construct a graph where:
        //
        // - nodes are all the API producer and consumer components
        // - we only include edges *to* components that produce server-managed
        //   APIs
        //
        // Check if this DAG is cyclic.  This can't be made to work.
        let (graph, nodes) = self.make_component_graph(filter, true)?;
        let reverse_nodes: BTreeMap<_, _> =
            nodes.iter().map(|(s_c, node)| (node, s_c)).collect();
        if let Err(error) = petgraph::algo::toposort(&graph, None) {
            bail!(
                "graph of server-managed components has a cycle (includes \
                 node: {:?})",
                reverse_nodes.get(&error.node_id()).unwrap()
            );
        }

        // Verify that the targets of any "non-dag" dependency filter rules are
        // indeed not part of the server-side-versioned DAG.
        for api in self.api_metadata.non_dag_apis() {
            if !matches!(api.versioned_how, VersionedHow::Client(..)) {
                bail!(
                    "API identified by client package {:?} ({}) is the \
                     \"client\" in a \"non-dag\" dependency rule, but its \
                     \"versioned_how\" is not \"client\"",
                    api.client_package_name,
                    api.label,
                );
            }
        }

        // Use some heuristics to propose next steps.
        //
        // We're only looking for possible next steps here -- we don't have to
        // programmatically figure out the whole graph.

        let mut dag_check = DagCheck::new();

        for api in self.api_metadata.apis() {
            if !api.deployed() {
                if api.versioned_how == VersionedHow::Unknown {
                    dag_check.propose_server(
                        &api.client_package_name,
                        String::from("not produced by a deployed component"),
                    );
                }
                continue;
            }

            for consumer in
                self.api_consumers(&api.client_package_name, filter)?
            {
                // Are there any unexpected consumers?
                match consumer.status {
                    ApiConsumerStatus::NoAssertion
                    | ApiConsumerStatus::Expected { .. } => {}
                    ApiConsumerStatus::Unexpected => {
                        dag_check.report_unexpected_consumer(
                            &api.client_package_name,
                            consumer.server_pkgname,
                        );
                    }
                }
            }

            // Are there any missing consumers?
            match &api.consumers {
                ApiExpectedConsumers::Any => {}
                ApiExpectedConsumers::Exactly(consumers) => {
                    for expected in consumers {
                        if !self
                            .api_consumers(&api.client_package_name, filter)?
                            .contains_key(&expected.name)
                        {
                            dag_check.report_missing_consumer(
                                &api.client_package_name,
                                expected,
                            );
                        }
                    }
                }
            }

            for producer in self.api_producers(&api.client_package_name) {
                let apis_consumed: BTreeSet<_> = self
                    .component_apis_consumed(producer, filter)?
                    .map(|(client_pkgname, _dep_path)| client_pkgname)
                    .collect();
                let consumers = self
                    .api_consumers(&api.client_package_name, filter)
                    .unwrap();

                if api.versioned_how == VersionedHow::Unknown {
                    // If we haven't determined how to manage versioning on this
                    // API, and it has no dependencies on "unknown" or
                    // client-managed APIs, then it can be made server-managed.
                    if !apis_consumed.iter().any(|client_pkgname| {
                        let api = self
                            .api_metadata
                            .client_pkgname_lookup(*client_pkgname)
                            .unwrap();
                        api.versioned_how != VersionedHow::Server
                    }) {
                        dag_check.propose_server(
                            &api.client_package_name,
                            String::from(
                                "has no unknown or client-managed dependencies",
                            ),
                        );
                    } else if apis_consumed.contains(&api.client_package_name) {
                        // If this thing depends on itself, it must be
                        // client-managed.
                        dag_check.propose_client(
                            &api.client_package_name,
                            String::from("depends on itself"),
                        );
                    } else if consumers.is_empty() {
                        // If something has no consumers in deployed components, it
                        // can be server-managed.  (These are generally debug APIs.)
                        dag_check.propose_server(
                            &api.client_package_name,
                            String::from(
                                "has no consumers among deployed components",
                            ),
                        );
                    }

                    continue;
                }

                let dependencies: BTreeMap<_, _> = apis_consumed
                    .iter()
                    .flat_map(|dependency_clientpkg| {
                        self.api_producers(dependency_clientpkg)
                            .map(|p| (p, *dependency_clientpkg))
                    })
                    .collect();

                // Look for one-step circular dependencies (i.e., API API A1 is
                // produced by component C1, which uses API A2 produced by C2, which
                // also uses A1).  In such cases, either A1 or A2 must be
                // client-managed (or both).
                for c in consumers {
                    if let Some(dependency_clientpkg) =
                        dependencies.get(c.server_pkgname)
                    {
                        let dependency_api = self
                            .api_metadata
                            .client_pkgname_lookup(*dependency_clientpkg)
                            .unwrap();

                        // If we're looking at a server-managed dependency and the
                        // other is unknown, then that one should be client-managed.
                        //
                        // Without loss of generality, we can ignore the reverse
                        // case (because we will catch that case when we're
                        // iterating over the dependency API).
                        if api.versioned_how == VersionedHow::Server
                            && dependency_api.versioned_how
                                == VersionedHow::Unknown
                        {
                            dag_check.propose_client(
                                dependency_clientpkg,
                                format!(
                                    "has cyclic dependency on {:?}, which is \
                                 server-managed",
                                    api.client_package_name,
                                ),
                            )
                        }

                        // If both are Unknown, tell the user to pick one.
                        if api.versioned_how == VersionedHow::Unknown
                            && dependency_api.versioned_how
                                == VersionedHow::Unknown
                        {
                            dag_check.propose_upick(
                                &api.client_package_name,
                                dependency_clientpkg,
                            );
                        }
                    }
                }
            }
        }

        Ok(dag_check)
    }
}

/// Describes proposals for assigning how APIs should be versioned, based on
/// heuristics applied while checking the DAG
pub struct DagCheck<'a> {
    /// set of APIs (identified by client package name) that we propose should
    /// be server-managed, along with a list of reasons why we think so
    proposed_server_managed: BTreeMap<&'a ClientPackageName, Vec<String>>,
    /// set of APIs (identified by client package name) that we propose should
    /// be client-managed, along with a list of reasons why we think so
    proposed_client_managed: BTreeMap<&'a ClientPackageName, Vec<String>>,
    /// set of pairs of APIs where we propose that the user must pick one
    /// package in each pair to be client-managed (because the two packages have
    /// a mutual dependency)
    ///
    /// The ordering in these pairs is not semantically significant.  The
    /// implementation will ensure that each pair of packages is represented at
    /// most once in this structure.
    proposed_upick:
        BTreeMap<&'a ClientPackageName, BTreeSet<&'a ClientPackageName>>,
    /// clients that failed assertions about consumers
    failed_consumers: IdOrdMap<FailedConsumerCheck<'a>>,
}

impl<'a> DagCheck<'a> {
    fn new() -> DagCheck<'a> {
        DagCheck {
            proposed_server_managed: BTreeMap::new(),
            proposed_client_managed: BTreeMap::new(),
            proposed_upick: BTreeMap::new(),
            failed_consumers: IdOrdMap::new(),
        }
    }

    fn propose_client(
        &mut self,
        client_pkgname: &'a ClientPackageName,
        reason: String,
    ) {
        self.proposed_client_managed
            .entry(client_pkgname)
            .or_insert_with(Vec::new)
            .push(reason);
    }

    fn propose_server(
        &mut self,
        client_pkgname: &'a ClientPackageName,
        reason: String,
    ) {
        self.proposed_server_managed
            .entry(client_pkgname)
            .or_insert_with(Vec::new)
            .push(reason);
    }

    /// Propose that one of these two packages should be client-managed (because
    /// they depend on each other, so they can't both be server-managed).
    fn propose_upick(
        &mut self,
        client_pkgname1: &'a ClientPackageName,
        client_pkgname2: &'a ClientPackageName,
    ) {
        // A "upick" is a situation where you (the person running the tool)
        // should choose either of `pkg1` or `pkg2` to be client-managed.  The
        // caller will identify this situation twice: once when looking at
        // `pkg1` and once when looking at `pkg2`.  But we only want to report
        // it once.  So we'll ignore duplicates here because it's easier here
        // than in the caller.
        //
        // To do that, first check whether the caller has already proposed this
        // "upick" with the packages in the other order.  If so, do nothing.
        if let Some(other_pkg_upicks) = self.proposed_upick.get(client_pkgname2)
        {
            if other_pkg_upicks.contains(client_pkgname1) {
                return;
            }
        }

        // Now go ahead and insert the pair in this order.  This construction
        // will also do nothing if this same pair has already been inserted in
        // this order.
        self.proposed_upick
            .entry(client_pkgname1)
            .or_insert_with(BTreeSet::new)
            .insert(client_pkgname2);
    }

    fn report_unexpected_consumer(
        &mut self,
        client_pkgname: &'a ClientPackageName,
        consumer_name: &'a ServerComponentName,
    ) {
        self.failed_consumers
            .entry(client_pkgname)
            .or_insert_with(|| FailedConsumerCheck::new(client_pkgname))
            .unexpected
            .insert(consumer_name);
    }

    fn report_missing_consumer(
        &mut self,
        client_pkgname: &'a ClientPackageName,
        expected: &'a ApiExpectedConsumer,
    ) {
        self.failed_consumers
            .entry(client_pkgname)
            .or_insert_with(|| FailedConsumerCheck::new(client_pkgname))
            .missing
            .insert(&expected.name, &expected.reason);
    }

    /// Returns a list of APIs (identified by client package name) that look
    /// like they could use server-side versioning, along with reasons
    pub fn proposed_server_managed(
        &self,
    ) -> impl Iterator<Item = (&'_ ClientPackageName, &Vec<String>)> {
        self.proposed_server_managed.iter().map(|(c, r)| (*c, r))
    }

    /// Returns a list of APIs (identified by client package name) that look
    /// like they should use client-side versioning, along with reasons
    pub fn proposed_client_managed(
        &self,
    ) -> impl Iterator<Item = (&'_ ClientPackageName, &Vec<String>)> {
        self.proposed_client_managed.iter().map(|(c, r)| (*c, r))
    }

    /// Returns a list of pairs of APIs (identified by client package names) for
    /// which we have not yet picked client-side or server-side versioning and
    /// where there is a direct mutual dependency
    ///
    /// At least one of these APIs will need to be marked client-managed.
    pub fn proposed_upick(
        &self,
    ) -> impl Iterator<Item = (&'_ ClientPackageName, &'_ ClientPackageName)>
    {
        self.proposed_upick
            .iter()
            .flat_map(|(c, others)| others.iter().map(|o| (*c, *o)))
    }

    /// Returns the map of all client package names and consumers that failed
    /// consumers checks.
    pub fn failed_consumers(&self) -> &IdOrdMap<FailedConsumerCheck<'a>> {
        &self.failed_consumers
    }
}

/// Information about an API that failed consumers checks.
#[derive(Clone, Debug)]
pub struct FailedConsumerCheck<'a> {
    /// The API's client package name.
    pub client_pkgname: &'a ClientPackageName,
    /// Components that actually consume this API but that were not present in
    /// the list of expected consumers.
    pub unexpected: BTreeSet<&'a ServerComponentName>,
    /// Components that were expected to consume this API but that were not
    /// present in the list of actual consumers, along with the reason they
    /// should be present.
    pub missing: BTreeMap<&'a ServerComponentName, &'a str>,
}

impl<'a> IdOrdItem for FailedConsumerCheck<'a> {
    type Key<'b>
        = &'a ClientPackageName
    where
        Self: 'b;
    fn key(&self) -> Self::Key<'_> {
        self.client_pkgname
    }
    id_upcast!();
}

impl<'a> FailedConsumerCheck<'a> {
    pub fn new(client_pkgname: &'a ClientPackageName) -> Self {
        Self {
            client_pkgname,
            unexpected: BTreeSet::new(),
            missing: BTreeMap::new(),
        }
    }
}

/// Helper for building structures to index which deployment units contain which
/// server components and what APIs those components expose
///
/// See `SystemApis::load()` for how this is used.
struct ServerComponentsTracker<'a> {
    // inputs
    known_server_packages:
        &'a BTreeMap<ServerPackageName, Vec<&'a ApiMetadata>>,

    // outputs (structures that we're building up)
    server_component_units: BTreeMap<ServerComponentName, DeploymentUnitName>,
    unit_server_components:
        BTreeMap<DeploymentUnitName, BTreeSet<ServerComponentName>>,
    api_producers: BTreeMap<ClientPackageName, ApiProducerMap>,
}

impl<'a> ServerComponentsTracker<'a> {
    pub fn new(
        known_server_packages: &'a BTreeMap<
            ServerPackageName,
            Vec<&'a ApiMetadata>,
        >,
    ) -> ServerComponentsTracker<'a> {
        ServerComponentsTracker {
            known_server_packages,
            server_component_units: BTreeMap::new(),
            unit_server_components: BTreeMap::new(),
            api_producers: BTreeMap::new(),
        }
    }

    /// Record that `server_pkgname` exposes API `api` by virtue of the
    /// dependency chain `dep_path`
    pub fn found_api_producer(
        &mut self,
        api: &ApiMetadata,
        server_pkgname: &ServerComponentName,
        dep_path: &DepPath,
    ) {
        // TODO dns-server is used by both the dns-server component *and*
        // omicron-sled-agent's simulated sled agent.  This program does not
        // support that.  But we don't care about the simulated sled agent,
        // either, so just ignore it.
        //
        // This exception cannot currently be encoded in the
        // "dependency_filter_rules" metadata because that metadata is applied
        // as a postprocessing step.  But we can't even build up our data model
        // in the first place unless we ignore this here.
        if **server_pkgname == "omicron-sled-agent"
            && *api.client_package_name == "dns-service-client"
        {
            eprintln!(
                "note: ignoring Cargo dependency from omicron-sled-agent -> \
                 dns-server",
            );
            return;
        }

        // TODO Crucible Pantry depends on Crucible (Upstairs).  But Crucible
        // Upstairs exposes an API (the Crucible Control API).  That makes it
        // look (from tracking Cargo dependencies) like Crucible Pantry exposes
        // that API.  But it doesn't.
        //
        // Like the above dns-server dependency, we can't build up our data
        // model without ignoring this, so it can't currently be encoded in the
        // "dependency_filter_rules" metadata.
        if **server_pkgname == "crucible-pantry"
            && *api.client_package_name == "crucible-control-client"
        {
            eprintln!(
                "note: ignoring Cargo dependency from crucible-pantry -> \
                 ... -> crucible-control-client",
            );
            return;
        }

        self.api_producers
            .entry(api.client_package_name.clone())
            .or_default()
            .entry(server_pkgname.clone())
            .or_default()
            .push(dep_path.clone());
    }

    /// Record that deployment unit package `dunit_pkgname` depends on package
    /// `pkgname` via dependency chain `dep_path`
    ///
    /// This only records anything if `pkgname` turns out to be a known API
    /// client package name, in which case this records that the server
    /// component consumes the corresponding API.
    pub fn found_package(
        &mut self,
        dunit_pkgname: &ServerComponentName,
        pkgname: &str,
        dep_path: &DepPath,
    ) {
        let Some(apis) = self.known_server_packages.get(pkgname) else {
            return;
        };

        for api in apis {
            self.found_api_producer(api, dunit_pkgname, dep_path);
        }
    }

    /// Record that the given package is one of the deployment unit's top-level
    /// packages (server components)
    pub fn found_deployment_unit_package(
        &mut self,
        deployment_unit: &DeploymentUnitName,
        server_component: &ServerComponentName,
    ) -> Result<()> {
        if let Some(previous) = self
            .server_component_units
            .insert(server_component.clone(), deployment_unit.clone())
        {
            bail!(
                "server component {:?} found in multiple deployment \
                 units (at least {} and {})",
                server_component,
                deployment_unit,
                previous
            );
        }

        assert!(
            self.unit_server_components
                .entry(deployment_unit.clone())
                .or_default()
                .insert(server_component.clone())
        );
        Ok(())
    }
}

/// Helper for building structures to track which APIs are consumed by which
/// server components
struct ClientDependenciesTracker<'a> {
    // inputs
    api_metadata: &'a AllApiMetadata,

    // outputs (structures that we're building up)
    apis_consumed: BTreeMap<
        ServerComponentName,
        BTreeMap<ClientPackageName, Vec<DepPath>>,
    >,
    api_consumers: BTreeMap<ClientPackageName, IdOrdMap<ApiConsumer>>,
}

impl<'a> ClientDependenciesTracker<'a> {
    fn new(api_metadata: &'a AllApiMetadata) -> ClientDependenciesTracker<'a> {
        ClientDependenciesTracker {
            api_metadata,
            apis_consumed: BTreeMap::new(),
            api_consumers: BTreeMap::new(),
        }
    }

    /// Record that comopnent `server_pkgname` consumes package `pkgname` via
    /// dependency chain `dep_path`
    ///
    /// This only records cases where `pkgname` is a known client package for
    /// one of our APIs, in which case it records that this server component
    /// consumes the corresponding API.
    fn found_dependency(
        &mut self,
        server_pkgname: &ServerComponentName,
        pkgname: &str,
        dep_path: &DepPath,
    ) {
        let Some(api) = self.api_metadata.client_pkgname_lookup(pkgname) else {
            return;
        };

        // This is the name of a known client package.  Record it.
        let status = api.consumers.status(server_pkgname);
        let client_pkgname = ClientPackageName::from(pkgname.to_owned());
        self.api_consumers
            .entry(client_pkgname.clone())
            .or_insert_with(IdOrdMap::new)
            .entry(&server_pkgname)
            .or_insert_with(|| ApiConsumer::new(server_pkgname.clone(), status))
            .add_path(dep_path.clone());
        self.apis_consumed
            .entry(server_pkgname.clone())
            .or_insert_with(BTreeMap::new)
            .entry(client_pkgname)
            .or_insert_with(Vec::new)
            .push(dep_path.clone());
    }
}

/// Specifies which API dependencies to include vs. ignore when iterating
/// dependencies
#[derive(Clone, Copy, Debug, Default, Display, FromStr)]
#[display(style = "kebab-case")]
pub enum ApiDependencyFilter {
    /// Include all dependencies found from Cargo package metadata
    All,

    /// Include _only_ bogus dependencies (mainly useful for seeing what's
    /// normally being excluded)
    Bogus,

    /// Include all dependencies found from Cargo package metadata that have not
    /// been explicitly marked as bogus (false positives)
    ///
    /// Relative to the default, this includes dependencies that have been
    /// explicitly excluded from the online update DAG as well as dependencies
    /// from programs that are not deployed (but within packages that are
    /// deployed).
    NonBogus,

    /// Include dependencies that have been explicitly excluded from the online
    /// update DAG
    IncludeNonDag,

    /// Exclude found dependencies that are:
    ///
    /// - explicitly marked as outside the update DAG
    /// - bogus (do not reflect real dependencies)
    /// - not part of production deployments
    #[default]
    Default,
}

impl ApiDependencyFilter {
    /// Return whether this filter should include a dependency on
    /// `client_pkgname` that goes through dependency path `dep_path`
    fn should_include(
        &self,
        api_metadata: &AllApiMetadata,
        workspaces: &Workspaces,
        client_pkgname: &ClientPackageName,
        dep_path: &DepPath,
    ) -> Result<bool> {
        let evaluation = api_metadata
            .evaluate_dependency(workspaces, client_pkgname, dep_path)
            .with_context(|| format!("error applying filter {:?}", self))?;

        Ok(match self {
            ApiDependencyFilter::All => true,
            ApiDependencyFilter::Bogus => {
                matches!(evaluation, Evaluation::Bogus)
            }
            ApiDependencyFilter::NonBogus => {
                !matches!(evaluation, Evaluation::Bogus)
            }
            ApiDependencyFilter::IncludeNonDag => !matches!(
                evaluation,
                Evaluation::Bogus | Evaluation::NotDeployed
            ),
            ApiDependencyFilter::Default => !matches!(
                evaluation,
                Evaluation::NonDag
                    | Evaluation::Bogus
                    | Evaluation::NotDeployed
            ),
        })
    }
}
