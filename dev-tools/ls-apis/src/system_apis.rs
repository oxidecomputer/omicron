// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Query information about the Dropshot/OpenAPI/Progenitor-based APIs within
//! the Oxide system

use crate::ClientPackageName;
use crate::LoadArgs;
use crate::ServerComponentName;
use crate::ServerPackageName;
use crate::api_metadata::AllApiMetadata;
use crate::api_metadata::ApiConsumerStatus;
use crate::api_metadata::ApiExpectedConsumer;
use crate::api_metadata::ApiExpectedConsumers;
use crate::api_metadata::ApiMetadata;
use crate::api_metadata::Evaluation;
use crate::api_metadata::RawApiMetadata;
use crate::api_metadata::ServerComponentKind;
use crate::api_metadata::VersionedHow;
use crate::cargo::DepPath;
use crate::errors::ErrorAccumulator;
use crate::errors::LoadError;
use crate::errors::LoadErrors;
use crate::parse_toml_file;
use crate::workspaces::Workspaces;
use anyhow::Result;
use anyhow::{Context, anyhow, bail};
use camino::Utf8PathBuf;
use cargo_metadata::PackageId;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_deployment_graph::DagEdge;
use omicron_deployment_graph::DagEdgesFile;
use omicron_deployment_graph::DeploymentUnitName;
use parse_display::{Display, FromStr};
use petgraph::dot::Dot;
use petgraph::graph::Graph;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

/// Query information about the Dropshot/OpenAPI/Progenitor-based APIs within
/// the Oxide system
pub struct SystemApis {
    /// maps a deployment unit to its list of service components
    ///
    /// The reverse mapping, of component to deployment unit, is available via
    /// [`SystemApis::server_component_unit`].
    unit_server_components:
        BTreeMap<DeploymentUnitName, BTreeSet<ServerComponentName>>,

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
    /// API consumers that were expected but not found
    missing_expected_consumers:
        BTreeMap<ClientPackageName, ApiMissingConsumers>,

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

/// An API consumer returned by [`SystemApis::api_consumers`].
#[derive(Debug)]
pub struct FilteredApiConsumer<'a> {
    /// The name of the consumer.
    pub server_pkgname: &'a ServerComponentName,
    /// The list of paths through which this consumer depends on the client,
    /// after filters have been applied.
    pub dep_paths: Vec<&'a DepPath>,
    /// The status of the consumer, such as whether it is expected to be present.
    pub status: &'a ApiConsumerStatus,
}

impl<'a> IdOrdItem for FilteredApiConsumer<'a> {
    type Key<'b>
        = &'a ServerComponentName
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
    pub fn load(args: LoadArgs) -> Result<SystemApis, LoadErrors> {
        let mut errors = ErrorAccumulator::new();

        // Parse the API manifest.  If the file can't be read or parsed, there's
        // nothing else to validate, so report that and exit early.
        let raw: RawApiMetadata = match parse_toml_file(&args.api_manifest_path)
        {
            Ok(raw) => raw,
            Err(source) => {
                errors.push(LoadError::ReadManifest {
                    path: args.api_manifest_path.clone(),
                    source,
                });
                return Err(errors
                    .take_load_errors()
                    .expect("the manifest read error was just recorded"));
            }
        };

        // Validate the manifest.  Every later pass assumes valid metadata, so
        // we stop early if the manifest had any problems.
        let Some(api_metadata) = AllApiMetadata::from_raw(raw, &mut errors)
        else {
            return Err(errors.take_load_errors().expect(
                "from_raw returns None only after recording an error",
            ));
        };

        // Load Cargo metadata and cross-check it against the manifest.
        let Some(workspaces) = Workspaces::load(
            &api_metadata,
            &args.workspace_root,
            args.patched_dep_policy,
            &mut errors,
        ) else {
            return Err(errors.take_load_errors().expect(
                "Workspaces::load returns None only after recording an error",
            ));
        };

        // Create an index of server package names, mapping each one to the list
        // of APIs that it produces.
        let mut server_packages = BTreeMap::new();
        for api in api_metadata.apis() {
            server_packages
                .entry(api.server_package_name.clone())
                .or_insert_with(Vec::new)
                .push(api);
        }

        // For each component, compute the set of package IDs to omit when
        // walking its dependencies.  An embedded component's package is
        // omitted from its parent package's walk, so that the embedded
        // component's dependency subgraph is attributed to the embedded
        // component rather than to its parent.
        let mut omitted_nodes: BTreeMap<
            &ServerComponentName,
            BTreeSet<&PackageId>,
        > = BTreeMap::new();
        // Maps each omitted package ID back to the embedded component that
        // contributed it.  Used after the producer walk to produce a helpful
        // error if an embedded component's omission turned out to be a no-op.
        let mut omitted_pkgid_embedded = BTreeMap::new();
        for component in api_metadata.server_components() {
            // Ensure every component has an entry, even if it omits nothing.
            omitted_nodes.entry(component.name()).or_default();

            match component.kind() {
                ServerComponentKind::TopLevel => {}
                ServerComponentKind::Embedded { inside } => {
                    // An embedded component is a library crate linked into
                    // the `inside` binary, so it necessarily lives in the
                    // same workspace as `inside`.  Resolve its package within
                    // that workspace specifically.
                    let workspace =
                        match workspaces.find_package_workspace(inside) {
                            Ok((workspace, _)) => workspace,
                            Err(source) => {
                                errors.push(
                                LoadError::ResolveEmbeddedComponentWorkspace {
                                    embedded_component: component
                                        .name()
                                        .clone(),
                                    inside: inside.clone(),
                                    source,
                                },
                            );
                                continue;
                            }
                        };
                    let Some(embedded_pkg) =
                        workspace.find_workspace_package(component.name())
                    else {
                        errors.push(
                            LoadError::EmbeddedComponentNotInWorkspace {
                                embedded_component: component.name().clone(),
                                workspace: workspace.name().to_owned(),
                                inside: inside.clone(),
                                deployment_unit: component
                                    .deployment_unit()
                                    .clone(),
                            },
                        );
                        continue;
                    };
                    omitted_pkgid_embedded.insert(&embedded_pkg.id, component);
                    omitted_nodes
                        .entry(inside)
                        .or_default()
                        .insert(&embedded_pkg.id);
                }
            }
        }

        // Walk the deployment units, then walk each one's list of packages, and
        // then walk all of its dependencies.  Along the way, record whenever we
        // find a package whose name matches a known server package.  If we find
        // this, we've found which deployment unit (and which top-level package)
        // contains that server.  The result of this process is a set of data
        // structures that allow us to look up the components in a deployment
        // unit, the servers in each component, etc.
        let mut tracker = ServerComponentsTracker::new(&server_packages);
        for dunit_info in api_metadata.deployment_units() {
            for component_name in dunit_info.component_names() {
                let component =
                    api_metadata.server_component(component_name).expect(
                        "a deployment unit's component names are all \
                         registered server components",
                    );
                tracker.found_deployment_unit_package(
                    &dunit_info.name,
                    component_name,
                );

                match component.kind() {
                    ServerComponentKind::TopLevel => {
                        let (workspace, server_pkg) = match workspaces
                            .find_package_workspace(component_name)
                        {
                            Ok(found) => found,
                            Err(source) => {
                                errors.push(
                                    LoadError::ResolveServerComponentWorkspace {
                                        component: component_name.clone(),
                                        source,
                                    },
                                );
                                continue;
                            }
                        };
                        let dep_path = DepPath::for_pkg(server_pkg.id.clone());
                        tracker.found_package(
                            component_name,
                            component_name,
                            std::slice::from_ref(&dep_path),
                        );

                        let omitted = omitted_nodes.get(component_name).expect(
                            "every server component has an omitted_nodes entry",
                        );
                        let outcome = match workspace
                            .walk_required_deps_recursively(server_pkg, omitted)
                        {
                            Ok(outcome) => outcome,
                            Err(source) => {
                                errors.push(LoadError::WalkDependencies {
                                    component: component_name.clone(),
                                    source,
                                });
                                continue;
                            }
                        };
                        for pkg_outcome in &outcome.found {
                            tracker.found_package(
                                component_name,
                                &pkg_outcome.package.name,
                                &pkg_outcome.dep_paths,
                            );
                        }

                        // Every embedded component declared `inside` this
                        // package must actually be one of its required Cargo
                        // dependencies.  Otherwise, omitting it from this
                        // walk did nothing, and its `embedded_components`
                        // entry is stale or wrong.
                        if let Some(&pkgid) =
                            omitted.difference(&outcome.omitted_seen).next()
                        {
                            let embedded = omitted_pkgid_embedded
                                .get(pkgid)
                                .copied()
                                .expect(
                                    "every omitted package ID was \
                                     registered with its embedded component",
                                );
                            errors.push(LoadError::StaleEmbeddedComponent {
                                embedded_component: embedded.name().clone(),
                                deployment_unit: embedded
                                    .deployment_unit()
                                    .clone(),
                                inside: component_name.clone(),
                            });
                        }
                    }
                    ServerComponentKind::Embedded { .. } => {
                        // Embedded components host no Dropshot APIs, so they
                        // are not walked as API producers here.  Their
                        // dependency subgraph is walked separately, as a
                        // consumer, in the loop below.
                    }
                }
            }
        }

        let (unit_server_components, api_producers) =
            (tracker.unit_server_components, tracker.api_producers);

        // The remaining passes interpret the producer/consumer graph we just
        // built.  If constructing the graph hit any problems, it is incomplete
        // and further checks would report spurious errors, so bail out.
        if let Some(load_errors) = errors.take_load_errors() {
            return Err(load_errors);
        }

        // Ensure that if restricted_to_consumers is defined, all consumers
        // listed are specified by at least one deployment unit.
        for api in api_metadata.apis() {
            match &api.restricted_to_consumers {
                ApiExpectedConsumers::Unrestricted => {}
                ApiExpectedConsumers::Restricted(consumers) => {
                    for consumer in consumers {
                        if api_metadata
                            .server_component(&consumer.name)
                            .is_none()
                        {
                            errors.push(
                                LoadError::ApiUnknownRestrictedConsumer {
                                    client: api.client_package_name.clone(),
                                    consumer: consumer.name.clone(),
                                    reason: consumer.reason.clone(),
                                },
                            );
                        }
                    }
                }
            }
        }

        // Now that we've figured out what servers are where, walk dependencies
        // of each server component and assemble structures to find which APIs
        // are produced and consumed by which components.  The same omitted
        // nodes used during the producer walk apply here too.
        let mut deps_tracker = ClientDependenciesTracker::new(&api_metadata);
        for server_component in api_metadata.server_components() {
            let server_pkgname = server_component.name();
            let (workspace, pkg) = match workspaces
                .find_package_workspace(server_pkgname)
            {
                Ok(found) => found,
                Err(source) => {
                    errors.push(LoadError::ResolveServerComponentWorkspace {
                        component: server_pkgname.clone(),
                        source,
                    });
                    continue;
                }
            };
            let omitted = omitted_nodes
                .get(server_pkgname)
                .expect("every server component has an omitted_nodes entry");
            let outcome =
                match workspace.walk_required_deps_recursively(pkg, omitted) {
                    Ok(outcome) => outcome,
                    Err(source) => {
                        errors.push(LoadError::WalkDependencies {
                            component: server_pkgname.clone(),
                            source,
                        });
                        continue;
                    }
                };
            for pkg_outcome in &outcome.found {
                deps_tracker.found_dependency(
                    server_pkgname,
                    &pkg_outcome.package.name,
                    &pkg_outcome.dep_paths,
                );
            }
        }

        let (apis_consumed, api_consumers) =
            (deps_tracker.apis_consumed, deps_tracker.api_consumers);
        let mut missing_expected_consumers = BTreeMap::new();

        // Make sure that each API is produced by at least one producer.
        for api in api_metadata.apis() {
            let found_producer = api_producers.get(&api.client_package_name);
            if api.deployed() {
                if found_producer.is_none() {
                    errors.push(LoadError::NoProducerForApi {
                        client: api.client_package_name.clone(),
                        server: api.server_package_name.clone(),
                    });
                }
            } else if let Some(found) = found_producer {
                errors.push(LoadError::UnexpectedProducerForApi {
                    client: api.client_package_name.clone(),
                    producers: found.keys().cloned().collect(),
                });
            }

            // Do any of the expected consumers of this API not actually use it?
            match &api.restricted_to_consumers {
                ApiExpectedConsumers::Unrestricted => {}
                ApiExpectedConsumers::Restricted(expected_consumers) => {
                    let actual_consumers =
                        api_consumers.get(&api.client_package_name);
                    let missing: IdOrdMap<_> = expected_consumers
                        .iter()
                        .filter(|c| {
                            actual_consumers.map_or(false, |actual| {
                                !actual.contains_key(&c.name)
                            })
                        })
                        .cloned()
                        .collect();
                    if !missing.is_empty() {
                        missing_expected_consumers.insert(
                            api.client_package_name.clone(),
                            ApiMissingConsumers { missing },
                        );
                    }
                }
            }
        }

        // Validate that the IDU-only edges' components belong to the same
        // deployment unit.
        //
        // Each `(server, client)` edge produces one of the following outcomes.
        // `from_raw` already validates that both `server` and `client` are
        // known, so the two "internal" rows are unreachable in practice.
        //
        // Row 3 fires only for non-deployed APIs: a *deployed* API with no
        // producer is already reported as `NoProducerForApi` in the
        // producer/consumer pass above, and we report each misconfiguration
        // exactly once.
        //
        //      server known?  client known?  has producer?  in server's unit? | outcome
        //
        //  (1)      no              -              -                -         | IduServerNotTracked (internal)
        //  (2)      yes            no              -                -         | IduClientNotTracked (internal)
        //  (3)      yes            yes            no                -         | IduClientWithoutProducer (non-deployed only)
        //  (4)      yes            yes            yes              no         | IduProducersNotInServerUnit
        //  (5)      yes            yes            yes              yes        | ok (edge satisfied)
        for edge in api_metadata.intra_deployment_unit_only_edges() {
            let server = &edge.server;
            let Some(server_unit) = api_metadata
                .server_component(server)
                .map(|c| c.deployment_unit())
            else {
                // Row 1 above.
                errors.push(LoadError::IduServerNotTracked {
                    server: server.clone(),
                });
                continue;
            };

            let client = &edge.client;
            let Some(client_api) = api_metadata.client_pkgname_lookup(client)
            else {
                // Row 2 above.
                //
                // (This is checked against the full set of known APIs, not
                // against `api_producers`, which only contains APIs that have a
                // producer.)
                errors.push(LoadError::IduClientNotTracked {
                    client: client.clone(),
                });
                continue;
            };

            let Some(producers) = api_producers.get(client) else {
                // Row 3 above.
                //
                // A known API with no producer is a real error in the manifest:
                // the edge names an API that nothing produces, so it can never
                // be satisfied.  A *deployed* API in this state was already
                // reported as `NoProducerForApi` above, so to report each
                // misconfiguration exactly once we only fire here for
                // non-deployed APIs, which that pass deliberately ignores.
                if !client_api.deployed() {
                    errors.push(LoadError::IduClientWithoutProducer {
                        server: server.clone(),
                        client: client.clone(),
                    });
                }
                continue;
            };

            if !producers.iter().any(|(p, _)| {
                api_metadata
                    .server_component(p)
                    .map(|c| c.deployment_unit() == server_unit)
                    .unwrap_or(false)
            }) {
                // Row 4 above.
                errors.push(LoadError::IduProducersNotInServerUnit {
                    server: server.clone(),
                    deployment_unit: server_unit.clone(),
                    client: client.clone(),
                    producers: producers.keys().cloned().collect(),
                });
            }
        }

        // Report everything collected by the final group of validation passes.
        if let Some(load_errors) = errors.take_load_errors() {
            return Err(load_errors);
        }

        Ok(SystemApis {
            unit_server_components,
            apis_consumed,
            api_consumers,
            missing_expected_consumers,
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
        self.api_metadata
            .server_component(server_component)
            .map(|c| c.deployment_unit())
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

    /// Returns the note for a intra-deployment-unit-only edge, if one matches.
    pub fn idu_only_edge_note(
        &self,
        server: &ServerComponentName,
        client: &ClientPackageName,
    ) -> Option<&str> {
        self.api_metadata
            .intra_deployment_unit_only_edges()
            .iter()
            .find(|edge| edge.matches(server, client))
            .map(|edge| edge.note.as_str())
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
    ) -> Result<IdOrdMap<FilteredApiConsumer<'_>>> {
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
                rv.insert_unique(FilteredApiConsumer {
                    server_pkgname: &api_consumer.server_pkgname,
                    dep_paths: include,
                    status: &api_consumer.status,
                })
                .expect("api_consumers is uniquely indexed by server_pkgname");
            }
        }

        Ok(rv)
    }

    /// Get the consumers for an API that were expected but not found.
    ///
    /// Returns `None` if there are no missing expected consumers, or if the set
    /// of expected consumers is unrestricted.
    pub fn missing_expected_consumers(
        &self,
        client: &ClientPackageName,
    ) -> Option<&ApiMissingConsumers> {
        self.missing_expected_consumers.get(client)
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
        let (graph, _) =
            self.make_deployment_unit_graph(filter, EdgeFilter::All)?;
        Ok(Dot::new(&graph).to_string())
    }

    /// Returns the deployment unit dependency DAG for server-side-versioned
    /// APIs.
    ///
    /// The result contains:
    ///
    /// * `edges`: each edge represents a consumer that depends on a producer,
    ///   meaning the producer must be fully updated before the consumer starts
    ///   updating.
    /// * `units_without_server_side_apis`: deployment units that have no edges
    ///   in the server-side-versioned DAG.
    pub fn deployment_unit_dag(&self) -> Result<DagEdgesFile> {
        let idu_only_edges = self.validate_idu_only_edges()?;
        let (graph, _nodes) = self.make_deployment_unit_graph(
            ApiDependencyFilter::Default,
            EdgeFilter::DagOnly(&idu_only_edges),
        )?;

        let mut edges = BTreeSet::new();
        let mut units_in_dag = BTreeSet::new();
        for edge_ref in graph.edge_references() {
            let consumer = graph[edge_ref.source()].clone();
            let producer = graph[edge_ref.target()].clone();
            units_in_dag.insert(consumer.clone());
            units_in_dag.insert(producer.clone());
            edges.insert(DagEdge { consumer, producer });
        }

        let units_without_server_side_apis = self
            .deployment_units()
            .filter(|u| !units_in_dag.contains(*u))
            .cloned()
            .collect();

        Ok(DagEdgesFile { units_without_server_side_apis, edges })
    }

    /// Returns whether `component`'s consumed-API edges participate in the
    /// upgrade DAG.
    ///
    /// Components with a non-steady-state lifecycle can't be affected by
    /// version skew during an online upgrade, so their API dependencies are
    /// excluded from the DAG and from cycle checks.
    ///
    /// Panics if `component` is not a registered deployment-unit component.
    fn component_in_upgrade_dag(
        &self,
        component: &ServerComponentName,
    ) -> bool {
        self.api_metadata
            .server_component(component)
            .expect(
                "component came from the API metadata or apis_consumed, \
                 both of which only contain registered deployment-unit \
                 components",
            )
            .in_upgrade_dag()
    }

    /// Returns whether an edge from `server` to `client` should be excluded
    /// under `edge_filter`.
    fn edge_filtered_out(
        &self,
        edge_filter: EdgeFilter<'_>,
        server: &ServerComponentName,
        client: &ClientPackageName,
    ) -> bool {
        match edge_filter {
            EdgeFilter::All => false,
            EdgeFilter::DagOnly(idu_edges) => {
                // unwrap(): every `client` encountered here came from
                // `component_apis_consumed`, which only yields known APIs.
                let api =
                    self.api_metadata.client_pkgname_lookup(client).unwrap();
                if api.versioned_how != VersionedHow::Server {
                    return true;
                }

                if !self.component_in_upgrade_dag(server) {
                    return true;
                }

                // Intra-deployment-unit-only edges always connect components in
                // the same *instance* of the same deployment unit, so they
                // can't induce an update-ordering constraint and shouldn't
                // count as cycles.
                idu_edges.contains(&(server.clone(), client.clone()))
            }
        }
    }

    // The complex type below is only used in this one place: the return value
    // of this internal helper function.  A type alias doesn't seem better.
    #[allow(clippy::type_complexity)]
    fn make_deployment_unit_graph(
        &self,
        dependency_filter: ApiDependencyFilter,
        edge_filter: EdgeFilter<'_>,
    ) -> Result<(
        Graph<&DeploymentUnitName, &ClientPackageName>,
        BTreeMap<&DeploymentUnitName, NodeIndex>,
    )> {
        let mut graph = Graph::new();
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
                    self.component_apis_consumed(server_pkg, dependency_filter)?
                {
                    if self.edge_filtered_out(
                        edge_filter,
                        server_pkg,
                        client_pkg,
                    ) {
                        continue;
                    }

                    // Multiple server components may produce an API. However,
                    // if an API is produced by multiple server components
                    // within the same deployment unit, we would like to only
                    // create one edge per unit.  Thus, use a BTreeSet here to
                    // de-duplicate the producing units.
                    let other_units: BTreeSet<_> = self
                        .api_producers(client_pkg)
                        .map(|other_component| {
                            self.server_component_unit(other_component).unwrap()
                        })
                        .collect();
                    for other_unit in other_units {
                        let other_node = nodes.get(other_unit).unwrap();
                        graph.update_edge(*my_node, *other_node, client_pkg);
                    }
                }
            }
        }

        Ok((graph, nodes))
    }

    /// Returns a string that can be passed to `dot(1)` to render a graph of
    /// API dependencies among server components
    pub fn dot_by_server_component(
        &self,
        filter: ApiDependencyFilter,
    ) -> Result<String> {
        let (graph, _nodes) =
            self.make_component_graph(filter, EdgeFilter::All)?;
        Ok(Dot::new(&graph).to_string())
    }

    // The complex type below is only used in this one place: the return value
    // of this internal helper function.  A type alias doesn't seem better.
    #[allow(clippy::type_complexity)]
    fn make_component_graph(
        &self,
        dependency_filter: ApiDependencyFilter,
        edge_filter: EdgeFilter<'_>,
    ) -> Result<(
        Graph<&ServerComponentName, &ClientPackageName>,
        BTreeMap<&ServerComponentName, NodeIndex>,
    )> {
        let mut graph = Graph::new();
        let nodes: BTreeMap<_, _> = self
            .api_metadata
            .server_components()
            .map(|component| {
                let name = component.name();
                (name, graph.add_node(name))
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
                if self.edge_filtered_out(
                    edge_filter,
                    server_component,
                    client_pkg,
                ) {
                    continue;
                }

                for other_component in self.api_producers(client_pkg) {
                    let other_node = nodes.get(other_component).unwrap();
                    graph.add_edge(*my_node, *other_node, client_pkg);
                }
            }
        }

        Ok((graph, nodes))
    }

    /// Computes the set of (server, client) edges for server-side-only-
    /// versioned APIs where the server and client are in the same deployment
    /// unit.
    ///
    /// See the caller for more on this.
    fn compute_required_idu_edges(
        &self,
    ) -> Result<BTreeSet<(ServerComponentName, ClientPackageName)>> {
        let filter = ApiDependencyFilter::Default;
        let mut required = BTreeSet::new();

        for component in self.api_metadata.server_components() {
            let server = component.name();
            let server_unit = component.deployment_unit();
            // Components that don't participate in the upgrade DAG don't
            // require an IDU annotation for their edges.
            if !self.component_in_upgrade_dag(server) {
                continue;
            }

            for (client, _) in self.component_apis_consumed(server, filter)? {
                // Only consider server-side-versioned APIs.
                let api = self
                    .api_metadata
                    .client_pkgname_lookup(client)
                    .expect("consumed API must have metadata");
                if api.versioned_how != VersionedHow::Server {
                    continue;
                }

                // Check if any producer is in the same deployment unit.
                for producer in self.api_producers(client) {
                    let producer_unit = self
                        .server_component_unit(producer)
                        .expect("API producer must be in some deployment unit");

                    if server_unit == producer_unit {
                        // This edge would create an intra-unit dependency for
                        // a server-versioned API, so it must be in the
                        // intra-deployment-unit-only list.
                        required.insert((server.clone(), client.clone()));
                        break;
                    }
                }
            }
        }

        Ok(required)
    }

    /// Validates that these two sets of (server, client) edges match:
    ///
    /// - API dependencies found by this tool *within* a deployment unit for a
    ///   server-side-versioned API
    /// - API dependencies annotated in the metadata as being
    ///   intra-deployment-unit
    ///
    /// Why?  Recall that a server-side-only-versioned API means that the server
    /// is always updated before its clients.  Further, the update system does
    /// not (and cannot) guarantee anything about the ordering of updates for a
    /// particular kind of deployment unit.  (Example: for host OS, the update
    /// system does not say that any particular sleds are updated before any
    /// others.)  Thus, if you have a server-side-only-versioned API with a
    /// client in the same deployment unit, that's only allowable if the client
    /// is always talking to an instance of the server in the same *instance* of
    /// the same deployment unit.  A dependency from Sled Agent to Propolis in
    /// the *same* host OS is okay.  A dependency from Sled Agent to a Propolis
    /// on a different sled is not.
    ///
    /// If we found a same-deployment-unit edge that's not labeled in the
    /// manifest as intra-deployment-unit, that means we've identified either a
    /// manifest bug or an update bug waiting to happen.
    ///
    /// Returns the validated set of edges for use by
    /// make_deployment_unit_graph.
    fn validate_idu_only_edges(
        &self,
    ) -> Result<BTreeSet<(ServerComponentName, ClientPackageName)>> {
        let required = self.compute_required_idu_edges()?;

        // Build the configured set from the manifest.
        let mut configured = BTreeSet::new();
        for edge in self.api_metadata.intra_deployment_unit_only_edges() {
            configured.insert((edge.server.clone(), edge.client.clone()));
        }

        // Compare the two sets.
        let missing: BTreeSet<_> = required.difference(&configured).collect();
        let extra: BTreeSet<_> = configured.difference(&required).collect();

        if !missing.is_empty() || !extra.is_empty() {
            let mut msg = String::new();
            for (server, client) in missing {
                msg.push_str(&format!(
                    "The following API dependendency exists between two \
                     components in the same deployment unit, but is not \
                     present in `intra_deployment_unit_only_edges`: \
                     server {:?} client {:?}\n\
                     If this client only ever uses this API with a server \
                     in the same *instance* of the same deployment unit, \
                     then add it to `intra_deployment_unit_only_edges`. \
                     Otherwise, this relationship is incompatible with \
                     automated upgrade.\n",
                    server, client,
                ));
            }

            for (server, client) in extra {
                msg.push_str(&format!(
                    "`intra_deployment_unit_only_edges` contains an edge \
                     between server {:?} and client {:?}, but either \
                     this API dependency was not found, or the API is not \
                     server-side-only-versioned, or this client and server \
                     are not in the same deployment unit.\n",
                    server, client,
                ));
            }

            bail!("{}", msg);
        }

        Ok(required)
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
        // Recall that there are three ways that the metadata can specify that a
        // particular API is "not part of the update DAG" (which is equivalent
        // to client-side-managed):
        //
        // - a specific class of Cargo dependencies can be marked "non-DAG" via
        //   a dependency filter rule.  The only case of this today is where we
        //   say that Cargo dependencies from "oximeter-producer" to
        //   "nexus-client" are "non-DAG".  This means we promise to make the
        //   Nexus internal API client-managed (i.e., not part of the update
        //   DAG).  We verify this promise below.
        // - an embedded component can be marked as not being part of the
        //   steady-state lifecycle (e.g., `lifecycle = "rack-init"`).  Edges
        //   from such components are excluded from the DAG.
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

        // Validate that all configured intra_deployment_unit_only_edges are
        // correct and match the required set exactly.
        let idu_only_edges = self.validate_idu_only_edges()?;

        // Construct a graph where:
        //
        // - nodes are all the API producer and consumer components
        // - we only include edges *to* components that produce server-managed
        //   APIs
        //
        // Check if this DAG is cyclic.  This can't be made to work.
        let (graph, nodes) = self.make_component_graph(
            filter,
            EdgeFilter::DagOnly(&idu_only_edges),
        )?;
        let reverse_nodes: BTreeMap<_, _> =
            nodes.iter().map(|(s_c, node)| (node, s_c)).collect();
        if let Err(error) = petgraph::algo::toposort(&graph, None) {
            bail!(
                "graph of server-managed API dependencies between components \
                 has a cycle (includes node: {:?})",
                reverse_nodes.get(&error.node_id()).unwrap()
            );
        }

        // Do the same with a graph of deployment units.
        let (graph, nodes) = self.make_deployment_unit_graph(
            filter,
            EdgeFilter::DagOnly(&idu_only_edges),
        )?;
        let reverse_nodes: BTreeMap<_, _> =
            nodes.iter().map(|(d_u, node)| (node, d_u)).collect();
        if let Err(error) = petgraph::algo::toposort(&graph, None) {
            bail!(
                "graph of server-managed API dependencies between deployment \
                 units has a cycle (includes node: {:?})",
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
            if let Some(missing) =
                self.missing_expected_consumers(&api.client_package_name)
            {
                dag_check.report_missing_expected_consumers(
                    &api.client_package_name,
                    missing,
                );
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

#[derive(Clone, Copy)]
enum EdgeFilter<'a> {
    /// Include every edge.
    All,
    /// Include only edges that participate in the upgrade DAG. This excludes:
    ///
    /// * Client-side versioned APIs
    /// * Components for which `Lifecycle::in_upgrade_dag` returns `false`
    /// * Intra-deployment-unit-only edges
    ///
    /// The carried data is the set of intra-deployment-unit-only
    /// `(server_component, client_package)` pairs to exclude.
    DagOnly(&'a BTreeSet<(ServerComponentName, ClientPackageName)>),
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

    fn report_missing_expected_consumers(
        &mut self,
        client_pkgname: &'a ClientPackageName,
        missing: &'a ApiMissingConsumers,
    ) {
        self.failed_consumers
            .entry(client_pkgname)
            .or_insert_with(|| FailedConsumerCheck::new(client_pkgname))
            .missing = Some(missing);
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

/// A map of missing consumers for an API.
#[derive(Debug)]
pub struct ApiMissingConsumers {
    missing: IdOrdMap<ApiExpectedConsumer>,
}

impl ApiMissingConsumers {
    pub fn error_count(&self) -> usize {
        self.missing.len()
    }

    pub fn display<'a>(
        &'a self,
        apis: &'a SystemApis,
    ) -> ApiMissingConsumersDisplay<'a> {
        ApiMissingConsumersDisplay { missing: &self.missing, apis }
    }
}

pub struct ApiMissingConsumersDisplay<'a> {
    missing: &'a IdOrdMap<ApiExpectedConsumer>,
    apis: &'a SystemApis,
}

impl fmt::Display for ApiMissingConsumersDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for consumer in self.missing {
            let deployment_unit = self
                .apis
                .server_component_unit(&consumer.name)
                .unwrap_or_else(|| {
                    panic!(
                        "consumer {} doesn't have an associated \
                         deployment unit (this is checked at load time, so \
                         if you're seeing this message, there's a bug in that \
                         check)",
                        consumer.name
                    );
                });
            writeln!(
                f,
                "error: missing expected dependency on {} \
                 (part of {})",
                consumer.name, deployment_unit
            )?;
            writeln!(
                f,
                "    reason this consumer is expected: {}",
                consumer.reason
            )?;
        }

        Ok(())
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
    pub missing: Option<&'a ApiMissingConsumers>,
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
        Self { client_pkgname, unexpected: BTreeSet::new(), missing: None }
    }

    pub fn error_count(&self) -> usize {
        self.unexpected.len() + self.missing.map_or(0, |m| m.error_count())
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

    // outputs (the structures that we're building up)
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
    /// `pkgname` via each of the given dependency chains `dep_paths`
    ///
    /// This only records anything if `pkgname` turns out to be a known API
    /// client package name, in which case this records that the server
    /// component consumes the corresponding API.
    pub fn found_package(
        &mut self,
        dunit_pkgname: &ServerComponentName,
        pkgname: &str,
        dep_paths: &[DepPath],
    ) {
        let Some(apis) = self.known_server_packages.get(pkgname) else {
            return;
        };

        for dep_path in dep_paths {
            for api in apis {
                self.found_api_producer(api, dunit_pkgname, dep_path);
            }
        }
    }

    /// Record that the given package is one of the deployment unit's top-level
    /// packages or embedded components (collectively, server components)
    pub fn found_deployment_unit_package(
        &mut self,
        deployment_unit: &DeploymentUnitName,
        server_component: &ServerComponentName,
    ) {
        // Metadata validation guarantees each component belongs to exactly one
        // deployment unit and appears once within it, so the insert is always
        // of a new server component.
        assert!(
            self.unit_server_components
                .entry(deployment_unit.clone())
                .or_default()
                .insert(server_component.clone()),
            "server component {server_component} appears more than once across \
             deployment units",
        );
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
    /// each of the given dependency chains `dep_paths`
    ///
    /// This only records cases where `pkgname` is a known client package for
    /// one of our APIs, in which case it records that this server component
    /// consumes the corresponding API.
    fn found_dependency(
        &mut self,
        server_pkgname: &ServerComponentName,
        pkgname: &str,
        dep_paths: &[DepPath],
    ) {
        let Some(api) = self.api_metadata.client_pkgname_lookup(pkgname) else {
            return;
        };

        // This is the name of a known client package.  Record it.
        let status = api.restricted_to_consumers.status(server_pkgname);
        let client_pkgname = ClientPackageName::from(pkgname.to_owned());
        for dep_path in dep_paths {
            self.api_consumers
                .entry(client_pkgname.clone())
                .or_insert_with(IdOrdMap::new)
                .entry(&server_pkgname)
                .or_insert_with(|| {
                    ApiConsumer::new(server_pkgname.clone(), status.clone())
                })
                .add_path(dep_path.clone());
            self.apis_consumed
                .entry(server_pkgname.clone())
                .or_insert_with(BTreeMap::new)
                .entry(client_pkgname.clone())
                .or_insert_with(Vec::new)
                .push(dep_path.clone());
        }
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
