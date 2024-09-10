// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Query information about the Dropshot/OpenAPI/Progenitor-based APIs within
//! the Oxide system

use crate::api_metadata::AllApiMetadata;
use crate::api_metadata::ApiMetadata;
use crate::cargo::DepPath;
use crate::cargo::Workspace;
use crate::parse_toml_file;
use crate::workspaces::Workspaces;
use crate::ClientPackageName;
use crate::DeploymentUnitName;
use crate::LoadArgs;
use crate::ServerComponentName;
use crate::ServerPackageName;
use anyhow::{anyhow, bail, Context, Result};
use camino::Utf8PathBuf;
use cargo_metadata::Package;
use parse_display::{Display, FromStr};
use petgraph::dot::Dot;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use crate::api_metadata::Evaluation;

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
    api_consumers: BTreeMap<
        ClientPackageName,
        BTreeMap<ServerComponentName, Vec<DepPath>>,
    >,

    /// maps an API name to the server component that exposes that API
    api_producers: BTreeMap<ClientPackageName, (ServerComponentName, DepPath)>,

    /// source of developer-maintained API metadata
    api_metadata: AllApiMetadata,
    /// source of Cargo package metadata
    workspaces: Workspaces,
}

impl SystemApis {
    /// Load information about APIs in the system based on both developer-
    /// maintained metadata and Cargo-provided metadata
    pub fn load(args: LoadArgs) -> Result<SystemApis> {
        // Load the API manifest.
        let api_metadata: AllApiMetadata =
            parse_toml_file(&args.api_manifest_path)?;
        // Load Cargo metadata and validate it against the manifest.
        let (workspaces, warnings) =
            Workspaces::load(&args.extra_repos_path, &api_metadata)?;
        if !warnings.is_empty() {
            for e in warnings {
                eprintln!("warning: {:#}", e);
            }
        }

        // Create an index of server package names, mapping each one to the API
        // that it corresponds to.
        let server_packages: BTreeMap<_, _> = api_metadata
            .apis()
            .map(|api| (api.server_package_name.clone(), api))
            .collect();

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

        if !tracker.errors.is_empty() {
            for e in tracker.errors {
                eprintln!("error: {:#}", e);
            }

            bail!("found at least one API exported by multiple servers");
        }

        let (server_component_units, unit_server_components, api_producers) = (
            tracker.server_component_units,
            tracker.unit_server_components,
            tracker.api_producers,
        );

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
                        if !ignore_dependency(workspace, p, dep_path) {
                            deps_tracker.found_dependency(
                                server_pkgname,
                                &p.name,
                                dep_path,
                            );
                        }
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

    /// For one deployment unit, iterate over the servers contained in it
    pub fn deployment_unit_servers(
        &self,
        unit: &DeploymentUnitName,
    ) -> Result<impl Iterator<Item = &ServerComponentName>> {
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
    ) -> Result<impl Iterator<Item = (&ClientPackageName, &DepPath)> + '_> {
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
    /// component that provides it
    pub fn api_producer(
        &self,
        client: &ClientPackageName,
    ) -> Result<&ServerComponentName> {
        self.api_producers
            .get(client)
            .ok_or_else(|| anyhow!("unknown client API: {:?}", client))
            .map(|s| &s.0)
    }

    /// Given the client package name for an API, return the list of server
    /// components that consume it, along with the Cargo dependency path that
    /// connects each server to the client package
    pub fn api_consumers(
        &self,
        client: &ClientPackageName,
        filter: ApiDependencyFilter,
    ) -> Result<impl Iterator<Item = (&ServerComponentName, &DepPath)> + '_>
    {
        let mut rv = Vec::new();

        let Some(api_consumers) = self.api_consumers.get(client) else {
            return Ok(rv.into_iter());
        };

        for (server_pkgname, dep_paths) in api_consumers {
            let mut include = None;
            for p in dep_paths {
                if filter.should_include(
                    &self.api_metadata,
                    &self.workspaces,
                    &client,
                    p,
                )? {
                    include = Some(p);
                    break;
                }
            }

            if let Some(p) = include {
                rv.push((server_pkgname, p))
            }
        }

        Ok(rv.into_iter())
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
        Ok(format!("{}:{}", workspace.name(), pkgpath))
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
                    let other_component =
                        self.api_producer(client_pkg).unwrap();
                    let other_unit = self
                        .server_component_units
                        .get(other_component)
                        .unwrap();
                    let other_node = nodes.get(other_unit).unwrap();
                    graph.add_edge(*my_node, *other_node, client_pkg.clone());
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
        let mut graph = petgraph::graph::Graph::new();
        let nodes: BTreeMap<_, _> = self
            .server_component_units
            .keys()
            .map(|server_component| {
                (server_component.clone(), graph.add_node(server_component))
            })
            .collect();

        // Now walk through the server components, walk through each one of the
        // clients used by those, and create a corresponding edge.
        for server_component in self.apis_consumed.keys() {
            // unwrap(): we created a node for each server component above.
            let my_node = nodes.get(server_component).unwrap();
            let consumed_apis =
                self.component_apis_consumed(server_component, filter)?;
            for (client_pkg, _) in consumed_apis {
                let other_component = self.api_producer(client_pkg).unwrap();
                let other_node = nodes.get(other_component).unwrap();
                graph.add_edge(*my_node, *other_node, client_pkg.clone());
            }
        }

        Ok(Dot::new(&graph).to_string())
    }
}

/// Helper for building structures to index which deployment units contain which
/// server components and what APIs those components expose
///
/// See `SystemApis::load()` for how this is used.
struct ServerComponentsTracker<'a> {
    // inputs
    known_server_packages: &'a BTreeMap<ServerPackageName, &'a ApiMetadata>,

    // outputs (structures that we're building up)
    errors: Vec<anyhow::Error>,
    server_component_units: BTreeMap<ServerComponentName, DeploymentUnitName>,
    unit_server_components:
        BTreeMap<DeploymentUnitName, BTreeSet<ServerComponentName>>,
    api_producers: BTreeMap<ClientPackageName, (ServerComponentName, DepPath)>,
}

impl<'a> ServerComponentsTracker<'a> {
    pub fn new(
        known_server_packages: &'a BTreeMap<ServerPackageName, &'a ApiMetadata>,
    ) -> ServerComponentsTracker<'a> {
        ServerComponentsTracker {
            known_server_packages,
            errors: Vec::new(),
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
        // TODO
        // Also debatable: dns-server is used by both the
        // dns-server component *and* omicron-sled-agent's
        // simulated sled agent.  This program does not support
        // that.  But we don't care about the simulated sled
        // agent, either, so just ignore it.
        if **server_pkgname == "omicron-sled-agent"
            && *api.client_package_name == "dns-service-client"
        {
            eprintln!(
                "warning: ignoring legit dependency from \
                 omicron-sled-agent -> dns-server",
            );
            return;
        }

        if let Some((previous, _)) = self.api_producers.insert(
            api.client_package_name.clone(),
            (server_pkgname.clone(), dep_path.clone()),
        ) {
            self.errors.push(anyhow!(
                "API for client {} appears to be exported by multiple \
                 components: at least {} and {} ({:?})",
                api.client_package_name,
                previous,
                server_pkgname,
                dep_path
            ));
        }
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
        let Some(api) = self.known_server_packages.get(pkgname) else {
            return;
        };

        self.found_api_producer(api, dunit_pkgname, dep_path);
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

        assert!(self
            .unit_server_components
            .entry(deployment_unit.clone())
            .or_default()
            .insert(server_component.clone()));
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
    api_consumers: BTreeMap<
        ClientPackageName,
        BTreeMap<ServerComponentName, Vec<DepPath>>,
    >,
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
        if self.api_metadata.client_pkgname_lookup(pkgname).is_none() {
            return;
        }

        // This is the name of a known client package.  Record it.
        let client_pkgname = ClientPackageName::from(pkgname.to_owned());
        self.api_consumers
            .entry(client_pkgname.clone())
            .or_insert_with(BTreeMap::new)
            .entry(server_pkgname.clone())
            .or_insert_with(Vec::new)
            .push(dep_path.clone());
        self.apis_consumed
            .entry(server_pkgname.clone())
            .or_insert_with(BTreeMap::new)
            .entry(client_pkgname)
            .or_insert_with(Vec::new)
            .push(dep_path.clone());
    }
}

/// Returns whether a particular Rust package dependency should be ignored when
/// trying to infer which of our packages depend on APIs.
fn ignore_dependency(
    workspace: &Workspace,
    package: &Package,
    dep_path: &DepPath,
) -> bool {
    // unwrap(): the workspace must know about each of these packages.
    let parent_id = dep_path.bottom();
    let parent = workspace.find_pkg_by_id(parent_id).unwrap();
    let parent_name = &parent.name;
    let package_name = &package.name;

    // TODO
    // omicron_common depends on mg-admin-client solely to impl some `From`
    // conversions.  That makes it look like just about everything depends on
    // mg-admin-client, which isn't true.  We should consider reversing this,
    // since most clients put those conversions into the client rather than
    // omicron_common.  But for now, let's just ignore this particular
    // dependency.
    if package_name == "mg-admin-client" && parent_name == "omicron-common" {
        return true;
    }

    // TODO internal-dns depends on dns-service-client to use its types.
    // They're only used when *configuring* DNS, which is only done in a couple
    // of components.  But many components use internal-dns to *read* DNS.  So
    // like above, this makes it look like everything uses the DNS server API,
    // but that's not true.  We should consider splitting this crate in two.
    // But for now, just ignore the specific dependency from internal-dns to
    // dns-service-client.  If a consumer actually calls the DNS server, it will
    // have a separate dependency.
    if package_name == "dns-service-client" && parent_name == "internal-dns" {
        return true;
    }

    // TODO nexus-types depends on dns-service-client and gateway-client for
    // defining some types, but again, this doesn't mean that somebody using
    // nexus-types is actually calling out to these services.  If they were,
    // they'd need to have some other dependency on them.
    if parent_name == "nexus-types"
        && (package_name == "dns-service-client"
            || package_name == "gateway-client")
    {
        return true;
    }

    false
}

/// Specifies which API dependencies to include vs. ignore when iterating
/// dependencies
#[derive(Clone, Copy, Debug, Default, Display, FromStr)]
#[display(style = "kebab-case")]
pub enum ApiDependencyFilter {
    /// Include all dependencies found from Cargo package metadata
    All,

    /// Exclude dependencies that are explicitly marked as outside the update
    /// DAG
    #[default]
    NotNonDag,
}

impl ApiDependencyFilter {
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
            ApiDependencyFilter::NotNonDag => evaluation != Evaluation::NonDag,
        })
    }
}
