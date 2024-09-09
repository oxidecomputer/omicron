// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Higher-level helpers for working with compiled API information

use crate::api_metadata::AllApiMetadata;
use crate::api_metadata::ApiMetadata;
use crate::cargo::DepPath;
use crate::cargo::Workspace;
use crate::ClientPackageName;
use crate::DeploymentUnitName;
use crate::ServerComponentName;
use crate::ServerPackageName;
use anyhow::bail;
use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::Package;
use petgraph::dot::Dot;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Parameters for loading information about system APIs
pub struct LoadArgs {
    /// path to developer-maintained API metadata
    pub api_manifest_path: Utf8PathBuf,
    /// path to a directory containing clones of repos that may contain APIs
    pub extra_repos_path: Utf8PathBuf,
}

/// Query information about the Dropshot/OpenAPI/Progenitor-based APIs within
/// the Oxide system
pub struct SystemApis {
    server_component_units: BTreeMap<ServerComponentName, DeploymentUnitName>,
    unit_server_components:
        BTreeMap<DeploymentUnitName, BTreeMap<ServerComponentName, DepPath>>,
    deployment_units: BTreeSet<DeploymentUnitName>,
    apis_consumed:
        BTreeMap<ServerComponentName, BTreeMap<ClientPackageName, DepPath>>,
    api_consumers:
        BTreeMap<ClientPackageName, BTreeMap<ServerComponentName, DepPath>>,
    api_producers: BTreeMap<ClientPackageName, (ServerComponentName, DepPath)>,
    api_metadata: AllApiMetadata,
    workspaces: Workspaces,
}

impl SystemApis {
    /// Load information about APIs in the system based on both
    /// developer-maintained metadata and Cargo-provided metadata
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

        // Compute the mapping from deployment units to the set of API server
        // packages that are inside each unit.  We do this by walking the
        // dependencies of each of the deployment units' packages.
        let mut deployment_units = BTreeSet::new();
        let mut server_component_units = BTreeMap::new();
        let mut unit_server_components = BTreeMap::new();
        let mut api_producers = BTreeMap::new();
        let mut errors = Vec::new();

        for (deployment_unit, dunit_info) in api_metadata.deployment_units() {
            deployment_units.insert(deployment_unit.clone());
            let mut servers_found = BTreeMap::new();

            let mut found_api_producer =
                |api: &ApiMetadata,
                 server_pkgname: ServerComponentName,
                 dep_path: &DepPath| {
                    // TODO
                    // Also debatable: dns-server is used by both the
                    // dns-server component *and* omicron-sled-agent's
                    // simulated sled agent.  This program does not support
                    // that.  But we don't care about the simulated sled
                    // agent, either, so just ignore it.
                    if *server_pkgname == "omicron-sled-agent"
                        && *api.client_package_name == "dns-service-client"
                    {
                        eprintln!(
                            "warning: ignoring legit dependency from \
                             omicron-sled-agent -> dns-server",
                        );
                        return;
                    }

                    if let Some((previous, _)) = api_producers.insert(
                        api.client_package_name.clone(),
                        (server_pkgname.clone(), dep_path.clone()),
                    ) {
                        errors.push(anyhow!(
                            "API for client {} appears to be \
                                 exported by multiple components: at \
                                 least {} and {} ({:?})",
                            api.client_package_name,
                            previous,
                            server_pkgname,
                            dep_path
                        ));
                    }
                };

            for dunit_pkg in &dunit_info.packages {
                let (workspace, server_pkg) =
                    workspaces.find_package_workspace(dunit_pkg)?;
                let server_component_name =
                    ServerComponentName::from(dunit_pkg.to_string());
                let self_dep_path = DepPath::for_pkg(server_pkg.id.clone());
                if let Some(previous) = servers_found.insert(
                    server_component_name.clone(),
                    self_dep_path.clone(),
                ) {
                    bail!(
                        "deployment unit {}: package {} appears twice \
                         in this deployment unit (at least: {:?} and {:?})",
                        deployment_unit,
                        dunit_pkg,
                        self_dep_path,
                        previous,
                    );
                }

                // In some cases, the server API package is exactly the same as
                // one of the deployment unit packages.
                let server_pkgname =
                    ServerPackageName::from(server_pkg.name.clone());
                if let Some(api) = server_packages.get(&server_pkgname) {
                    found_api_producer(api, dunit_pkg.clone(), &self_dep_path);
                }

                // In other cases, the deployment unit package depends (possibly
                // indirectly) on the API package.
                workspace.walk_required_deps_recursively(
                    server_pkg,
                    &mut |p: &Package, dep_path: &DepPath| {
                        let Some(api) = server_packages.get(&p.name) else {
                            return;
                        };

                        found_api_producer(api, dunit_pkg.clone(), dep_path);
                    },
                )?;
            }

            if !errors.is_empty() {
                for e in errors {
                    eprintln!("error: {:#}", e);
                }

                bail!("found at least one API exported by multiple servers");
            }

            for (server_component, _) in &servers_found {
                if let Some(previous) = server_component_units
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
            }

            assert!(unit_server_components
                .insert(deployment_unit.clone(), servers_found)
                .is_none());
        }

        // For each server component, determine which client APIs it depends on
        // by walking its dependencies.
        let mut apis_consumed = BTreeMap::new();
        let mut api_consumers = BTreeMap::new();
        for server_pkgname in server_component_units.keys() {
            let (workspace, pkg) =
                workspaces.find_package_workspace(server_pkgname)?;
            let mut clients_used: BTreeMap<ClientPackageName, DepPath> =
                BTreeMap::new();
            workspace
                .walk_required_deps_recursively(
                    pkg,
                    &mut |p: &Package, dep_path: &DepPath| {
                        // unwrap(): the workspace must know about each of these
                        // packages.
                        let parent_id = dep_path.bottom();
                        let parent =
                            workspace.find_pkg_by_id(parent_id).unwrap();

                        // TODO
                        // omicron_common depends on mg-admin-client solely to
                        // impl some `From` conversions.  That makes it look
                        // like just about everything depends on
                        // mg-admin-client, which isn't true.  We should
                        // consider reversing this, since most clients put those
                        // conversions into the client rather than
                        // omicron_common.  But for now, let's just ignore this
                        // particular dependency.
                        if p.name == "mg-admin-client"
                            && parent.name == "omicron-common"
                        {
                            return;
                        }

                        // TODO internal-dns depends on dns-service-client to use
                        // its types.  They're only used when *configuring* DNS,
                        // which is only done in a couple of components.  But
                        // many components use internal-dns to *read* DNS.  So
                        // like above, this makes it look like everything uses
                        // the DNS server API, but that's not true.  We should
                        // consider splitting this crate in two.  But for now,
                        // just ignore the specific dependency from internal-dns
                        // to dns-service-client.  If a consumer actually calls
                        // the DNS server, it will have a separate dependency.
                        if p.name == "dns-service-client"
                            && (parent.name == "internal-dns")
                        {
                            return;
                        }

                        // TODO nexus-types depends on dns-service-client and
                        // gateway-client for defining some types, but again,
                        // this doesn't mean that somebody using nexus-types is
                        // actually calling out to these services.  If they
                        // were, they'd need to have some other dependency on
                        // them.
                        if parent.name == "nexus-types"
                            && (p.name == "dns-service-client"
                                || p.name == "gateway-client")
                        {
                            return;
                        }

                        // TODO
                        // This one's debatable.  Everything with an Oximeter
                        // producer talks to Nexus.  But let's ignore those for
                        // now.  Maybe we could improve this by creating a
                        // separate API inside Nexus for this?
                        if parent.name == "oximeter-producer"
                            && p.name == "nexus-client"
                        {
                            eprintln!(
                                "warning: ignoring legit dependency from \
                                 oximeter-producer -> nexus_client"
                            );
                            return;
                        }

                        if api_metadata
                            .client_pkgname_lookup(&ClientPackageName::from(
                                p.name.to_owned(),
                            ))
                            .is_some()
                        {
                            clients_used.insert(
                                p.name.clone().into(),
                                dep_path.clone(),
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

            for (client_name, dep_path) in &clients_used {
                api_consumers
                    .entry(client_name.clone())
                    .or_insert_with(BTreeMap::new)
                    .insert(server_pkgname.clone(), dep_path.clone());
            }

            apis_consumed.insert(server_pkgname.clone(), clients_used);
        }

        Ok(SystemApis {
            server_component_units,
            deployment_units,
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
            .keys())
    }

    /// Returns the developer-maintained API metadata
    pub fn api_metadata(&self) -> &AllApiMetadata {
        &self.api_metadata
    }

    /// Given a server component, return the APIs consumed by this component
    pub fn component_apis_consumed(
        &self,
        server_component: &ServerComponentName,
    ) -> Box<dyn Iterator<Item = (&ClientPackageName, &DepPath)> + '_> {
        match self.apis_consumed.get(server_component) {
            Some(l) => Box::new(l.iter()),
            None => Box::new(std::iter::empty()),
        }
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
    ) -> Box<dyn Iterator<Item = (&ServerComponentName, &DepPath)> + '_> {
        match self.api_consumers.get(client) {
            Some(l) => Box::new(l.iter()),
            None => Box::new(std::iter::empty()),
        }
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
    pub fn dot_by_unit(&self) -> String {
        let mut graph = petgraph::graph::Graph::new();
        let nodes: BTreeMap<_, _> = self
            .deployment_units
            .iter()
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
                for (client_pkg, _) in self.component_apis_consumed(server_pkg)
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

        Dot::new(&graph).to_string()
    }

    /// Returns a string that can be passed to `dot(1)` to render a graph of
    /// API dependencies among server components
    pub fn dot_by_server_component(&self) -> String {
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
        for (server_component, consumed_apis) in &self.apis_consumed {
            // unwrap(): we created a node for each server component above.
            let my_node = nodes.get(server_component).unwrap();
            for client_pkg in consumed_apis.keys() {
                let other_component = self.api_producer(client_pkg).unwrap();
                let other_node = nodes.get(other_component).unwrap();
                graph.add_edge(*my_node, *other_node, client_pkg.clone());
            }
        }

        Dot::new(&graph).to_string()
    }
}

/// Thin wrapper around a list of workspaces that makes it easy to query which
/// workspace has which package
struct Workspaces {
    workspaces: BTreeMap<String, Workspace>,
}

impl Workspaces {
    /// Given repository checkouts at `extra_repos_path`, use `cargo metadata`
    /// to load workspace metadata for all the workspaces that we care about
    ///
    /// The data found is validated against `api_metadata`.
    ///
    /// On success, returns `(workspaces, warnings)`, where `warnings` is a list
    /// of potential inconsistencies between API metadata and Cargo metadata.
    pub fn load(
        extra_repos_path: &Utf8Path,
        api_metadata: &AllApiMetadata,
    ) -> Result<(Workspaces, Vec<anyhow::Error>)> {
        // Load information about each of the known workspaces.
        //
        // Each of these involves running `cargo metadata`, which is pretty I/O
        // intensive.  Overall latency benefits significantly from
        // parallelizing.
        //
        // If we had many more repos than this, we'd probably want to limit the
        // concurrency.
        let handles: Vec<_> =
            ["omicron", "crucible", "maghemite", "propolis", "dendrite"]
                .into_iter()
                .map(|repo_name| {
                    let extra_arg = if repo_name == "omicron" {
                        None
                    } else {
                        Some(extra_repos_path.to_owned())
                    };
                    std::thread::spawn(move || {
                        let arg = extra_arg.as_ref().map(|s| s.as_path());
                        Workspace::load(repo_name, arg)
                    })
                })
                .collect();

        let workspaces: BTreeMap<_, _> = handles
            .into_iter()
            .map(|join_handle| {
                let thr_result = join_handle.join().map_err(|e| {
                    anyhow!("workspace load thread panicked: {:?}", e)
                })?;
                let workspace = thr_result?;
                Ok::<_, anyhow::Error>((workspace.name().to_owned(), workspace))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        // Validate the metadata against what we found in the workspaces.
        let mut client_pkgnames_unused: BTreeSet<_> =
            api_metadata.client_pkgnames().collect();
        let mut warnings = Vec::new();
        for (_, workspace) in &workspaces {
            for client_pkgname in workspace.client_packages() {
                if api_metadata.client_pkgname_lookup(client_pkgname).is_some()
                {
                    // It's possible that we will find multiple references
                    // to the same client package name.  That's okay.
                    client_pkgnames_unused.remove(client_pkgname);
                } else {
                    warnings.push(anyhow!(
                        "found client package missing from API manifest: {}",
                        client_pkgname
                    ));
                }
            }
        }

        for c in client_pkgnames_unused {
            warnings.push(anyhow!(
                "API manifest refers to unknown client package: {}",
                c
            ));
        }

        Ok((Workspaces { workspaces }, warnings))
    }

    /// Given the name of a workspace package from one of our workspaces, return
    /// the corresponding `Workspace` and `Package`
    ///
    /// This is only for finding packages defined *in* one of these workspaces.
    /// For any other kind of package (e.g., transitive dependencies, which
    /// might come from crates.io or other Git repositories), the name is not
    /// unique and you'd need to use some other mechanism to get information
    /// about it.
    pub fn find_package_workspace(
        &self,
        server_pkgname: &str,
    ) -> Result<(&Workspace, &Package)> {
        // Figure out which workspace has this package.
        let found_in_workspaces: Vec<_> = self
            .workspaces
            .values()
            .filter_map(|w| {
                w.find_workspace_package(&server_pkgname).map(|p| (w, p))
            })
            .collect();
        // XXX-dap there are actually two separate packages called "dpd-client".
        // One is in the Omicron workspace.  The other is in the Dendrite
        // workspace.  I don't know how we ever know which one gets used!
        if server_pkgname == "dpd-client" && found_in_workspaces.len() == 2 {
            if found_in_workspaces[0].0.name() == "omicron" {
                return Ok(found_in_workspaces[0]);
            }
            if found_in_workspaces[1].0.name() == "omicron" {
                return Ok(found_in_workspaces[1]);
            }
        }
        ensure!(
            !found_in_workspaces.is_empty(),
            "server package {:?} was not found in any workspace",
            server_pkgname
        );
        ensure!(
            found_in_workspaces.len() == 1,
            "server package {:?} was found in more than one workspace: {}",
            server_pkgname,
            found_in_workspaces
                .into_iter()
                .map(|(w, _)| w.name())
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(found_in_workspaces[0])
    }
}

fn parse_toml_file<T: DeserializeOwned>(path: &Utf8Path) -> Result<T> {
    let s = std::fs::read_to_string(path)
        .with_context(|| format!("read {:?}", path))?;
    toml::from_str(&s).with_context(|| format!("parse {:?}", path))
}
