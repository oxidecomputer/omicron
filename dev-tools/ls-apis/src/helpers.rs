// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Higher-level helpers for working with compiled API information

use crate::api_metadata::AllApiMetadata;
use crate::cargo::Workspace;
use crate::ClientPackageName;
use crate::DeploymentUnit;
use crate::ServerComponent;
use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::Package;
use petgraph::dot::Dot;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub struct LoadArgs {
    pub api_manifest_path: Utf8PathBuf,
    pub extra_repos_path: Utf8PathBuf,
}

pub struct Apis {
    server_component_units: BTreeMap<ServerComponent, DeploymentUnit>,
    unit_server_components: BTreeMap<DeploymentUnit, Vec<ServerComponent>>,
    deployment_units: BTreeSet<DeploymentUnit>,
    apis_consumed: BTreeMap<ServerComponent, BTreeSet<ClientPackageName>>,
    api_metadata: AllApiMetadata,
}

impl Apis {
    pub fn load(args: LoadArgs) -> Result<Apis> {
        let helper = ApisHelper::load(args)?;
        if !helper.warnings.is_empty() {
            for e in &helper.warnings {
                eprintln!("warning: {:#}", e);
            }
        }

        // Collect the distinct set of deployment units for all the APIs.
        let deployment_units: BTreeSet<_> =
            helper.api_metadata.apis().map(|a| a.group().clone()).collect();

        // Create a mapping from server package to its deployment unit.
        let server_component_units: BTreeMap<_, _> = helper
            .api_metadata
            .apis()
            .map(|a| (a.server_component.clone(), a.group().clone()))
            .collect();

        // Compute a reverse mapping from deployment unit to the server
        // packages deployed inside it.
        let mut unit2components = BTreeMap::new();
        for (server_component, deployment_unit) in &server_component_units {
            let list = unit2components
                .entry(deployment_unit.clone())
                .or_insert_with(Vec::new);
            list.push(server_component.clone());
        }

        // For each server component, determine which client APIs it depends on
        // by walking its dependencies.
        // XXX-dap figure out how to abstract this
        let mut apis_consumed = BTreeMap::new();
        for server_pkgname in helper.api_metadata.server_components() {
            let (workspace, pkg) =
                helper.find_package_workspace(server_pkgname)?;
            let mut clients_used = BTreeSet::new();
            workspace
                .walk_required_deps_recursively(
                    pkg,
                    &mut |parent: &Package, p: &Package| {
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

                        if helper
                            .api_metadata
                            .client_pkgname_lookup(&p.name)
                            .is_some()
                        {
                            clients_used.insert(p.name.clone().into());
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

            apis_consumed.insert(server_pkgname.clone(), clients_used);
        }

        Ok(Apis {
            server_component_units,
            deployment_units,
            unit_server_components: unit2components,
            apis_consumed,
            api_metadata: helper.api_metadata,
        })
    }

    fn all_deployment_unit_components(
        &self,
    ) -> impl Iterator<Item = (&DeploymentUnit, &Vec<ServerComponent>)> {
        self.unit_server_components.iter()
    }

    fn component_apis_consumed(
        &self,
        server_component: &ServerComponent,
    ) -> Box<dyn Iterator<Item = &ClientPackageName> + '_> {
        match self.apis_consumed.get(server_component) {
            Some(l) => Box::new(l.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

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
        for (deployment_unit, server_components) in
            self.all_deployment_unit_components()
        {
            let my_node = nodes.get(deployment_unit).unwrap();
            for server_pkg in server_components {
                for client_pkg in self.component_apis_consumed(server_pkg) {
                    let api = self
                        .api_metadata
                        .client_pkgname_lookup(client_pkg)
                        .unwrap();
                    let other_component = &api.server_component;
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
}

pub struct ApisHelper {
    api_metadata: AllApiMetadata,
    workspaces: BTreeMap<String, Workspace>,
    warnings: Vec<anyhow::Error>,
}

impl ApisHelper {
    pub fn load(args: LoadArgs) -> Result<ApisHelper> {
        // Load the API manifest.
        let api_metadata: AllApiMetadata =
            parse_toml_file(&args.api_manifest_path)?;

        // Load information about each of the workspaces.
        let mut workspaces = BTreeMap::new();
        workspaces.insert(
            String::from("omicron"),
            Workspace::load("omicron", None)
                .context("loading Omicron workspace metadata")?,
        );

        for repo in ["crucible", "maghemite", "propolis", "dendrite"] {
            workspaces.insert(
                String::from(repo),
                Workspace::load(repo, Some(&args.extra_repos_path))
                    .with_context(|| {
                        format!("load metadata for workspace {:?}", repo)
                    })?,
            );
        }

        // Validate the metadata against what we found in the workspaces.
        let mut client_pkgnames_unused: BTreeSet<_> =
            api_metadata.client_pkgnames().collect();
        let mut errors = Vec::new();
        for (_, workspace) in &workspaces {
            for client_pkgname in workspace.client_packages() {
                if api_metadata.client_pkgname_lookup(client_pkgname).is_some()
                {
                    // It's possible that we will find multiple references
                    // to the same client package name.  That's okay.
                    client_pkgnames_unused.remove(client_pkgname);
                } else {
                    errors.push(anyhow!(
                        "found client package missing from API manifest: {}",
                        client_pkgname
                    ));
                }
            }
        }

        for c in client_pkgnames_unused {
            errors.push(anyhow!(
                "API manifest refers to unknown client package: {}",
                c
            ));
        }

        Ok(ApisHelper { api_metadata, workspaces, warnings: errors })
    }

    pub fn find_package_workspace(
        &self,
        server_pkgname: &str,
    ) -> Result<(&Workspace, &Package)> {
        // Figure out which workspace has this package.
        let found_in_workspaces: Vec<_> = self
            .workspaces
            .values()
            .filter_map(|w| w.find_package(&server_pkgname).map(|p| (w, p)))
            .collect();
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
