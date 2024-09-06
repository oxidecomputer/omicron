// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extract API metadata from Cargo metadata

use crate::ClientPackageName;
use anyhow::bail;
use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::Package;
use cargo_metadata::{DependencyKind, PackageId};
use std::collections::BTreeSet;
use std::collections::{BTreeMap, VecDeque};

pub type DepPath = VecDeque<PackageId>;

pub struct Workspace {
    name: String,
    workspace_root: Utf8PathBuf,
    packages_by_id: BTreeMap<PackageId, Package>,
    nodes_by_id: BTreeMap<PackageId, cargo_metadata::Node>,
    progenitor_clients: BTreeSet<ClientPackageName>,
    workspace_packages_by_name: BTreeMap<String, PackageId>,
}

// Packages that may be flagged as clients because they directly depend on
// Progenitor, but which are not really clients.
const IGNORED_NON_CLIENTS: &[&str] = &[
    // omicron-common depends on progenitor so that it can define some generic
    // error handling and a generic macro for defining clients.  omicron-common
    // itself is not a progenitor-based client.
    "omicron-common",
    // propolis-mock-server uses Progenitor to generate types that are
    // compatible with the real Propolis server.  It doesn't actually use the
    // client and we don't really care about it for these purposes anyway.
    "propolis-mock-server",
];

impl Workspace {
    pub fn load(name: &str, extra_repos: Option<&Utf8Path>) -> Result<Self> {
        eprintln!("loading metadata for workspace {name}");

        let mut cmd = cargo_metadata::MetadataCommand::new();
        if let Some(extra_repos) = extra_repos {
            cmd.manifest_path(extra_repos.join(name).join("Cargo.toml"));
        }
        let metadata = cmd.exec().context("loading metadata")?;
        let workspace_root = metadata.workspace_root;

        // Build an index of all packages by id.  Identify duplicates because we
        // assume this isn't possible and we want to know if our assumptions are
        // wrong.
        //
        // Also build an index of workspaces packages by name so that we can
        // quickly find their id.
        let mut packages_by_id = BTreeMap::new();
        let mut workspace_packages_by_name = BTreeMap::new();
        for pkg in metadata.packages {
            if pkg.source.is_none() {
                if workspace_packages_by_name
                    .insert(pkg.name.clone(), pkg.id.clone())
                    .is_some()
                {
                    bail!(
                        "workspace {:?}: unexpected duplicate workspace \
                         package with name {:?}",
                        name,
                        pkg.name,
                    );
                }
            }

            if let Some(previous) = packages_by_id.insert(pkg.id.clone(), pkg) {
                bail!(
                    "workspace {:?}: unexpected duplicate package with id {:?}",
                    name,
                    previous.id
                );
            }
        }

        // Build an index mapping packages to their corresponding node in the
        // resolved dependency tree.
        //
        // While we're walking the resolved dependency tree, identify any
        // Progenitor clients.
        let mut progenitor_clients = BTreeSet::new();
        let mut nodes_by_id = BTreeMap::new();
        let resolve = metadata.resolve.ok_or_else(|| {
            anyhow!(
                "workspace {:?}: has no package resolution information",
                name
            )
        })?;
        for node in resolve.nodes {
            let Some(pkg) = packages_by_id.get(&node.id) else {
                bail!(
                    "workspace {:?}: found resolution information for package \
                     with id {:?}, but no associated package",
                    name,
                    node.id,
                );
            };

            if node.deps.iter().any(|d| {
                d.name == "progenitor"
                    && d.dep_kinds.iter().any(|k| {
                        matches!(
                            k.kind,
                            DependencyKind::Normal | DependencyKind::Build
                        )
                    })
            }) {
                if pkg.name.ends_with("-client") {
                    progenitor_clients
                        .insert(ClientPackageName::from(pkg.name.clone()));
                } else if !IGNORED_NON_CLIENTS.contains(&pkg.name.as_str()) {
                    eprintln!(
                        "workspace {:?}: ignoring apparent non-client that \
                         uses progenitor: {}",
                        name, pkg.name
                    );
                }
            }

            if let Some(previous) = nodes_by_id.insert(node.id.clone(), node) {
                bail!(
                    "workspace {:?}: unexpected duplicate resolution for \
                     package {:?}",
                    name,
                    previous.id,
                );
            }
        }

        // There should be resolution information for every package that we
        // found.
        for pkgid in packages_by_id.keys() {
            ensure!(
                nodes_by_id.contains_key(pkgid),
                "workspace {:?}: found package {:?} with no resolution \
                 information",
                name,
                pkgid,
            );
        }

        Ok(Workspace {
            name: name.to_owned(),
            workspace_root,
            packages_by_id,
            nodes_by_id,
            progenitor_clients,
            workspace_packages_by_name,
        })
    }

    pub fn all_package_ids(&self) -> impl Iterator<Item = &PackageId> {
        self.packages_by_id.keys()
    }

    pub fn client_packages(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.progenitor_clients.iter()
    }

    pub fn find_workspace_package(&self, pkgname: &str) -> Option<&Package> {
        self.workspace_packages_by_name
            .get(pkgname)
            .and_then(|pkgid| self.packages_by_id.get(pkgid))
    }

    pub fn find_pkg_by_id(&self, pkgid: &PackageId) -> Option<&Package> {
        self.packages_by_id.get(pkgid)
    }

    pub fn find_workspace_package_path(
        &self,
        pkgname: &str,
    ) -> Result<Utf8PathBuf> {
        let pkg = self.find_workspace_package(pkgname).ok_or_else(|| {
            anyhow!("workspace {:?} has no package {:?}", self.name, pkgname)
        })?;
        let manifest_path = &pkg.manifest_path;
        let relative_path =
            manifest_path.strip_prefix(&self.workspace_root).map_err(|_| {
                anyhow!(
                    "workspace {:?} package {:?} manifest is not under \
                         the workspace root ({:?})",
                    self.name,
                    pkgname,
                    &self.workspace_root,
                )
            })?;
        let path = cargo_toml_parent(&relative_path, &manifest_path)?;
        Ok(path)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn walk_required_deps_recursively(
        &self,
        root: &Package,
        func: &mut dyn FnMut(&Package, &DepPath),
    ) -> Result<()> {
        struct Remaining<'a> {
            node: &'a cargo_metadata::Node,
            path: VecDeque<PackageId>,
        }

        let root_node = self.nodes_by_id.get(&root.id).ok_or_else(|| {
            anyhow!(
                "workspace {:?}: walking dependencies for package {:?}: \
                 package is not known in this workspace",
                self.name,
                root.name
            )
        })?;

        let mut remaining = vec![Remaining {
            node: root_node,
            path: VecDeque::from([root.id.clone()]),
        }];
        let mut seen: BTreeSet<PackageId> = BTreeSet::new();

        while let Some(Remaining { node: next, path }) = remaining.pop() {
            for d in &next.deps {
                let did = &d.pkg;
                if seen.contains(did) {
                    continue;
                }

                seen.insert(did.clone());
                if !d.dep_kinds.iter().any(|k| {
                    matches!(
                        k.kind,
                        DependencyKind::Normal | DependencyKind::Build
                    )
                }) {
                    continue;
                }

                // unwraps: We verified during loading that we have metadata for
                // all package ids for which we have nodes in the dependency
                // tree.  We also verified during loading that we have nodes in
                // the dependency tree for all package ids for which we have
                // package metadata.
                let dep_pkg = self.packages_by_id.get(did).unwrap();
                let dep_node = self.nodes_by_id.get(did).unwrap();
                func(dep_pkg, &path);
                let mut dep_path = path.clone();
                dep_path.push_front(did.clone());
                remaining.push(Remaining { node: dep_node, path: dep_path })
            }
        }

        Ok(())
    }
}

fn cargo_toml_parent(
    path: &Utf8Path,
    label_path: &Utf8Path,
) -> Result<Utf8PathBuf> {
    ensure!(
        path.file_name() == Some("Cargo.toml"),
        "unexpected manifest path: {:?}",
        label_path
    );
    let path = path
        .parent()
        .ok_or_else(|| anyhow!("unexpected manifest path: {:?}", label_path))?
        .to_owned();
    Ok(path)
}
