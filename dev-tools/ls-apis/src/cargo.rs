// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extract API metadata from Cargo metadata

use crate::ClientPackageName;
use anyhow::bail;
use anyhow::{Context, Result, anyhow, ensure};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::{CargoOpt, Package};
use cargo_metadata::{DependencyKind, PackageId};
use std::collections::BTreeSet;
use std::collections::{BTreeMap, VecDeque};

/// Query package and dependency-related information about a Cargo workspace
pub struct Workspace {
    /// human-readable label for the workspace
    /// (generally the basename of the repo's URL)
    name: String,

    /// local path to the root of the workspace
    workspace_root: Utf8PathBuf,

    /// list of all package metadata, by package id
    ///
    /// The dependency information in `Package` should not be used.  It
    /// describes what's written in the Cargo files.  `nodes_by_id` reflects
    /// precisely what Cargo actually resolved instead.
    packages_by_id: BTreeMap<PackageId, Package>,

    /// list of all packages' dependency information, by package id
    nodes_by_id: BTreeMap<PackageId, cargo_metadata::Node>,

    /// list of all workspace-level packages, by name
    workspace_packages_by_name: BTreeMap<String, PackageId>,

    /// list of all packages that appear to be Progenitor-based clients
    /// (having a direct dependency on `progenitor`)
    progenitor_clients: BTreeSet<ClientPackageName>,
}

impl Workspace {
    /// Use `cargo metadata` to load information about a workspace called `name`
    ///
    /// If `workspace_manifest` is `None`, then information is loaded about the
    /// current workspace.  Otherwise, that path is used as the workspace
    /// manifest.
    pub fn load(
        name: &str,
        manifest_path: Option<&Utf8Path>,
        extra_features: Option<CargoOpt>,
        ignored_non_clients: &BTreeSet<ClientPackageName>,
    ) -> Result<Self> {
        eprintln!(
            "loading metadata for workspace {name} from {}",
            manifest_path
                .as_ref()
                .map(|p| p.to_string())
                .as_deref()
                .unwrap_or("current workspace")
        );

        let mut cmd = cargo_metadata::MetadataCommand::new();
        if let Some(manifest_path) = manifest_path {
            cmd.manifest_path(manifest_path);
        }
        if let Some(extra_features) = extra_features {
            cmd.features(extra_features);
        }
        let metadata = match cmd.exec() {
            Err(original_err) if name == "maghemite" => {
                dendrite_workaround(cmd, original_err)?
            }
            otherwise => otherwise.with_context(|| {
                format!("failed to load metadata for {name}")
            })?,
        };
        let workspace_root = metadata.workspace_root;

        // Build an index of all packages by id.  Identify duplicates because we
        // assume there shouldn't be any but we want to know if that assumption
        // is wrong.
        //
        // Also build an index of workspaces packages by name so that we can
        // quickly find their id.
        let mut packages_by_id = BTreeMap::new();
        let mut workspace_packages_by_name = BTreeMap::new();
        for pkg in metadata.packages {
            if pkg.source.is_none() {
                if workspace_packages_by_name
                    .insert(pkg.name.to_string(), pkg.id.clone())
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
                d.name.as_str() == "progenitor"
                    && d.dep_kinds.iter().any(|k| {
                        matches!(
                            k.kind,
                            DependencyKind::Normal | DependencyKind::Build
                        )
                    })
            }) {
                if pkg.name.ends_with("-client") {
                    progenitor_clients
                        .insert(ClientPackageName::from(pkg.name.to_string()));
                } else if !ignored_non_clients.contains(pkg.name.as_str()) {
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

    /// Return the name of this workspace
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a list of workspace packages that appear to be Progenitor
    /// clients
    pub fn client_packages(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.progenitor_clients.iter()
    }

    /// Returns information about package `pkgname` in the workspace
    ///
    /// Note that this only returns information about workspace packages (i.e.,
    /// packages that are defined in the workspace itself).  To find information
    /// about transitive dependencies, you need to be more specific about which
    /// version you want.  Use `pkgids()` for that.
    pub fn find_workspace_package(&self, pkgname: &str) -> Option<&Package> {
        self.workspace_packages_by_name
            .get(pkgname)
            .and_then(|pkgid| self.packages_by_id.get(pkgid))
    }

    /// Given a workspace package, return the relative path from the root of the
    /// workspace to that package.
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

    /// Iterate over the required dependencies of package `root`, invoking
    /// `func` for each one as:
    ///
    /// ```ignore
    /// func(package: &Package, dep_path: &DepPath)
    /// ```
    ///
    /// where `package` is the package that is (directly or indirectly) a
    /// dependency of `root` and `dep_path` describes the dependency path from
    /// `root` to `package`.
    pub fn walk_required_deps_recursively(
        &self,
        root: &Package,
        func: &mut dyn FnMut(&Package, &DepPath),
    ) -> Result<()> {
        struct Remaining<'a> {
            node: &'a cargo_metadata::Node,
            path: DepPath,
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
            path: DepPath::for_pkg(root.id.clone()),
        }];
        let mut seen: BTreeSet<PackageId> = BTreeSet::new();

        while let Some(Remaining { node: next, path }) = remaining.pop() {
            for d in &next.deps {
                let did = &d.pkg;
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
                if seen.contains(did) {
                    continue;
                }

                seen.insert(did.clone());
                let dep_path = path.with_dependency_on(did.clone());
                remaining.push(Remaining { node: dep_node, path: dep_path });
            }
        }

        Ok(())
    }

    /// Return all package ids for the given `pkgname`
    ///
    /// `pkgname` does not need to be a workspace package.  There may be many
    /// packages with this name, generally at different versions.
    pub fn pkgids<'a>(
        &'a self,
        pkgname: &'a str,
    ) -> impl Iterator<Item = &'a PackageId> + 'a {
        self.packages_by_id.iter().filter_map(move |(pkgid, pkg)| {
            if pkg.name.as_str() == pkgname { Some(pkgid) } else { None }
        })
    }

    /// Return information about a package by id
    ///
    /// This does not need to be a workspace package.
    pub fn pkg_by_id(&self, pkgid: &PackageId) -> Option<&Package> {
        self.packages_by_id.get(pkgid)
    }
}

/// Given a path to a `Cargo.toml` file for a package, return the parent
/// directory
///
/// Fails explicitly if the path doesn't match what we'd expect.
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

/// Describes a "dependency path": a path through the Cargo dependency graph
/// from one package to another, which describes how one package depends on
/// another
#[derive(Debug, Clone)]
pub struct DepPath(VecDeque<PackageId>);

impl DepPath {
    /// Creates a new `DepPath` for package `pkgid`
    pub fn for_pkg(pkgid: PackageId) -> DepPath {
        DepPath(VecDeque::from([pkgid]))
    }

    /// Returns the bottom-most node in this path
    ///
    /// In a dependency chain from root package `p1` to its dependency `p2` that
    /// depends on `p3`, the bottom-most node would be `p3`.
    pub fn bottom(&self) -> &PackageId {
        &self.0[0]
    }

    /// Iterates over the nodes in this path, from the bottom to the root
    pub fn nodes(&self) -> impl Iterator<Item = &PackageId> {
        self.0.iter()
    }

    /// Creates a new dependency path based on this one, but where the bottom of
    /// this path depends on package `pkgid`
    pub fn with_dependency_on(&self, pkgid: PackageId) -> DepPath {
        let mut rv = self.clone();
        rv.0.push_front(pkgid);
        rv
    }

    /// Returns whether any component of the path contains any of the given
    /// pkgids
    pub fn contains_any(&self, pkgids: &BTreeSet<&PackageId>) -> bool {
        self.0.iter().any(|p| pkgids.contains(p))
    }
}

// Dendrite is not (yet) a public repository, but it's a dependency of
// Maghemite. There are two expected cases for running Omicron tests locally
// that we know of:
// - The developer has a Git credential helper of some kind set up to
//   successfully clone private repositories over HTTPS.
// - The developer has an SSH agent or other local SSH key that they use to
//   clone repositories over SSH.
// We call this function when we fail to fetch the Dendrite repository over
// HTTPS. Under the assumption that the user falls in the second group.
// we attempt to use SSH to fetch the repository by setting `GIT_CONFIG_*`
// environment variables to rewrite the repository URL to an SSH URL. If that
// fails, we'll verbosely inform the user as to how both methods failed and
// provide some context.
//
// This entire workaround can and very much should go away once Dendrite is
// public.
fn dendrite_workaround(
    mut cmd: cargo_metadata::MetadataCommand,
    original_err: cargo_metadata::Error,
) -> Result<cargo_metadata::Metadata> {
    eprintln!(
        "warning: failed to load metadata for maghemite; \
        trying dendrite workaround"
    );

    let count = std::env::var_os("GIT_CONFIG_COUNT")
        .map(|s| -> Result<u64> {
            s.into_string()
                .map_err(|_| anyhow!("$GIT_CONFIG_COUNT is not an integer"))?
                .parse()
                .context("$GIT_CONFIG_COUNT is not an integer")
        })
        .transpose()?
        .unwrap_or_default();
    cmd.env("CARGO_NET_GIT_FETCH_WITH_CLI", "true");
    cmd.env(
        format!("GIT_CONFIG_KEY_{count}"),
        "url.git@github.com:oxidecomputer/dendrite.insteadOf",
    );
    cmd.env(
        format!("GIT_CONFIG_VALUE_{count}"),
        "https://github.com/oxidecomputer/dendrite",
    );
    cmd.env("GIT_CONFIG_COUNT", (count + 1).to_string());
    cmd.exec().map_err(|err| {
        let cmd = cmd.cargo_command();
        let original_err = anyhow::Error::from(original_err);
        let err = anyhow::Error::from(err);
        anyhow::anyhow!("failed to load metadata for maghemite

`cargo xtask ls-apis` expects to be able to run `cargo metadata` on the
Maghemite workspace that Omicron depends on. Maghemite has a dependency on a
private repository (Dendrite), so `cargo metadata` can fail if you are unable
to clone Dendrite via an HTTPS URL. As a fallback, we also tried to run `cargo
metadata` with environment variables that force `cargo metadata` to use an SSH
URL; unfortunately that also failed.

To successfully run this command (or expectorate test), your environment needs
to be set up to clone a private Oxide repository from GitHub. This can be done
with either a Git credential helper or an SSH key:

https://doc.rust-lang.org/cargo/appendix/git-authentication.html
https://docs.github.com/en/get-started/getting-started-with-git/caching-your-github-credentials-in-git

(If you don't have access to private Oxide repos, you won't be able to
successfully run this command or test.)

More context: https://github.com/oxidecomputer/omicron/issues/6839

===== The fallback command that failed: =====
{cmd:?}

===== The error that occurred while fetching using HTTPS: =====
{original_err:?}

===== The error that occurred while fetching using SSH (fallback): =====
{err:?}")
    })
}
