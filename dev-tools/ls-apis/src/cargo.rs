// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extract API metadata from Cargo metadata

use crate::ClientPackageName;
use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::DependencyKind;
use cargo_metadata::Metadata;
use cargo_metadata::Package;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Display;
use url::Url;

pub struct Workspace {
    name: String,
    all_packages: BTreeMap<String, Package>, // XXX-dap memory usage
    workspace_packages: BTreeMap<String, Package>, // XXX-dap memory usage
    progenitor_clients: BTreeSet<ClientPackageName>,
}

impl Workspace {
    pub fn load(name: &str, extra_repos: Option<&Utf8Path>) -> Result<Self> {
        eprintln!("loading metadata for workspace {name}");

        let mut cmd = cargo_metadata::MetadataCommand::new();
        if let Some(extra_repos) = extra_repos {
            cmd.manifest_path(extra_repos.join(name).join("Cargo.toml"));
        }
        let metadata = cmd.exec().context("loading metadata")?;

        // Find all packages with a direct non-dev, non-build dependency on
        // "progenitor".  These generally ought to be suffixed with "-client".
        let progenitor_clients = direct_dependents(&metadata, "progenitor")?
            .into_iter()
            .filter_map(|mypkg| {
                if mypkg.name.ends_with("-client") {
                    Some(ClientPackageName::from(mypkg.name))
                } else {
                    eprintln!("ignoring apparent non-client: {}", mypkg.name);
                    None
                }
            })
            .collect();

        // XXX-dap do we want workspace packages or all packages?
        let all_packages = metadata
            .packages
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();

        let workspace_packages = metadata
            .packages
            .iter()
            .filter_map(|p| {
                if p.source.is_none() {
                    Some((p.name.clone(), p.clone()))
                } else {
                    None
                }
            })
            .collect();

        Ok(Workspace {
            name: name.to_owned(),
            all_packages,
            workspace_packages,
            progenitor_clients,
        })
    }

    pub fn client_packages(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.progenitor_clients.iter()
    }

    pub fn find_package(&self, pkgname: &str) -> Option<&Package> {
        self.all_packages.get(pkgname)
    }

    pub fn find_workspace_package(&self, pkgname: &str) -> Option<&Package> {
        self.workspace_packages.get(pkgname)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn walk_required_deps_recursively(
        &self,
        root: &Package,
        func: &mut dyn FnMut(&Package, &Package),
    ) -> Result<()> {
        let mut remaining = vec![root];
        let mut seen: BTreeSet<String> = BTreeSet::new();

        while let Some(next) = remaining.pop() {
            for d in &next.dependencies {
                if seen.contains(&d.name) {
                    continue;
                }

                seen.insert(d.name.clone());

                if d.optional {
                    continue;
                }

                if !matches!(
                    d.kind,
                    DependencyKind::Normal | DependencyKind::Build
                ) {
                    continue;
                }

                // XXX-dap this is getting an arbitrary package with this name,
                // not necessarily the one depended-on here.
                let pkg = self.find_package(&d.name).ok_or_else(|| {
                    anyhow!(
                        "package {:?} has dependency {:?} not in workspace \
                         metadata",
                        next.name,
                        d.name
                    )
                })?;
                func(next, pkg);
                remaining.push(pkg);
            }
        }

        Ok(())
    }
}

pub struct MyPackage {
    name: String,
    location: MyPackageLocation,
}

impl MyPackage {
    fn new(workspace: &Metadata, pkg: &Package) -> Result<MyPackage> {
        // Figure out where this thing is.  It's generally one of two places:
        // (1) In a remote repository.  In that case, it will have a "source"
        //     property that's the URL to a package.
        // (2) Inside this workspace.  In that case, it will have no "source",
        //     but it will have a manifest_path that's inside this workspace.
        let location = if let Some(source) = &pkg.source {
            let source_repo_str = &source.repr;
            let repo_name =
                source_repo_name(source_repo_str).with_context(|| {
                    format!("parsing source {:?}", source_repo_str)
                })?;

            // Figuring out where in that repo the package lives is trickier.
            // Here we encode some knowledge of where Cargo would have checked
            // out the repo.
            let cargo_home = std::env::var("CARGO_HOME")
                .context("looking up CARGO_HOME in environment")?;
            let cargo_path =
                Utf8PathBuf::from(cargo_home).join("git").join("checkouts");
            let path =
                pkg.manifest_path.strip_prefix(&cargo_path).map_err(|_| {
                    anyhow!(
                    "expected non-local package manifest path ({:?}) to be \
                     under {:?}",
                    pkg.manifest_path,
                    cargo_path,
                )
                })?;

            // There should be two extra leading directory components here.
            // Remove them.  We've gone too far if the file name isn't right
            // after that.
            let tail: Utf8PathBuf = path.components().skip(2).collect();
            let path = cargo_toml_parent(&tail, &pkg.manifest_path)?;
            MyPackageLocation::RemoteRepo { oxide_github_repo: repo_name, path }
        } else {
            let manifest_path = &pkg.manifest_path;
            let relative_path = manifest_path
                .strip_prefix(&workspace.workspace_root)
                .map_err(|_| {
                    anyhow!(
                    "no \"source\", so assuming this package is inside this \
                     repo, but its manifest path ({:?}) is not under the \
                     workspace root ({:?})",
                    manifest_path,
                    &workspace.workspace_root
                )
                })?;
            let path = cargo_toml_parent(&relative_path, &manifest_path)?;

            MyPackageLocation::Omicron { path }
        };

        Ok(MyPackage { name: pkg.name.clone(), location })
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

pub enum MyPackageLocation {
    Omicron { path: Utf8PathBuf },
    RemoteRepo { oxide_github_repo: String, path: Utf8PathBuf },
}

impl Display for MyPackageLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyPackageLocation::Omicron { path } => {
                write!(f, "omicron:{}", path)
            }
            MyPackageLocation::RemoteRepo { oxide_github_repo, path } => {
                write!(f, "{}:{}", oxide_github_repo, path)
            }
        }
    }
}

fn source_repo_name(raw: &str) -> Result<String> {
    let repo_url =
        Url::parse(raw).with_context(|| format!("parsing {:?}", raw))?;
    ensure!(repo_url.scheme() == "git+https", "unsupported URL scheme",);
    ensure!(
        matches!(repo_url.host_str(), Some(h) if h == "github.com"),
        "unexpected URL host (expected \"github.com\")",
    );
    let path_segments: Vec<_> = repo_url
        .path_segments()
        .ok_or_else(|| anyhow!("expected URL to contain path segments"))?
        .collect();
    ensure!(
        path_segments.len() == 2,
        "expected exactly two path segments in URL",
    );
    ensure!(
        path_segments[0] == "oxidecomputer",
        "expected repo under Oxide's GitHub organization",
    );

    Ok(path_segments[1].to_string())
}

fn direct_dependents(
    workspace: &Metadata,
    pkg_name: &str,
) -> Result<Vec<MyPackage>> {
    workspace
        .packages
        .iter()
        .filter_map(|pkg| {
            if pkg.dependencies.iter().any(|dep| {
                matches!(
                    dep.kind,
                    DependencyKind::Normal | DependencyKind::Build
                ) && dep.name == pkg_name
            }) {
                Some(
                    MyPackage::new(workspace, pkg)
                        .with_context(|| format!("package {:?}", pkg.name)),
                )
            } else {
                None
            }
        })
        .collect()
}
