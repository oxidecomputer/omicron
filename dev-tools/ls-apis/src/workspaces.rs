// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combines information about multiple `Workspace`s

use crate::ClientPackageName;
use crate::api_metadata::AllApiMetadata;
use crate::cargo::Workspace;
use anyhow::{Context, Result, anyhow, bail, ensure};
use camino::Utf8Path;
use cargo_metadata::CargoOpt;
use cargo_metadata::Package;
use cargo_metadata::PackageId;
use omicron_zone_package::package::PackageSource;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::LazyLock;

struct RelatedRepoConfig {
    repo_name: &'static str,
    expected_pkg_name: &'static str,
    extra_cargo_features: Option<CargoOpt>,
}

static RELATED_REPOS: LazyLock<[RelatedRepoConfig; 5]> = LazyLock::new(|| {
    [
        RelatedRepoConfig {
            repo_name: "crucible",
            expected_pkg_name: "crucible-agent-client",
            extra_cargo_features: None,
        },
        RelatedRepoConfig {
            repo_name: "dendrite",
            expected_pkg_name: "dpd-client",
            extra_cargo_features: None,
        },
        RelatedRepoConfig {
            repo_name: "propolis",
            expected_pkg_name: "propolis-client",
            // The artifacts shipped from the Propolis repo (particularly,
            // `propolis-server`) are built with the `omicron-build`
            // feature, which is not enabled by default.  Enable this
            // feature when loading the Propolis repo metadata so that we
            // see the dependency tree that a shipping system will have.
            extra_cargo_features: Some(CargoOpt::SomeFeatures(vec![
                String::from("omicron-build"),
            ])),
        },
        RelatedRepoConfig {
            repo_name: "maghemite",
            expected_pkg_name: "mg-admin-client",
            extra_cargo_features: None,
        },
        RelatedRepoConfig {
            repo_name: "lldp",
            expected_pkg_name: "lldpd-client",
            extra_cargo_features: None,
        },
    ]
});

/// Thin wrapper around a list of workspaces that makes it easy to query which
/// workspace has which package
pub(crate) struct Workspaces {
    workspaces: BTreeMap<String, Workspace>,
}

impl Workspaces {
    /// Use `cargo metadata` to load workspace metadata for all the workspaces
    /// that we care about
    ///
    /// The data found is validated against `api_metadata`.
    ///
    /// On success, returns `(workspaces, warnings)`, where `warnings` is a list
    /// of inconsistencies between API metadata and Cargo metadata.
    pub fn load(
        api_metadata: &AllApiMetadata,
        workspace_root: &Utf8Path,
    ) -> Result<(Workspaces, Vec<anyhow::Error>)> {
        // First, load information about the "omicron" workspace.  This is the
        // current workspace so we don't need to provide the path to it.
        let ignored_non_clients = api_metadata.ignored_non_clients();
        let omicron = Arc::new(Workspace::load(
            "omicron",
            None,
            None,
            ignored_non_clients,
        )?);

        // Next, load the top level package manifest.  This will tell us for
        // each related component (like Crucible, Maghemite, etc.), which commit
        // of that component's repo Omicron actually deploys to running systems.
        let package_manifest_path =
            workspace_root.join("package-manifest.toml");
        let package_manifest =
            omicron_zone_package::config::parse(&package_manifest_path)
                .with_context(|| {
                    format!("parsing {package_manifest_path:?}")
                })?;
        let mut related_repo_commits = BTreeMap::new();
        for related_repo in &*RELATED_REPOS {
            let commit =
                find_repo_commit(&package_manifest, related_repo.repo_name)?;
            related_repo_commits.insert(related_repo.repo_name, commit);
        }

        // In order to assemble this metadata, Cargo already has a clone of most
        // of the other workspaces that we care about.  We'll use those clones
        // rather than manage our own.
        //
        // To find each of these other repos, we'll need to look up a package
        // that comes from each of these workspaces and look at where its local
        // manifest file is.
        //
        // Loading each workspace involves running `cargo metadata`, which is
        // pretty I/O intensive.  Latency benefits significantly from
        // parallelizing, though we have to respect the dependencies.  We can't
        // look up a package in "maghemite" before we've loaded Maghemite.
        //
        // If we had many more repos than this, we'd probably want to limit the
        // concurrency.
        let handles: Vec<_> = RELATED_REPOS
            .iter()
            .map(|repo_config| {
                let RelatedRepoConfig {
                    repo_name,
                    expected_pkg_name,
                    extra_cargo_features,
                } = repo_config;
                let mine = omicron.clone();
                let my_ignored = ignored_non_clients.clone();
                // unwrap(): we loaded a commit for each repo in the loop above
                let expected_commit =
                    (*related_repo_commits.get(repo_name).unwrap()).clone();
                std::thread::spawn(move || {
                    load_dependent_repo(
                        &mine,
                        repo_name,
                        expected_pkg_name,
                        extra_cargo_features.clone(),
                        my_ignored,
                        &expected_commit,
                    )
                })
            })
            .collect();

        let mut workspaces: BTreeMap<_, _> = handles
            .into_iter()
            .map(|join_handle| {
                let thr_result = join_handle.join().map_err(|e| {
                    anyhow!("workspace load thread panicked: {:?}", e)
                })?;
                let workspace = thr_result?;
                Ok::<_, anyhow::Error>((workspace.name().to_owned(), workspace))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        workspaces.insert(
            String::from("omicron"),
            Arc::into_inner(omicron).expect("no more Omicron Arc references"),
        );

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
                        "workspace {}: found Progenitor-based client package \
                         missing from API manifest: {}",
                        workspace.name(),
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

    /// Returns the set of distinct pkgids for package "pkg" among all
    /// workspaces.
    pub fn workspace_pkgids<'a>(
        &'a self,
        pkgname: &'a str,
    ) -> BTreeSet<&'a PackageId> {
        self.workspaces.values().flat_map(move |w| w.pkgids(pkgname)).collect()
    }
}

/// Load a `Workspace` for a repo `repo` using the manifest path inferred by
/// looking up one of its packages `pkgname` in `workspace`
///
/// For example, we might locate the Crucible repo by looking up the
/// `crucible-pantry-client` package in the Omicron workspace, finding its
/// manifest path, and locating the containing Crucible workspace.
fn load_dependent_repo(
    workspace: &Workspace,
    repo: &str,
    pkgname: &str,
    extra_features: Option<CargoOpt>,
    ignored_non_clients: BTreeSet<ClientPackageName>,
    expected_commit: &str,
) -> Result<Workspace> {
    // It's possible to have more than one non-workspace package with a given
    // name.  For example, Omicron references `dpd-client` in multiple ways:
    // from Nexus and through lldpd-client.  So which version do we want?  Well,
    // the goal of looking up `dpd-client` is really to find the source for
    // Dendrite.  And we want the Dendrite that's actually going to run on a
    // real system.  That's the one specified by package-manifest.toml.  The
    // caller already figured out which commit that is and provided it in
    // `expected_commit`.
    //
    // In summary: we're going to choose the package matching `expected_commit`.
    let mut found_pkg = None;

    for pkgid in workspace.pkgids(pkgname) {
        let pkginfo = workspace.pkg_by_id(pkgid).with_context(|| {
            format!("failed to find metadata for found package {pkgid:?}")
        })?;

        let Some(source) = &pkginfo.source else {
            eprintln!(
                "warn: looking up {pkgid:?}: unexpectedly found source `None`"
            );
            continue;
        };

        // This is cheesy, but it works okay for now and fails safely.
        if source.repr.contains(expected_commit) {
            found_pkg = Some(pkginfo);
            break;
        }

        eprintln!(
            "warn: looking up {pkgid:?}: looking for git commit \
             {expected_commit} (based on package-manifest.toml), found \
             source {source:?}"
        );
        eprintln!(
            "If another version of package {pkgname:?} is found corresponding \
             with this commit, then it may be suspicious to have multiple version \
             of this package, but it will not break this tool."
        );
        eprintln!(
            "If not, there's a mismatch between commits in package-manifest.toml \
             and Cargo.toml or there is a bug in this tool."
        );
    }

    let Some(pkg) = found_pkg else {
        bail!(
            "found no versions of package {pkgname:?} matching the git commit \
             found in package-manifest.toml ({})",
            expected_commit,
        );
    };

    // The package metadata should show where the package's manifest file should
    // be.  This may be buried deep in the workspace.  How do we find the root
    // of the workspace?  Fortunately, `cargo locate-project` can do this.
    let cargo_var = std::env::var("CARGO");
    let cargo = cargo_var.as_deref().unwrap_or("cargo");
    let output = std::process::Command::new(cargo)
        .arg("locate-project")
        .arg("--workspace")
        .arg("--manifest-path")
        .arg(&pkg.manifest_path)
        .arg("--message-format")
        .arg("plain")
        .output()
        .context("`cargo locate-project`")
        .and_then(|output| {
            if !output.status.success() {
                Err(anyhow!(
                    "`cargo locate-project` exited with {:?}: stderr: {:?}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr),
                ))
            } else {
                String::from_utf8(output.stdout).map_err(|_| {
                    anyhow!("`cargo locate-project` output was not UTF-8")
                })
            }
        })
        .with_context(|| {
            format!(
                "locating workspace for {:?} (from {:?}) with \
                 `cargo locate-project`",
                pkgname, &pkg.manifest_path
            )
        })?;
    let workspace_manifest = Utf8Path::new(output.trim_end());
    Workspace::load(
        repo,
        Some(workspace_manifest),
        extra_features,
        &ignored_non_clients,
    )
}

fn find_repo_commit(
    package_manifest: &omicron_zone_package::config::Config,
    repo_name: &str,
) -> Result<String> {
    // We want to figure out which version of repo `repo_name` is actually
    // deployed on real systems.  To do that, we look at what's in the package
    // manifest.  All of the repos we care about are packaged into Omicron as
    // prebuilt packages.  The one hitch is that the same repo may appear
    // multiple times in the package manifest.  In that case, we require that
    // they all be the same commit.
    let candidates: BTreeSet<&String> = package_manifest
        .packages
        .iter()
        .filter_map(|(_, pkg)| match &pkg.source {
            PackageSource::Local { .. }
            | PackageSource::Composite { .. }
            | PackageSource::Manual => None,
            PackageSource::Prebuilt { repo, commit, .. } => {
                if repo == repo_name { Some(commit) } else { None }
            }
        })
        .collect();

    if candidates.is_empty() {
        bail!("expected repo {repo_name:?} to appear in package-manifest.toml");
    }

    if candidates.len() > 1 {
        bail!(
            "expected repo {repo_name:?} to have exactly one git commit \
             in package-manifest.toml, but found multiple: {candidates:?}",
        );
    }

    // unwrap(): we already checked that there's at least one
    Ok(candidates.into_iter().next().unwrap().clone())
}
