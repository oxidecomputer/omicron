// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combines information about multiple `Workspace`s

use crate::ClientPackageName;
use crate::PatchedDepPolicy;
use crate::api_metadata::AllApiMetadata;
use crate::cargo::Workspace;
use crate::errors::{ErrorAccumulator, LoadError};
use anyhow::{Context, Result, anyhow, bail, ensure};
use camino::Utf8Path;
use cargo_metadata::CargoOpt;
use cargo_metadata::Package;
use cargo_metadata::PackageId;
use omicron_zone_package::package::PackageSource;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

/// A Git commit identifier (typically a full SHA-1 hash) identifying a
/// specific revision of one of the related repositories.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct GitCommit(String);
NewtypeDebug! { () pub struct GitCommit(String); }
NewtypeDeref! { () pub struct GitCommit(String); }
NewtypeDisplay! { () pub struct GitCommit(String); }
NewtypeFrom! { () pub struct GitCommit(String); }

struct RelatedRepoConfig {
    repo_name: &'static str,
    expected_pkg_name: &'static str,
    extra_cargo_features: &'static [&'static str],
}

static RELATED_REPOS: [RelatedRepoConfig; 5] = [
    RelatedRepoConfig {
        repo_name: "crucible",
        expected_pkg_name: "crucible-agent-client",
        extra_cargo_features: &[],
    },
    RelatedRepoConfig {
        repo_name: "dendrite",
        expected_pkg_name: "dpd-client",
        extra_cargo_features: &[],
    },
    RelatedRepoConfig {
        repo_name: "propolis",
        expected_pkg_name: "propolis-client",
        // The artifacts shipped from the Propolis repo (particularly,
        // `propolis-server`) are built with the `omicron-build` feature,
        // which is not enabled by default.  Enable this feature when
        // loading the Propolis repo metadata so that we see the dependency
        // tree that a shipping system will have.
        extra_cargo_features: &["omicron-build"],
    },
    RelatedRepoConfig {
        repo_name: "maghemite",
        expected_pkg_name: "mg-admin-client",
        extra_cargo_features: &[],
    },
    RelatedRepoConfig {
        repo_name: "lldp",
        expected_pkg_name: "lldpd-client",
        extra_cargo_features: &[],
    },
];

/// Thin wrapper around a list of workspaces that makes it easy to query which
/// workspace has which package
pub(crate) struct Workspaces {
    workspaces: BTreeMap<String, Workspace>,
}

impl Workspaces {
    /// Use `cargo metadata` to load workspace metadata for all the workspaces
    /// that we care about
    ///
    /// The data found is validated against `api_metadata`.  Any inconsistencies
    /// between the API metadata and the Cargo metadata are recorded into
    /// `errors`.
    ///
    /// Returns `None` if the workspaces couldn't be loaded at all (e.g., a
    /// `cargo metadata` invocation failed) or if an inconsistency was
    /// recorded.
    pub fn load(
        api_metadata: &AllApiMetadata,
        workspace_root: &Utf8Path,
        patched_dep_policy: PatchedDepPolicy,
        errors: &mut ErrorAccumulator,
    ) -> Option<Workspaces> {
        // If `cargo metadata` won't run, there's no `Workspaces` to return, so
        // record that as a single fatal error.
        let workspaces = match Self::load_workspace_map(
            api_metadata,
            workspace_root,
            patched_dep_policy,
        ) {
            Ok(workspaces) => workspaces,
            Err(source) => {
                errors.push(LoadError::LoadWorkspaces { source });
                return None;
            }
        };

        // Validate the metadata against what we found in the workspaces.
        let mut client_pkgnames_unused: BTreeSet<_> =
            api_metadata.client_pkgnames().collect();
        for (_, workspace) in &workspaces {
            for client_pkgname in workspace.client_packages() {
                if api_metadata.client_pkgname_lookup(client_pkgname).is_some()
                {
                    // It's possible that we will find multiple references
                    // to the same client package name.  That's okay.
                    client_pkgnames_unused.remove(client_pkgname);
                } else {
                    errors.push(LoadError::ClientPackageMissingFromManifest {
                        workspace: workspace.name().to_owned(),
                        client: client_pkgname.clone(),
                    });
                }
            }
        }

        for c in client_pkgnames_unused {
            errors.push(LoadError::UnknownClientPackageInManifest {
                client: c.clone(),
            });
        }

        if errors.has_errors() {
            return None;
        }

        Some(Workspaces { workspaces })
    }

    /// Load every workspace we care about via `cargo metadata`.
    fn load_workspace_map(
        api_metadata: &AllApiMetadata,
        workspace_root: &Utf8Path,
        patched_dep_policy: PatchedDepPolicy,
    ) -> Result<BTreeMap<String, Workspace>> {
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
        let related_repo_commits: Vec<(&RelatedRepoConfig, GitCommit)> =
            RELATED_REPOS
                .iter()
                .map(|related_repo| {
                    let commit = find_repo_commit(
                        &package_manifest,
                        related_repo.repo_name,
                    )?;
                    Ok((related_repo, commit))
                })
                .collect::<Result<_>>()?;

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
        // parallelizing, though if we had many more repos than this, we'd
        // probably want to limit the concurrency.
        let handles: Vec<_> = related_repo_commits
            .into_iter()
            .map(|(repo_config, expected_commit)| {
                let RelatedRepoConfig {
                    repo_name,
                    expected_pkg_name,
                    extra_cargo_features,
                } = repo_config;
                let mine = omicron.clone();
                let my_ignored = ignored_non_clients.clone();
                let extra_features = if extra_cargo_features.is_empty() {
                    None
                } else {
                    Some(CargoOpt::SomeFeatures(
                        extra_cargo_features
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ))
                };
                std::thread::spawn(move || {
                    load_dependent_repo(
                        &mine,
                        repo_name,
                        expected_pkg_name,
                        extra_features,
                        my_ignored,
                        &expected_commit,
                        patched_dep_policy,
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
        Ok(workspaces)
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
    expected_commit: &GitCommit,
    patched_dep_policy: PatchedDepPolicy,
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
    // If we don't find such a match but we do encounter a candidate with no
    // source (likely a local Cargo `[patch]` override), we may fall back to
    // that one depending on `patched_dep_policy`.  Along the way, we keep
    // track of any candidates whose git source doesn't match
    // `expected_commit` so that we can produce a helpful message.
    let mut found_pkg = None;
    let mut patched_pkg = None;
    let mut mismatched: Vec<(&PackageId, &cargo_metadata::Source)> = Vec::new();

    for pkgid in workspace.pkgids(pkgname) {
        let pkginfo = workspace.pkg_by_id(pkgid).with_context(|| {
            format!("failed to find metadata for found package {pkgid:?}")
        })?;

        let Some(source) = &pkginfo.source else {
            // A package with no source is most likely one that has been
            // overridden by a local Cargo `[patch]` pointing at a filesystem
            // path.  There is no reliable way to check that such a checkout
            // corresponds to `expected_commit`.  We defer the decision about
            // whether to accept it until after we've scanned every candidate,
            // in case a real commit match is also present.
            if patched_pkg.is_none() {
                patched_pkg = Some(pkginfo);
            }
            continue;
        };

        // Determine if this instance of this package matches the git commit
        // that we expect.
        //
        // This implementation is cheesy, but it works okay for now and fails
        // safely.
        if source
            .repr
            .rsplit_once('#')
            .is_some_and(|(_, hash)| hash == expected_commit.as_str())
        {
            // We know at this point that we're going to succeed and return this
            // package.  But we still want to report any diverging versions
            // below if we find them.
            if found_pkg.is_none() {
                found_pkg = Some(pkginfo);
            }
        } else {
            mismatched.push((pkgid, source));
        }
    }

    let pkg = match (found_pkg, patched_pkg, patched_dep_policy) {
        (Some(pkg), _, _) => {
            if !mismatched.is_empty() {
                eprintln!(
                    "note: package {pkgname:?}: found a version matching \
                     the git commit in package-manifest.toml \
                     ({expected_commit}), but also found other versions at \
                     different commits.  This does not affect this tool.  But \
                     it's potentially suspicious that different components are \
                     using different versions."
                );
                for (pkgid, source) in &mismatched {
                    eprintln!("  - {pkgid:?}: {source:?}");
                }
            }
            pkg
        }
        (None, Some(pkg), PatchedDepPolicy::AssumeMatch) => {
            eprintln!(
                "note: package {pkgname:?}: no candidate matched the commit \
                 pinned in package-manifest.toml ({expected_commit}), but a \
                 candidate with no source (likely a local Cargo [patch]) was \
                 found.  Proceeding with that candidate because \
                 --assume-patched-deps-match was given.  This tool cannot \
                 verify that the patched checkout matches the pinned commit."
            );
            pkg
        }
        (None, Some(_), PatchedDepPolicy::Reject) => {
            bail!(
                "found no versions of package {pkgname:?} matching the git \
                 commit found in package-manifest.toml ({expected_commit}).  \
                 A candidate with no source (likely a local Cargo [patch]) \
                 was found, but this tool cannot verify that it matches.  \
                 If you believe the patched version is correct, re-run with \
                 --assume-patched-deps-match."
            );
        }
        (None, None, _) => {
            if !mismatched.is_empty() {
                eprintln!(
                    "note: found the following versions of package \
                     {pkgname:?}, none of which matched the git commit in \
                     package-manifest.toml ({expected_commit}):"
                );
                for (pkgid, source) in &mismatched {
                    eprintln!("  - {pkgid:?}: {source:?}");
                }
                eprintln!(
                    "There may be a mismatch between commits in \
                     package-manifest.toml and Cargo.toml or a bug in this \
                     tool."
                );
            }
            bail!(
                "found no versions of package {pkgname:?} matching the git \
                 commit found in package-manifest.toml ({expected_commit})",
            );
        }
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
) -> Result<GitCommit> {
    // We want to figure out which version of repo `repo_name` is actually
    // deployed on real systems.  To do that, we look at what's in the package
    // manifest.  All of the repos we care about are packaged into Omicron as
    // prebuilt packages.  The one hitch is that the same repo may appear
    // multiple times in the package manifest.  In that case, we require that
    // they all be the same commit.
    let candidates: BTreeSet<GitCommit> = package_manifest
        .packages
        .values()
        .filter_map(|pkg| match &pkg.source {
            PackageSource::Local { .. }
            | PackageSource::Composite { .. }
            | PackageSource::Manual => None,
            PackageSource::Prebuilt { repo, commit, .. } => {
                if repo == repo_name {
                    Some(GitCommit::from(commit.clone()))
                } else {
                    None
                }
            }
        })
        .collect();

    if candidates.is_empty() {
        bail!(
            "expected repo {repo_name:?} to appear in package-manifest.toml \
             as a 'prebuilt' package"
        );
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

#[cfg(test)]
mod test {
    use super::*;
    use omicron_zone_package::config::{Config, PackageName, ServiceName};
    use omicron_zone_package::package::{Package, PackageOutput};

    const TEST_REPO: &str = "testrepo";

    /// Builds a package manifest for testing `find_repo_commit`.
    ///
    /// `commits` contains an array of arbitrary strings that stand in for Git
    /// commit SHAs.  For each entry in this array, a `Prebuilt` package will be
    /// included for repo `TEST_REPO` having the corresponding Git commit SHA.
    /// This can be used to test cases where the config has no copies of the
    /// test package, one copy of it, multiple copies with the same SHA, or
    /// multiple copies with different SHAs.
    ///
    /// The returned manifest always contains a hardcoded set of "noise"
    /// packages that should never match `TEST_REPO`: one `Local`, one
    /// `Composite`, one `Manual`, and one `Prebuilt` for a different repo than
    /// the one we're testing with.
    fn make_manifest(commits: &[&str]) -> Config {
        let mut packages = BTreeMap::new();

        packages.insert(
            PackageName::new_const("some-local"),
            Package {
                service_name: ServiceName::new_const("some-local"),
                source: PackageSource::Local {
                    blobs: None,
                    buildomat_blobs: None,
                    rust: None,
                    paths: Vec::new(),
                },
                output: PackageOutput::Tarball,
                only_for_targets: None,
                setup_hint: None,
            },
        );

        packages.insert(
            PackageName::new_const("some-composite"),
            Package {
                service_name: ServiceName::new_const("some-composite"),
                source: PackageSource::Composite { packages: Vec::new() },
                output: PackageOutput::Tarball,
                only_for_targets: None,
                setup_hint: None,
            },
        );

        packages.insert(
            PackageName::new_const("some-manual"),
            Package {
                service_name: ServiceName::new_const("some-manual"),
                source: PackageSource::Manual,
                output: PackageOutput::Tarball,
                only_for_targets: None,
                setup_hint: None,
            },
        );

        packages.insert(
            PackageName::new_const("other-repo-prebuilt"),
            Package {
                service_name: ServiceName::new_const("other-repo-prebuilt"),
                source: PackageSource::Prebuilt {
                    repo: String::from("otherrepo"),
                    commit: String::from("11111111111111111111"),
                    sha256: String::from("deadbeef"),
                },
                output: PackageOutput::Tarball,
                only_for_targets: None,
                setup_hint: None,
            },
        );

        for (i, commit) in commits.iter().enumerate() {
            let name = format!("testrepo-pkg-{i}");
            packages.insert(
                PackageName::new(name.clone()).unwrap(),
                Package {
                    service_name: ServiceName::new(name).unwrap(),
                    source: PackageSource::Prebuilt {
                        repo: String::from(TEST_REPO),
                        commit: String::from(*commit),
                        sha256: String::from("deadbeef"),
                    },
                    output: PackageOutput::Tarball,
                    only_for_targets: None,
                    setup_hint: None,
                },
            );
        }

        Config { packages, target: Default::default() }
    }

    #[test]
    fn find_repo_commit_success() {
        // Two prebuilt entries for the same repo pointing at the same commit
        // should collapse to a single commit.
        let commit = "abcdef1234567890abcdef1234567890abcdef12";
        let manifest = make_manifest(&[commit, commit]);
        let found = find_repo_commit(&manifest, TEST_REPO).unwrap();
        assert_eq!(found.as_str(), commit);
    }

    #[test]
    fn find_repo_commit_multiple_prebuilts_different_commits() {
        let commit_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let commit_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let manifest = make_manifest(&[commit_a, commit_b]);
        let err = find_repo_commit(&manifest, TEST_REPO).unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "expected repo \"testrepo\" to have exactly one git commit \
             in package-manifest.toml, but found multiple: \
             {\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \
             \"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"}",
        );
    }

    #[test]
    fn find_repo_commit_repo_absent() {
        // Empty `commits` means no prebuilt entries for `TEST_REPO` at all,
        // but the manifest still contains other packages (including a
        // prebuilt for a different repo) that should all be ignored.
        let manifest = make_manifest(&[]);
        let err = find_repo_commit(&manifest, TEST_REPO).unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "expected repo \"testrepo\" to appear in package-manifest.toml \
             as a 'prebuilt' package",
        );
    }
}
