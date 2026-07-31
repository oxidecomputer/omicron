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

/// One entry in `MANIFEST_PREBUILT_REPOS`, describing a repo that appears in
/// `package-manifest.toml` as a `prebuilt` package.
struct PrebuiltRepoConfig {
    repo_name: &'static str,
    behavior: PrebuiltRepoBehavior,
}

/// Describes what this tool should do with a prebuilt repo listed in
/// `package-manifest.toml`.
enum PrebuiltRepoBehavior {
    /// Look up the given package in the Omicron workspace and use its
    /// resolved location to load the repo's own Cargo workspace metadata.
    Inspect {
        expected_pkg_name: &'static str,
        extra_cargo_features: &'static [&'static str],
    },

    /// Verify that the repo appears in `package-manifest.toml` as a
    /// `prebuilt` package but otherwise do not load any Cargo metadata for
    /// it.  This is appropriate for prebuilt repos whose contents this tool
    /// doesn't need to trace.
    Ignore,
}

/// Every repo that appears in `package-manifest.toml` as a `prebuilt` package
/// must appear here.  The reverse must also hold: any entry here must
/// correspond to a `prebuilt` package in `package-manifest.toml`.  Both
/// directions are checked at runtime.
static MANIFEST_PREBUILT_REPOS: &[PrebuiltRepoConfig] = &[
    PrebuiltRepoConfig {
        repo_name: "crucible",
        behavior: PrebuiltRepoBehavior::Inspect {
            expected_pkg_name: "crucible-agent-client",
            extra_cargo_features: &[],
        },
    },
    PrebuiltRepoConfig {
        repo_name: "dendrite",
        behavior: PrebuiltRepoBehavior::Inspect {
            expected_pkg_name: "dpd-client",
            extra_cargo_features: &[],
        },
    },
    PrebuiltRepoConfig {
        repo_name: "lldp",
        behavior: PrebuiltRepoBehavior::Inspect {
            expected_pkg_name: "lldpd-client",
            extra_cargo_features: &[],
        },
    },
    PrebuiltRepoConfig {
        repo_name: "maghemite",
        behavior: PrebuiltRepoBehavior::Inspect {
            expected_pkg_name: "mg-admin-client",
            extra_cargo_features: &[],
        },
    },
    PrebuiltRepoConfig {
        repo_name: "management-gateway-service",
        behavior: PrebuiltRepoBehavior::Inspect {
            expected_pkg_name: "gateway-messages",
            extra_cargo_features: &[],
        },
    },
    PrebuiltRepoConfig {
        repo_name: "propolis",
        behavior: PrebuiltRepoBehavior::Inspect {
            expected_pkg_name: "propolis-client",
            // The artifacts shipped from the Propolis repo (particularly,
            // `propolis-server`) are built with the `omicron-build` feature,
            // which is not enabled by default.  Enable this feature when
            // loading the Propolis repo metadata so that we see the
            // dependency tree that a shipping system will have.
            extra_cargo_features: &["omicron-build"],
        },
    },
    // pumpkind is ignored because it does not appear to contain Progenitor
    // clients or APIs and we have no easy way to find a clone of the repo.
    PrebuiltRepoConfig {
        repo_name: "pumpkind",
        behavior: PrebuiltRepoBehavior::Ignore,
    },
    // thundermuffin is ignored because it does not appear to contain Progenitor
    // clients or APIs and we have no easy way to find a clone of the repo.
    PrebuiltRepoConfig {
        repo_name: "thundermuffin",
        behavior: PrebuiltRepoBehavior::Ignore,
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
        let mut manifest_commits =
            find_prebuilt_repo_commits(&package_manifest)?;

        // Verify that our static list agrees with the manifest in both
        // directions: every prebuilt repo in the manifest must be listed in
        // MANIFEST_PREBUILT_REPOS, and every entry in MANIFEST_PREBUILT_REPOS
        // must appear in the manifest.  Both mismatches are errors so we
        // don't silently drop coverage for a repo that has been added or
        // removed.
        //
        // We do this by walking the static list once, removing each entry
        // from `manifest_commits` as we go.  A `None` return from `remove()`
        // means the static list references a repo the manifest lacks; any
        // commits still in `manifest_commits` afterward correspond to repos
        // the manifest has but the static list doesn't.  Pairing each static
        // entry with its commit falls out of the same pass.
        let mut paired: Vec<(&PrebuiltRepoConfig, GitCommit)> = Vec::new();
        let mut missing_from_manifest: Vec<&str> = Vec::new();
        for repo_config in MANIFEST_PREBUILT_REPOS {
            match manifest_commits.remove(repo_config.repo_name) {
                Some(commit) => paired.push((repo_config, commit)),
                None => missing_from_manifest.push(repo_config.repo_name),
            }
        }
        let missing_from_static: Vec<&str> =
            manifest_commits.keys().map(String::as_str).collect();
        if !missing_from_static.is_empty() || !missing_from_manifest.is_empty()
        {
            let mut msg = String::from(
                "mismatch between prebuilt repos in \
                 package-manifest.toml and MANIFEST_PREBUILT_REPOS",
            );
            if !missing_from_static.is_empty() {
                msg.push_str(&format!(
                    "; repos in package-manifest.toml but not in \
                     MANIFEST_PREBUILT_REPOS: {missing_from_static:?}"
                ));
            }
            if !missing_from_manifest.is_empty() {
                msg.push_str(&format!(
                    "; repos in MANIFEST_PREBUILT_REPOS but not in \
                     package-manifest.toml: {missing_from_manifest:?}"
                ));
            }
            bail!("{msg}");
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
        // parallelizing, though if we had many more repos than this, we'd
        // probably want to limit the concurrency.  We only spawn threads for
        // repos with `Inspect` behavior; `Ignore` entries have already been
        // validated by the presence check above.
        let handles: Vec<_> = paired
            .into_iter()
            .filter_map(|(repo_config, expected_commit)| {
                let PrebuiltRepoConfig { repo_name, behavior } = repo_config;
                let PrebuiltRepoBehavior::Inspect {
                    expected_pkg_name,
                    extra_cargo_features,
                } = behavior
                else {
                    return None;
                };
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
                Some(std::thread::spawn(move || {
                    load_dependent_repo(
                        &mine,
                        repo_name,
                        expected_pkg_name,
                        extra_features,
                        my_ignored,
                        &expected_commit,
                        patched_dep_policy,
                    )
                }))
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
/// For example, we locate the Crucible repo by looking up the
/// `crucible-agent-client` package in the Omicron workspace, finding its
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
                pkgname, pkg.manifest_path
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

/// Scans `package_manifest` and returns a map from repo name to the git
/// commit at which each `Prebuilt` package for that repo is pinned.
///
/// If a repo appears in multiple `Prebuilt` entries, all of them must agree
/// on the commit; otherwise this function returns an error.
fn find_prebuilt_repo_commits(
    package_manifest: &omicron_zone_package::config::Config,
) -> Result<BTreeMap<String, GitCommit>> {
    // Collect every distinct commit for every repo that appears as a
    // `Prebuilt` package.
    let mut candidates: BTreeMap<String, BTreeSet<GitCommit>> = BTreeMap::new();
    for pkg in package_manifest.packages.values() {
        match &pkg.source {
            PackageSource::Local { .. }
            | PackageSource::Composite { .. }
            | PackageSource::Manual => {}
            PackageSource::Prebuilt { repo, commit, .. } => {
                candidates
                    .entry(repo.clone())
                    .or_default()
                    .insert(GitCommit::from(commit.clone()));
            }
        }
    }

    // For each repo, verify that all `Prebuilt` entries agree on the commit.
    let mut result = BTreeMap::new();
    for (repo, commits) in candidates {
        if commits.len() > 1 {
            bail!(
                "expected repo {repo:?} to have exactly one git commit \
                 in package-manifest.toml, but found multiple: {commits:?}",
            );
        }
        // unwrap(): `candidates.or_default()` always inserts at least one
        // commit before we get here.
        let commit = commits.into_iter().next().unwrap();
        result.insert(repo, commit);
    }
    Ok(result)
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_zone_package::config::{Config, PackageName, ServiceName};
    use omicron_zone_package::package::{Package, PackageOutput};

    const TEST_REPO: &str = "testrepo";
    const OTHER_REPO: &str = "otherrepo";
    const OTHER_REPO_COMMIT: &str = "11111111111111111111";

    /// Builds a package manifest for testing `find_prebuilt_repo_commits`.
    ///
    /// `commits` contains an array of arbitrary strings that stand in for Git
    /// commit SHAs.  For each entry in this array, a `Prebuilt` package will be
    /// included for repo `TEST_REPO` having the corresponding Git commit SHA.
    /// This can be used to test cases where the config has no copies of the
    /// test package, one copy of it, multiple copies with the same SHA, or
    /// multiple copies with different SHAs.
    ///
    /// The returned manifest always contains a hardcoded set of "noise"
    /// packages: one `Local`, one `Composite`, one `Manual`, and one
    /// `Prebuilt` for `OTHER_REPO` (which should always show up in the
    /// result).
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
                    repo: String::from(OTHER_REPO),
                    commit: String::from(OTHER_REPO_COMMIT),
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
    fn find_prebuilt_repo_commits_success() {
        // Two prebuilt entries for the same repo pointing at the same commit
        // should collapse to a single commit.  The unrelated `OTHER_REPO`
        // entry should also appear in the result.
        let commit = "abcdef1234567890abcdef1234567890abcdef12";
        let manifest = make_manifest(&[commit, commit]);
        let found = find_prebuilt_repo_commits(&manifest).unwrap();
        assert_eq!(found.get(TEST_REPO).unwrap().as_str(), commit);
        assert_eq!(found.get(OTHER_REPO).unwrap().as_str(), OTHER_REPO_COMMIT);
        // No other repos should appear.
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn find_prebuilt_repo_commits_multiple_prebuilts_different_commits() {
        let commit_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let commit_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let manifest = make_manifest(&[commit_a, commit_b]);
        let err = find_prebuilt_repo_commits(&manifest).unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "expected repo \"testrepo\" to have exactly one git commit \
             in package-manifest.toml, but found multiple: \
             {\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \
             \"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"}",
        );
    }

    #[test]
    fn find_prebuilt_repo_commits_repo_absent() {
        // Empty `commits` means no prebuilt entries for `TEST_REPO` at all,
        // so the result should only contain the noise entry for
        // `OTHER_REPO`.
        let manifest = make_manifest(&[]);
        let found = find_prebuilt_repo_commits(&manifest).unwrap();
        assert_eq!(found.get(TEST_REPO), None);
        assert_eq!(found.get(OTHER_REPO).unwrap().as_str(), OTHER_REPO_COMMIT);
        assert_eq!(found.len(), 1);
    }
}
