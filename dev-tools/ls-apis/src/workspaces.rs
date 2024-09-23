// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combines information about multiple `Workspace`s

use crate::api_metadata::AllApiMetadata;
use crate::cargo::Workspace;
use crate::ClientPackageName;
use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8PathBuf;
use cargo_metadata::Package;
use cargo_metadata::PackageId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

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
    /// of potential inconsistencies between API metadata and Cargo metadata.
    pub fn load(
        api_metadata: &AllApiMetadata,
    ) -> Result<(Workspaces, Vec<anyhow::Error>)> {
        // First, load information about the "omicron" workspace.  This is the
        // current workspace so we don't need to provide the path to it.
        let ignored_non_clients = api_metadata.ignored_non_clients();
        let omicron =
            Arc::new(Workspace::load("omicron", None, ignored_non_clients)?);

        // In order to assemble this metadata, Cargo already has a clone of most
        // of the other workspaces that we care about.  We'll use those clones
        // rather than manage our own.
        //
        // To find each of these other repos, we'll need to look up a package
        // that comes from each of these workspaces and look at where its local
        // manifest file is.
        //
        // Loading each workspace involves running `cargo metaata`, which is
        // pretty I/O intensive.  Latency benefits significantly from
        // parallelizing, though we have to respect the dependencies.  We can't
        // look up a package in "maghemite" before we've loaded Maghemite.
        //
        // If we had many more repos than this, we'd probably want to limit the
        // concurrency.
        let handles: Vec<_> = [
            // To find this repo ... look up this package in Omicron
            //   v                       v
            ("crucible", "crucible-agent-client"),
            ("propolis", "propolis-client"),
            ("maghemite", "mg-admin-client"),
        ]
        .into_iter()
        .map(|(repo, omicron_pkg)| {
            let mine = omicron.clone();
            let my_ignored = ignored_non_clients.clone();
            std::thread::spawn(move || {
                load_dependent_repo(&mine, repo, omicron_pkg, my_ignored)
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

        // To load Dendrite, we need to look something up in Maghemite (loaded
        // above).
        let maghemite = workspaces
            .get("maghemite")
            .ok_or_else(|| anyhow!("missing maghemite workspaces"))?;

        workspaces.insert(
            String::from("dendrite"),
            load_dependent_repo(
                &maghemite,
                "dendrite",
                "dpd-client",
                ignored_non_clients.clone(),
            )?,
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
                        "workspace {}: found client package missing from API \
                         manifest: {}",
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

        // TODO As of this writing, we have two distinct packages called
        // "dpd-client":
        //
        // - There's one in the "dendrite" repo.  This is used by:
        //   - `swadm` (in Dendrite)
        //   - `tfportd` (in Dendrite)`
        //   - `ddm` (in Maghemite)
        //   - `ddmd` (in Maghemite)
        //   - `mgd` (via `mg-lower`) (in Maghemite)
        // - There's one in the "omicron" repo.  This is used by:
        //   - `wicketd` (in Omicron)
        //   - `omicron-sled-agent` (in Omicron)
        //   - `omicron-nexus` (in Omicron)
        //
        // This is problematic for two reasons:
        //
        // 1. This tool assumes that every API has exactly one client and it
        //    uses the client as the primary key to identify the API.
        //
        // 2. The Rust package name is supposed to be unique.  This happens to
        //    work, probably in part because the packages in the above two
        //    groups are never built in the same workspace.  This tool _does_
        //    merge information from all these workspaces, and it's likely
        //    conflating the two packages.  That happens to be a good thing
        //    because it keeps (1) from being an actual problem.  That is: if
        //    this tool actually realized they were separate Rust packages, then
        //    it would be upset that we had two different clients for the same
        //    API.
        //
        // To keep things working, we just have this function always report the
        // one in the Omicron repo.
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
    ignored_non_clients: BTreeSet<ClientPackageName>,
) -> Result<Workspace> {
    // `Workspace` doesn't let us look up a non-workspace package by name
    // because there may be many of them.  So list all the pkgids and take any
    // one of them -- any of them should work for our purpoes.
    let pkgid = workspace.pkgids(pkgname).next().ok_or_else(|| {
        anyhow!(
            "workspace {} did not contain expected package {}",
            workspace.name(),
            pkgname
        )
    })?;

    // Now we can look up the package metadata.
    let pkg = workspace.pkg_by_id(pkgid).ok_or_else(|| {
        anyhow!(
            "workspace {}: did not contain expected package id {}",
            workspace.name(),
            pkgname
        )
    })?;

    // The package metadata should show where the package's manifest file should
    // be.  This may be buried deep in the workspace.  How do we find the root
    // of the workspace?  We assume (and verify) that the workspace is checked
    // out in the usual place under CARGO_HOME.  Then the workspace root is the
    // full path two component deeper than the checkouts directory.
    let cargo_home = std::env::var("CARGO_HOME").context("CARGO_HOME")?;
    let git_checkouts: Utf8PathBuf =
        [&cargo_home, "git", "checkouts"].into_iter().collect();
    let relative_path =
        pkg.manifest_path.strip_prefix(&git_checkouts).map_err(|_| {
            anyhow!(
                "workspace {}: package {}: manifest path {:?} was not under \
                 expected path {:?}",
                workspace.name(),
                pkgname,
                pkg.manifest_path,
                git_checkouts
            )
        })?;
    let checkout_name: Utf8PathBuf =
        relative_path.components().take(2).collect();
    let workspace_manifest =
        git_checkouts.join(checkout_name).join("Cargo.toml");
    Workspace::load(repo, Some(&workspace_manifest), &ignored_non_clients)
}
