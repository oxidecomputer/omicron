// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combines information about multiple `Workspace`s

use crate::api_metadata::AllApiMetadata;
use crate::cargo::Workspace;
use anyhow::{anyhow, ensure, Result};
use camino::Utf8Path;
use cargo_metadata::Package;
use cargo_metadata::PackageId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Thin wrapper around a list of workspaces that makes it easy to query which
/// workspace has which package
pub(crate) struct Workspaces {
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
