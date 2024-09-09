// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combines information about multiple `Workspace`s

use crate::api_metadata::AllApiMetadata;
use crate::cargo::Workspace;
use anyhow::{anyhow, ensure, Result};
use camino::Utf8Path;
use cargo_metadata::Package;
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
