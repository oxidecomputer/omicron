// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::{ApiIdent, ManagedApis},
    cmd::output::{headers, OutputOpts, Styles},
    combined::{ApiSpecFileWhich, CheckStale},
    git::GitRevision,
    spec::Environment,
    spec_files_blessed::BlessedFiles,
    spec_files_generated::GeneratedFiles,
    spec_files_generic::ApiSpecFile,
    spec_files_local::LocalFiles,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use anyhow::{Context, Result};
use camino::Utf8Path;
use owo_colors::OwoColorize;
use semver::Version;
use std::{collections::BTreeMap, ops::Deref, process::ExitCode};

pub(crate) fn dump_impl(
    blessed_revision: Option<&str>,
    directory: &Utf8Path,
    output: &OutputOpts,
) -> anyhow::Result<()> {
    let apis = ManagedApis::all()?;

    // Print information about local files.
    let local_dir = directory;
    println!("Loading local files from {:?}", local_dir);
    let local_files = LocalFiles::load_from_directory(&local_dir, &apis)?;
    dump_structure(
        &local_files.spec_files,
        &local_files.errors,
        &local_files.warnings,
    );

    // Print information about what we found in Git.
    if let Some(git_revision) = blessed_revision {
        println!("Loading blessed files from revision {:?}", git_revision);
        let revision = GitRevision::from(git_revision.to_owned());
        let blessed =
            BlessedFiles::load_from_git_revision(&revision, local_dir, &apis)?;
        dump_structure(&blessed.spec_files, &blessed.errors, &blessed.warnings);
    } else {
        println!("Blessed files skipped (no git revision specified)");
    }

    // Print information about generated files.
    println!("Generating specs from API definitions");
    let generated = GeneratedFiles::generate(&apis)?;
    dump_structure(
        &generated.spec_files,
        &generated.errors,
        &generated.warnings,
    );

    Ok(())
}

fn dump_structure<T: Deref<Target = ApiSpecFile>>(
    spec_files: &BTreeMap<ApiIdent, BTreeMap<Version, Vec<T>>>,
    errors: &[anyhow::Error],
    warnings: &[anyhow::Error],
) {
    println!("warnings: {}", warnings.len());
    for w in warnings {
        println!("    warn: {:#}", w);
    }
    println!("errors: {}", errors.len());
    for e in errors {
        println!("    error: {:#}", e);
    }

    for (api_ident, version_map) in spec_files {
        println!("    API: {}", api_ident);
        for (version, files) in version_map {
            println!("        version {}:", version);
            for f in files {
                let api_spec: &ApiSpecFile = f.deref();
                println!(
                    "            file {} (v{})",
                    api_spec.spec_file_name().path(),
                    api_spec.version()
                );
            }
        }
    }
}
