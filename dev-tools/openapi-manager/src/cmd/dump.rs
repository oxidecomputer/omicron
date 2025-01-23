// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::ManagedApis,
    cmd::output::{headers, OutputOpts, Styles},
    combined::{ApiSpecFileWhich, CheckStale},
    spec::Environment,
    spec_files::LocalFiles,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use anyhow::{Context, Result};
use owo_colors::OwoColorize;
use std::process::ExitCode;

pub(crate) fn dump_impl(
    env: &Environment,
    output: &OutputOpts,
) -> anyhow::Result<()> {
    let apis = ManagedApis::all()?;

    let local_dir = &env.openapi_dir;
    println!("Loading local files from {:?}", local_dir);
    let local_files = LocalFiles::load_from_directory(&local_dir, &apis)?;
    println!("warnings: {}", local_files.warnings.len());
    for w in &local_files.warnings {
        println!("    warn: {:#}", w);
    }
    println!("errors: {}", local_files.errors.len());
    for e in &local_files.errors {
        println!("    error: {:#}", e);
    }

    for (api_ident, version_map) in &local_files.spec_files {
        println!("    API: {}", api_ident);
        for (version, files) in version_map {
            println!("        version {}:", version);
            for f in files {
                let api_spec = &f.0;
                println!(
                    "            file {} (v{})",
                    api_spec.spec_file_name().path(),
                    api_spec.version()
                );
            }
        }
    }

    Ok(())
}
