// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::ManagedApis,
    cmd::output::{OutputOpts, Styles},
    environment::{BlessedSource, GeneratedSource},
    resolved::Resolved,
    spec::Environment,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use anyhow::{bail, Result};
use std::process::ExitCode;

#[derive(Clone, Copy, Debug)]
pub(crate) enum NewCheckResult {
    Success,
    NeedsUpdate,
    Failures,
}

impl NewCheckResult {
    pub(crate) fn to_exit_code(self) -> ExitCode {
        match self {
            NewCheckResult::Success => ExitCode::SUCCESS,
            NewCheckResult::NeedsUpdate => NEEDS_UPDATE_EXIT_CODE.into(),
            NewCheckResult::Failures => FAILURE_EXIT_CODE.into(),
        }
    }
}

pub(crate) fn new_check_impl(
    env: &Environment,
    blessed_source: &BlessedSource,
    generated_source: &GeneratedSource,
    output: &OutputOpts,
) -> Result<NewCheckResult> {
    let mut styles = Styles::default();
    let mut found_problems = false;
    let mut found_unfixable = false;
    if output.use_color(supports_color::Stream::Stderr) {
        styles.colorize();
    }

    let apis = ManagedApis::all()?;
    let generated = generated_source.load(&apis)?;
    print_warnings(&generated.warnings, &generated.errors)?;
    let local_files = env.local_source.load(&apis)?;
    print_warnings(&local_files.warnings, &local_files.errors)?;
    let blessed = blessed_source.load(&apis)?;
    print_warnings(&blessed.warnings, &blessed.errors)?;

    let resolved =
        Resolved::new(env, &apis, &blessed, &generated, &local_files);
    for note in resolved.notes() {
        println!("NOTE: {}", note);
    }
    for problem in resolved.general_problems() {
        println!("PROBLEM: {}", problem);
        found_problems = true;
    }

    println!("Checking OpenAPI documents...");
    let mut napis = 0;
    let mut nversions = 0;
    for api in apis.iter_apis() {
        napis += 1;
        let ident = api.ident();
        for version in api.iter_versions_semver() {
            nversions += 1;
            // unwrap(): there should be a resolution for every managed API
            let resolution =
                resolved.resolution_for_api_version(ident, version).unwrap();
            let problems: Vec<_> = resolution.problems().collect();
            let summary = if problems.len() == 0 {
                "OK"
            } else if problems.iter().all(|p| p.is_fixable()) {
                found_problems = true;
                "STALE"
            } else {
                found_unfixable = true;
                "ERROR"
            };

            println!(
                "{:5} {} ({}) v{}",
                summary,
                ident,
                if api.is_versioned() { "versioned" } else { "lockstep" },
                version
            );

            for p in problems {
                println!("problem: {}", p);
            }
        }
    }

    println!("Checked {} total versions across {} APIs", nversions, napis);

    if found_unfixable {
        // XXX-dap wording
        println!(
            "Error: please fix one or more problems above and re-run the tool"
        );
        Ok(NewCheckResult::Failures)
    } else if found_problems {
        println!("Stale: one or more versions needs an update");
        Ok(NewCheckResult::NeedsUpdate)
    } else {
        println!("Success");
        Ok(NewCheckResult::Success)
    }
}

fn print_warnings(
    warnings: &[anyhow::Error],
    errors: &[anyhow::Error],
) -> anyhow::Result<()> {
    for w in warnings {
        println!("    warn: {:#}", w);
    }

    for e in errors {
        println!("    error: {:#}", e);
    }

    if !errors.is_empty() {
        bail!(
            "bailing out after error{} above",
            if errors.len() != 1 { "s" } else { "" }
        );
    }

    Ok(())
}
