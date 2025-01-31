// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::ManagedApis,
    cmd::{
        check::print_warnings,
        output::{OutputOpts, Styles},
    },
    environment::{BlessedSource, GeneratedSource},
    resolved::{Resolution, Resolved},
    spec::Environment,
    FAILURE_EXIT_CODE,
};
use anyhow::Result;
use std::process::ExitCode;

#[derive(Clone, Copy, Debug)]
pub(crate) enum GenerateResult {
    Success,
    Failures,
}

impl GenerateResult {
    pub(crate) fn to_exit_code(self) -> ExitCode {
        match self {
            GenerateResult::Success => ExitCode::SUCCESS,
            GenerateResult::Failures => FAILURE_EXIT_CODE.into(),
        }
    }
}

// XXX-dap this is mostly copy/paste from check_impl
pub(crate) fn generate_impl(
    env: &Environment,
    blessed_source: &BlessedSource,
    generated_source: &GeneratedSource,
    output: &OutputOpts,
) -> Result<GenerateResult> {
    let mut styles = Styles::default();
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
    for p in resolved.general_problems() {
        println!("PROBLEM: {}", p);
        if !p.is_fixable() {
            found_unfixable = true;
        }
        // XXX-dap this is temporary -- see the comment on enum Problem
        let fake_resolution = Resolution::new_blessed(Vec::new());
        if let Some(fixes) = p.fix(&fake_resolution)? {
            for f in fixes {
                println!("{}", f);
            }
        }
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
                if let Some(fixes) = p.fix(&resolution)? {
                    for f in fixes {
                        println!("{}", f);
                    }
                }
            }
        }
    }

    println!("Checked {} total versions across {} APIs", nversions, napis);

    if found_unfixable {
        // XXX-dap wording
        println!(
            "Error: found unfixable errors above.  Please fix those first."
        );
        return Ok(GenerateResult::Failures);
    }

    // XXX-dap this is where we could confirm
    let mut nproblems = 0;
    let mut nerrors = 0;
    let mut nfixed = 0;
    for p in resolved.general_problems() {
        nproblems += 1;
        // XXX-dap this is temporary -- see the comment on enum Problem
        let fake_resolution = Resolution::new_blessed(Vec::new());
        if let Some(fixes) = p.fix(&fake_resolution)? {
            for f in fixes {
                eprint!("{f}");
                if let Err(error) = f.execute(env) {
                    eprintln!("fix failed: {error:#}");
                    nerrors += 1;
                } else {
                    nfixed += 1;
                }
            }
        }
    }

    for api in apis.iter_apis() {
        let ident = api.ident();
        for version in api.iter_versions_semver() {
            println!("API {} version {}", ident, version);
            // unwrap(): there should be a resolution for every managed API
            let resolution =
                resolved.resolution_for_api_version(ident, version).unwrap();
            for p in resolution.problems() {
                nproblems += 1;

                if let Some(fixes) = p.fix(&resolution)? {
                    for f in fixes {
                        eprint!("{f}");
                        if let Err(error) = f.execute(env) {
                            eprintln!("fix failed: {error:#}");
                            nerrors += 1;
                        } else {
                            nfixed += 1;
                        }
                    }
                }
            }
        }
    }

    println!("problems found: {}", nproblems);
    println!("fixes required: {}", nerrors + nfixed);
    println!("fixes applied:  {}", nfixed);
    println!("fixes failed:   {}", nerrors);
    if nerrors > 0 {
        println!("FAILED");
        return Ok(GenerateResult::Failures);
    }
    if nproblems == 0 {
        println!("SUCCESS");
        return Ok(GenerateResult::Success);
    }

    // Now reload the local files and make sure we have no more problems.
    println!("Re-checking by reloading local files ... ");
    nproblems = 0;
    let local_files = env.local_source.load(&apis)?;
    print_warnings(&local_files.warnings, &local_files.errors)?;
    let resolved =
        Resolved::new(env, &apis, &blessed, &generated, &local_files);
    for p in resolved.general_problems() {
        println!("PROBLEM: {p}");
        nproblems += 1;
    }
    for api in apis.iter_apis() {
        let ident = api.ident();
        for version in api.iter_versions_semver() {
            // unwrap(): there should be a resolution for every managed API
            let resolution =
                resolved.resolution_for_api_version(ident, version).unwrap();
            for p in resolution.problems() {
                println!("PROBLEM: {p}");
                nproblems += 1;
            }
        }
    }

    if nproblems == 0 {
        println!("Success");
        Ok(GenerateResult::Success)
    } else {
        println!(
            "FAILED: unexpectedly found problems after successfully \
             applying all fixes"
        );
        Ok(GenerateResult::Failures)
    }
}
