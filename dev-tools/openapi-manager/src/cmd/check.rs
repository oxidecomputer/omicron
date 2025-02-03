// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::{ManagedApi, ManagedApis},
    cmd::output::{
        display_api_spec_version, headers::*, plural, OutputOpts, Styles,
    },
    environment::{BlessedSource, GeneratedSource},
    resolved::{iter_only, Resolution, Resolved},
    spec::Environment,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use anyhow::{bail, Context, Result};
use owo_colors::OwoColorize;
use std::process::ExitCode;

#[derive(Clone, Copy, Debug)]
pub(crate) enum CheckResult {
    Success,
    NeedsUpdate,
    Failures,
}

impl CheckResult {
    pub(crate) fn to_exit_code(self) -> ExitCode {
        match self {
            CheckResult::Success => ExitCode::SUCCESS,
            CheckResult::NeedsUpdate => NEEDS_UPDATE_EXIT_CODE.into(),
            CheckResult::Failures => FAILURE_EXIT_CODE.into(),
        }
    }
}

pub(crate) fn check_impl(
    env: &Environment,
    blessed_source: &BlessedSource,
    generated_source: &GeneratedSource,
    output: &OutputOpts,
) -> Result<CheckResult> {
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
    for p in resolved.general_problems() {
        println!("PROBLEM: {}", p);
        found_problems = true;
        if !p.is_fixable() {
            found_unfixable = true;
        }
        if let Some(fix) = p.fix() {
            println!("{}", fix);
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
                if let Some(fix) = p.fix() {
                    println!("{}", fix);
                }
            }
        }
    }

    println!("Checked {} total versions across {} APIs", nversions, napis);

    // XXX-dap
    summarize(&apis, &resolved, output, &styles)

    // if found_unfixable {
    //     // XXX-dap wording
    //     println!(
    //         "Error: please fix one or more problems above and re-run the tool"
    //     );
    //     Ok(CheckResult::Failures)
    // } else if found_problems {
    //     println!("Stale: one or more versions needs an update");
    //     Ok(CheckResult::NeedsUpdate)
    // } else {
    //     println!("Success");
    //     Ok(CheckResult::Success)
    // }
}

// XXX-dap put somewhere where it can be re-used
pub fn print_warnings(
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

// XXX-dap put somewhere where it can be re-used
pub fn summarize(
    apis: &ManagedApis,
    resolved: &Resolved,
    output: &OutputOpts,
    styles: &Styles,
) -> anyhow::Result<CheckResult> {
    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);

    let total = resolved.nexpected_documents();

    eprintln!(
        "{:>HEADER_WIDTH$} {} OpenAPI {}...",
        CHECKING.style(styles.success_header),
        total.style(styles.bold),
        plural::documents(total),
    );

    let mut num_fresh = 0;
    let mut num_stale = 0;
    let mut num_failed = 0;

    for api in apis.iter_apis() {
        let ident = api.ident();

        for version in api.iter_versions_semver() {
            let resolution = resolved
                .resolution_for_api_version(ident, version)
                .expect("resolution for all supported API versions");
            if resolution.has_errors() {
                num_failed += 1;
            } else if resolution.has_problems() {
                num_stale += 1;
            } else {
                num_fresh += 1;
            }
            summarize_one(api, version, resolution, output, styles);
        }
    }

    let status_header = if num_failed > 0 {
        FAILURE.style(styles.failure_header)
    } else if num_stale > 0 {
        STALE.style(styles.warning_header)
    } else {
        SUCCESS.style(styles.success_header)
    };

    eprintln!(
        "{:>HEADER_WIDTH$} {} {} checked: {} fresh, {} stale, {} failed",
        status_header,
        total.style(styles.bold),
        plural::documents(total),
        num_fresh.style(styles.bold),
        num_stale.style(styles.bold),
        num_failed.style(styles.bold),
    );
    if num_failed > 0 {
        eprintln!(
            "{:>HEADER_WIDTH$} (fix failures, then run {} to update)",
            "",
            "cargo xtask openapi generate".style(styles.bold)
        );
        Ok(CheckResult::Failures)
    } else if num_stale > 0 {
        eprintln!(
            "{:>HEADER_WIDTH$} (run {} to update)",
            "",
            "cargo xtask openapi generate".style(styles.bold)
        );
        Ok(CheckResult::NeedsUpdate)
    } else {
        Ok(CheckResult::Success)
    }
    // XXX-dap todo: general problems and notes
}

fn summarize_one(
    api: &ManagedApi,
    version: &semver::Version,
    resolution: &Resolution<'_>,
    output: &OutputOpts,
    styles: &Styles,
) {
    let problems: Vec<_> = resolution.problems().collect();
    if problems.is_empty() {
        // Success case: file is up-to-date.
        // XXX-dap this used to print a summary and "extra"
        eprintln!(
            "{:>HEADER_WIDTH$} {}",
            FRESH.style(styles.success_header),
            display_api_spec_version(api, version, &styles),
        );
        return;
    }

    // There were one or more problems, some of which may be unfixable.
    eprintln!(
        "{:>HEADER_WIDTH$} {}",
        if resolution.has_errors() {
            FAILURE.style(styles.failure_header)
        } else {
            assert!(resolution.has_problems());
            STALE.style(styles.warning_header)
        },
        display_api_spec_version(api, version, &styles),
    );

    for p in &problems {
        let subheader_width = HEADER_WIDTH + 4;
        let first_indent = format!(
            "{:>subheader_width$}: ",
            if p.is_fixable() {
                "problem".style(styles.warning_header)
            } else {
                "error".style(styles.failure_header)
            }
        );
        let more_indent = " ".repeat(subheader_width + 2);
        eprintln!(
            "{}",
            textwrap::fill(
                &p.to_string(),
                textwrap::Options::with_termwidth()
                    .initial_indent(&first_indent)
                    .subsequent_indent(&more_indent)
            )
        );

        let Some(fix) = p.fix() else {
            continue;
        };

        let first_indent = format!(
            "{:>subheader_width$}: ",
            "fix".style(styles.warning_header)
        );
        let fix_str = fix.to_string();
        let steps = fix_str.trim_right().split("\n");
        for s in steps {
            eprintln!(
                "{}",
                textwrap::fill(
                    &format!("will {}", s),
                    textwrap::Options::with_termwidth()
                        .initial_indent(&first_indent)
                        .subsequent_indent(&more_indent)
                )
            );
        }
    }
}
