// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::{ManagedApi, ManagedApis},
    environment::{BlessedSource, GeneratedSource},
    output::{
        display_api_spec_version, headers::*, plural, OutputOpts, Styles,
    },
    resolved::{Problem, Resolution, Resolved},
    spec::Environment,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use anyhow::{bail, Result};
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
    if output.use_color(supports_color::Stream::Stderr) {
        styles.colorize();
    }

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);
    let apis = ManagedApis::all()?;
    let generated = generated_source.load(&apis, &styles)?;
    print_warnings(&generated.warnings, &generated.errors)?;
    let local_files = env.local_source.load(&apis, &styles)?;
    print_warnings(&local_files.warnings, &local_files.errors)?;
    let blessed = blessed_source.load(&apis, &styles)?;
    print_warnings(&blessed.warnings, &blessed.errors)?;

    let resolved =
        Resolved::new(env, &apis, &blessed, &generated, &local_files);

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);
    summarize(&apis, &resolved, &styles)
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
/// Summarize the results of checking all supported API versions, plus other
/// problems found during resolution
pub fn summarize(
    apis: &ManagedApis,
    resolved: &Resolved,
    styles: &Styles,
) -> anyhow::Result<CheckResult> {
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

    // Print problems associated with a supported API version
    // (i.e., one of the expected OpenAPI documents).
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
            summarize_one(api, version, resolution, styles);
        }
    }

    // Print problems not associated with any supported version, if any.
    let general_problems: Vec<_> = resolved.general_problems().collect();
    let num_general_problems = if !general_problems.is_empty() {
        eprintln!(
            "\n{:>HEADER_WIDTH$} problems not associated with a specific \
             supported API version:",
            "Other".style(styles.warning_header),
        );

        let (fixable, unfixable): (Vec<&Problem>, Vec<&Problem>) =
            general_problems.iter().partition(|p| p.is_fixable());
        num_failed += unfixable.len();
        print_problems(general_problems, styles);
        fixable.len()
    } else {
        0
    };

    // Print informational notes, if any.
    for n in resolved.notes() {
        let initial_indent =
            format!("{:>HEADER_WIDTH$} ", "Note".style(styles.warning_header));
        let more_indent = " ".repeat(HEADER_WIDTH + " ".len());
        eprintln!(
            "\n{}\n",
            textwrap::fill(
                &n.to_string(),
                textwrap::Options::with_termwidth()
                    .initial_indent(&initial_indent)
                    .subsequent_indent(&more_indent)
            )
        );
    }

    // Print a summary line.
    let status_header = if num_failed > 0 {
        FAILURE.style(styles.failure_header)
    } else if num_stale > 0 {
        STALE.style(styles.warning_header)
    } else {
        SUCCESS.style(styles.success_header)
    };

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);
    eprintln!(
        "{:>HEADER_WIDTH$} {} {} checked: {} fresh, {} stale, {} failed, \
         {} other {}",
        status_header,
        total.style(styles.bold),
        plural::documents(total),
        num_fresh.style(styles.bold),
        num_stale.style(styles.bold),
        num_failed.style(styles.bold),
        num_general_problems.style(styles.bold),
        plural::problems(num_general_problems),
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
}

/// Summarize the "check" status of one supported API version
fn summarize_one(
    api: &ManagedApi,
    version: &semver::Version,
    resolution: &Resolution<'_>,
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
    } else {
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

        print_problems(problems, styles);
    }
}

/// Print a formatted list of Problems
pub fn print_problems<'a, T>(problems: T, styles: &Styles)
where
    T: IntoIterator<Item = &'a Problem<'a>>,
{
    for p in problems.into_iter() {
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
        let steps = fix_str.trim_end().split("\n");
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
