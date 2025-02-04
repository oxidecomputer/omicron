// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::ManagedApis,
    cmd::check::{print_problems, print_warnings, summarize, CheckResult},
    environment::{BlessedSource, GeneratedSource},
    output::{
        display_api_spec_version,
        headers::{self, *},
        plural, OutputOpts, Styles,
    },
    resolved::{Problem, Resolved},
    spec::Environment,
    spec_files_blessed::BlessedFiles,
    spec_files_generated::GeneratedFiles,
    FAILURE_EXIT_CODE,
};
use anyhow::{bail, Result};
use owo_colors::OwoColorize;
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
    if output.use_color(supports_color::Stream::Stderr) {
        styles.colorize();
    }

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

    match summarize(&apis, &resolved, &styles)? {
        CheckResult::Failures => {
            // summarize() already printed a useful bottom-line summary.
            return Ok(GenerateResult::Failures);
        }
        CheckResult::Success => {
            let ndocuments = resolved.nexpected_documents();
            print_final_status(&styles, ndocuments, 0, ndocuments, 0);
            Ok(GenerateResult::Success)
        }
        CheckResult::NeedsUpdate => {
            do_generate(env, &apis, &blessed, &generated, &resolved, &styles)
        }
    }
}

fn print_final_status(
    styles: &Styles,
    ndocuments: usize,
    num_updated: usize,
    num_unchanged: usize,
    num_errors: usize,
) {
    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);
    let status_header = if num_errors == 0 {
        headers::SUCCESS.style(styles.success_header)
    } else {
        headers::FAILURE.style(styles.failure_header)
    };
    eprintln!(
        "{:>HEADER_WIDTH$} {} {}: {} updated, {} unchanged, {} failed",
        status_header,
        ndocuments.style(styles.bold),
        plural::documents(ndocuments),
        num_updated.style(styles.bold),
        num_unchanged.style(styles.bold),
        num_errors.style(styles.bold),
    );
}

fn do_generate(
    env: &Environment,
    apis: &ManagedApis,
    blessed: &BlessedFiles,
    generated: &GeneratedFiles,
    resolved: &Resolved,
    styles: &Styles,
) -> anyhow::Result<GenerateResult> {
    // XXX-dap this is where we could confirm

    let total = resolved.nexpected_documents();
    eprintln!(
        "{:>HEADER_WIDTH$} {} OpenAPI {}...",
        "Updating".style(styles.success_header),
        total.style(styles.bold),
        plural::documents(total),
    );

    let mut num_updated = 0;
    let mut num_unchanged = 0;
    let mut num_errors = 0;

    // Apply fixes for problems with supported API versions.
    for api in apis.iter_apis() {
        let ident = api.ident();
        for version in api.iter_versions_semver() {
            // unwrap(): there should be a resolution for every managed API
            let resolution =
                resolved.resolution_for_api_version(ident, version).unwrap();
            assert!(
                !resolution.has_errors(),
                "found unfixable problems, but that should have been \
                 checked above"
            );

            let problems: Vec<_> = resolution.problems().collect();
            if problems.is_empty() {
                eprintln!(
                    "{:>HEADER_WIDTH$} {}",
                    UNCHANGED.style(styles.unchanged_header),
                    display_api_spec_version(api, version, &styles),
                );
                num_unchanged += 1;
            } else {
                eprintln!(
                    "{:>HEADER_WIDTH$} {}",
                    STALE.style(styles.warning_header),
                    display_api_spec_version(api, version, &styles),
                );

                fix_problems(
                    env,
                    problems,
                    styles,
                    &mut num_updated,
                    &mut num_errors,
                );
            }
        }
    }

    // Fix problems not associated with any supported version, if any.
    let general_problems: Vec<_> = resolved.general_problems().collect();
    fix_problems(
        env,
        general_problems,
        styles,
        &mut num_updated,
        &mut num_errors,
    );

    if num_errors > 0 {
        print_final_status(
            styles,
            total,
            num_updated,
            num_unchanged,
            num_errors,
        );
        return Ok(GenerateResult::Failures);
    }

    // Finally, check again for any problems.  Since we expect this should have
    // fixed everything, be quiet unless we find something amiss.
    let mut nproblems = 0;
    let local_files = env.local_source.load(&apis, &styles)?;
    eprintln!(
        "{:>HEADER_WIDTH$} all local files",
        "Rechecking".style(styles.success_header),
    );
    print_warnings(&local_files.warnings, &local_files.errors)?;
    let resolved =
        Resolved::new(env, &apis, &blessed, &generated, &local_files);
    let general_problems: Vec<_> = resolved.general_problems().collect();
    nproblems += general_problems.len();
    if !general_problems.is_empty() {
        print_problems(general_problems, styles);
    }
    for api in apis.iter_apis() {
        let ident = api.ident();
        for version in api.iter_versions_semver() {
            // unwrap(): there should be a resolution for every managed API
            let resolution =
                resolved.resolution_for_api_version(ident, version).unwrap();
            let problems: Vec<_> = resolution.problems().collect();
            nproblems += problems.len();
            if !problems.is_empty() {
                eprintln!(
                    "found unexpected problem with API {} version {} \
                     (this is a bug)",
                    ident, version
                );
                print_problems(problems, styles);
            }
        }
    }

    if nproblems > 0 {
        bail!("found problems after successfully fixing everything");
    } else {
        print_final_status(
            styles,
            total,
            num_updated,
            num_unchanged,
            num_errors,
        );
        Ok(GenerateResult::Success)
    }
}

fn fix_problems<'a, T>(
    env: &Environment,
    problems: T,
    styles: &Styles,
    num_updated: &mut usize,
    num_errors: &mut usize,
) where
    T: IntoIterator<Item = &'a Problem<'a>>,
{
    for p in problems.into_iter() {
        // We should have already bailed out if there were any unfixable
        // problems.
        let fix = p.fix().expect("attempting to fix unfixable problem");
        match fix.execute(env) {
            Ok(steps) => {
                *num_updated += 1;
                for s in steps {
                    eprintln!(
                        "{:>HEADER_WIDTH$} {}",
                        "Fixed".style(styles.success_header),
                        s,
                    );
                }
            }
            Err(error) => {
                *num_errors += 1;
                eprintln!(
                    "{:>HEADER_WIDTH$} fix {:?}: {:#}",
                    "FIX FAILED".style(styles.failure_header),
                    fix.to_string(),
                    error
                );
            }
        }
    }
}
