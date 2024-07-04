// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::ExitCode;

use anyhow::Result;
use camino::Utf8Path;
use indent_write::io::IndentWriter;
use owo_colors::OwoColorize;
use similar::TextDiff;

use crate::{
    output::{
        display_error, write_diff, OutputOpts, Styles, CHECK, CROSS, STAR,
        WARNING,
    },
    spec::{all_apis, CheckStatus},
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};

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
    dir: &Utf8Path,
    output: &OutputOpts,
) -> Result<CheckResult> {
    let mut styles = Styles::default();
    if output.use_color(supports_color::Stream::Stderr) {
        styles.colorize();
    }

    let all_apis = all_apis();

    eprintln!("{STAR} checking {} APIs...", all_apis.len().style(styles.bold));
    let mut num_up_to_date = 0;
    let mut num_outdated = 0;
    let mut num_failed = 0;

    for api in &all_apis {
        let status = match api.check(&dir) {
            Ok(status) => status,
            Err(err) => {
                eprint!("  {} {}: ", CROSS.style(styles.failure), api.filename);
                display_error(
                    &err,
                    styles.failure,
                    &mut IndentWriter::new_skip_initial(
                        "      ",
                        std::io::stderr(),
                    ),
                )?;

                num_failed += 1;
                continue;
            }
        };

        match status {
            CheckStatus::Ok => {
                eprintln!(
                    "  {} {}: {}",
                    CHECK.style(styles.success),
                    api.filename,
                    "up-to-date".style(styles.success)
                );

                num_up_to_date += 1;
            }
            CheckStatus::Mismatch { full_path, actual, expected } => {
                eprintln!(
                    "  {} {}: {}",
                    WARNING.style(styles.warning),
                    api.filename,
                    "mismatch".style(styles.warning),
                );

                let diff = TextDiff::from_lines(&actual, &expected);
                write_diff(
                    &diff,
                    &full_path,
                    &styles,
                    // Add an indent to align diff with the status message.
                    &mut IndentWriter::new("    ", std::io::stderr()),
                )?;

                num_outdated += 1;
            }
            CheckStatus::Missing => {
                println!(
                    "  {} {}: {}",
                    WARNING.style(styles.warning),
                    api.filename,
                    "missing".style(styles.warning),
                );
                num_outdated += 1;
            }
        }
    }

    let status_icon = if num_failed > 0 {
        CROSS.style(styles.failure)
    } else if num_outdated > 0 {
        WARNING.style(styles.warning)
    } else {
        CHECK.style(styles.success)
    };

    eprintln!(
        "{} {} APIs checked: {} up-to-date, {} out of date, {} failed",
        status_icon,
        all_apis.len().style(styles.bold),
        num_up_to_date.style(styles.bold),
        num_outdated.style(styles.bold),
        num_failed.style(styles.bold),
    );
    if num_failed > 0 {
        eprintln!(
            "(fix failures, then run {} to update)",
            "cargo xtask openapi generate".style(styles.bold)
        );
        Ok(CheckResult::Failures)
    } else if num_outdated > 0 {
        eprintln!(
            "(run {} to update)",
            "cargo xtask openapi generate".style(styles.bold)
        );
        Ok(CheckResult::NeedsUpdate)
    } else {
        Ok(CheckResult::Success)
    }
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;

    use crate::spec::find_openapi_dir;

    use super::*;

    #[test]
    fn check_apis_up_to_date() -> Result<ExitCode> {
        let output = OutputOpts { color: clap::ColorChoice::Auto };
        let dir = find_openapi_dir()?;

        let result = check_impl(&dir, &output)?;
        Ok(result.to_exit_code())
    }
}
