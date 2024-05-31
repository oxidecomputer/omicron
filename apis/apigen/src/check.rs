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
    output::{write_diff, OutputOpts, Styles, CHECK, CROSS, STAR, WARNING},
    spec::{all_apis, CheckStatus},
};

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
    let mut num_errors = 0;
    let mut num_outdated = 0;

    for api in all_apis {
        let status = match api.check(&dir) {
            Ok(status) => status,
            Err(err) => {
                eprintln!(
                    "  {} {}: {}",
                    CROSS.style(styles.failure),
                    api.filename,
                    err.style(styles.failure),
                );
                num_errors += 1;
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
            CheckStatus::Missing { .. } => {
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

    let mut errors = Vec::new();
    if num_outdated > 0 {
        errors.push(format!(
            "{} APIs are {}",
            num_outdated.style(styles.bold),
            "out of date".style(styles.failure),
        ));
    }

    if num_errors > 0 {
        errors.push(format!(
            "{} {} encountered",
            num_errors.style(styles.bold),
            "errors".style(styles.failure),
        ));
    }

    if !errors.is_empty() {
        eprintln!(
            "{} {}: run {} to update",
            CROSS.style(styles.failure),
            errors.join(", "),
            "cargo xtask apigen generate".style(styles.bold),
        );

        Ok(CheckResult::OutOfDate)
    } else {
        eprintln!(
            "{} all APIs are {}",
            CHECK.style(styles.success),
            "up-to-date".style(styles.success)
        );
        Ok(CheckResult::Ok)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CheckResult {
    Ok,
    OutOfDate,
}

impl CheckResult {
    pub(crate) fn to_exit_code(self) -> ExitCode {
        match self {
            CheckResult::Ok => ExitCode::SUCCESS,
            // Use a code that's not 0 or 1 (errors) to indicate out-of-date.
            CheckResult::OutOfDate => 2.into(),
        }
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
