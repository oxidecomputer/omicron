// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{io::Write, process::ExitCode};

use anyhow::Result;
use camino::Utf8Path;
use indent_write::io::IndentWriter;
use owo_colors::OwoColorize;
use similar::TextDiff;

use crate::{
    output::{
        display_api_spec, display_error, display_summary, headers::*, plural,
        write_diff, OutputOpts, Styles,
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
    let total = all_apis.len();
    let count_width = total.to_string().len();
    let continued_indent = continued_indent(count_width);

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);

    eprintln!(
        "{:>HEADER_WIDTH$} {} OpenAPI {}...",
        CHECKING.style(styles.success_header),
        total.style(styles.bold),
        plural::documents(total),
    );
    let mut num_up_to_date = 0;
    let mut num_stale = 0;
    let mut num_missing = 0;
    let mut num_failed = 0;

    for (ix, spec) in all_apis.iter().enumerate() {
        let count = ix + 1;

        match spec.check(&dir) {
            Ok(status) => match status {
                CheckStatus::Ok(summary) => {
                    eprintln!(
                            "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}: {}",
                            UP_TO_DATE.style(styles.success_header),
                            display_api_spec(spec, &styles),
                            display_summary(&summary, &styles),
                        );

                    num_up_to_date += 1;
                }
                CheckStatus::Stale { full_path, actual, expected } => {
                    eprintln!(
                        "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}",
                        STALE.style(styles.warning_header),
                        display_api_spec(spec, &styles),
                    );

                    let diff = TextDiff::from_lines(&actual, &expected);
                    write_diff(
                        &diff,
                        &full_path,
                        &styles,
                        // Add an indent to align diff with the status message.
                        &mut IndentWriter::new(
                            &continued_indent,
                            std::io::stderr(),
                        ),
                    )?;

                    num_stale += 1;
                }
                CheckStatus::Missing => {
                    eprintln!(
                        "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}",
                        MISSING.style(styles.warning_header),
                        display_api_spec(spec, &styles),
                    );

                    num_missing += 1;
                }
            },
            Err(error) => {
                eprint!(
                    "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}",
                    FAILURE.style(styles.failure_header),
                    display_api_spec(spec, &styles),
                );
                let display = display_error(&error, styles.failure);
                write!(
                    IndentWriter::new(&continued_indent, std::io::stderr()),
                    "{}",
                    display,
                )?;

                num_failed += 1;
            }
        };
    }

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);

    let status_header = if num_failed > 0 {
        FAILURE.style(styles.failure_header)
    } else if num_stale > 0 {
        STALE.style(styles.warning_header)
    } else {
        SUCCESS.style(styles.success_header)
    };

    eprintln!(
        "{:>HEADER_WIDTH$} {} {} checked: {} up-to-date, {} stale, {} missing, {} failed",
        status_header,
        total.style(styles.bold),
        plural::documents(total),
        num_up_to_date.style(styles.bold),
        num_stale.style(styles.bold),
        num_missing.style(styles.bold),
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
