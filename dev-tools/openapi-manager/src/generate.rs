// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{io::Write, process::ExitCode};

use anyhow::Result;
use indent_write::io::IndentWriter;
use owo_colors::OwoColorize;

use crate::{
    output::{
        display_api_spec, display_error, display_summary, headers::*, plural,
        OutputOpts, Styles,
    },
    spec::{all_apis, Environment},
    FAILURE_EXIT_CODE,
};

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

pub(crate) fn generate_impl(
    env: &Environment,
    output: &OutputOpts,
) -> Result<GenerateResult> {
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
        GENERATING.style(styles.success_header),
        total.style(styles.bold),
        plural::documents(total),
    );
    let mut num_updated = 0;
    let mut num_unchanged = 0;
    let mut num_failed = 0;

    for (ix, spec) in all_apis.iter().enumerate() {
        let count = ix + 1;

        match spec.overwrite(env) {
            Ok(status) => {
                let updated_count = status.updated_count();

                if updated_count > 0 {
                    eprintln!(
                        "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}: {} ({} {} updated)",
                        UPDATED.style(styles.success_header),
                        display_api_spec(spec, &styles),
                        display_summary(&status.summary, &styles),
                        updated_count.style(styles.bold),
                        plural::files(updated_count),
                    );
                    num_updated += 1;
                } else {
                    eprintln!(
                        "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}: {}",
                        UNCHANGED.style(styles.unchanged_header),
                        display_api_spec(spec, &styles),
                        display_summary(&status.summary, &styles),
                    );
                    num_unchanged += 1;
                }
            }
            Err(err) => {
                eprintln!(
                    "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}",
                    FAILURE.style(styles.failure_header),
                    display_api_spec(spec, &styles),
                );
                let display = display_error(&err, styles.failure);
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
    } else {
        SUCCESS.style(styles.success_header)
    };

    eprintln!(
        "{:>HEADER_WIDTH$} {} {} generated: \
         {} updated, {} unchanged, {} failed",
        status_header,
        total.style(styles.bold),
        plural::documents(total),
        num_updated.style(styles.bold),
        num_unchanged.style(styles.bold),
        num_failed.style(styles.bold),
    );

    if num_failed > 0 {
        Ok(GenerateResult::Failures)
    } else {
        Ok(GenerateResult::Success)
    }
}
