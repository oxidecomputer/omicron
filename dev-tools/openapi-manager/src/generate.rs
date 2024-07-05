// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::ExitCode;

use anyhow::Result;
use camino::Utf8Path;
use indent_write::io::IndentWriter;
use owo_colors::OwoColorize;

use crate::{
    output::{
        display_error, display_summary, headers::*, plural, OutputOpts, Styles,
    },
    spec::{all_apis, OverwriteStatus},
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
    dir: &Utf8Path,
    output: &OutputOpts,
) -> Result<GenerateResult> {
    let mut styles = Styles::default();
    if output.use_color(supports_color::Stream::Stderr) {
        styles.colorize();
    }

    let all_apis = all_apis();
    let total = all_apis.len();
    let count_width = total.to_string().len();

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

    for (ix, api) in all_apis.iter().enumerate() {
        let count = ix + 1;

        match api.overwrite(&dir) {
            Ok((status, summary)) => match status {
                OverwriteStatus::Updated => {
                    eprintln!(
                        "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}: {}",
                        UPDATED.style(styles.success_header),
                        api.filename,
                        display_summary(&summary, &styles),
                    );
                    num_updated += 1;
                }
                OverwriteStatus::Unchanged => {
                    eprintln!(
                        "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}: {}",
                        UNCHANGED.style(styles.unchanged_header),
                        api.filename,
                        display_summary(&summary, &styles),
                    );
                    num_unchanged += 1;
                }
            },
            Err(err) => {
                eprint!(
                    "{:>HEADER_WIDTH$} [{count:>count_width$}/{total}] {}: ",
                    FAILURE.style(styles.failure_header),
                    api.filename
                );
                display_error(
                    &err,
                    styles.failure,
                    &mut IndentWriter::new_skip_initial(
                        "      ",
                        std::io::stderr(),
                    ),
                )?;

                num_failed += 1;
            }
        };
    }

    let status_header = if num_failed > 0 {
        FAILURE.style(styles.failure_header)
    } else {
        SUCCESS.style(styles.success_header)
    };

    eprintln!(
        "{:>HEADER_WIDTH$} {} {} generated: \
         {} updated, {} unchanged, {} failed",
        status_header,
        all_apis.len().style(styles.bold),
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
