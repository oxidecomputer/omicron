// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::ExitCode;

use anyhow::Result;
use camino::Utf8Path;
use indent_write::io::IndentWriter;
use owo_colors::OwoColorize;

use crate::{
    output::{display_error, OutputOpts, Styles, CHECK, CROSS, STAR},
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

    eprintln!(
        "{STAR} generating {} APIs...",
        all_apis.len().style(styles.bold)
    );
    let mut num_updated = 0;
    let mut num_unchanged = 0;
    let mut num_failed = 0;

    for api in &all_apis {
        let status = match api.overwrite(&dir) {
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
            OverwriteStatus::Updated => {
                eprintln!(
                    "  {} {}: {}",
                    CHECK.style(styles.success),
                    api.filename,
                    "updated".style(styles.warning),
                );
                num_updated += 1;
            }
            OverwriteStatus::Unchanged => {
                eprintln!(
                    "  {} {}: {}",
                    CHECK.style(styles.success),
                    api.filename,
                    "unchanged".style(styles.unchanged),
                );
                num_unchanged += 1;
            }
        }
    }

    let status_icon = if num_failed > 0 {
        CROSS.style(styles.failure)
    } else {
        CHECK.style(styles.success)
    };

    eprintln!(
        "{} {} APIs generated: {} updated, {} unchanged, {} failed",
        status_icon,
        all_apis.len().style(styles.bold),
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
