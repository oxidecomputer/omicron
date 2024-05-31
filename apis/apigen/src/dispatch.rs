// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::ExitCode;

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use owo_colors::OwoColorize;

use crate::{
    check::check_impl,
    output::{OutputOpts, Styles, CHECK, STAR},
    spec::{all_apis, openapi_dir, OverwriteStatus},
};

/// Manage OpenAPI specifications.
#[derive(Debug, Parser)]
pub struct App {
    #[clap(flatten)]
    output_opts: OutputOpts,

    #[clap(subcommand)]
    command: Command,
}

impl App {
    pub fn exec(self) -> Result<ExitCode> {
        match self.command {
            Command::Generate(args) => args.exec(&self.output_opts),
            Command::Check(args) => args.exec(&self.output_opts),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Generate APIs.
    Generate(GenerateArgs),

    /// Check that APIs are up-to-date.
    Check(CheckArgs),
}

#[derive(Debug, Parser)]
pub struct GenerateArgs {
    /// The directory to write generated APIs to (default: <workspace root>/openapi)
    #[clap(long)]
    dir: Option<Utf8PathBuf>,
}

impl GenerateArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let mut styles = Styles::default();
        if output.use_color(supports_color::Stream::Stderr) {
            styles.colorize();
        }

        let dir = openapi_dir(self.dir)?;
        let all_apis = all_apis();

        eprintln!(
            "{STAR} generating {} APIs...",
            all_apis.len().style(styles.bold)
        );
        let mut num_unchanged = 0;

        for api in &all_apis {
            let status = api.overwrite(&dir)?;

            match status {
                OverwriteStatus::Unchanged => {
                    eprintln!(
                        "  {} {}: {}",
                        CHECK.style(styles.success),
                        api.filename,
                        "unchanged".style(styles.unchanged),
                    );
                    num_unchanged += 1;
                }
                OverwriteStatus::Updated => {
                    eprintln!(
                        "  {} {}: {}",
                        CHECK.style(styles.success),
                        api.filename,
                        "updated".style(styles.warning),
                    );
                }
            }
        }

        eprintln!(
            "{} all {} APIs are {} ({} {})",
            CHECK.style(styles.success),
            all_apis.len().style(styles.bold),
            "up-to-date".style(styles.success),
            num_unchanged.style(styles.bold),
            "unchanged".style(styles.unchanged),
        );

        Ok(ExitCode::SUCCESS)
    }
}

#[derive(Debug, Parser)]
pub struct CheckArgs {
    /// The directory to read generated APIs from.
    #[clap(long)]
    dir: Option<Utf8PathBuf>,
}

impl CheckArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let dir = openapi_dir(self.dir)?;
        Ok(check_impl(&dir, output)?.to_exit_code())
    }
}
