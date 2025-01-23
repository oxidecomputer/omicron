// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::ExitCode;

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::{Args, Parser, Subcommand};

use crate::{
    cmd::{
        check::check_impl, dump::dump_impl, generate::generate_impl,
        list::list_impl, new_check::new_check_impl, output::OutputOpts,
    },
    spec::Environment,
};

/// Manage OpenAPI specifications.
///
/// For more information, see dev-tools/openapi-manager/README.adoc.
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
            Command::Dump(args) => args.exec(&self.output_opts),
            Command::List(args) => args.exec(&self.output_opts),
            Command::Generate(args) => args.exec(&self.output_opts),
            Command::Check(args) => args.exec(&self.output_opts),
            Command::NewCheck(args) => args.exec(&self.output_opts),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Dump debug information about everything the tool knows
    Dump(DumpArgs),

    /// List managed APIs.
    ///
    /// Returns information purely from code without consulting JSON files on
    /// disk. To compare against files on disk, use the `check` command.
    List(ListArgs),

    /// Generate APIs.
    Generate(GenerateArgs),

    /// Check that APIs are up-to-date.
    Check(CheckArgs),

    /// Check that APIs are up-to-date.
    NewCheck(NewCheckArgs),
}

#[derive(Debug, Args)]
pub struct DumpArgs {
    /// The directory to read generated APIs from.
    #[clap(long)]
    dir: Option<Utf8PathBuf>,

    /// Git revision to look for blessed specs
    #[clap(long)]
    blessed_revision: Option<String>,
}

impl DumpArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let dir = Environment::new(self.dir)?;
        dump_impl(self.blessed_revision.as_deref(), &dir, output)?;
        Ok(ExitCode::SUCCESS)
    }
}

#[derive(Debug, Args)]
pub struct ListArgs {
    /// Show verbose output including descriptions.
    #[clap(long, short)]
    verbose: bool,
}

impl ListArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        list_impl(self.verbose, output)?;
        Ok(ExitCode::SUCCESS)
    }
}

#[derive(Debug, Args)]
pub struct GenerateArgs {
    /// The directory to write generated APIs to (default: workspace root/openapi)
    #[clap(long)]
    dir: Option<Utf8PathBuf>,
}

impl GenerateArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let dir = Environment::new(self.dir)?;
        Ok(generate_impl(&dir, output)?.to_exit_code())
    }
}

#[derive(Debug, Args)]
pub struct CheckArgs {
    /// The directory to read generated APIs from.
    #[clap(long)]
    dir: Option<Utf8PathBuf>,
}

impl CheckArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let env = Environment::new(self.dir)?;
        Ok(check_impl(&env, output)?.to_exit_code())
    }
}

#[derive(Debug, Args)]
pub struct NewCheckArgs {
    /// The directory to read generated APIs from.
    #[clap(long)]
    dir: Option<Utf8PathBuf>,
}

impl NewCheckArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let env = Environment::new(self.dir)?;
        Ok(new_check_impl(&env, output)?.to_exit_code())
    }
}

// This code is not 0 or 1 (general anyhow errors) and indicates out-of-date.
pub(crate) const NEEDS_UPDATE_EXIT_CODE: u8 = 2;

// This code indicates failures during generation, e.g. validation errors.
pub(crate) const FAILURE_EXIT_CODE: u8 = 100;
