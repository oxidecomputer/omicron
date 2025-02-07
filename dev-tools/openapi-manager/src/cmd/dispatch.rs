// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap do we now need a "diff" command to see what's changed between
// versions?

use std::process::ExitCode;

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::{Args, Parser, Subcommand};

use crate::{
    cmd::{
        check::check_impl, debug::debug_impl, generate::generate_impl,
        list::list_impl,
    },
    environment::{BlessedSource, Environment, GeneratedSource},
    git::GitRevision,
    output::OutputOpts,
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
    pub fn exec(self) -> ExitCode {
        let result = match self.command {
            Command::Debug(args) => args.exec(&self.output_opts),
            Command::List(args) => args.exec(&self.output_opts),
            Command::Generate(args) => args.exec(&self.output_opts),
            Command::Check(args) => args.exec(&self.output_opts),
        };

        match result {
            Ok(exit_code) => exit_code,
            Err(error) => {
                eprintln!("failure: {:#}", error);
                ExitCode::FAILURE
            }
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Dump debug information about everything the tool knows
    Debug(DebugArgs),

    /// List managed APIs.
    ///
    /// Returns information purely from code without consulting JSON files on
    /// disk. To compare against files on disk, use the `check` command.
    List(ListArgs),

    /// Generate latest OpenAPI documents and validate the results.
    Generate(GenerateArgs),

    /// Check that OpenAPI documents are up-to-date and valid.
    Check(CheckArgs),
}

// XXX-dap TODO-doc, examples
#[derive(Debug, Args)]
struct BlessedSourceArgs {
    /// Loads blessed OpenAPI documents from path PATH in the given Git
    /// REVISION.
    ///
    /// The REVISION is not used as-is; instead, the tool always looks at the
    /// merge-base between HEAD and REVISION.  So if you provide main:openapi,
    /// then it will look at the merge-base of HEAD and "main", in directory
    /// "openapi" in that commit.
    ///
    /// The default behavior is equivalent to `--blessed-from-git=main:openapi`.
    /// Specifying this option is mainly useful if your branch's parent branch
    /// is *not* `main`.
    #[clap(
        long,
        env("OPENAPI_MGR_BLESSED_FROM_GIT"),
        value_name("REVISION:PATH")
    )]
    blessed_from_git: Option<String>,

    /// Loads blessed OpenAPI documents from a local directory (instead of the
    /// default, from Git).
    ///
    /// This is intended for testing and debugging this tool.
    #[clap(
        long,
        conflicts_with("blessed_from_git"),
        env("OPENAPI_MGR_BLESSED_FROM_DIR"),
        value_name("DIRECTORY")
    )]
    blessed_from_dir: Option<Utf8PathBuf>,
}

impl TryFrom<BlessedSourceArgs> for BlessedSource {
    type Error = anyhow::Error;

    fn try_from(b: BlessedSourceArgs) -> Result<Self, Self::Error> {
        // XXX-dap test this
        // clap should not allow both of these to be set.
        assert!(b.blessed_from_dir.is_none() || b.blessed_from_git.is_none());

        if let Some(local_directory) = b.blessed_from_dir {
            return Ok(BlessedSource::Directory { local_directory });
        }

        let (revision_str, maybe_directory) = match &b.blessed_from_git {
            None => ("main", None),
            Some(arg) => match arg.split_once(":") {
                Some((r, d)) => (r, Some(d)),
                None => (arg.as_str(), None),
            },
        };
        let revision = GitRevision::from(String::from(revision_str));
        let directory = Utf8PathBuf::from(maybe_directory.unwrap_or("openapi"));
        Ok(BlessedSource::GitRevisionMergeBase { revision, directory })
    }
}

#[derive(Debug, Args)]
pub struct GeneratedSourceArgs {
    /// Instead of generating OpenAPI documents directly from the API
    /// implementation, load OpenAPI documents from this directory.
    ///
    ///
    #[clap(long, value_name("DIRECTORY"))]
    generated_from_dir: Option<Utf8PathBuf>,
}

impl From<GeneratedSourceArgs> for GeneratedSource {
    fn from(value: GeneratedSourceArgs) -> Self {
        match value.generated_from_dir {
            Some(local_directory) => {
                GeneratedSource::Directory { local_directory }
            }
            None => GeneratedSource::Generated,
        }
    }
}

#[derive(Debug, Args)]
pub struct LocalSourceArgs {
    /// Loads this workspace's OpenAPI documents from local path DIRECTORY.
    #[clap(long, env("OPENAPI_MGR_DIR"), value_name("DIRECTORY"))]
    dir: Option<Utf8PathBuf>,
}

#[derive(Debug, Args)]
pub struct DebugArgs {
    #[clap(flatten)]
    local: LocalSourceArgs,
    #[clap(flatten)]
    blessed: BlessedSourceArgs,
    #[clap(flatten)]
    generated: GeneratedSourceArgs,
}

impl DebugArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let env = Environment::new(self.local.dir)?;
        let blessed_source = BlessedSource::try_from(self.blessed)?;
        let generated_source = GeneratedSource::from(self.generated);
        debug_impl(&env, &blessed_source, &generated_source, output)?;
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
    #[clap(flatten)]
    local: LocalSourceArgs,
    #[clap(flatten)]
    blessed: BlessedSourceArgs,
    #[clap(flatten)]
    generated: GeneratedSourceArgs,
}

impl GenerateArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let env = Environment::new(self.local.dir)?;
        let blessed_source = BlessedSource::try_from(self.blessed)?;
        let generated_source = GeneratedSource::from(self.generated);
        Ok(generate_impl(&env, &blessed_source, &generated_source, output)?
            .to_exit_code())
    }
}

#[derive(Debug, Args)]
pub struct CheckArgs {
    #[clap(flatten)]
    local: LocalSourceArgs,
    #[clap(flatten)]
    blessed: BlessedSourceArgs,
    #[clap(flatten)]
    generated: GeneratedSourceArgs,
}

impl CheckArgs {
    fn exec(self, output: &OutputOpts) -> anyhow::Result<ExitCode> {
        let env = Environment::new(self.local.dir)?;
        let blessed_source = BlessedSource::try_from(self.blessed)?;
        let generated_source = GeneratedSource::from(self.generated);
        Ok(check_impl(&env, &blessed_source, &generated_source, output)?
            .to_exit_code())
    }
}

// This code is not 0 or 1 (general anyhow errors) and indicates out-of-date.
pub(crate) const NEEDS_UPDATE_EXIT_CODE: u8 = 2;

// This code indicates failures during generation, e.g. validation errors.
pub(crate) const FAILURE_EXIT_CODE: u8 = 100;
