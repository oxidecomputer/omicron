// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Context, Result};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use clap::{CommandFactory, Parser};
use omicron_common::{api::external::SemverVersion, update::ArtifactKind};
use tufaceous_lib::{
    assemble::{ArtifactManifest, OmicronRepoAssembler},
    AddArtifact, ArchiveExtractor, Key, OmicronRepo,
};

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(subcommand)]
    command: Command,

    #[clap(
        short = 'k',
        long = "key",
        env = "TUFACEOUS_KEY",
        required = false,
        global = true
    )]
    keys: Vec<Key>,

    #[clap(long, value_parser = crate::date::parse_duration_or_datetime, default_value = "7d", global = true)]
    expiry: DateTime<Utc>,

    /// TUF repository path (default: current working directory)
    #[clap(short = 'r', long, global = true)]
    repo: Option<Utf8PathBuf>,
}

impl Args {
    /// Executes these arguments.
    pub async fn exec(self, log: &slog::Logger) -> Result<()> {
        let repo_path = match self.repo {
            Some(repo) => repo,
            None => std::env::current_dir()?.try_into()?,
        };

        match self.command {
            Command::Init { system_version, no_generate_key } => {
                let keys = maybe_generate_keys(self.keys, no_generate_key)?;

                let repo = OmicronRepo::initialize(
                    &log,
                    &repo_path,
                    system_version,
                    keys,
                    self.expiry,
                )
                .await?;
                slog::info!(
                    log,
                    "Initialized TUF repository in {}",
                    repo.repo_path()
                );
                Ok(())
            }
            Command::Add { kind, allow_unknown_kinds, path, name, version } => {
                if !allow_unknown_kinds {
                    // Try converting kind to a known kind.
                    if kind.to_known().is_none() {
                        // Simulate a failure to parse (though ideally there would
                        // be a way to also specify the underlying error -- there
                        // doesn't appear to be a public API to do so in clap 4).
                        let mut error = clap::Error::new(
                            clap::error::ErrorKind::ValueValidation,
                        )
                        .with_cmd(&Args::command());
                        error.insert(
                            clap::error::ContextKind::InvalidArg,
                            clap::error::ContextValue::String(
                                "<KIND>".to_owned(),
                            ),
                        );
                        error.insert(
                            clap::error::ContextKind::InvalidValue,
                            clap::error::ContextValue::String(kind.to_string()),
                        );
                        error.exit();
                    }
                }

                let repo = OmicronRepo::load_untrusted_ignore_expiration(
                    &log, &repo_path,
                )
                .await?;
                let mut editor = repo.into_editor().await?;

                let new_artifact =
                    AddArtifact::from_path(kind, name, version, path)?;

                editor
                    .add_artifact(&new_artifact)
                    .context("error adding artifact")?;
                editor.sign_and_finish(self.keys, self.expiry).await?;
                println!(
                    "added {} {}, version {}",
                    new_artifact.kind(),
                    new_artifact.name(),
                    new_artifact.version()
                );
                Ok(())
            }
            Command::Archive { output_path } => {
                // The filename must end with "zip".
                if output_path.extension() != Some("zip") {
                    bail!("output path `{output_path}` must end with .zip");
                }

                let repo = OmicronRepo::load_untrusted_ignore_expiration(
                    &log, &repo_path,
                )
                .await?;
                repo.archive(&output_path)?;

                Ok(())
            }
            Command::Extract { archive_file, dest } => {
                let mut extractor = ArchiveExtractor::from_path(&archive_file)?;
                extractor.extract(&dest)?;

                // Now load the repository and ensure it's valid.
                let repo = OmicronRepo::load_untrusted(&log, &dest)
                    .await
                    .with_context(|| {
                        format!(
                            "error loading extracted repository at `{dest}` \
                         (extracted files are still available)"
                        )
                    })?;
                repo.read_artifacts().await.with_context(|| {
                    format!(
                        "error loading artifacts.json from extracted archive \
                         at `{dest}`"
                    )
                })?;

                Ok(())
            }
            Command::Assemble {
                manifest_path,
                output_path,
                build_dir,
                no_generate_key,
                skip_all_present,
            } => {
                // The filename must end with "zip".
                if output_path.extension() != Some("zip") {
                    bail!("output path `{output_path}` must end with .zip");
                }

                let manifest = ArtifactManifest::from_path(&manifest_path)
                    .context("error reading manifest")?;
                if !skip_all_present {
                    manifest.verify_all_present()?;
                }

                let keys = maybe_generate_keys(self.keys, no_generate_key)?;
                let mut assembler = OmicronRepoAssembler::new(
                    &log,
                    manifest,
                    keys,
                    self.expiry,
                    output_path,
                );
                if let Some(dir) = build_dir {
                    assembler.set_build_dir(dir);
                }

                assembler.build().await?;

                Ok(())
            }
        }
    }
}

#[derive(Debug, Parser)]
enum Command {
    /// Create a new rack update TUF repository
    Init {
        /// The system version.
        system_version: SemverVersion,

        /// Disable random key generation and exit if no keys are provided
        #[clap(long)]
        no_generate_key: bool,
    },
    Add {
        /// The kind of artifact this is.
        kind: ArtifactKind,

        /// Allow artifact kinds that aren't known to tufaceous
        #[clap(long)]
        allow_unknown_kinds: bool,

        /// Path to the artifact.
        path: Utf8PathBuf,

        /// Override the name for this artifact (default: filename with extension stripped)
        #[clap(long)]
        name: Option<String>,

        /// Artifact version.
        version: SemverVersion,
    },
    /// Archives this repository to a zip file.
    Archive {
        /// The path to write the archive to (must end with .zip).
        output_path: Utf8PathBuf,
    },
    /// Validates and extracts a repository created by the `archive` command.
    Extract {
        /// The file to extract.
        archive_file: Utf8PathBuf,

        /// The destination to extract the file to.
        dest: Utf8PathBuf,
    },
    /// Assembles a repository from a provided manifest.
    Assemble {
        /// Path to artifact manifest.
        manifest_path: Utf8PathBuf,

        /// The path to write the archive to (must end with .zip).
        output_path: Utf8PathBuf,

        /// Directory to use for building artifacts [default: temporary directory]
        #[clap(long)]
        build_dir: Option<Utf8PathBuf>,

        /// Disable random key generation and exit if no keys are provided
        #[clap(long)]
        no_generate_key: bool,

        /// Skip checking to ensure all expected artifacts are present.
        #[clap(long)]
        skip_all_present: bool,
    },
}

fn maybe_generate_keys(
    keys: Vec<Key>,
    no_generate_key: bool,
) -> Result<Vec<Key>> {
    Ok(if !no_generate_key && keys.is_empty() {
        let key = Key::generate_ed25519()?;
        crate::hint::generated_key(&key);
        vec![key]
    } else {
        keys
    })
}
