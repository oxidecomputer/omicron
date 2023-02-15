mod date;
mod hint;

use anyhow::{bail, Context, Result};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use clap::Parser;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use tufaceous_lib::{AddArtifact, ArchiveExtractor, Key, OmicronRepo};

#[derive(Debug, Parser)]
struct Args {
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

#[derive(Debug, Parser)]
enum Command {
    /// Create a new rack update TUF repository
    Init {
        /// Disable random key generation and exit if no keys are provided
        #[clap(long)]
        no_generate_key: bool,
    },
    Add {
        /// The kind of artifact this is.
        kind: KnownArtifactKind,

        /// Path to the artifact.
        path: Utf8PathBuf,

        /// Override the name for this artifact (default: filename with extension stripped)
        #[clap(long)]
        name: Option<String>,

        /// Artifact version.
        version: String,
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
}

fn main() -> Result<()> {
    let args = Args::parse();
    let repo_path = match args.repo {
        Some(repo) => repo,
        None => std::env::current_dir()?.try_into()?,
    };

    match args.command {
        Command::Init { no_generate_key } => {
            let keys = if !no_generate_key && args.keys.is_empty() {
                let key = Key::generate_ed25519();
                crate::hint::generated_key(&key);
                vec![key]
            } else {
                args.keys
            };

            let repo = OmicronRepo::initialize(&repo_path, keys, args.expiry)?;
            println!("Initialized TUF repository in {}", repo.repo_path());
            Ok(())
        }
        Command::Add { kind, path, name, version } => {
            let repo = OmicronRepo::load_ignore_expiration(&repo_path)?;
            let mut editor = repo.into_editor()?;

            let new_artifact = AddArtifact::new(kind, path, name, version)?;

            editor
                .add_artifact(&new_artifact)
                .context("error adding artifact")?;
            editor.sign_and_finish(args.keys, args.expiry)?;
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

            let repo = OmicronRepo::load_ignore_expiration(&repo_path)?;
            repo.archive(&output_path)?;

            Ok(())
        }
        Command::Extract { archive_file, dest } => {
            let mut extractor = ArchiveExtractor::from_path(&archive_file)?;
            extractor.extract(&dest)?;

            // Now load the repository and ensure it's valid.
            let repo = OmicronRepo::load(&dest).with_context(|| {
                format!(
                    "error loading extracted repository at `{dest}` \
                     (extracted files are still available)"
                )
            })?;
            repo.read_artifacts().with_context(|| {
                format!(
                    "error loading artifacts.json from extracted archive \
                     at `{dest}`"
                )
            })?;

            Ok(())
        }
    }
}
