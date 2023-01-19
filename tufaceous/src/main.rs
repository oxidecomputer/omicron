mod date;
mod hint;
mod key;
mod root;

use crate::key::Key;
use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use fs_err as fs;
use std::num::NonZeroU64;
use std::path::PathBuf;
use tough::editor::RepositoryEditor;

#[derive(Debug, Parser)]
struct Args {
    #[clap(subcommand)]
    command: Command,

    #[clap(short = 'k', long = "key", env = "TUFACEOUS_KEY", required = false)]
    keys: Vec<Key>,

    #[clap(long, value_parser = crate::date::parse_duration_or_datetime, default_value = "7d")]
    expiry: DateTime<Utc>,

    /// TUF repository path (default: current working directory)
    #[clap(short = 'r', long)]
    repo: Option<PathBuf>,
}

#[derive(Debug, Parser)]
enum Command {
    /// Create a new rack update TUF repository
    Init {
        /// Disable random key generation and exit if no keys are provided
        #[clap(long)]
        no_generate_key: bool,
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
            let root = crate::root::new_root(keys.clone(), args.expiry)?;

            let metadata_dir = repo_path.join("metadata");
            let targets_dir = repo_path.join("targets");
            let root_path = metadata_dir
                .join(format!("{}.root.json", root.signed().signed.version));

            fs::create_dir_all(&metadata_dir)?;
            fs::create_dir_all(&targets_dir)?;
            fs::write(&root_path, root.buffer())?;

            let mut editor = RepositoryEditor::new(&root_path)?;
            let version = u64::try_from(Utc::now().timestamp())
                .and_then(NonZeroU64::try_from)
                .expect("bad epoch");
            editor.snapshot_version(version);
            editor.targets_version(version)?;
            editor.timestamp_version(version);
            editor.snapshot_expires(args.expiry);
            editor.targets_expires(args.expiry)?;
            editor.timestamp_expires(args.expiry);

            // todo: add empty artifacts.json target

            let signed = editor.sign(&crate::key::boxed_keys(keys))?;
            signed.write(&metadata_dir)?;
            println!(
                "Initialized TUF repository in {}",
                fs::canonicalize(&repo_path)?.display()
            );
            Ok(())
        }
    }
}
