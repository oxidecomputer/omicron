mod hint;

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use clap::Parser;
use tufaceous_lib::{parse_duration_or_datetime, AddZone, Key, OmicronRepo};

#[derive(Debug, Parser)]
struct Args {
    #[clap(subcommand)]
    command: Command,

    #[clap(short = 'k', long = "key", env = "TUFACEOUS_KEY", required = false)]
    keys: Vec<Key>,

    #[clap(long, value_parser = parse_duration_or_datetime, default_value = "7d")]
    expiry: DateTime<Utc>,

    /// TUF repository path (default: current working directory)
    #[clap(short = 'r', long)]
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
    AddZone {
        /// Override the name for this zone (default: zone filename with extension stripped)
        #[clap(long)]
        name: Option<String>,

        zone: Utf8PathBuf,
        version: String,
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
        Command::AddZone { name, zone, version } => {
            let repo = OmicronRepo::load(&repo_path)?;
            let mut editor = repo.into_editor()?;

            let add_zone = AddZone::new(zone, name, version)?;

            editor
                .add_zone(&add_zone, args.expiry)
                .context("error adding zone")?;
            editor.sign_and_finish(args.keys)?;
            println!(
                "added zone {}, version {}",
                add_zone.name(),
                add_zone.version()
            );
            Ok(())
        }
    }
}
