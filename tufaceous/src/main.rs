mod date;
mod hint;
mod key;
mod root;
mod target;

use crate::key::Key;
use crate::target::TargetWriter;
use anyhow::{bail, Context, Result};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use clap::Parser;
use fs_err::{self as fs, File};
use omicron_common::api::internal::nexus::UpdateArtifactKind;
use omicron_common::update::{Artifact, ArtifactsDocument};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use tough::editor::RepositoryEditor;
use tough::{Repository, RepositoryLoader};
use url::Url;

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
        None => std::env::current_dir()?,
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
            update_versions(&mut editor, args.expiry)?;

            let artifacts = ArtifactsDocument::default();
            let mut file = TargetWriter::new(&targets_dir, "artifacts.json")?;
            serde_json::to_writer_pretty(&mut file, &artifacts)?;
            file.finish(&mut editor)?;

            let signed = editor.sign(&crate::key::boxed_keys(keys))?;
            signed.write(&metadata_dir)?;
            println!(
                "Initialized TUF repository in {}",
                fs::canonicalize(&repo_path)?.display()
            );
            Ok(())
        }
        Command::AddZone { name, zone, version } => {
            let repo = load_repo(&repo_path)?;
            let mut artifacts =
                match repo.read_target(&"artifacts.json".try_into()?)? {
                    Some(target) => serde_json::from_reader(target)?,
                    None => ArtifactsDocument::default(),
                };
            let existing_targets = repo
                .targets()
                .signed
                .targets_iter()
                .map(|(name, _)| name.to_owned())
                .collect::<Vec<_>>();
            let mut editor = RepositoryEditor::from_repo(
                repo_path
                    .join("metadata")
                    .join(format!("{}.root.json", repo.root().signed.version)),
                repo,
            )?;
            update_versions(&mut editor, args.expiry)?;

            let name = match &name {
                Some(name) => name,
                None => zone
                    .file_name()
                    .context("zone path is a directory")?
                    .split('.')
                    .next()
                    .expect("str::split has at least 1 element"),
            };
            let filename = format!("{}-{}.tar.gz", name, version);

            // if we already have an artifact of this name/version/kind, replace it.
            if let Some(artifact) =
                artifacts.artifacts.iter_mut().find(|artifact| {
                    artifact.name == name
                        && artifact.version == version
                        && artifact.kind == UpdateArtifactKind::Zone.into()
                })
            {
                editor.remove_target(&artifact.target.as_str().try_into()?)?;
                artifact.target = filename.clone();
            } else {
                // if we don't, make sure we're not overriding another target.
                if existing_targets.iter().any(|target_name| {
                    target_name.raw() == filename
                        && target_name.resolved() == filename
                }) {
                    bail!(
                        "a target named {:?} already exists in the repository",
                        filename
                    );
                }
                artifacts.artifacts.push(Artifact {
                    name: name.to_owned(),
                    version: version.clone(),
                    kind: UpdateArtifactKind::Zone.into(),
                    target: filename.clone(),
                })
            }

            let targets_dir = repo_path.join("targets");

            let mut file = TargetWriter::new(&targets_dir, filename)?;
            std::io::copy(&mut File::open(&zone)?, &mut file)?;
            file.finish(&mut editor)?;

            let mut file = TargetWriter::new(&targets_dir, "artifacts.json")?;
            serde_json::to_writer_pretty(&mut file, &artifacts)?;
            file.finish(&mut editor)?;

            let signed = editor.sign(&crate::key::boxed_keys(args.keys))?;
            signed.write(repo_path.join("metadata"))?;
            println!("added zone {}, version {}", name, version);
            Ok(())
        }
    }
}

fn load_repo(repo_path: &Path) -> Result<Repository> {
    let repo_path = fs::canonicalize(repo_path)?;
    Ok(RepositoryLoader::new(
        File::open(repo_path.join("metadata").join("1.root.json"))?,
        Url::from_file_path(repo_path.join("metadata"))
            .expect("the canonical path is not absolute?"),
        Url::from_file_path(repo_path.join("targets"))
            .expect("the canonical path is not absolute?"),
    )
    .expiration_enforcement(tough::ExpirationEnforcement::Unsafe)
    .load()?)
}

fn update_versions(
    editor: &mut RepositoryEditor,
    expiry: DateTime<Utc>,
) -> Result<()> {
    let version = u64::try_from(Utc::now().timestamp())
        .and_then(NonZeroU64::try_from)
        .expect("bad epoch");
    editor.snapshot_version(version);
    editor.targets_version(version)?;
    editor.timestamp_version(version);
    editor.snapshot_expires(expiry);
    editor.targets_expires(expiry)?;
    editor.timestamp_expires(expiry);
    Ok(())
}
