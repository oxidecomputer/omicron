/*!
 * Utility for bundling target binaries as tarfiles.
 */

use oxide_api_prototype::packaging::sha256_digest;

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::env;
use std::fs::{create_dir_all, OpenOptions};
use std::path::PathBuf;
use structopt::StructOpt;
use tar::Builder;

#[derive(Debug, StructOpt)]
#[structopt(name = "packaging tool")]
struct Args {
    /// The binary profile to package.
    ///
    /// True: release, False: debug.
    #[structopt(short, long, help = "True if bundling release-mode binaries")]
    release: bool,
}

/// Describes a bundled binary.
struct Package<'a> {
    name: &'a str,
}

impl<'a> Package<'a> {
    fn new(name: &str) -> Package {
        Package { name }
    }

    fn binary(&self, release: bool) -> PathBuf {
        PathBuf::from(format!(
            "target/{}/{}",
            if release { "release" } else { "debug" },
            self.name
        ))
    }

    fn smf(&self) -> PathBuf {
        PathBuf::from(format!("smf/{}", self.name))
    }

    fn name(&self) -> &str {
        self.name
    }
}

fn main() -> Result<()> {
    let args = Args::from_args_safe().map_err(|err| anyhow!(err))?;

    let packages = &[
        &Package::new("bootstrap_agent"),
        &Package::new("oxide_controller"),
        &Package::new("sled_agent"),
    ];

    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    env::set_current_dir(&root)?;
    let output_directory = root.join("out");
    create_dir_all(&output_directory)
        .map_err(|err| anyhow!("Cannot create output directory: {}", err))?;

    // As we emit each package bundle, capture their digests for
    // verification purposes.
    let mut digests = HashMap::<String, Vec<u8>>::new();

    for package in packages {
        let tarfile = output_directory.join(format!("{}.tar", package.name));
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .truncate(true)
            .create(true)
            .open(&tarfile)
            .map_err(|err| anyhow!("Cannot create tarfile: {}", err))?;

        // Create anarchive filled with:
        // - The binary
        // - The corresponding SMF directory
        //
        // TODO: We could add compression here, if we'd like?
        let mut archive = Builder::new(file);
        archive.mode(tar::HeaderMode::Deterministic);
        archive
            .append_path_with_name(package.binary(args.release), package.name)
            .map_err(|err| {
                anyhow!("Cannot append binary to tarfile: {}", err)
            })?;
        for entry in walkdir::WalkDir::new(package.smf()) {
            let entry =
                entry.map_err(|err| anyhow!("Cannot access entry: {}", err))?;
            if entry.path().starts_with(".") {
                // Omit hidden files - we don't want to include text editor
                // artifacts in the tarfile.
                continue;
            }
            archive.append_path(entry.path()).map_err(|err| {
                anyhow!("Cannot append file to tarfile: {}", err)
            })?;
        }
        archive
            .finish()
            .map_err(|err| anyhow!("Failed to finalize archive: {}", err))?;

        // Once we've created the archive, acquire a digest which can
        // later be used for verification.
        let digest = sha256_digest(&tarfile)?;
        digests.insert(package.name().into(), digest.as_ref().into());
    }

    let toml = toml::to_string(&digests)?;
    std::fs::write(output_directory.join("digest.toml"), &toml)?;

    Ok(())
}
