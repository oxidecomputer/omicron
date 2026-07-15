// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, bail};
use camino::{Utf8DirEntry, Utf8Path};
use clap::{Parser, Subcommand};
use iddqd::IdOrdMap;
use omicron_common::update::{OmicronInstallManifest, OmicronInstallMetadata};
use sha2::{Digest, Sha256};
use std::fs::File;
use tufaceous_artifact_v2::ArtifactHash;
use uuid::Uuid;

/// Return a UUID if the `DirEntry` contains a directory that parses into a UUID.
fn get_uuid_dir(result: std::io::Result<Utf8DirEntry>) -> Option<Uuid> {
    let Ok(entry) = result else {
        return None;
    };
    let Ok(file_type) = entry.file_type() else {
        return None;
    };
    if !file_type.is_dir() {
        return None;
    }
    let file_name = entry.file_name();
    file_name.parse().ok()
}

#[derive(Debug)]
pub struct Pools {
    pub internal: Vec<Uuid>,
    pub external: Vec<Uuid>,
}

impl Pools {
    pub fn read() -> anyhow::Result<Pools> {
        let internal = Utf8Path::new("/pool/int/")
            .read_dir_utf8()
            .context("Failed to read /pool/int")?
            .filter_map(get_uuid_dir)
            .collect();
        let external = Utf8Path::new("/pool/ext/")
            .read_dir_utf8()
            .context("Failed to read /pool/ext")?
            .filter_map(get_uuid_dir)
            .collect();
        Ok(Pools { internal, external })
    }
}

#[derive(Subcommand)]
enum CorpusCommand {
    /// Verify that all corpus files are present and accounted for
    /// on all disks
    Check,
    /// Regenerate the `measurements.json` file on all disks. This will
    /// be saved as `measurements-regenerated.json`. It is up to you to
    /// do the replacement!
    Regenerate,
}

/// Adjust the `measurement.json` in an install dataset
#[derive(Parser)]
struct CorpusAdjust {
    #[clap(subcommand)]
    command: CorpusCommand,
}

fn measurement_names(
    dir: &Utf8Path,
) -> anyhow::Result<IdOrdMap<OmicronInstallMetadata>> {
    if !dir.is_dir() {
        bail!("Not a directory");
    }

    let mut files = IdOrdMap::new();

    for entry in dir.read_dir_utf8()? {
        let entry = entry?;

        if !entry.file_name().contains("measurements.json") {
            continue;
        }

        let mut hasher = Sha256::new();
        std::io::copy(&mut File::open(&entry.path())?, &mut hasher)?;

        let meta = entry.metadata()?;

        let entry = OmicronInstallMetadata {
            file_name: entry.file_name().to_owned(),
            file_size: meta.len(),
            hash: ArtifactHash(hasher.finalize().into()),
        };

        files
            .insert_unique(entry)
            .map_err(|_| anyhow::anyhow!("duplicate?"))?;
    }

    Ok(files)
}

fn main() -> anyhow::Result<()> {
    let arg = CorpusAdjust::parse();

    match arg.command {
        CorpusCommand::Check => {
            let pools = Pools::read()?;

            let all = pools.internal.into_iter().map(|u| {
                Utf8Path::new("/pool/int")
                    .join(format!("{u}"))
                    .join("install")
                    .join("measurements")
            });
            for p in all {
                let mut names = measurement_names(&p)?;

                let f = p.join("measurements.json");
                let bytes = std::fs::read(f)?;

                let manifest =
                    serde_json::from_slice::<OmicronInstallManifest>(&bytes)?;

                for f in manifest.files {
                    if let Some(entry) = names.remove(f.file_name.as_str()) {
                        if entry != f {
                            bail!(
                                "file {} in manifest does not match disk {p}",
                                f.file_name
                            );
                        }
                    } else {
                        bail!(
                            "file {} in manifest but not found on disk {p}",
                            f.file_name
                        );
                    }
                }

                if !names.is_empty() {
                    println!("some files were not in the manifest {p}:");
                    for n in names {
                        println!("{}", n.file_name);
                    }
                    bail!("Fix your manifest");
                }
            }
        }
        CorpusCommand::Regenerate => {
            let pools = Pools::read()?;

            let all = pools.internal.into_iter().map(|u| {
                Utf8Path::new("/pool/int")
                    .join(format!("{u}"))
                    .join("install")
                    .join("measurements")
            });
            for p in all {
                let names = measurement_names(&p)?;

                let f = p.join("measurements.json");
                let bytes = std::fs::read(f)?;

                let manifest =
                    serde_json::from_slice::<OmicronInstallManifest>(&bytes)?;

                let updated = OmicronInstallManifest {
                    source: manifest.source,
                    files: names,
                };

                let json = serde_json::to_vec(&updated)?;

                std::fs::write(p.join("measurements-regenerated.json"), &json)?;
            }

            println!("done");
        }
    }

    Ok(())
}
