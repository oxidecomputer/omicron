// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::collections::HashMap;

use anyhow::Context;
use anyhow::Result;
use camino::Utf8PathBuf;
use fs_err::tokio as fs;
use futures::future::TryFutureExt;
use semver::Version;
use serde::Deserialize;
use slog::Logger;
use slog::warn;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;
use tufaceous_lib::assemble::DeserializedArtifactData;
use tufaceous_lib::assemble::DeserializedArtifactSource;
use tufaceous_lib::assemble::DeserializedFileArtifactSource;
use tufaceous_lib::assemble::DeserializedManifest;

use crate::RETRY_ATTEMPTS;

pub(crate) async fn fetch_hubris_artifacts(
    logger: Logger,
    base_url: &'static str,
    client: reqwest::Client,
    manifest_list: Utf8PathBuf,
    output_dir: Utf8PathBuf,
    name_check: &'static str,
) -> Result<()> {
    macro_rules! zip {
        ($expr:expr) => {
            output_dir.join(format!("{}.zip", $expr))
        };
    }

    fs::create_dir_all(&output_dir).await?;

    // This could be parallelized with FuturesUnordered but in practice this
    // takes less time than OS builds.

    let mut manifest = DeserializedManifest {
        system_version: Version::new(0, 0, 0),
        artifacts: BTreeMap::new(),
    };

    for line in fs::read_to_string(manifest_list).await?.lines() {
        if let Some(hash) = line.split_whitespace().next() {
            let data = fetch_hash(&logger, base_url, &client, hash).await?;
            let str = String::from_utf8(data).with_context(|| {
                format!("hubris artifact manifest {} was not UTF-8", hash)
            })?;
            let hash_manifest: Manifest =
                toml::from_str(&str).with_context(|| {
                    format!(
                        "failed to deserialize hubris artifact manifest {}",
                        hash
                    )
                })?;
            for (kind, artifacts) in hash_manifest.artifacts {
                for artifact in artifacts {
                    let (source, hashes) = match artifact.source {
                        Source::File(file) => (
                            DeserializedArtifactSource::File {
                                path: zip!(file.hash),
                            },
                            vec![file.hash],
                        ),
                        Source::CompositeRot { archive_a, archive_b } => (
                            DeserializedArtifactSource::CompositeRot {
                                archive_a:
                                    DeserializedFileArtifactSource::File {
                                        path: zip!(archive_a.hash),
                                    },
                                archive_b:
                                    DeserializedFileArtifactSource::File {
                                        path: zip!(archive_b.hash),
                                    },
                            },
                            vec![archive_a.hash, archive_b.hash],
                        ),
                    };
                    manifest.artifacts.entry(kind).or_default().push(
                        DeserializedArtifactData {
                            name: artifact.name.clone(),
                            version: artifact
                                .version
                                .to_string()
                                .parse::<ArtifactVersion>()
                                .with_context(|| {
                                    format!(
                                        "artifact {} has invalid version: {}",
                                        artifact.name, artifact.version
                                    )
                                })?,
                            source,
                        },
                    );
                    for hash in hashes {
                        let data =
                            fetch_hash(&logger, base_url, &client, &hash)
                                .await?;
                        fs::write(output_dir.join(zip!(hash)), data).await?;
                    }
                }
            }
        }
    }

    workaround_check(&manifest, name_check)?;
    fs::write(
        output_dir.join("manifest.toml"),
        toml::to_string_pretty(&manifest)?.into_bytes(),
    )
    .await?;
    Ok(())
}

// omicron#9437 fixed an issue where an incorrect RoT image could be chosen.
// Because the RoT is upgraded before any omicron components, the risk is
// the incorrect RoT gets chosen and blocks the update with the fix in it.
// For the purposes of working around the bug and making sure we can deploy
// the fix, ensure our images are in the "correct" order
fn workaround_check(
    manifest: &DeserializedManifest,
    name_check: &'static str,
) -> Result<()> {
    // This bug only affects `gimlet_rot` and `gimlet_rot_bootloader`
    // We search for these by name. This is ugly but temporary.
    let gimlet_artifacts =
        manifest.artifacts.get(&KnownArtifactKind::GimletRot).unwrap();

    let gimlet = gimlet_artifacts
        .iter()
        .position(|x| x.name == format!("oxide-rot-1-{}-gimlet", name_check))
        .expect("failed to find gimlet");

    let cosmo = gimlet_artifacts
        .iter()
        .position(|x| x.name == format!("oxide-rot-1-{}-cosmo", name_check))
        .expect("failed to find cosmo");

    // We need the indicies to be relative for each key set
    if gimlet > cosmo {
        anyhow::bail!(
            "The gimlet entry for `GimletRot` comes after cosmo. This may cause problems."
        );
    }

    let gimlet_artifacts = manifest
        .artifacts
        .get(&KnownArtifactKind::GimletRotBootloader)
        .unwrap();

    let gimlet = gimlet_artifacts
        .iter()
        .position(|x| {
            x.name == format!("gimlet_rot_bootloader-{}-gimlet", name_check)
        })
        .expect("failed to find gimlet");

    let cosmo = gimlet_artifacts
        .iter()
        .position(|x| {
            x.name == format!("gimlet_rot_bootloader-{}-cosmo", name_check)
        })
        .expect("failed to find cosmo");

    // We need the indicies to be relative for each key set
    if gimlet > cosmo {
        anyhow::bail!(
            "The gimlet entry for `GimletRotBootloader` comes after cosmo. This may cause problems."
        );
    }

    Ok(())
}

async fn fetch_hash(
    logger: &Logger,
    base_url: &'static str,
    client: &reqwest::Client,
    hash: &str,
) -> Result<Vec<u8>> {
    let url = format!("{}/artifact/{}", base_url, hash);
    for attempt in 1..=RETRY_ATTEMPTS {
        let result = client
            .get(&url)
            .send()
            .and_then(|response| {
                futures::future::ready(response.error_for_status())
            })
            .and_then(|response| response.json())
            .await
            .with_context(|| {
                format!(
                    "failed to fetch hubris artifact {} from {}",
                    hash, base_url
                )
            });
        match result {
            Ok(data) => return Ok(data),
            Err(err) => {
                if attempt == RETRY_ATTEMPTS {
                    return Err(err);
                } else {
                    warn!(logger, "fetching {} failed, retrying: {}", url, err);
                }
            }
        }
    }
    unreachable!();
}

// These structs are similar to `DeserializeManifest` and friends from
// tufaceous-lib, except that the source is a hash instead of a file path. This
// hash is used to download the artifact from Permission Slip.
#[derive(Deserialize)]
struct Manifest {
    #[serde(rename = "artifact")]
    artifacts: HashMap<KnownArtifactKind, Vec<Artifact>>,
}

#[derive(Deserialize)]
struct Artifact {
    name: String,
    version: Version,
    source: Source,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum Source {
    File(FileSource),
    CompositeRot { archive_a: FileSource, archive_b: FileSource },
}

#[derive(Deserialize)]
struct FileSource {
    hash: String,
}
