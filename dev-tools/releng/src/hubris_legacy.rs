// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::ErrorKind;

use anyhow::Context as _;
use anyhow::Result;
use anyhow::ensure;
use camino::Utf8PathBuf;
use fs_err::tokio as fs;
use futures::future::TryFutureExt;
use semver::Version;
use serde::Deserialize;
use serde::Deserializer;
use slog::Logger;
use slog::warn;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;
use tufaceous_lib::assemble::DeserializedArtifactData;
use tufaceous_lib::assemble::DeserializedArtifactSource;
use tufaceous_lib::assemble::DeserializedFileArtifactSource;
use tufaceous_lib::assemble::DeserializedManifest;

use crate::RETRY_ATTEMPTS;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Environment {
    pub(crate) full_name: &'static str,
    pub(crate) short_name: &'static str,
    pub(crate) base_url: &'static str,
}

impl Environment {
    pub(crate) const ALL: [Environment; 2] = [
        Environment {
            full_name: "staging-devel",
            short_name: "staging",
            base_url: "https://permslip-staging.corp.oxide.computer",
        },
        Environment {
            full_name: "production-release",
            short_name: "production",
            base_url: "https://signer-us-west.corp.oxide.computer",
        },
    ];
}

pub(crate) async fn fetch_hubris_artifacts(
    logger: Logger,
    env: Environment,
    client: reqwest::Client,
    output_dir: Utf8PathBuf,
) -> Result<()> {
    let output_dir = output_dir.join(format!("hubris-{}", env.short_name));
    let ctx = Context { logger, env, client, output_dir };
    fs::create_dir_all(&ctx.output_dir).await?;

    // This could be parallelized with FuturesUnordered but in practice this
    // takes less time than OS builds.

    let mut manifest = DeserializedManifest {
        system_version: Version::new(0, 0, 0),
        artifacts: BTreeMap::new(),
    };

    let manifest_path = crate::WORKSPACE_DIR
        .join(format!("tools/permslip_{}", ctx.env.short_name));
    for line in fs::read_to_string(manifest_path).await?.lines() {
        let Some(hash) = line.split_whitespace().next() else { continue };
        let data = ctx.fetch_hash(hash.parse()?, "toml").await?.data;
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
        for (kind, artifacts) in hash_manifest.artifact {
            for artifact in artifacts {
                let source = match artifact.source {
                    Source::File(file) => {
                        let path = ctx.fetch_hash(file.hash, "zip").await?.path;
                        DeserializedArtifactSource::File { path }
                    }
                    Source::CompositeRot { archive_a, archive_b } => {
                        let path_a =
                            ctx.fetch_hash(archive_a.hash, "zip").await?.path;
                        let path_b =
                            ctx.fetch_hash(archive_b.hash, "zip").await?.path;
                        DeserializedArtifactSource::CompositeRot {
                            archive_a: DeserializedFileArtifactSource::File {
                                path: path_a,
                            },
                            archive_b: DeserializedFileArtifactSource::File {
                                path: path_b,
                            },
                        }
                    }
                };
                manifest.artifacts.entry(kind).or_default().push(
                    DeserializedArtifactData {
                        name: artifact.name,
                        version: artifact.version,
                        source,
                    },
                );
            }
        }
        if let Some(FileSource { hash }) = hash_manifest.measurement_corpus {
            let Output { data, path } = ctx.fetch_hash(hash, "corim").await?;
            let corim = rats_corim::Corim::from_bytes(&data)?;
            manifest
                .artifacts
                .entry(KnownArtifactKind::MeasurementCorpus)
                .or_default()
                .push(DeserializedArtifactData {
                    name: format!("{}-{}", ctx.env.short_name, corim.id),
                    version: corim.get_version()?.parse()?,
                    source: DeserializedArtifactSource::File { path },
                });
        }
    }

    workaround_check(&manifest, ctx.env.full_name)?;
    fs::write(
        ctx.output_dir.join("manifest.toml"),
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
    env_name: &'static str,
) -> Result<()> {
    // This bug only affects `gimlet_rot` and `gimlet_rot_bootloader`
    // We search for these by name. This is ugly but temporary.
    let gimlet_artifacts =
        manifest.artifacts.get(&KnownArtifactKind::GimletRot).unwrap();

    let gimlet = gimlet_artifacts
        .iter()
        .position(|x| x.name == format!("oxide-rot-1-{env_name}-gimlet"))
        .expect("failed to find gimlet");

    let cosmo = gimlet_artifacts
        .iter()
        .position(|x| x.name == format!("oxide-rot-1-{env_name}-cosmo"))
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
            x.name == format!("gimlet_rot_bootloader-{env_name}-gimlet")
        })
        .expect("failed to find gimlet");

    let cosmo = gimlet_artifacts
        .iter()
        .position(|x| {
            x.name == format!("gimlet_rot_bootloader-{env_name}-cosmo")
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

struct Context {
    logger: Logger,
    env: Environment,
    client: reqwest::Client,
    output_dir: Utf8PathBuf,
}

struct Output {
    data: Vec<u8>,
    path: Utf8PathBuf,
}

impl Context {
    async fn fetch_hash(
        &self,
        hash: blake3::Hash,
        extension: &str,
    ) -> Result<Output> {
        let path =
            self.output_dir.join(hash.to_string()).with_extension(extension);
        match fs::read(&path).await {
            Ok(data) => {
                let cached_hash = blake3::hash(&data);
                if hash == cached_hash {
                    return Ok(Output { data, path });
                }
                warn!(
                    self.logger,
                    "cached hash mismatch: {path} has hash {cached_hash}"
                );
                fs::remove_file(&path).await?;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }

        let url = format!("{}/artifact/{}", self.env.base_url, hash);
        for attempt in 1..=RETRY_ATTEMPTS {
            let result = self
                .client
                .get(&url)
                .send()
                .and_then(|response| {
                    futures::future::ready(response.error_for_status())
                })
                .and_then(|response| response.json::<Vec<u8>>())
                .map_err(anyhow::Error::from)
                .and_then(async |data| {
                    let fetched_hash = blake3::hash(&data);
                    ensure!(
                        hash == fetched_hash,
                        "hash mismatch: expected {hash}, fetched {fetched_hash}"
                    );
                    Ok(data)
                })
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch hubris artifact {} from {}",
                        hash, self.env.base_url
                    )
                });
            match result {
                Ok(data) => {
                    fs::write(&path, &data).await?;
                    return Ok(Output { data, path });
                }
                Err(err) => {
                    if attempt == RETRY_ATTEMPTS {
                        return Err(err);
                    } else {
                        warn!(
                            self.logger,
                            "fetching {} failed, retrying: {}", url, err
                        );
                    }
                }
            }
        }
        unreachable!();
    }
}

// These structs are similar to `DeserializeManifest` and friends from
// tufaceous-lib, except that the source is a hash instead of a file path. This
// hash is used to download the artifact from Permission Slip.
#[derive(Deserialize)]
struct Manifest {
    artifact: HashMap<KnownArtifactKind, Vec<Artifact>>,
    // Add a default for backwards compatibility
    measurement_corpus: Option<FileSource>,
}

#[derive(Deserialize)]
struct Artifact {
    name: String,
    version: ArtifactVersion,
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
    #[serde(deserialize_with = "deserialize_hash")]
    hash: blake3::Hash,
}

fn deserialize_hash<'de, D>(de: D) -> Result<blake3::Hash, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(de)?.parse().map_err(serde::de::Error::custom)
}
