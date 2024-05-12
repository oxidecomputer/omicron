// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

use anyhow::Context;
use anyhow::Result;
use camino::Utf8PathBuf;
use fs_err::tokio as fs;
use fs_err::tokio::File;
use futures::future::TryFutureExt;
use futures::stream::FuturesUnordered;
use futures::stream::TryStreamExt;
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tufaceous_lib::assemble::DeserializedArtifactData;
use tufaceous_lib::assemble::DeserializedArtifactSource;
use tufaceous_lib::assemble::DeserializedFileArtifactSource;

async fn fetch_one(
    base_url: &'static str,
    client: reqwest::Client,
    hash: &str,
) -> Result<Vec<u8>> {
    client
        .get(format!("{}/artifact/{}", base_url, hash))
        .send()
        .and_then(|response| response.json())
        .await
        .with_context(|| {
            format!(
                "failed to fetch hubris artifact {} from {}",
                hash, base_url
            )
        })
}

pub(crate) async fn fetch_hubris_artifacts(
    base_url: &'static str,
    client: reqwest::Client,
    manifest_list: Utf8PathBuf,
    output_dir: Utf8PathBuf,
) -> Result<()> {
    fs::create_dir_all(&output_dir).await?;

    let (manifests, hashes) = fs::read_to_string(manifest_list)
        .await?
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(|hash| {
            let hash = hash.to_owned();
            let client = client.clone();
            async move {
                let data = fetch_one(base_url, client, &hash).await?;
                let str = String::from_utf8(data)
                    .context("hubris artifact manifest was not UTF-8")?;
                let hash_manifest: Manifest<Artifact> = toml::from_str(&str)
                    .context(
                        "failed to deserialize hubris artifact manifest",
                    )?;

                let mut hashes = Vec::new();
                for artifact in hash_manifest.artifacts.values().flatten() {
                    match &artifact.source {
                        Source::File(file) => hashes.push(file.hash.clone()),
                        Source::CompositeRot { archive_a, archive_b } => hashes
                            .extend([
                                archive_a.hash.clone(),
                                archive_b.hash.clone(),
                            ]),
                    }
                }

                let path_manifest: Manifest<DeserializedArtifactData> =
                    hash_manifest.into();
                anyhow::Ok((path_manifest, hashes))
            }
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<(Vec<_>, Vec<_>)>()
        .await?;

    let mut output_manifest =
        File::create(output_dir.join("manifest.toml")).await?;
    for manifest in manifests {
        output_manifest
            .write_all(toml::to_string_pretty(&manifest)?.as_bytes())
            .await?;
    }

    hashes
        .into_iter()
        .flatten()
        .map(|hash| {
            let client = client.clone();
            let output_dir = output_dir.clone();
            async move {
                let data = fetch_one(base_url, client, &hash).await?;
                fs::write(output_dir.join(hash).with_extension("zip"), data)
                    .await?;
                anyhow::Ok(())
            }
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<()>()
        .await?;

    output_manifest.sync_data().await?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Manifest<T> {
    #[serde(rename = "artifact")]
    artifacts: HashMap<KnownArtifactKind, Vec<T>>,
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

impl From<Manifest<Artifact>> for Manifest<DeserializedArtifactData> {
    fn from(
        manifest: Manifest<Artifact>,
    ) -> Manifest<DeserializedArtifactData> {
        fn zip(hash: String) -> Utf8PathBuf {
            Utf8PathBuf::from(hash).with_extension("zip")
        }

        let mut artifacts = HashMap::new();
        for (kind, old_data) in manifest.artifacts {
            let mut new_data = Vec::new();
            for artifact in old_data {
                let source = match artifact.source {
                    Source::File(file) => DeserializedArtifactSource::File {
                        path: zip(file.hash),
                    },
                    Source::CompositeRot { archive_a, archive_b } => {
                        DeserializedArtifactSource::CompositeRot {
                            archive_a: DeserializedFileArtifactSource::File {
                                path: zip(archive_a.hash),
                            },
                            archive_b: DeserializedFileArtifactSource::File {
                                path: zip(archive_b.hash),
                            },
                        }
                    }
                };
                new_data.push(DeserializedArtifactData {
                    name: artifact.name,
                    version: SemverVersion(artifact.version),
                    source,
                });
            }
            artifacts.insert(kind, new_data);
        }

        Manifest { artifacts }
    }
}
