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
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use semver::Version;
use serde::Deserialize;
use slog::warn;
use slog::Logger;
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
        system_version: SemverVersion(Version::new(0, 0, 0)),
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
                            name: artifact.name,
                            version: artifact.version,
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

    fs::write(
        output_dir.join("manifest.toml"),
        toml::to_string_pretty(&manifest)?.into_bytes(),
    )
    .await?;
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
    version: SemverVersion,
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
