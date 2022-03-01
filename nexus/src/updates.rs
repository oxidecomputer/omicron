// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db;
use parse_display::Display;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;

// Simple metadata base URL + targets base URL pair, useful in several places.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct BaseUrlPair {
    /// The metadata base URL.
    pub metadata: String,
    /// The targets base URL.
    pub targets: String,
}

// Schema for the `artifacts.json` target in the TUF update repository.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArtifactsDocument {
    pub artifacts: Vec<UpdateArtifact>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UpdateArtifact {
    pub name: String,
    pub version: i64,
    // FIXME(iliana): this needs to have a fallback or else we can never add a new kind of artifact
    // in production!!
    pub kind: UpdateArtifactKind,
    pub target: String,
}

#[derive(Clone, Debug, Display, Deserialize, Serialize)]
#[display(style = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum UpdateArtifactKind {
    Zone,
}

// TODO(iliana): make async/.await. awslabs/tough#213
pub fn read_artifacts(
    trusted_root: &[u8],
    base_urls: BaseUrlPair,
) -> Result<
    Vec<db::model::UpdateAvailableArtifact>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    use std::io::Read;

    let repository = tough::RepositoryLoader::new(
        trusted_root,
        base_urls.metadata.parse()?,
        base_urls.targets.parse()?,
    )
    .load()?;

    let mut artifact_document = Vec::new();
    match repository.read_target(&"artifacts.json".parse()?)? {
        Some(mut target) => target.read_to_end(&mut artifact_document)?,
        None => return Err("artifacts.json missing".into()),
    };
    let artifacts: ArtifactsDocument =
        serde_json::from_slice(&artifact_document)?;

    let valid_until = repository
        .root()
        .signed
        .expires
        .min(repository.snapshot().signed.expires)
        .min(repository.targets().signed.expires)
        .min(repository.timestamp().signed.expires);

    let mut v = Vec::new();
    for artifact in artifacts.artifacts {
        if let Some(target) =
            repository.targets().signed.targets.get(&artifact.target.parse()?)
        {
            v.push(db::model::UpdateAvailableArtifact {
                name: artifact.name,
                version: artifact.version,
                kind: db::model::UpdateArtifactKind(artifact.kind),
                targets_role_version: repository
                    .targets()
                    .signed
                    .version
                    .get()
                    .try_into()?,
                valid_until,
                target_name: artifact.target,
                target_sha256: hex::encode(&target.hashes.sha256),
                target_length: target.length.try_into()?,
            });
        }
    }
    Ok(v)
}
