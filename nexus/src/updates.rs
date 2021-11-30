use crate::db;
use parse_display::Display;
use serde::Deserialize;
use std::convert::TryInto;

// Schema for the `artifacts.json` target in the TUF update repository.
#[derive(Clone, Debug, Deserialize)]
pub struct ArtifactsDocument {
    pub artifacts: Vec<UpdateArtifact>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpdateArtifact {
    pub name: String,
    pub version: i64,
    pub kind: UpdateArtifactKind,
    pub target: String,
}

#[derive(Clone, Debug, Display, Deserialize)]
#[display(style = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum UpdateArtifactKind {
    Zone,
}

// TODO(iliana): make async/.await. awslabs/tough#213
pub fn read_artifacts(
    rack: &db::model::Rack,
    tuf_trusted_root: &[u8],
) -> Result<
    Vec<db::model::UpdateAvailableArtifact>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    use std::io::Read;

    let repository = tough::RepositoryLoader::new(
        tuf_trusted_root,
        rack.tuf_metadata_base_url.parse()?,
        rack.tuf_targets_base_url.parse()?,
    )
    .load()?;

    let mut artifact_document = Vec::new();
    match repository.read_target(&"artifacts.json".parse()?)? {
        Some(mut target) => target.read_to_end(&mut artifact_document)?,
        None => return Err("artifacts.json missing".into()),
    };
    let artifacts: ArtifactsDocument =
        serde_json::from_slice(&artifact_document)?;

    let earliest_expiration = repository
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
                targets_version: repository
                    .targets()
                    .signed
                    .version
                    .get()
                    .try_into()?,
                metadata_expiration: earliest_expiration,
                target_name: artifact.target,
                target_sha256: hex::encode(&target.hashes.sha256),
                target_length: target.length.try_into()?,
            });
        }
    }
    Ok(v)
}
