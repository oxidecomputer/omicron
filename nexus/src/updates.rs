// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use buf_list::BufList;
use futures::TryStreamExt;
use nexus_db_queries::db;
use omicron_common::update::ArtifactsDocument;
use std::convert::TryInto;

// TODO(iliana): make async/.await. awslabs/tough#213
pub(crate) async fn read_artifacts(
    trusted_root: &[u8],
    mut base_url: String,
) -> Result<
    Vec<db::model::UpdateArtifact>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    if !base_url.ends_with('/') {
        base_url.push('/');
    }

    let repository = tough::RepositoryLoader::new(
        &trusted_root,
        format!("{}metadata/", base_url).parse()?,
        format!("{}targets/", base_url).parse()?,
    )
    .load()
    .await?;

    let artifact_document =
        match repository.read_target(&"artifacts.json".parse()?).await? {
            Some(target) => target.try_collect::<BufList>().await?,
            None => return Err("artifacts.json missing".into()),
        };
    let artifacts: ArtifactsDocument =
        serde_json::from_reader(buf_list::Cursor::new(&artifact_document))?;

    let valid_until = repository
        .root()
        .signed
        .expires
        .min(repository.snapshot().signed.expires)
        .min(repository.targets().signed.expires)
        .min(repository.timestamp().signed.expires);

    let mut v = Vec::new();
    for artifact in artifacts.artifacts {
        // Skip any artifacts where we don't recognize its kind or the target
        // name isn't in the repository
        let target =
            repository.targets().signed.targets.get(&artifact.target.parse()?);
        let (kind, target) = match (artifact.kind.to_known(), target) {
            (Some(kind), Some(target)) => (kind, target),
            _ => break,
        };

        v.push(db::model::UpdateArtifact {
            name: artifact.name,
            version: db::model::SemverVersion(artifact.version),
            kind: db::model::KnownArtifactKind(kind),
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
    Ok(v)
}
