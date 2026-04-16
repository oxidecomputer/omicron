// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collects metadata about the bundle itself

use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;
use camino::Utf8Path;

/// Writes the bundle ID to a file
pub async fn collect_bundle_id(
    collection: &BundleCollection,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    tokio::fs::write(
        dir.join("bundle_id.txt"),
        collection.bundle().id.to_string(),
    )
    .await?;

    Ok(CollectionStepOutput::None)
}

/// Writes the reason for creation to the meta directory
pub async fn collect_reason_for_creation(
    collection: &BundleCollection,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    let meta_dir = dir.join("meta");
    tokio::fs::create_dir_all(&meta_dir).await?;

    tokio::fs::write(
        meta_dir.join("reason_for_creation.txt"),
        &collection.bundle().reason_for_creation,
    )
    .await?;

    Ok(CollectionStepOutput::None)
}
