// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collects metadata about the bundle itself (currently only the ID)

use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;
use camino::Utf8Path;

pub async fn collect(
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
