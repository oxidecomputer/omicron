// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collects metadata about the bundle itself

use crate::collection::BundleCollection;
use crate::output::BundleOutput;
use crate::step::CollectionStepOutput;

/// Writes the bundle ID to a file
pub async fn collect_bundle_id(
    collection: &BundleCollection,
    output: &BundleOutput,
) -> anyhow::Result<CollectionStepOutput> {
    output.write("bundle_id.txt", collection.bundle().id.to_string()).await?;

    Ok(CollectionStepOutput::None)
}

/// Writes the reason for creation to the meta directory
pub async fn collect_reason_for_creation(
    collection: &BundleCollection,
    output: &BundleOutput,
) -> anyhow::Result<CollectionStepOutput> {
    output
        .write(
            "meta/reason_for_creation.txt",
            &collection.bundle().reason_for_creation,
        )
        .await?;

    Ok(CollectionStepOutput::None)
}
