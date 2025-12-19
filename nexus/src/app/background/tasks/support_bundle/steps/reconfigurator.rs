// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect reconfigurator state for support bundles

use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;

use anyhow::Context;
use camino::Utf8Path;
use nexus_reconfigurator_preparation::reconfigurator_state_load;

pub async fn collect(
    collection: &BundleCollection,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    let (log, opctx, datastore, request) = (
        collection.log(),
        collection.opctx(),
        collection.datastore(),
        collection.request(),
    );

    if !request.include_reconfigurator_data() {
        return Ok(CollectionStepOutput::Skipped);
    }

    // Collect reconfigurator state
    const NMAX_BLUEPRINTS: usize = 300;
    match reconfigurator_state_load(&opctx, &datastore, NMAX_BLUEPRINTS).await {
        Ok(state) => {
            let file_path = dir.join("reconfigurator_state.json");
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&file_path)
                .with_context(|| format!("failed to open {}", file_path))?;
            serde_json::to_writer_pretty(&file, &state).with_context(|| {
                format!(
                    "failed to serialize reconfigurator state to {}",
                    file_path
                )
            })?;
            info!(
                log,
                "Support bundle: collected reconfigurator state";
                "target_blueprint" => ?state.target_blueprint,
                "num_blueprints" => state.blueprints.len(),
                "num_collections" => state.collections.len(),
            );
        }
        Err(err) => {
            warn!(
                log,
                "Support bundle: failed to collect reconfigurator state";
                "err" => ?err,
            );
        }
    };

    Ok(CollectionStepOutput::None)
}
