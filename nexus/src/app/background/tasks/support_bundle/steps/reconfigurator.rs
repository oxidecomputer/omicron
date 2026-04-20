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
    let (log, opctx, datastore) =
        (collection.log(), collection.opctx(), collection.datastore());

    if !collection.data_selection().contains_reconfigurator() {
        return Ok(CollectionStepOutput::Skipped);
    }

    const NMAX_BLUEPRINTS: usize = 300;
    let state = tokio::select! {
        _ = collection.cancelled() => return Ok(CollectionStepOutput::None),
        result = reconfigurator_state_load(&opctx, &datastore, NMAX_BLUEPRINTS) => {
            match result {
                Ok(state) => state,
                Err(err) => {
                    warn!(
                        log,
                        "Support bundle: failed to collect reconfigurator state";
                        "err" => ?err,
                    );
                    return Ok(CollectionStepOutput::None);
                }
            }
        }
    };

    // serde_json's serialization API is synchronous, so use
    // spawn_blocking to avoid blocking the async runtime.
    let file_path = dir.join("reconfigurator_state.json");
    let target_blueprint = state.target_blueprint;
    let num_blueprints = state.blueprints.len();
    let num_collections = state.collections.len();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .with_context(|| format!("failed to open {}", file_path))?;
        serde_json::to_writer_pretty(&file, &state).with_context(|| {
            format!("failed to serialize reconfigurator state to {}", file_path)
        })
    })
    .await??;
    info!(
        log,
        "Support bundle: collected reconfigurator state";
        "target_blueprint" => ?target_blueprint,
        "num_blueprints" => num_blueprints,
        "num_collections" => num_collections,
    );

    Ok(CollectionStepOutput::None)
}
