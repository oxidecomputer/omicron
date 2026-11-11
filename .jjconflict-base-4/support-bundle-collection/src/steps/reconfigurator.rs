// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect reconfigurator state for support bundles

use crate::collection::BundleCollection;
use crate::output::BundleOutput;
use crate::step::CollectionStepOutput;
use crate::step::StepError;

use anyhow::Context;
use nexus_reconfigurator_preparation::reconfigurator_state_load;
use slog::info;
use slog::warn;

pub async fn collect(
    collection: &BundleCollection,
    output: &BundleOutput,
) -> Result<CollectionStepOutput, StepError> {
    let (log, opctx, datastore) =
        (collection.log(), collection.opctx(), collection.datastore());

    if !collection.data_selection().contains_reconfigurator() {
        return Ok(CollectionStepOutput::Skipped);
    }

    const NMAX_BLUEPRINTS: usize = 300;
    let state = tokio::select! {
        _ = collection.stopped() => return Err(StepError::Stopped),
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

    // serde_json's serialization API is synchronous, so `write_with` runs it
    // on a blocking thread, to avoid blocking the async runtime.
    let target_blueprint = state.target_blueprint;
    let num_blueprints = state.blueprints.len();
    let num_collections = state.collections.len();
    output
        .write_with("reconfigurator_state.json", move |w| {
            serde_json::to_writer_pretty(w, &state)
                .context("failed to serialize reconfigurator state")
        })
        .await?;
    info!(
        log,
        "Support bundle: collected reconfigurator state";
        "target_blueprint" => ?target_blueprint,
        "num_blueprints" => num_blueprints,
        "num_collections" => num_collections,
    );

    Ok(CollectionStepOutput::None)
}
