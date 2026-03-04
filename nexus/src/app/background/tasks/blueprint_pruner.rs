// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pruning old blueprints

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::BlueprintPrunerDetails;
use nexus_types::internal_api::background::BlueprintPrunerStatus;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

/// Background task that prunes old blueprints from the database
pub struct BlueprintPruner {
    datastore: Arc<DataStore>,
}

impl BlueprintPruner {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for BlueprintPruner {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            match blueprint_prune(opctx, &self.datastore).await {
                Ok(status) => match serde_json::to_value(status) {
                    Ok(val) => val,
                    Err(err) => json!({
                        "error": format!(
                            "could not serialize task status: {}",
                            InlineErrorChain::new(&err)
                        ),
                    }),
                },
                Err(error) => json!({
                    "error": InlineErrorChain::new(&*error).to_string(),
                }),
            }
        })
    }
}

async fn blueprint_prune(
    _opctx: &OpContext,
    _datastore: &DataStore,
) -> Result<BlueprintPrunerStatus, anyhow::Error> {
    // TODO: implement blueprint pruning logic
    Ok(BlueprintPrunerStatus::Enabled(BlueprintPrunerDetails {
        deleted: vec![],
        nkept: 0,
        warnings: vec![],
    }))
}
