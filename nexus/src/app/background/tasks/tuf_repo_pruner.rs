// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for determining when to prune artifacts from TUF repos

use super::reconfigurator_config::ReconfiguratorConfigLoaderState;
use crate::app::background::BackgroundTask;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_auth::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::PlanningReport;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use nexus_types::internal_api::background::BlueprintPlannerStatus;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid as _;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch::{self, Receiver, Sender};

// XXX-dap
#[derive(Serialize, JsonSchema)]
struct TufRepoPrunerStatus;

/// Background task that marks TUF repos for pruning
pub struct TufRepoPruner {
    datastore: Arc<DataStore>,
}

impl TufRepoPruner {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    fn tuf_repos_prune(&self, opctx: &OpContext) -> TufRepoPrunerStatus {
        todo!();
    }
}

impl BackgroundTask for TufRepoPruner {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let status = self.tuf_repos_prune(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => json!({
                    "error": format!(
                        "could not serialize task status: {}",
                         InlineErrorChain::new(&err)
                    ),
                }),
            }
        })
    }
}
