use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_networking::{sled_client, sled_client_from_address};
use nexus_types::{external_api::views::Sled, identity::Asset};
use omicron_common::api::external::DataPageParams;
use serde_json::json;

use super::common::BackgroundTask;

pub struct V2PManager {
    datastore: Arc<DataStore>,
}

impl V2PManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for V2PManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            // Get ids of active / available sleds
            let log = opctx.log.clone();
            let sleds = match self
                .datastore
                .sled_list(opctx, &DataPageParams::max_page())
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("failed enumerate sleds: {:#}", e);
                    error!(&log, "{msg}");
                    return json!({"error": msg});
                }
            };

            // Map ids of sleds to sled clients, skip client on failure
            let sled_clients: Vec<_> = sleds
                .into_iter()
                .map(|sled| {
                    let client = sled_client_from_address(
                        sled.id(),
                        sled.address(),
                        &log,
                    );
                    (sled, client)
                })
                .collect();

            // sled_client_from_address(sled_id, address, log)

            // Get all active instance vnics

            // for each sled client, send set of expected v2p mappings
            json!({})
        }
    }
}
