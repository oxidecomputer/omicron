// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that fills in the Region record's port if it is missing.
//!
//! Originally the Region model did not contain the port that the Agent selected
//! for it, and this wasn't required early on. However, that column was added,
//! and this background task is responsible for filling in Regions that don't
//! have a recorded port.

use crate::app::background::BackgroundTask;
use anyhow::Result;
use crucible_agent_client::types::Region;
use crucible_agent_client::types::RegionId;
use crucible_agent_client::Client as CrucibleAgentClient;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::LookupRegionPortStatus;
use serde_json::json;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

pub struct LookupRegionPort {
    datastore: Arc<DataStore>,
}

impl LookupRegionPort {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        LookupRegionPort { datastore }
    }
}

async fn get_region_from_agent(
    agent_address: &SocketAddrV6,
    region_id: Uuid,
) -> Result<Region> {
    let url = format!("http://{}", agent_address);
    let client = CrucibleAgentClient::new(&url);

    let result = client.region_get(&RegionId(region_id.to_string())).await?;

    Ok(result.into_inner())
}

impl BackgroundTask for LookupRegionPort {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            info!(&log, "lookup region port task started");

            let mut status = LookupRegionPortStatus::default();

            let regions_missing_ports =
                match self.datastore.regions_missing_ports(opctx).await {
                    Ok(regions) => regions,

                    Err(e) => {
                        let s = format!(
                            "could not find regions missing ports: {e}"
                        );

                        error!(log, "{s}");
                        status.errors.push(s);

                        return json!(status);
                    }
                };

            for region in regions_missing_ports {
                let dataset_id = region.dataset_id();

                let dataset = match self.datastore.dataset_get(dataset_id).await
                {
                    Ok(dataset) => dataset,

                    Err(e) => {
                        let s =
                            format!("could not get dataset {dataset_id}: {e}");

                        error!(log, "{s}");
                        status.errors.push(s);

                        continue;
                    }
                };

                let returned_region = match get_region_from_agent(
                    &dataset.address(),
                    region.id(),
                )
                .await
                {
                    Ok(returned_region) => returned_region,

                    Err(e) => {
                        let s = format!(
                            "could not get region {} from agent: {e}",
                            region.id(),
                        );

                        error!(log, "{s}");
                        status.errors.push(s);

                        continue;
                    }
                };

                match self
                    .datastore
                    .region_set_port(region.id(), returned_region.port_number)
                    .await
                {
                    Ok(()) => {
                        let s = format!(
                            "set region {} port as {}",
                            region.id(),
                            returned_region.port_number,
                        );

                        info!(log, "{s}");
                        status.found_port_ok.push(s);
                    }

                    Err(e) => {
                        let s = format!(
                            "could not set region {} port: {e}",
                            region.id(),
                        );

                        error!(log, "{s}");
                        status.errors.push(s);
                    }
                }
            }

            info!(&log, "lookup region port task done");

            json!(status)
        }
        .boxed()
    }
}
