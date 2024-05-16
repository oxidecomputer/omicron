// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating VPC routes (system and custom) to sleds.

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::{Sled, SledState};
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_networking::sled_client_from_address;
use nexus_types::{
    deployment::SledFilter, external_api::views::SledPolicy, identity::Asset,
};
use omicron_common::api::{external::Vni, internal::shared::RouterId};
use serde_json::json;
use sled_agent_client::types::ReifiedVpcRoute;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct VpcRouteManager {
    datastore: Arc<DataStore>,
}

impl VpcRouteManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for VpcRouteManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            // XX: copied from omicron#5566
            let sleds = match self
                .datastore
                .sled_list_all_batched(opctx, SledFilter::InService)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("failed to enumerate sleds: {:#}", e);
                    error!(&log, "{msg}");
                    return json!({"error": msg});
                }
            }
            .into_iter()
            .filter(|sled| {
                matches!(sled.state(), SledState::Active)
                    && matches!(sled.policy(), SledPolicy::InService { .. })
            });

            // Map sled db records to sled-agent clients
            let sled_clients: Vec<(Sled, sled_agent_client::Client)> = sleds
                .map(|sled| {
                    let client = sled_client_from_address(
                        sled.id(),
                        sled.address(),
                        &log,
                    );
                    (sled, client)
                })
                .collect();

            // XX: actually reify rules.
            let mut known_rules: HashMap<RouterId, HashSet<ReifiedVpcRoute>> =
                HashMap::new();

            for (sled, client) in sled_clients {
                let Ok(a) = client.list_vpc_routes().await else {
                    warn!(
                        log,
                        "failed to fetch current VPC route state from sled";
                        "sled" => sled.serial_number(),
                    );
                    continue;
                };

                // XX: Who decides what we want? Do we figure out the NICs
                //     here? Or take the sled at their word for what subnets
                //     they want?

                todo!()
            }

            json!({})
        }
        .boxed()
    }
}
