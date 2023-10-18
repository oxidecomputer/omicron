// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reading inventory for the rack

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use internal_dns::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::inventory::Collection;
use serde_json::json;
use std::sync::Arc;

/// Background task that reads inventory for the rack
pub struct InventoryCollector {
    datastore: Arc<DataStore>,
    resolver: internal_dns::resolver::Resolver,
    creator: String,
    nkeep: u32,
}

impl InventoryCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns::resolver::Resolver,
        creator: &str,
        nkeep: u32,
    ) -> InventoryCollector {
        InventoryCollector {
            datastore,
            resolver,
            creator: creator.to_owned(),
            nkeep,
        }
    }
}

impl BackgroundTask for InventoryCollector {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, serde_json::Value>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            match inventory_activate(
                opctx,
                &self.datastore,
                &self.resolver,
                &self.creator,
                self.nkeep,
            )
            .await
            .context("failed to collect inventory")
            {
                Err(error) => {
                    let message = format!("{:#}", error);
                    warn!(opctx.log, "inventory collection failed";
                        "error" => message.clone());
                    json!({ "error": message })
                }
                Ok(collection) => {
                    debug!(opctx.log, "inventory collection complete";
                        "collection_id" => collection.id.to_string(),
                        "time_started" => collection.time_started.to_string(),
                    );
                    json!({
                        "collection_id": collection.id.to_string(),
                        "time_started": collection.time_started.to_string(),
                        "time_done": collection.time_done.to_string()
                    })
                }
            }
        }
        .boxed()
    }
}

async fn inventory_activate(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &internal_dns::resolver::Resolver,
    creator: &str,
    nkeep: u32,
) -> Result<Collection, anyhow::Error> {
    // Prune old collections.  We do this first, here, to ensure that we never
    // develop an unbounded backlog of collections.  (If this process were done
    // by a separate task, it would be possible for the backlog to grow
    // unbounded if that task were simply slower than the collection process,
    // let alone if there were some kind of extended operational issue
    // blocking deletion.)
    datastore
        .inventory_prune_collections(opctx, nkeep)
        .await
        .context("pruning old collections")?;

    // Find MGS clients.
    let mgs_clients = resolver
        .lookup_all_socket_v6(ServiceName::ManagementGatewayService)
        .await
        .context("looking up MGS addresses")?
        .into_iter()
        .map(|sockaddr| {
            let url = format!("http://{}", sockaddr);
            let log = opctx.log.new(o!("gateway_url" => url.clone()));
            Arc::new(gateway_client::Client::new(&url, log))
        })
        .collect::<Vec<_>>();

    // Run a collection.
    let inventory = nexus_inventory::Collector::new(
        creator,
        &mgs_clients,
        opctx.log.clone(),
    );
    let collection =
        inventory.collect_all().await.context("collecting inventory")?;

    // Write it to the database.
    datastore
        .inventory_insert_collection(opctx, &collection)
        .await
        .context("saving inventory to database")?;

    Ok(collection)
}
