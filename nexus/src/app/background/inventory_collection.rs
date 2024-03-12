// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reading inventory for the rack

use super::common::BackgroundTask;
use anyhow::ensure;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use internal_dns::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_inventory::InventoryError;
use nexus_types::identity::Asset;
use nexus_types::inventory::Collection;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::Arc;

/// How many rows to request in each paginated database query
const DB_PAGE_SIZE: u32 = 1024;

/// Background task that reads inventory for the rack
pub struct InventoryCollector {
    datastore: Arc<DataStore>,
    resolver: internal_dns::resolver::Resolver,
    creator: String,
    nkeep: u32,
    disable: bool,
}

impl InventoryCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns::resolver::Resolver,
        creator: &str,
        nkeep: u32,
        disable: bool,
    ) -> InventoryCollector {
        InventoryCollector {
            datastore,
            resolver,
            creator: creator.to_owned(),
            nkeep,
            disable,
        }
    }
}

impl BackgroundTask for InventoryCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            match inventory_activate(
                opctx,
                &self.datastore,
                &self.resolver,
                &self.creator,
                self.nkeep,
                self.disable,
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
    disabled: bool,
) -> Result<Collection, anyhow::Error> {
    // If we're disabled, don't do anything.  (This switch is only intended for
    // unforeseen production emergencies.)
    ensure!(!disabled, "disabled by explicit configuration");

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

    // Create an enumerator to find sled agents.
    let page_size = NonZeroU32::new(DB_PAGE_SIZE).unwrap();
    let sled_enum = DbSledAgentEnumerator { opctx, datastore, page_size };

    // Run a collection.
    let inventory = nexus_inventory::Collector::new(
        creator,
        &mgs_clients,
        &sled_enum,
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

/// Determine which sleds to inventory based on what's in the database
///
/// We only want to inventory what's actually part of the control plane (i.e.,
/// has a "sled" record).
struct DbSledAgentEnumerator<'a> {
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    page_size: NonZeroU32,
}

impl<'a> nexus_inventory::SledAgentEnumerator for DbSledAgentEnumerator<'a> {
    fn list_sled_agents(
        &self,
    ) -> BoxFuture<'_, Result<Vec<String>, InventoryError>> {
        async {
            let mut all_sleds = Vec::new();
            let mut paginator = Paginator::new(self.page_size);
            while let Some(p) = paginator.next() {
                let records_batch = self
                    .datastore
                    .sled_list(&self.opctx, &p.current_pagparams())
                    .await
                    .context("listing sleds")?;
                paginator = p.found_batch(
                    &records_batch,
                    &|s: &nexus_db_model::Sled| s.id(),
                );
                all_sleds.extend(
                    records_batch
                        .into_iter()
                        .map(|sled| format!("http://{}", sled.address())),
                );
            }

            Ok(all_sleds)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::app::background::common::BackgroundTask;
    use crate::app::background::inventory_collection::DbSledAgentEnumerator;
    use crate::app::background::inventory_collection::InventoryCollector;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::DataStoreInventoryTest;
    use nexus_inventory::SledAgentEnumerator;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_test_utils::dev::poll;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // Test that each activation creates a new collection and that we prune old
    // collections, too.
    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Nexus starts the very background task that we're also testing
        // manually here.  As a result, we should find a collection in the
        // database before too long.  Wait for it so that after it appears, we
        // can assume the rest of the collections came from the instance that
        // we're testing.
        let mut last_collections =
            poll::wait_for_condition::<_, anyhow::Error, _, _>(
                || async {
                    let collections = datastore
                        .inventory_collections()
                        .await
                        .map_err(poll::CondCheckError::Failed)?;
                    if collections.is_empty() {
                        Err(poll::CondCheckError::NotYet)
                    } else {
                        Ok(collections)
                    }
                },
                &std::time::Duration::from_millis(50),
                &std::time::Duration::from_secs(15),
            )
            .await
            .expect("background task did not populate initial collection");

        let resolver = internal_dns::resolver::Resolver::new_from_addrs(
            cptestctx.logctx.log.clone(),
            &[cptestctx.internal_dns.dns_server.local_address()],
        )
        .unwrap();

        // Now we'll create our own copy of the background task and activate it
        // a bunch and make sure that it always creates a new collection and
        // does not allow a backlog to accumulate.
        let nkeep = 3;
        let mut task = InventoryCollector::new(
            datastore.clone(),
            resolver.clone(),
            "me",
            nkeep,
            false,
        );
        let nkeep = usize::try_from(nkeep).unwrap();
        for i in 0..10 {
            let _ = task.activate(&opctx).await;
            let collections = datastore.inventory_collections().await.unwrap();
            println!(
                "iter {}: last = {:?}, current = {:?}",
                i, last_collections, collections
            );

            let expected_from_last: Vec<_> = if last_collections.len() <= nkeep
            {
                last_collections
            } else {
                last_collections.into_iter().skip(1).collect()
            };
            let expected_from_current: Vec<_> =
                collections.iter().rev().skip(1).rev().cloned().collect();
            assert_eq!(expected_from_last, expected_from_current);
            assert_eq!(collections.len(), std::cmp::min(i + 2, nkeep + 1));
            last_collections = collections;
        }

        // Create a disabled task and make sure that does nothing.
        let mut task = InventoryCollector::new(
            datastore.clone(),
            resolver,
            "disabled",
            3,
            true,
        );
        let previous = datastore.inventory_collections().await.unwrap();
        let _ = task.activate(&opctx).await;
        let latest = datastore.inventory_collections().await.unwrap();
        assert_eq!(previous, latest);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_db_sled_enumerator(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let db_enum = DbSledAgentEnumerator {
            opctx: &opctx,
            datastore: &datastore,
            page_size: NonZeroU32::new(3).unwrap(),
        };

        // There will be two sled agents set up as part of the test context.
        let found_urls = db_enum.list_sled_agents().await.unwrap();
        assert_eq!(found_urls.len(), 2);

        // Insert some sleds.
        let rack_id = Uuid::new_v4();
        let mut sleds = Vec::new();
        for i in 0..64 {
            let sled = SledUpdate::new(
                Uuid::new_v4(),
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1200 + i, 0, 0),
                SledBaseboard {
                    serial_number: format!("serial-{}", i),
                    part_number: String::from("fake-sled"),
                    revision: 3,
                },
                SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 12,
                    usable_physical_ram: ByteCount::from_gibibytes_u32(16)
                        .into(),
                    reservoir_size: ByteCount::from_gibibytes_u32(8).into(),
                },
                rack_id,
                Generation::new(),
            );
            sleds.push(datastore.sled_upsert(sled).await.unwrap());
        }

        // The same enumerator should immediately find all the new sleds.
        let mut expected_urls: Vec<_> = found_urls
            .into_iter()
            .chain(sleds.into_iter().map(|s| format!("http://{}", s.address())))
            .collect();
        expected_urls.sort();
        println!("expected_urls: {:?}", expected_urls);

        let mut found_urls = db_enum.list_sled_agents().await.unwrap();
        found_urls.sort();
        assert_eq!(expected_urls, found_urls);

        // We should get the same result even with a page size of 1.
        let db_enum = DbSledAgentEnumerator {
            opctx: &opctx,
            datastore: &datastore,
            page_size: NonZeroU32::new(1).unwrap(),
        };
        let mut found_urls = db_enum.list_sled_agents().await.unwrap();
        found_urls.sort();
        assert_eq!(expected_urls, found_urls);

        // We should get the same result even with a page size much larger than
        // we need.
        let db_enum = DbSledAgentEnumerator {
            opctx: &opctx,
            datastore: &datastore,
            page_size: NonZeroU32::new(1024).unwrap(),
        };
        let mut found_urls = db_enum.list_sled_agents().await.unwrap();
        found_urls.sort();
        assert_eq!(expected_urls, found_urls);
    }
}
