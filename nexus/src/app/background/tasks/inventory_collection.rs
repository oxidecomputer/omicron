// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reading inventory for the rack

use crate::app::background::BackgroundTask;
use anyhow::Context;
use anyhow::ensure;
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::ResolveError;
use internal_dns_types::names::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_inventory::InventoryError;
use nexus_types::deployment::SledFilter;
use nexus_types::inventory::Collection;
use omicron_cockroach_metrics::CockroachClusterAdminClient;
use omicron_uuid_kinds::CollectionUuid;
use serde_json::json;
use slog::{debug, o, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that reads inventory for the rack
pub struct InventoryCollector {
    datastore: Arc<DataStore>,
    resolver: internal_dns_resolver::Resolver,
    creator: String,
    nkeep: u32,
    disable: bool,
    tx: watch::Sender<Option<CollectionUuid>>,
    cockroach_admin_client: CockroachClusterAdminClient,
}

impl InventoryCollector {
    pub fn new(
        opctx: &OpContext,
        datastore: Arc<DataStore>,
        resolver: internal_dns_resolver::Resolver,
        creator: &str,
        nkeep: u32,
        disable: bool,
    ) -> InventoryCollector {
        let (tx, _) = watch::channel(None);
        let cockroach_admin_client = CockroachClusterAdminClient::new(
            opctx
                .log
                .new(slog::o!("component" => "inventory_cockroach_client")),
        );
        InventoryCollector {
            datastore,
            resolver,
            creator: creator.to_owned(),
            nkeep,
            disable,
            tx,
            cockroach_admin_client,
        }
    }

    pub fn watcher(&self) -> watch::Receiver<Option<CollectionUuid>> {
        self.tx.subscribe()
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
                &self.cockroach_admin_client,
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
                    let json = json!({
                        "collection_id": collection.id.to_string(),
                        "time_started": collection.time_started.to_string(),
                        "time_done": collection.time_done.to_string()
                    });
                    self.tx.send_replace(Some(collection.id));
                    json
                }
            }
        }
        .boxed()
    }
}

async fn inventory_activate(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &internal_dns_resolver::Resolver,
    creator: &str,
    nkeep: u32,
    disabled: bool,
    cockroach_admin_client: &CockroachClusterAdminClient,
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
            gateway_client::Client::new(&url, log)
        })
        .collect::<Vec<_>>();

    // Find clickhouse-admin-keeper servers if there are any.
    let keeper_admin_clients = match resolver
        .lookup_all_socket_v6(ServiceName::ClickhouseAdminKeeper)
        .await
    {
        Ok(sockaddrs) => sockaddrs
            .into_iter()
            .map(|sockaddr| {
                let url = format!("http://{}", sockaddr);
                let log = opctx
                    .log
                    .new(o!("clickhouse_admin_keeper_url" => url.clone()));
                clickhouse_admin_keeper_client::Client::new(&url, log)
            })
            .collect::<Vec<_>>(),
        Err(err) => match err {
            // When DNS resolution fails because no clickhouse-keeper-admin
            // servers have been found, we allow this and move on. This is
            // because multi-node clickhouse may not be enabled, and therefore
            // there will not be any clickhouse-keeper-admin servers to find.
            //
            // In the long term, we expect multi-node clickhouse to always
            // be enabled, and therefore we may want to bubble up any error
            // we find, including `NotFound`. However, since we must enable
            // multi-node clickhouse via reconfigurator, and not RSS, we may
            // find ourselves with a small gap early on where the names don't
            // yet exist. This would block the rest of inventory collection if
            // we early return. We may be able to resolve this problem at rack
            // handoff time, but it's worth considering whether we want to error
            // here in case a gap remains.
            //
            // See https://github.com/oxidecomputer/omicron/issues/7005
            ResolveError::NotFound(_) | ResolveError::NotFoundByString(_) => {
                vec![]
            }
            ResolveError::Resolve(hickory_err)
                if is_no_records_found(&hickory_err) =>
            {
                vec![]
            }
            _ => {
                return Err(err)
                    .context("looking up clickhouse-admin-keeper addresses");
            }
        },
    };

    // Update CockroachDB cluster backends.
    let cockroach_addresses = resolver
        .lookup_all_socket_v6(ServiceName::Cockroach)
        .await
        .context("looking up cockroach addresses")?;

    // TODO: Allow a hard-coded option to find the admin interface here.
    let admin_addresses: Vec<_> = cockroach_addresses
        .into_iter()
        .map(|mut addr| {
            addr.set_port(omicron_common::address::COCKROACH_ADMIN_PORT);
            SocketAddr::V6(addr)
        })
        .collect();

    cockroach_admin_client.update_backends(admin_addresses.as_slice()).await;

    // Create an enumerator to find sled agents.
    let sled_enum = DbSledAgentEnumerator { opctx, datastore };

    // Run a collection.
    let inventory = nexus_inventory::Collector::new(
        creator,
        mgs_clients,
        keeper_admin_clients,
        cockroach_admin_client,
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

fn is_no_records_found(err: &hickory_resolver::ResolveError) -> bool {
    match err.kind() {
        hickory_resolver::ResolveErrorKind::Proto(proto_error) => {
            match proto_error.kind() {
                hickory_resolver::proto::ProtoErrorKind::NoRecordsFound {
                    ..
                } => true,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Determine which sleds to inventory based on what's in the database
///
/// We only want to inventory what's actually part of the control plane (i.e.,
/// has a "sled" record).
struct DbSledAgentEnumerator<'a> {
    opctx: &'a OpContext,
    datastore: &'a DataStore,
}

impl nexus_inventory::SledAgentEnumerator for DbSledAgentEnumerator<'_> {
    fn list_sled_agents(
        &self,
    ) -> BoxFuture<'_, Result<Vec<String>, InventoryError>> {
        async {
            Ok(self
                .datastore
                .sled_list_all_batched(
                    &self.opctx,
                    SledFilter::QueryDuringInventory,
                )
                .await
                .context("listing sleds")?
                .into_iter()
                .map(|sled| format!("http://{}", sled.address()))
                .collect())
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::DbSledAgentEnumerator;
    use super::InventoryCollector;
    use crate::app::authz;
    use crate::app::background::BackgroundTask;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::DataStoreInventoryTest;
    use nexus_inventory::SledAgentEnumerator;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::LookupType;
    use omicron_uuid_kinds::CollectionUuid;
    use std::collections::BTreeSet;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // Test that each activation creates a new collection and that we prune old
    // collections, too.
    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let resolver = internal_dns_resolver::Resolver::new_from_addrs(
            cptestctx.logctx.log.clone(),
            &[cptestctx.internal_dns.dns_server.local_address()],
        )
        .unwrap();

        // Create our own copy of the background task and activate it a bunch
        // and make sure that it always creates a new collection and does not
        // allow a backlog to accumulate.
        let nkeep = 3;
        let mut task = InventoryCollector::new(
            &opctx,
            datastore.clone(),
            resolver.clone(),
            "me",
            nkeep,
            false,
        );
        let nkeep = usize::try_from(nkeep).unwrap();
        let mut all_our_collection_ids = Vec::new();
        for i in 0..20 {
            let _ = task.activate(&opctx).await;
            let collections = datastore.inventory_collections().await.unwrap();

            // Nexus is creating inventory collections concurrently with us,
            // so our expectations here have to be flexible to account for the
            // fact that there might be collections other than the ones we've
            // activated interspersed with the ones we care about.
            let num_collections = collections.len();

            // We should have at least one collection (the one we just
            // activated).
            assert!(num_collections > 0);

            // Regardless of the activation source, we should have at
            // most `nkeep + 1` collections.
            assert!(num_collections <= nkeep + 1);

            // Filter down to just the collections we activated. (This could be
            // empty if Nexus shoved several collections in!)
            let our_collections = collections
                .into_iter()
                .filter(|c| c.collector == "me")
                .map(|c| CollectionUuid::from(c.id))
                .collect::<Vec<_>>();

            // If we have no collections, we have nothing else to check; Nexus
            // has pushed us out.
            if our_collections.is_empty() {
                println!(
                    "iter {i}: no test collections ({num_collections} Nexus \
                     collections)",
                );
                continue;
            }

            // The most recent collection should be new.
            let new_collection_id = our_collections.last().unwrap();
            assert!(!all_our_collection_ids.contains(new_collection_id));
            all_our_collection_ids.push(*new_collection_id);

            // Push this onto the collections we've seen, then assert that the
            // tail of all IDs we've seen matches the ones we saw in this
            // iteration (i.e., we're pushing out old collections in order).
            println!(
                "iter {i}: saw {our_collections:?}; should match tail of \
                 {all_our_collection_ids:?}"
            );
            assert_eq!(
                all_our_collection_ids
                    [all_our_collection_ids.len() - our_collections.len()..],
                our_collections
            );
        }

        // Create a disabled task and make sure that does nothing.
        let mut task = InventoryCollector::new(
            &opctx,
            datastore.clone(),
            resolver,
            "disabled",
            3,
            true,
        );
        let _ = task.activate(&opctx).await;

        // It's possible that Nexus is concurrently running with us still, so
        // we'll activate this task and ensure that:
        //
        // (a) at least one of the collections is from `"me"` above, and
        // (b) there is no collection from `"disabled"`
        //
        // This is technically still racy if Nexus manages to collect `nkeep +
        // 1` collections in between the loop above and this check, but we don't
        // expect that to be the case.
        let latest_collectors = datastore
            .inventory_collections()
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.collector)
            .collect::<BTreeSet<_>>();
        println!("latest_collectors: {latest_collectors:?}");
        assert!(latest_collectors.contains("me"));
        assert!(!latest_collectors.contains("disabled"));
    }

    #[nexus_test(server = crate::Server, extra_sled_agents = 1)]
    async fn test_db_sled_enumerator(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let db_enum =
            DbSledAgentEnumerator { opctx: &opctx, datastore: &datastore };

        // There will be two sled agents set up as part of the test context.
        let initial_found_urls = db_enum.list_sled_agents().await.unwrap();
        assert_eq!(initial_found_urls.len(), 2);

        // Insert some sleds.
        let rack_id = Uuid::new_v4();
        let mut sleds = Vec::new();
        for i in 0..64 {
            let sled = SledUpdate::new(
                Uuid::new_v4(),
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1200 + i, 0, 0),
                1200 + i,
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
            let (sled, _) = datastore.sled_upsert(sled).await.unwrap();
            sleds.push(sled);
        }

        // The same enumerator should immediately find all the new sleds.
        let mut expected_urls: Vec<_> = initial_found_urls
            .into_iter()
            .chain(sleds.iter().map(|s| format!("http://{}", s.address())))
            .collect();
        expected_urls.sort();
        println!("expected_urls: {:?}", expected_urls);

        let mut found_urls = db_enum.list_sled_agents().await.unwrap();
        found_urls.sort();
        assert_eq!(expected_urls, found_urls);

        // Now mark one expunged.  We should not find that sled any more.
        let expunged_sled = &sleds[0];
        let expunged_sled_id = expunged_sled.id();
        let authz_sled = authz::Sled::new(
            authz::FLEET,
            expunged_sled_id,
            LookupType::ById(expunged_sled_id),
        );
        datastore
            .sled_set_policy_to_expunged(&opctx, &authz_sled)
            .await
            .expect("failed to mark sled expunged");
        let expunged_sled_url = format!("http://{}", expunged_sled.address());
        let (remaining_urls, removed_urls): (Vec<_>, Vec<_>) = expected_urls
            .into_iter()
            .partition(|sled_url| *sled_url != expunged_sled_url);
        assert_eq!(
            removed_urls.len(),
            1,
            "expected to find exactly one sled URL matching our expunged \
             sled's URL"
        );
        let mut found_urls = db_enum.list_sled_agents().await.unwrap();
        found_urls.sort();
        assert_eq!(remaining_urls, found_urls);
    }
}
