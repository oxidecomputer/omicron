// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for collecting the Cockroach Node ID for running CRDB zones
//!
//! Cockroach assigns a node ID when the node is initially started and joins the
//! cluster. The node IDs are 1-up counters that are never reused. Cluster
//! management operations (e.g., decommissioning nodes) are keyed off of the
//! node ID. However, because node IDs aren't assigned until the node has
//! started and joins the cluster, it means there is a gap between when Omicron
//! creates a CRDB zone (and picks an Omicron zone ID for it) and when that zone
//! gets a CRDB node ID. This RPW exists to backfill the mapping from Omicron
//! zone ID <-> CRDB node ID for Cockroach zones.
//!
//! This isn't foolproof. If a Cockroach node fails to start, it won't have a
//! node ID and therefore this RPW won't be able to make an assignment. If a
//! Cockroach node succeeds in starting and gets a node ID but then fails in an
//! unrecoverable way before this RPW has collected its node ID, that will also
//! result in a missing assignment. Consumers of the Omicron zone ID <-> CRDB
//! node ID don't have a way of distinguishing these two failure modes from this
//! RPW alone, and will need to gather other information (e.g., asking CRDB for
//! the status of all nodes and looking for orphans, perhaps) to determine
//! whether a zone without a known node ID ever existed.

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::stream;
use futures::FutureExt;
use futures::StreamExt;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use omicron_common::address::COCKROACH_ADMIN_PORT;
use omicron_uuid_kinds::OmicronZoneUuid;
use serde_json::json;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::sync::watch;

pub struct CockroachNodeIdCollector {
    datastore: Arc<DataStore>,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl CockroachNodeIdCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
    ) -> Self {
        Self { datastore, rx_blueprint }
    }

    /// Implementation for `BackgroundTask::activate` for `BlueprintExecutor`,
    /// added here to produce better compile errors.
    ///
    /// The presence of `boxed()` in `BackgroundTask::activate` has caused some
    /// confusion with compilation errors in the past. So separate this method
    /// out.
    async fn activate_impl<T: CockroachAdminFromBlueprint>(
        &mut self,
        opctx: &OpContext,
        addrs_from_blueprint: &T,
    ) -> serde_json::Value {
        // Get the latest blueprint, cloning to prevent holding a read lock
        // on the watch.
        let update = self.rx_blueprint.borrow_and_update().clone();

        let Some((_bp_target, blueprint)) = update.as_deref() else {
            warn!(
                &opctx.log, "Blueprint execution: skipped";
                "reason" => "no blueprint",
            );
            return json!({"error": "no blueprint" });
        };

        // With a bit of concurrency, confirm we know the node IDs for all the
        // CRDB zones in the blueprint.
        let mut results =
            stream::iter(addrs_from_blueprint.cockroach_admin_addrs(blueprint))
                .map(|(zone_id, admin_addr)| {
                    let datastore = &self.datastore;
                    async move {
                        ensure_node_id_known(
                            opctx, datastore, zone_id, admin_addr,
                        )
                        .await
                        .map_err(|err| (zone_id, err))
                    }
                })
                .buffer_unordered(8);

        let mut nsuccess = 0;
        let mut errors = vec![];
        while let Some(result) = results.next().await {
            match result {
                Ok(()) => {
                    nsuccess += 1;
                }
                Err((zone_id, err)) => {
                    errors.push(json!({
                        "zone_id": zone_id,
                        "err": format!("{err:#}"),
                    }));
                }
            }
        }

        if errors.is_empty() {
            json!({ "nsuccess": nsuccess })
        } else {
            json!({
                "nsuccess": nsuccess,
                "errors": errors,
            })
        }
    }
}

// This trait exists so we can inject addresses in our unit tests below that
// aren't required to have admin servers listening on the fixed
// `COCKROACH_ADMIN_PORT`.
trait CockroachAdminFromBlueprint {
    fn cockroach_admin_addrs<'a>(
        &self,
        blueprint: &'a Blueprint,
    ) -> impl Iterator<Item = (OmicronZoneUuid, SocketAddrV6)> + 'a;
}

struct CockroachAdminFromBlueprintViaFixedPort;

impl CockroachAdminFromBlueprint for CockroachAdminFromBlueprintViaFixedPort {
    fn cockroach_admin_addrs<'a>(
        &self,
        blueprint: &'a Blueprint,
    ) -> impl Iterator<Item = (OmicronZoneUuid, SocketAddrV6)> + 'a {
        // We can only actively collect from zones that should be running; if
        // there are CRDB zones in other states that still need their node ID
        // collected, we have to wait until they're running.
        let zone_filter = BlueprintZoneFilter::ShouldBeRunning;

        blueprint.all_omicron_zones(zone_filter).filter_map(
            |(_sled_id, zone)| match &zone.zone_type {
                BlueprintZoneType::CockroachDb(
                    blueprint_zone_type::CockroachDb { address, .. },
                ) => {
                    let mut admin_addr = *address;
                    admin_addr.set_port(COCKROACH_ADMIN_PORT);
                    Some((zone.id, admin_addr))
                }
                _ => None,
            },
        )
    }
}

async fn ensure_node_id_known(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_id: OmicronZoneUuid,
    admin_addr: SocketAddrV6,
) -> anyhow::Result<()> {
    // Do we already know the node ID for this zone?
    if datastore
        .cockroachdb_node_id(opctx, zone_id)
        .await
        .with_context(|| {
            format!("fetching existing node ID for zone {zone_id}")
        })?
        .is_some()
    {
        return Ok(());
    }

    // We don't know the address; contact the admin server and ask if it knows.
    let admin_url = format!("http://{admin_addr}");
    let admin_client =
        cockroach_admin_client::Client::new(&admin_url, opctx.log.clone());
    let node_id = admin_client
        .node_id()
        .await
        .with_context(|| {
            format!("failed to fetch node ID for zone {zone_id} at {admin_url}")
        })?
        .into_inner()
        .id;

    // Record this value. We have a harmless TOCTOU here; if multiple Nexus
    // instances all checked for a node ID, found none, and get here, this call
    // is idempotent (as long as they all are inserting the same node ID, which
    // they certainly should be!).
    datastore
        .set_cockroachdb_node_id(opctx, zone_id, node_id.clone())
        .await
        .with_context(|| {
            format!("failed to record node ID {node_id} for zone {zone_id}")
        })
}

impl BackgroundTask for CockroachNodeIdCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        self.activate_impl(opctx, &CockroachAdminFromBlueprintViaFixedPort)
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use omicron_uuid_kinds::SledUuid;
    use uuid::Uuid;

    // The `CockroachAdminFromBlueprintViaFixedPort` type above is the standard
    // way to map from a blueprint to an iterator of cockroach-admin addresses.
    // We can't use that in the more thorough test below (and it exists so we
    // can _write_ that test), so test it in isolation here.
    #[test]
    fn test_default_cockroach_admin_addrs_from_blueprint() {
        // Construct an empty blueprint with one sled.
        let sled_id = SledUuid::new_v4();
        let mut blueprint = BlueprintBuilder::build_empty_with_sleds(
            std::iter::once(sled_id),
            "test",
        );
        let bp_zones = blueprint
            .blueprint_zones
            .get_mut(&sled_id)
            .expect("found entry for test sled");

        let make_crdb_zone_config =
            |disposition, id, addr: SocketAddrV6| BlueprintZoneConfig {
                disposition,
                id,
                underlay_address: *addr.ip(),
                zone_type: BlueprintZoneType::CockroachDb(
                    blueprint_zone_type::CockroachDb {
                        address: addr,
                        dataset: nexus_types::inventory::OmicronZoneDataset {
                            pool_name: format!("oxp_{}", Uuid::new_v4())
                                .parse()
                                .unwrap(),
                        },
                    },
                ),
            };

        // Add a three CRDB zones with known addresses; the first and third are
        // in service, and the second is expunged. Only the first and third
        // should show up when we ask for addresses below.
        let crdb_id1 = OmicronZoneUuid::new_v4();
        let crdb_id2 = OmicronZoneUuid::new_v4();
        let crdb_id3 = OmicronZoneUuid::new_v4();
        let crdb_addr1: SocketAddrV6 = "[2001:db8::1]:1111".parse().unwrap();
        let crdb_addr2: SocketAddrV6 = "[2001:db8::2]:1234".parse().unwrap();
        let crdb_addr3: SocketAddrV6 = "[2001:db8::3]:1234".parse().unwrap();
        bp_zones.zones.push(make_crdb_zone_config(
            BlueprintZoneDisposition::InService,
            crdb_id1,
            crdb_addr1,
        ));
        bp_zones.zones.push(make_crdb_zone_config(
            BlueprintZoneDisposition::Expunged,
            crdb_id2,
            crdb_addr2,
        ));
        bp_zones.zones.push(make_crdb_zone_config(
            BlueprintZoneDisposition::InService,
            crdb_id3,
            crdb_addr3,
        ));

        // Also add a non-CRDB zone to ensure it's filtered out.
        bp_zones.zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: OmicronZoneUuid::new_v4(),
            underlay_address: "::1".parse().unwrap(),
            zone_type: BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry {
                    address: "[::1]:0".parse().unwrap(),
                },
            ),
        });

        // We expect to see CRDB zones 1 and 3 with their IPs but the ports
        // changed to `COCKROACH_ADMIN_PORT`.
        let expected = vec![
            (
                crdb_id1,
                SocketAddrV6::new(*crdb_addr1.ip(), COCKROACH_ADMIN_PORT, 0, 0),
            ),
            (
                crdb_id3,
                SocketAddrV6::new(*crdb_addr3.ip(), COCKROACH_ADMIN_PORT, 0, 0),
            ),
        ];

        let admin_addrs = CockroachAdminFromBlueprintViaFixedPort
            .cockroach_admin_addrs(&blueprint)
            .collect::<Vec<_>>();
        assert_eq!(expected, admin_addrs);
    }
}
