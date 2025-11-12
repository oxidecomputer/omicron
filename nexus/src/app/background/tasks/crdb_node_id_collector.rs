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

use crate::app::background::BackgroundTask;
use anyhow::Context;
use anyhow::ensure;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::blueprint_zone_type;
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

    /// Implementation for `BackgroundTask::activate`, added here to produce
    /// better compile errors.
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
//
// TODO(https://github.com/oxidecomputer/omicron/issues/8496): Add the admin
// service to DNS, remove this?
trait CockroachAdminFromBlueprint {
    fn cockroach_admin_addrs<'a>(
        &'a self,
        blueprint: &'a Blueprint,
    ) -> impl Iterator<Item = (OmicronZoneUuid, SocketAddrV6)> + 'a;
}

struct CockroachAdminFromBlueprintViaFixedPort;

impl CockroachAdminFromBlueprint for CockroachAdminFromBlueprintViaFixedPort {
    fn cockroach_admin_addrs<'a>(
        &'a self,
        blueprint: &'a Blueprint,
    ) -> impl Iterator<Item = (OmicronZoneUuid, SocketAddrV6)> + 'a {
        // We can only actively collect from zones that should be running; if
        // there are CRDB zones in other states that still need their node ID
        // collected, we have to wait until they're running.
        let zone_filter = BlueprintZoneDisposition::is_in_service;

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
    let node = admin_client
        .local_node_id()
        .await
        .with_context(|| {
            format!("failed to fetch node ID for zone {zone_id} at {admin_url}")
        })?
        .into_inner();

    // Ensure the address we have for this zone is the zone we think it is.
    // Absent bugs, the only way this can fail is if our blueprint is out of
    // date, and there's now a new zone running at `admin_addr`; we _should_
    // fail in that case, and we'll catch up to reality when we reload the
    // target blueprint.
    ensure!(
        zone_id == node.zone_id,
        "expected cockroach zone {zone_id} at {admin_url}, but found zone {}",
        node.zone_id
    );

    // Record this value. We have a harmless TOCTOU here; if multiple Nexus
    // instances all checked for a node ID, found none, and get here, this call
    // is idempotent (as long as they all are inserting the same node ID, which
    // they certainly should be!).
    datastore
        .set_cockroachdb_node_id(opctx, zone_id, node.node_id.clone())
        .await
        .with_context(|| {
            format!(
                "failed to record node ID {} for zone {zone_id}",
                node.node_id
            )
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
    use chrono::Utc;
    use httptest::Expectation;
    use httptest::matchers::any;
    use httptest::responders::json_encoded;
    use httptest::responders::status_code;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::planner::PlannerRng;
    use nexus_types::deployment::BlueprintSource;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::net::SocketAddr;

    // The `CockroachAdminFromBlueprintViaFixedPort` type above is the standard
    // way to map from a blueprint to an iterator of cockroach-admin addresses.
    // We can't use that in the more thorough test below (and it exists so we
    // can _write_ that test), so test it in isolation here.
    #[test]
    fn test_default_cockroach_admin_addrs_from_blueprint() {
        const TEST_NAME: &str =
            "test_default_cockroach_admin_addrs_from_blueprint";
        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // Build an example system with one sled.
        let (example_system, bp0) =
            ExampleSystemBuilder::new(log, TEST_NAME).nsleds(1).build();
        let input = example_system.input;

        // `ExampleSystemBuilder` doesn't place any cockroach nodes; assert so
        // we bail out early if that changes.
        let ncockroach = bp0
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, z)| z.zone_type.is_cockroach())
            .count();
        assert_eq!(ncockroach, 0);

        // This blueprint has no cockroach zones, so should have no admin addrs
        // either.
        let admin_addrs = CockroachAdminFromBlueprintViaFixedPort
            .cockroach_admin_addrs(&bp0)
            .collect::<Vec<_>>();
        assert_eq!(admin_addrs, Vec::new());

        // Add 5 cockroach zones to our sled.
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &bp0,
            &input,
            TEST_NAME,
            PlannerRng::from_entropy(),
        )
        .expect("constructed builder");
        let sled_id = bp0.sleds().next().expect("1 sled");
        for _ in 0..5 {
            builder
                .sled_add_zone_cockroachdb(
                    sled_id,
                    BlueprintZoneImageSource::InstallDataset,
                )
                .expect("added cockroach");
        }
        let mut bp1 = builder.build(BlueprintSource::Test);

        // Mutate this blueprint: expunge 2 of the 5 zones. Record the expected
        // admin addrs from the other three.
        let mut expected = BTreeSet::new();
        let sled_config = bp1.sleds.get_mut(&sled_id).unwrap();
        let mut seen = 0;
        for mut zone in sled_config.zones.iter_mut() {
            if !zone.zone_type.is_cockroach() {
                continue;
            }
            // Keep even; expunge odd.
            if seen % 2 == 0 {
                let ip = zone.underlay_ip();
                let addr = SocketAddrV6::new(ip, COCKROACH_ADMIN_PORT, 0, 0);
                expected.insert((zone.id, addr));
            } else {
                zone.disposition = BlueprintZoneDisposition::Expunged {
                    as_of_generation: Generation::new(),
                    ready_for_cleanup: false,
                };
            }
            seen += 1;
        }
        assert_eq!(seen, 5);
        assert_eq!(expected.len(), 3);

        // Confirm we only see the three expected addrs.
        let admin_addrs = CockroachAdminFromBlueprintViaFixedPort
            .cockroach_admin_addrs(&bp1)
            .collect::<BTreeSet<_>>();
        assert_eq!(expected, admin_addrs);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_activate_fails_if_no_blueprint() {
        let logctx = dev::test_setup_log("test_activate_fails_if_no_blueprint");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (_tx_blueprint, rx_blueprint) = watch::channel(None);
        let mut collector =
            CockroachNodeIdCollector::new(datastore.clone(), rx_blueprint);
        let result = collector.activate(&opctx).await;

        assert_eq!(result, json!({"error": "no blueprint"}));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    struct FakeCockroachAdminAddrs(Vec<(OmicronZoneUuid, SocketAddrV6)>);

    impl CockroachAdminFromBlueprint for FakeCockroachAdminAddrs {
        fn cockroach_admin_addrs<'a>(
            &'a self,
            _blueprint: &'a Blueprint,
        ) -> impl Iterator<Item = (OmicronZoneUuid, SocketAddrV6)> + 'a
        {
            self.0.iter().copied()
        }
    }

    #[tokio::test]
    async fn test_activate_with_no_unknown_node_ids() {
        let logctx =
            dev::test_setup_log("test_activate_with_no_unknown_node_ids");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let blueprint = BlueprintBuilder::build_empty("test");
        let blueprint_target = BlueprintTarget {
            target_id: blueprint.id,
            enabled: true,
            time_made_target: Utc::now(),
        };

        let (_tx_blueprint, rx_blueprint) =
            watch::channel(Some(Arc::new((blueprint_target, blueprint))));
        let mut collector =
            CockroachNodeIdCollector::new(datastore.clone(), rx_blueprint);

        // The blueprint is empty. This should be fine: we should get no
        // successes and no errors.
        let result = collector.activate(&opctx).await;
        assert_eq!(result, json!({"nsuccess": 0}));

        // Create a few fake CRDB zones, and assign them node IDs in the
        // datastore.
        let crdb_zones =
            (0..5).map(|_| OmicronZoneUuid::new_v4()).collect::<Vec<_>>();
        for (i, zone_id) in crdb_zones.iter().copied().enumerate() {
            datastore
                .set_cockroachdb_node_id(
                    &opctx,
                    zone_id,
                    format!("test-node-{i}"),
                )
                .await
                .expect("assigned fake node ID");
        }

        // Activate again, injecting our fake CRDB zones with arbitrary
        // cockroach-admin addresses. Because the node IDs are already in the
        // datastore, the collector shouldn't try to contact these addresses and
        // should instead report that all nodes are recorded successfully.
        let result = collector
            .activate_impl(
                &opctx,
                &FakeCockroachAdminAddrs(
                    crdb_zones
                        .iter()
                        .map(|&zone_id| (zone_id, "[::1]:0".parse().unwrap()))
                        .collect(),
                ),
            )
            .await;
        assert_eq!(result, json!({"nsuccess": crdb_zones.len()}));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_activate_with_unknown_node_ids() {
        // Test setup.
        let logctx = dev::test_setup_log("test_activate_with_unknown_node_ids");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let blueprint = BlueprintBuilder::build_empty("test");
        let blueprint_target = BlueprintTarget {
            target_id: blueprint.id,
            enabled: true,
            time_made_target: Utc::now(),
        };

        let (_tx_blueprint, rx_blueprint) =
            watch::channel(Some(Arc::new((blueprint_target, blueprint))));
        let mut collector =
            CockroachNodeIdCollector::new(datastore.clone(), rx_blueprint);

        // We'll send in three Cockroach nodes for the collector to gather:
        //
        // 1. Node 1 will succeed
        // 2. Node 2 will fail
        // 3. Node 3 will succeed, but will report an unexpected zone ID
        //
        // We should see one success and two errors in the activation result. We
        // need to start three fake cockroach-admin servers to handle the
        // requests.
        let make_httptest_server = || {
            httptest::ServerBuilder::new()
                .bind_addr("[::1]:0".parse().unwrap())
                .run()
                .expect("started httptest server")
        };
        let crdb_zone_id1 = OmicronZoneUuid::new_v4();
        let crdb_zone_id2 = OmicronZoneUuid::new_v4();
        let crdb_zone_id3 = OmicronZoneUuid::new_v4();
        let crdb_zone_id4 = OmicronZoneUuid::new_v4();
        let crdb_node_id1 = "fake-node-1";
        let crdb_node_id3 = "fake-node-3";
        let mut admin1 = make_httptest_server();
        let mut admin2 = make_httptest_server();
        let mut admin3 = make_httptest_server();
        let crdb_admin_addrs = FakeCockroachAdminAddrs(
            vec![
                (crdb_zone_id1, admin1.addr()),
                (crdb_zone_id2, admin2.addr()),
                (crdb_zone_id3, admin3.addr()),
            ]
            .into_iter()
            .map(|(zone_id, addr)| {
                let SocketAddr::V6(addr6) = addr else {
                    panic!("expected IPv6 addr; got {addr}");
                };
                (zone_id, addr6)
            })
            .collect(),
        );

        // Node 1 succeeds.
        admin1.expect(Expectation::matching(any()).times(1).respond_with(
            json_encoded(cockroach_admin_client::types::LocalNodeId {
                zone_id: crdb_zone_id1,
                node_id: crdb_node_id1.to_string(),
            }),
        ));
        // Node 2 fails.
        admin2.expect(
            Expectation::matching(any())
                .times(1)
                .respond_with(status_code(503)),
        );
        // Node 3 succeeds, but with an unexpected zone_id.
        admin3.expect(Expectation::matching(any()).times(1).respond_with(
            json_encoded(cockroach_admin_client::types::LocalNodeId {
                zone_id: crdb_zone_id4,
                node_id: crdb_node_id3.to_string(),
            }),
        ));

        let result = collector.activate_impl(&opctx, &crdb_admin_addrs).await;

        admin1.verify_and_clear();
        admin2.verify_and_clear();
        admin3.verify_and_clear();

        let result = result.as_object().expect("JSON object");

        // We should have one success (node 1).
        assert_eq!(
            result.get("nsuccess").expect("nsuccess key").as_number(),
            Some(&serde_json::Number::from(1))
        );
        let errors = result
            .get("errors")
            .expect("errors key")
            .as_array()
            .expect("errors array")
            .iter()
            .map(|val| {
                let error = val.as_object().expect("error object");
                let zone_id = error
                    .get("zone_id")
                    .expect("zone_id key")
                    .as_str()
                    .expect("zone_id string");
                let err = error
                    .get("err")
                    .expect("err key")
                    .as_str()
                    .expect("err string");
                (zone_id, err)
            })
            .collect::<BTreeMap<_, _>>();
        println!("errors: {errors:?}");
        assert_eq!(errors.len(), 2);

        // We should have an error for node 2. We don't check the specific
        // message because it may change if progenitor changes how it reports a
        // 503 with no body.
        assert!(errors.contains_key(crdb_zone_id2.to_string().as_str()));

        // The error message for node 3 should contain both the expected and
        // unexpected zone IDs.
        let crdb_zone_id3 = crdb_zone_id3.to_string();
        let crdb_zone_id4 = crdb_zone_id4.to_string();
        let crdb_err3 =
            errors.get(crdb_zone_id3.as_str()).expect("error for zone 3");
        assert!(crdb_err3.contains(&crdb_zone_id3));
        assert!(crdb_err3.contains(&crdb_zone_id4));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
