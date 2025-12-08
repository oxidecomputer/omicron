// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of Omicron zones to Sled Agents

use anyhow::Context;
use anyhow::bail;
use cockroach_admin_client::types::NodeDecommission;
use cockroach_admin_client::types::NodeId;
use futures::StreamExt;
use futures::stream;
use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::CollectorReassignment;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use omicron_common::address::COCKROACH_ADMIN_PORT;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddr;
use std::net::SocketAddrV6;

/// Idempontently perform any cleanup actions necessary for expunged zones.
pub(crate) async fn clean_up_expunged_zones<R: CleanupResolver>(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &R,
    blueprint: &Blueprint,
) -> Result<(), Vec<anyhow::Error>> {
    // TODO-correctness Decommissioning cockroach nodes is currently disabled
    // while we work through issues with timing; see
    // https://github.com/oxidecomputer/omicron/issues/8445. We keep the
    // implementation around and gate it via an argument because we already have
    // unit tests for it and because we'd like to restore it once
    // cockroach-admin knows how to gate decommissioning correctly.
    let decommission_cockroach = false;

    clean_up_expunged_zones_impl(
        opctx,
        datastore,
        resolver,
        decommission_cockroach,
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_ready_for_cleanup),
    )
    .await
}

async fn clean_up_expunged_zones_impl<R: CleanupResolver>(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &R,
    decommission_cockroach: bool,
    zones_to_clean_up: impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<anyhow::Error> = stream::iter(zones_to_clean_up)
        .filter_map(async |(sled_id, config)| {
            let log = opctx.log.new(slog::o!(
                "sled_id" => sled_id.to_string(),
                "zone_id" => config.id.to_string(),
                "zone_type" => config.zone_type.kind().report_str(),
            ));

            let result = match &config.zone_type {
                // Zones which need cleanup after expungement.
                BlueprintZoneType::Nexus(_) => Some(
                    datastore
                        .database_nexus_access_delete(&opctx, config.id)
                        .await
                        .map_err(|err| anyhow::anyhow!(err)),
                ),
                BlueprintZoneType::CockroachDb(_) => {
                    if decommission_cockroach {
                        Some(
                            decommission_cockroachdb_node(
                                opctx, datastore, resolver, config.id, &log,
                            )
                            .await,
                        )
                    } else {
                        None
                    }
                }
                BlueprintZoneType::Oximeter(_) => Some(
                    oximeter_cleanup(opctx, datastore, config.id, &log).await,
                ),

                // Zones that may or may not need cleanup work - we haven't
                // gotten to these yet!
                BlueprintZoneType::BoundaryNtp(_)
                | BlueprintZoneType::Clickhouse(_)
                | BlueprintZoneType::ClickhouseKeeper(_)
                | BlueprintZoneType::ClickhouseServer(_)
                | BlueprintZoneType::Crucible(_)
                | BlueprintZoneType::CruciblePantry(_)
                | BlueprintZoneType::ExternalDns(_)
                | BlueprintZoneType::InternalDns(_)
                | BlueprintZoneType::InternalNtp(_) => {
                    warn!(
                        log,
                        "unsupported zone type for expungement cleanup; \
                         not performing any cleanup";
                    );
                    None
                }
            }?;

            match result {
                Err(error) => {
                    warn!(
                        log, "failed to clean up expunged zone";
                        "error" => #%error,
                    );
                    Some(error)
                }
                Ok(()) => {
                    info!(log, "successfully cleaned up after expunged zone");
                    None
                }
            }
        })
        .collect()
        .await;

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

async fn oximeter_cleanup(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_id: OmicronZoneUuid,
    log: &Logger,
) -> anyhow::Result<()> {
    // Record that this Oximeter instance is gone.
    datastore
        .oximeter_expunge(opctx, zone_id.into_untyped_uuid())
        .await
        .context("failed to mark Oximeter instance deleted")?;

    // Reassign any producers it was collecting to other Oximeter instances.
    match datastore
        .oximeter_reassign_all_producers(opctx, zone_id.into_untyped_uuid())
        .await
        .context("failed to reassign metric producers")?
    {
        CollectorReassignment::Complete(n) => {
            info!(
                log,
                "successfully reassigned {n} metric producers \
                 to new Oximeter collectors"
            );
        }
        CollectorReassignment::NoCollectorsAvailable => {
            warn!(
                log,
                "metric producers need reassignment, but there are no \
                 available Oximeter collectors"
            );
        }
    }
    Ok(())
}

// TODO(https://github.com/oxidecomputer/omicron/issues/8496): If this service
// was fully in DNS, this would not be necessary.
//
// Helper trait that is implemented by `Resolver`, but allows unit tests to
// inject a fake resolver that points to a mock server when calling
// `decommission_cockroachdb_node()`.
pub(crate) trait CleanupResolver {
    async fn resolve_cockroach_admin_addresses(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = SocketAddr>>;
}

impl CleanupResolver for Resolver {
    async fn resolve_cockroach_admin_addresses(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = SocketAddr>> {
        let crdb_ips = self
            .lookup_all_ipv6(ServiceName::Cockroach)
            .await
            .context("failed to resolve cockroach IPs in internal DNS")?;
        let ip_to_admin_addr = |ip| {
            SocketAddr::V6(SocketAddrV6::new(ip, COCKROACH_ADMIN_PORT, 0, 0))
        };
        Ok(crdb_ips.into_iter().map(ip_to_admin_addr))
    }
}

async fn decommission_cockroachdb_node<R: CleanupResolver>(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &R,
    zone_id: OmicronZoneUuid,
    log: &Logger,
) -> anyhow::Result<()> {
    // We need the node ID of this zone. Check and see whether the
    // crdb_node_id_collector RPW found the node ID for this zone. If it hasn't,
    // we're in trouble: we don't know whether this zone came up far enough to
    // join the cockroach cluster (and therefore needs decommissioning) but was
    // expunged before the collector RPW could identify it, or if the zone never
    // joined the cockroach cluster (and therefore doesn't have a node ID and
    // doesn't need decommissioning).
    //
    // For now, we punt on this problem. If we were certain that the zone's
    // socket address could never be reused, we could ask one of the running
    // cockroach nodes for the status of all nodes, find the ID of the one with
    // this zone's listening address (if one exists), and decommission that ID.
    // But if the address could be reused, that risks decommissioning a live
    // node! We'll just log a warning and require a human to figure out how to
    // clean this up.
    //
    // TODO-cleanup Can we decommission nodes in this state automatically? If
    // not, can we be noisier than just a `warn!` log without failing blueprint
    // exeuction entirely?
    let Some(node_id) =
        datastore.cockroachdb_node_id(opctx, zone_id).await.with_context(
            || format!("failed to look up node ID of cockroach zone {zone_id}"),
        )?
    else {
        warn!(
            log,
            "expunged cockroach zone has no known node ID; \
             support intervention is required for zone cleanup"
        );
        return Ok(());
    };

    // To decommission a CRDB node, we need to talk to one of the still-running
    // nodes; look them up in DNS try them all in order. (It would probably be
    // fine to try them all concurrently, but this isn't a very common
    // operation, so keeping it simple seems fine.)
    //
    // TODO-cleanup: Replace this with a qorb-based connection pool once it's
    // ready.
    let admin_addrs = resolver.resolve_cockroach_admin_addresses().await?;

    let mut num_admin_addrs_tried = 0;
    for admin_addr in admin_addrs {
        let admin_url = format!("http://{admin_addr}");
        let log = log.new(slog::o!("admin_url" => admin_url.clone()));
        let client =
            cockroach_admin_client::Client::new(&admin_url, log.clone());

        let body = NodeId { node_id: node_id.clone() };
        match client
            .node_decommission(&body)
            .await
            .map(|response| response.into_inner())
        {
            Ok(NodeDecommission {
                is_decommissioning,
                is_draining,
                is_live,
                membership,
                node_id: _,
                notes,
                replicas,
            }) => {
                info!(
                    log, "successfully sent cockroach decommission request";
                    "is_decommissioning" => is_decommissioning,
                    "is_draining" => is_draining,
                    "is_live" => is_live,
                    "membership" => ?membership,
                    "notes" => ?notes,
                    "replicas" => replicas,
                );

                return Ok(());
            }

            Err(err) => {
                warn!(
                    log, "failed sending decommission request \
                          (will try other servers)";
                    "err" => InlineErrorChain::new(&err),
                );
                num_admin_addrs_tried += 1;
            }
        }
    }

    bail!(
        "failed to decommission cockroach zone {zone_id} (node {node_id}): \
         failed to contact {num_admin_addrs_tried} admin servers"
    );
}

#[cfg(test)]
mod test {
    use super::*;
    use cockroach_admin_client::types::NodeMembership;
    use httptest::Expectation;
    use httptest::matchers::{all_of, json_decoded, request};
    use httptest::responders::{json_encoded, status_code};
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        BlueprintZoneImageSource, blueprint_zone_type,
    };
    use omicron_common::api::external::Generation;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_types_migrations::latest::inventory::OmicronZoneDataset;
    use std::iter;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test]
    async fn test_clean_up_cockroach_zones(
        cptestctx: &ControlPlaneTestContext,
    ) {
        // The whole point of this test is to check that we send decommissioning
        // requests; enable that. (See the real `clean_up_expunged_zones()` for
        // more context.)
        let decommission_cockroach = true;

        // Test setup boilerplate.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Construct the cockroach zone we're going to try to clean up.
        let any_sled_id = SledUuid::new_v4();
        let crdb_zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::Expunged {
                as_of_generation: Generation::new(),
                ready_for_cleanup: true,
            },
            id: OmicronZoneUuid::new_v4(),
            filesystem_pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
            zone_type: BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb {
                    address: "[::1]:0".parse().unwrap(),
                    dataset: OmicronZoneDataset {
                        pool_name: format!("oxp_{}", Uuid::new_v4())
                            .parse()
                            .unwrap(),
                    },
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        };

        // Start a mock cockroach-admin server.
        let mut mock_admin = httptest::Server::run();

        // Create our fake resolver that will point to our mock server.
        struct FixedResolver(Vec<SocketAddr>);
        impl CleanupResolver for FixedResolver {
            async fn resolve_cockroach_admin_addresses(
                &self,
            ) -> anyhow::Result<impl Iterator<Item = SocketAddr>> {
                Ok(self.0.clone().into_iter())
            }
        }
        let fake_resolver = FixedResolver(vec![mock_admin.addr()]);

        // We haven't yet inserted a mapping from zone ID to cockroach node ID
        // in the db, so trying to clean up the zone should log a warning but
        // otherwise succeed, without attempting to contact our mock admin
        // server. (We don't have a good way to confirm the warning was logged,
        // so we'll just check for an Ok return and no contact to mock_admin.)
        clean_up_expunged_zones_impl(
            &opctx,
            datastore,
            &fake_resolver,
            decommission_cockroach,
            iter::once((any_sled_id, &crdb_zone)),
        )
        .await
        .expect("unknown node ID: no cleanup");
        mock_admin.verify_and_clear();

        // Record a zone ID <-> node ID mapping.
        let crdb_node_id = "test-node";
        datastore
            .set_cockroachdb_node_id(
                &opctx,
                crdb_zone.id,
                crdb_node_id.to_string(),
            )
            .await
            .expect("assigned node ID");

        // Cleaning up the zone should now contact the mock-admin server and
        // attempt to decommission it.
        let add_decommission_expecation =
            move |mock_server: &mut httptest::Server| {
                mock_server.expect(
                    Expectation::matching(all_of![
                        request::method_path("POST", "/node/decommission"),
                        request::body(json_decoded(move |n: &NodeId| {
                            n.node_id == crdb_node_id
                        }))
                    ])
                    .respond_with(json_encoded(
                        NodeDecommission {
                            is_decommissioning: true,
                            is_draining: true,
                            is_live: false,
                            membership: NodeMembership::Decommissioning,
                            node_id: crdb_node_id.to_string(),
                            notes: vec![],
                            replicas: 0,
                        },
                    )),
                );
            };
        add_decommission_expecation(&mut mock_admin);
        clean_up_expunged_zones_impl(
            &opctx,
            datastore,
            &fake_resolver,
            decommission_cockroach,
            iter::once((any_sled_id, &crdb_zone)),
        )
        .await
        .expect("decommissioned test node");
        mock_admin.verify_and_clear();

        // If we have multiple cockroach-admin servers, and the first N of them
        // don't respond successfully, we should keep trying until we find one
        // that does (or they all fail). We'll start with the "all fail" case.
        let mut mock_bad1 = httptest::Server::run();
        let mut mock_bad2 = httptest::Server::run();
        let add_decommission_failure_expecation =
            move |mock_server: &mut httptest::Server| {
                mock_server.expect(
                    Expectation::matching(all_of![
                        request::method_path("POST", "/node/decommission"),
                        request::body(json_decoded(move |n: &NodeId| {
                            n.node_id == crdb_node_id
                        }))
                    ])
                    .respond_with(status_code(503)),
                );
            };
        add_decommission_failure_expecation(&mut mock_bad1);
        add_decommission_failure_expecation(&mut mock_bad2);
        let mut fake_resolver =
            FixedResolver(vec![mock_bad1.addr(), mock_bad2.addr()]);
        let mut err = clean_up_expunged_zones_impl(
            &opctx,
            datastore,
            &fake_resolver,
            decommission_cockroach,
            iter::once((any_sled_id, &crdb_zone)),
        )
        .await
        .expect_err("no successful response should result in failure");
        assert_eq!(err.len(), 1);
        let err = err.pop().unwrap();
        assert_eq!(
            err.to_string(),
            format!(
                "failed to decommission cockroach zone {} \
                 (node {crdb_node_id}): failed to contact 2 admin servers",
                crdb_zone.id
            )
        );
        mock_bad1.verify_and_clear();
        mock_bad2.verify_and_clear();
        mock_admin.verify_and_clear();

        // Now we try again, but put the good server at the end of the list; we
        // should contact the two bad servers, but then succeed on the good one.
        add_decommission_failure_expecation(&mut mock_bad1);
        add_decommission_failure_expecation(&mut mock_bad2);
        add_decommission_expecation(&mut mock_admin);
        fake_resolver.0.push(mock_admin.addr());
        clean_up_expunged_zones_impl(
            &opctx,
            datastore,
            &fake_resolver,
            decommission_cockroach,
            iter::once((any_sled_id, &crdb_zone)),
        )
        .await
        .expect("decommissioned test node");
        mock_bad1.verify_and_clear();
        mock_bad2.verify_and_clear();
        mock_admin.verify_and_clear();
    }
}
