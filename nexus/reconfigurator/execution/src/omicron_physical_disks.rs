// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manges deployment of Omicron physical disks to Sled Agents.

use crate::Sled;
use anyhow::anyhow;
use anyhow::Context;
use futures::stream;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use slog::info;
use slog::o;
use slog::warn;
use std::collections::BTreeMap;
use std::num::NonZeroU32;

/// Idempotently ensure that the specified Omicron disks are deployed to the
/// corresponding sleds
pub(crate) async fn deploy_disks(
    opctx: &OpContext,
    sleds_by_id: &BTreeMap<SledUuid, Sled>,
    sled_configs: &BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<_> = stream::iter(sled_configs)
        .filter_map(|(sled_id, config)| async move {
            let log = opctx.log.new(o!(
                "sled_id" => sled_id.to_string(),
                "generation" => config.generation.to_string(),
            ));

            let db_sled = match sleds_by_id.get(&sled_id) {
                Some(sled) => sled,
                None => {
                    let err = anyhow!("sled not found in db list: {}", sled_id);
                    warn!(log, "{err:#}");
                    return Some(err);
                }
            };

            let client = nexus_networking::sled_client_from_address(
                sled_id.into_untyped_uuid(),
                db_sled.sled_agent_address,
                &log,
            );
            let result =
                client.omicron_physical_disks_put(&config).await.with_context(
                    || format!("Failed to put {config:#?} to sled {sled_id}"),
                );
            match result {
                Err(error) => {
                    warn!(log, "{error:#}");
                    Some(error)
                }
                Ok(result) => {
                    let (errs, successes): (Vec<_>, Vec<_>) = result
                        .into_inner()
                        .status
                        .into_iter()
                        .partition(|status| status.err.is_some());

                    if !errs.is_empty() {
                        warn!(
                            log,
                            "Failed to deploy storage for sled agent";
                            "successfully configured disks" => successes.len(),
                            "failed disk configurations" => errs.len(),
                        );
                        for err in &errs {
                            warn!(log, "{err:?}");
                        }
                        return Some(anyhow!(
                            "failure deploying disks: {:?}",
                            errs
                        ));
                    }

                    info!(
                        log,
                        "Successfully deployed storage for sled agent";
                        "successfully configured disks" => successes.len(),
                    );
                    None
                }
            }
        })
        .collect()
        .await;

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

const MAX_CLEANUP_BATCH: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

/// Decommissions all disks which are currently expunged
pub(crate) async fn decommission_expunged_disks(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), Vec<anyhow::Error>> {
    datastore
        .physical_disk_decommission_all_expunged(&opctx)
        .await
        .map_err(|e| vec![anyhow!(e)])?;
    Ok(())
}

/// Cleans up old database state from decommissioned disks.
///
/// This involves removing zpools and datasets allocated
/// to disks which are now decommissioned.
pub(crate) async fn clean_up_decommissioned_disks(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), Vec<anyhow::Error>> {
    let log = &opctx.log;

    let mut errors = vec![];
    let mut paginator = Paginator::new(MAX_CLEANUP_BATCH);
    while let Some(p) = paginator.next() {
        let maybe_batch = datastore
            .zpool_on_decommissioned_disk_list(opctx, &p.current_pagparams())
            .await;
        let batch = match maybe_batch {
            Ok(batch) => batch,
            Err(e) => {
                slog::error!(
                    log,
                    "list decommissioned zpools query failed: {e}"
                );
                errors.push(anyhow!(e));
                return Err(errors);
            }
        };
        paginator = p.found_batch(&batch, &|z| z.id());

        let mut batch = batch.into_iter();
        if let Some(zpool) = batch.next() {
            info!(log,
                "Found an out-of-service zpool";
                "zpool_id" => zpool.id().to_string(),
            );

            let zpool_id = ZpoolUuid::from_untyped_uuid(zpool.id());

            if let Err(e) = datastore
                .zpool_delete_self_and_all_datasets(opctx, zpool_id)
                .await
            {
                slog::error!(
                    log,
                    "failed to clean up decommissioned physical disk: {e}"
                );
                errors.push(anyhow!(e));
                continue;
            }
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::deploy_disks;
    use crate::Sled;
    use httptest::matchers::{all_of, json_decoded, request};
    use httptest::responders::json_encoded;
    use httptest::responders::status_code;
    use httptest::Expectation;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        Blueprint, BlueprintPhysicalDiskConfig, BlueprintPhysicalDisksConfig,
        BlueprintTarget, CockroachDbPreserveDowngrade,
    };
    use omicron_common::api::external::Generation;
    use omicron_common::disk::DiskIdentity;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    fn create_blueprint(
        blueprint_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
    ) -> (BlueprintTarget, Blueprint) {
        let id = Uuid::new_v4();
        (
            BlueprintTarget {
                target_id: id,
                enabled: true,
                time_made_target: chrono::Utc::now(),
            },
            Blueprint {
                id,
                blueprint_zones: BTreeMap::new(),
                blueprint_disks,
                sled_state: BTreeMap::new(),
                cockroachdb_setting_preserve_downgrade:
                    CockroachDbPreserveDowngrade::DoNotModify,
                parent_blueprint_id: None,
                internal_dns_version: Generation::new(),
                external_dns_version: Generation::new(),
                cockroachdb_fingerprint: String::new(),
                time_created: chrono::Utc::now(),
                creator: "test".to_string(),
                comment: "test blueprint".to_string(),
            },
        )
    }

    #[nexus_test]
    async fn test_deploy_omicron_disks(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Create some fake sled-agent servers to respond to disk puts and add
        // sleds to CRDB.
        let mut s1 = httptest::Server::run();
        let mut s2 = httptest::Server::run();
        let sled_id1 = SledUuid::new_v4();
        let sled_id2 = SledUuid::new_v4();
        let sleds_by_id: BTreeMap<SledUuid, Sled> =
            [(sled_id1, &s1), (sled_id2, &s2)]
                .into_iter()
                .map(|(sled_id, server)| {
                    let SocketAddr::V6(addr) = server.addr() else {
                        panic!("Expected Ipv6 address. Got {}", server.addr());
                    };
                    let sled = Sled {
                        id: sled_id,
                        sled_agent_address: addr,
                        is_scrimlet: false,
                    };
                    (sled_id, sled)
                })
                .collect();

        // Get a success result back when the blueprint has an empty set of
        // disks.
        let (_, blueprint) = create_blueprint(BTreeMap::new());
        deploy_disks(&opctx, &sleds_by_id, &blueprint.blueprint_disks)
            .await
            .expect("failed to deploy no disks");

        // Disks are updated in a particular order, but each request contains
        // the full set of disks that must be running.
        // See `rack_setup::service::ServiceInner::run` for more details.
        fn make_disks() -> BlueprintPhysicalDisksConfig {
            BlueprintPhysicalDisksConfig {
                generation: Generation::new(),
                disks: vec![BlueprintPhysicalDiskConfig {
                    identity: DiskIdentity {
                        vendor: "test-vendor".to_string(),
                        serial: "test-serial".to_string(),
                        model: "test-model".to_string(),
                    },
                    id: Uuid::new_v4(),
                    pool_id: ZpoolUuid::new_v4(),
                }],
            }
        }

        // Create a blueprint with only one disk for both servers
        // We reuse the same `OmicronDisksConfig` because the details don't
        // matter for this test.
        let disks1 = make_disks();
        let disks2 = make_disks();
        let (_, blueprint) = create_blueprint(BTreeMap::from([
            (sled_id1, disks1.clone()),
            (sled_id2, disks2.clone()),
        ]));

        // Set expectations for the initial requests sent to the fake
        // sled-agents.
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(all_of![
                    request::method_path("PUT", "/omicron-physical-disks",),
                    // Our generation number should be 1 and there should
                    // be only a single disk.
                    request::body(json_decoded(
                        |c: &BlueprintPhysicalDisksConfig| {
                            c.generation == 1u32.into() && c.disks.len() == 1
                        }
                    ))
                ])
                .respond_with(json_encoded(
                    sled_agent_client::types::DisksManagementResult {
                        status: vec![],
                    },
                )),
            );
        }

        // Execute it.
        deploy_disks(&opctx, &sleds_by_id, &blueprint.blueprint_disks)
            .await
            .expect("failed to deploy initial disks");

        s1.verify_and_clear();
        s2.verify_and_clear();

        // Do it again. This should trigger the same request.
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(request::method_path(
                    "PUT",
                    "/omicron-physical-disks",
                ))
                .respond_with(json_encoded(
                    sled_agent_client::types::DisksManagementResult {
                        status: vec![],
                    },
                )),
            );
        }
        deploy_disks(&opctx, &sleds_by_id, &blueprint.blueprint_disks)
            .await
            .expect("failed to deploy same disks");
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Take another lap, but this time, have one server fail the request and
        // try again.
        s1.expect(
            Expectation::matching(request::method_path(
                "PUT",
                "/omicron-physical-disks",
            ))
            .respond_with(json_encoded(
                sled_agent_client::types::DisksManagementResult {
                    status: vec![],
                },
            )),
        );
        s2.expect(
            Expectation::matching(request::method_path(
                "PUT",
                "/omicron-physical-disks",
            ))
            .respond_with(status_code(500)),
        );

        let errors =
            deploy_disks(&opctx, &sleds_by_id, &blueprint.blueprint_disks)
                .await
                .expect_err("unexpectedly succeeded in deploying disks");

        println!("{:?}", errors);
        assert_eq!(errors.len(), 1);
        assert!(errors[0]
            .to_string()
            .starts_with("Failed to put OmicronPhysicalDisksConfig"));
        s1.verify_and_clear();
        s2.verify_and_clear();

        // We can also observe "partial failures", where the HTTP-evel response
        // is successful, but it indicates that the disk provisioning ran into
        // problems.
        s1.expect(
            Expectation::matching(request::method_path(
                "PUT",
                "/omicron-physical-disks",
            ))
            .respond_with(json_encoded(
                sled_agent_client::types::DisksManagementResult {
                    status: vec![],
                },
            )),
        );
        s2.expect(
            Expectation::matching(request::method_path(
                "PUT",
                "/omicron-physical-disks",
            ))
            .respond_with(json_encoded(sled_agent_client::types::DisksManagementResult {
                status: vec![
                    sled_agent_client::types::DiskManagementStatus {
                        identity: omicron_common::disk::DiskIdentity {
                            vendor: "v".to_string(),
                            serial: "s".to_string(),
                            model: "m".to_string(),
                        },

                        // This error could occur if a disk is removed
                        err: Some(sled_agent_client::types::DiskManagementError::NotFound),
                    }
                ]
            })),
        );

        let errors =
            deploy_disks(&opctx, &sleds_by_id, &blueprint.blueprint_disks)
                .await
                .expect_err("unexpectedly succeeded in deploying disks");

        println!("{:?}", errors);
        assert_eq!(errors.len(), 1);
        assert!(
            errors[0].to_string().starts_with("failure deploying disks"),
            "{}",
            errors[0].to_string()
        );
        s1.verify_and_clear();
        s2.verify_and_clear();
    }
}
