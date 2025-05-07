// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of Omicron sled configuration to Sled Agents

use crate::Sled;
use anyhow::Context;
use anyhow::anyhow;
use futures::StreamExt;
use futures::stream;
use nexus_db_queries::context::OpContext;
use nexus_sled_agent_shared::inventory::OmicronSledConfigResult;
use nexus_types::deployment::BlueprintSledConfig;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;
use slog::info;
use slog::warn;
use std::collections::BTreeMap;
use update_engine::merge_anyhow_list;

/// Idempotently ensure that the specified Omicron sled configs are deployed to
/// the corresponding sleds
pub(crate) async fn deploy_sled_configs(
    opctx: &OpContext,
    sleds_by_id: &BTreeMap<SledUuid, Sled>,
    sled_configs: &BTreeMap<SledUuid, BlueprintSledConfig>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<_> = stream::iter(sled_configs)
        .filter_map(async |(sled_id, config)| {
            let log = opctx.log.new(slog::o!(
                "sled_id" => sled_id.to_string(),
                "generation" => i64::from(&config.sled_agent_generation),
            ));

            let db_sled = match sleds_by_id.get(&sled_id) {
                Some(sled) => sled,
                None => {
                    if config.are_all_items_expunged() {
                        info!(
                            log,
                            "Skipping config deployment to expunged sled";
                            "sled_id" => %sled_id
                        );
                        return None;
                    }
                    let err = anyhow!("sled not found in db list: {}", sled_id);
                    warn!(log, "{err:#}");
                    return Some(err);
                }
            };

            let client = nexus_networking::sled_client_from_address(
                sled_id.into_untyped_uuid(),
                db_sled.sled_agent_address(),
                &log,
            );

            let config = config.clone().into_in_service_sled_config();
            let result =
                client.omicron_config_put(&config).await.with_context(|| {
                    format!("Failed to put {config:#?} to sled {sled_id}")
                });
            match result {
                Err(error) => {
                    warn!(log, "{error:#}");
                    Some(error)
                }
                Ok(result) => {
                    parse_config_result(result.into_inner(), &log).err()
                }
            }
        })
        .collect()
        .await;

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

fn parse_config_result(
    result: OmicronSledConfigResult,
    log: &Logger,
) -> anyhow::Result<()> {
    let (disk_errs, disk_successes): (Vec<_>, Vec<_>) =
        result.disks.into_iter().partition(|status| status.err.is_some());

    if !disk_errs.is_empty() {
        warn!(
            log,
            "Failed to deploy disks for sled agent";
            "successfully configured disks" => disk_successes.len(),
            "failed disk configurations" => disk_errs.len(),
        );
        for err in &disk_errs {
            warn!(log, "{err:?}");
        }
        return Err(merge_anyhow_list(disk_errs.into_iter().map(|status| {
            anyhow!(
                "failed to deploy disk {:?}: {:#}",
                status.identity,
                // `disk_errs` was partitioned by `status.err.is_some()`, so
                // this is safe to unwrap.
                status.err.unwrap(),
            )
        })));
    }

    let (dataset_errs, dataset_successes): (Vec<_>, Vec<_>) =
        result.datasets.into_iter().partition(|status| status.err.is_some());

    if !dataset_errs.is_empty() {
        warn!(
            log,
            "Failed to deploy datasets for sled agent";
            "successfully configured datasets" => dataset_successes.len(),
            "failed dataset configurations" => dataset_errs.len(),
        );
        for err in &dataset_errs {
            warn!(log, "{err:?}");
        }
        return Err(merge_anyhow_list(dataset_errs.into_iter().map(
            |status| {
                anyhow!(
                    "failed to deploy dataset {}: {:#}",
                    status.dataset_name.full_name(),
                    // `dataset_errs` was partitioned by `status.err.is_some()`,
                    // so this is safe to unwrap.
                    status.err.unwrap(),
                )
            },
        )));
    }

    info!(
        log,
        "Successfully deployed config to sled agent";
        "successfully configured disks" => disk_successes.len(),
        "successfully configured datasets" => dataset_successes.len(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use id_map::IdMap;
    use nexus_sled_agent_shared::inventory::OmicronZonesConfig;
    use nexus_sled_agent_shared::inventory::SledRole;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::BlueprintDatasetConfig;
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use nexus_types::deployment::BlueprintPhysicalDiskConfig;
    use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledProvisionPolicy;
    use nexus_types::external_api::views::SledState;
    use omicron_common::address::REPO_DEPOT_PORT;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::shared::DatasetKind;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_common::disk::DatasetsConfig;
    use omicron_common::disk::DiskIdentity;
    use omicron_common::disk::OmicronPhysicalDisksConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::net::SocketAddr;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test]
    async fn test_deploy_config(cptestctx: &ControlPlaneTestContext) {
        // Set up.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let sim_sled_agent_addr = match cptestctx.sled_agents[0].local_addr() {
            SocketAddr::V6(addr) => addr,
            _ => panic!("Unexpected address type for sled agent (wanted IPv6)"),
        };
        let sim_sled_agent = &cptestctx.sled_agents[0].sled_agent();
        let sim_sled_agent_config_generation =
            sim_sled_agent.omicron_zones_list().generation;

        let sleds_by_id = BTreeMap::from([(
            sim_sled_agent.id,
            Sled::new(
                sim_sled_agent.id,
                SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::Provisionable,
                },
                sim_sled_agent_addr,
                REPO_DEPOT_PORT,
                SledRole::Scrimlet,
            ),
        )]);

        // This is a fully fabricated dataset list for a simulated sled agent.
        //
        // We're testing the validity of the deployment calls here, not of any
        // blueprint.

        // Create two disks which look like they came from the blueprint: One
        // which is in-service, and one which is expunged.
        //
        // During deployment, the in-service disk should be deployed, but the
        // expunged disk should be ignored.
        let disk_id = PhysicalDiskUuid::new_v4();
        let disk_pool_id = ZpoolUuid::new_v4();
        let expunged_disk_id = PhysicalDiskUuid::new_v4();
        let mut disks = IdMap::new();
        disks.insert(BlueprintPhysicalDiskConfig {
            disposition: BlueprintPhysicalDiskDisposition::InService,
            identity: DiskIdentity {
                vendor: "test-vendor".to_string(),
                model: "test-model".to_string(),
                serial: disk_id.to_string(),
            },
            id: disk_id,
            pool_id: disk_pool_id,
        });
        disks.insert(BlueprintPhysicalDiskConfig {
            disposition: BlueprintPhysicalDiskDisposition::Expunged {
                as_of_generation: Generation::new(),
                ready_for_cleanup: false,
            },
            identity: DiskIdentity {
                vendor: "test-vendor".to_string(),
                model: "test-model".to_string(),
                serial: expunged_disk_id.to_string(),
            },
            id: expunged_disk_id,
            pool_id: ZpoolUuid::new_v4(),
        });

        // Create two datasets which look like they came from the blueprint: One
        // which is in-service, and one which is expunged.
        //
        // During deployment, the in-service dataset should be deployed, but the
        // expunged dataset should be ignored.
        let dataset_id = DatasetUuid::new_v4();
        let dataset_pool = ZpoolName::new_external(disk_pool_id);
        let expunged_dataset_id = DatasetUuid::new_v4();
        let mut datasets = IdMap::new();
        datasets.insert(BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id: dataset_id,
            pool: dataset_pool,
            kind: DatasetKind::Crucible,
            address: None,
            quota: None,
            reservation: None,
            compression: CompressionAlgorithm::Off,
        });
        datasets.insert(BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::Expunged,
            id: expunged_dataset_id,
            pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
            kind: DatasetKind::Crucible,
            address: None,
            quota: None,
            reservation: None,
            compression: CompressionAlgorithm::Off,
        });

        // Create two zones which look like they came from the blueprint: One
        // which is in-service, and one which is expunged.
        //
        // During deployment, the in-service zone should be deployed, but the
        // expunged zone should be ignored.
        let zone_id = OmicronZoneUuid::new_v4();
        let mut zones = IdMap::new();
        zones.insert(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: dataset_pool,
            zone_type: BlueprintZoneType::Oximeter(
                blueprint_zone_type::Oximeter {
                    address: "[::1]:0".parse().unwrap(),
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });
        zones.insert(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::Expunged {
                as_of_generation: Generation::new(),
                ready_for_cleanup: false,
            },
            id: OmicronZoneUuid::new_v4(),
            filesystem_pool: dataset_pool,
            zone_type: BlueprintZoneType::Oximeter(
                blueprint_zone_type::Oximeter {
                    address: "[::1]:0".parse().unwrap(),
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });

        let sled_config = BlueprintSledConfig {
            state: SledState::Active,
            sled_agent_generation: sim_sled_agent_config_generation.next(),
            disks,
            datasets,
            zones,
        };
        let sled_configs =
            [(sim_sled_agent.id, sled_config.clone())].into_iter().collect();

        // Give the simulated sled agent a configuration to deploy
        deploy_sled_configs(&opctx, &sleds_by_id, &sled_configs)
            .await
            .expect("Deploying datasets should have succeeded");

        // Observe the latest configuration stored on the simulated sled agent,
        // and verify that this output matches the input.
        let observed_disks =
            sim_sled_agent.omicron_physical_disks_list().unwrap();
        let observed_datasets = sim_sled_agent.datasets_config_list().unwrap();
        let observed_zones = sim_sled_agent.omicron_zones_list();

        let in_service_config =
            sled_config.clone().into_in_service_sled_config();
        assert_eq!(
            observed_disks,
            OmicronPhysicalDisksConfig {
                generation: in_service_config.generation,
                disks: in_service_config.disks.into_iter().collect(),
            }
        );
        assert_eq!(
            observed_datasets,
            DatasetsConfig {
                generation: in_service_config.generation,
                datasets: in_service_config
                    .datasets
                    .into_iter()
                    .map(|d| (d.id, d))
                    .collect(),
            }
        );
        assert_eq!(
            observed_zones,
            OmicronZonesConfig {
                generation: in_service_config.generation,
                zones: in_service_config.zones.into_iter().collect(),
            }
        );

        // We expect to see each single in-service item we supplied as input.
        assert_eq!(observed_disks.disks.len(), 1);
        assert_eq!(observed_disks.disks[0].id, disk_id);
        assert_eq!(observed_datasets.datasets.len(), 1);
        assert!(observed_datasets.datasets.contains_key(&dataset_id));
        assert_eq!(observed_zones.zones.len(), 1);
        assert_eq!(observed_zones.zones[0].id, zone_id);
    }
}
