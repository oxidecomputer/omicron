// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures dataset records required by a given blueprint

use crate::Sled;

use anyhow::Context;
use anyhow::anyhow;
use futures::StreamExt;
use futures::stream;
use nexus_db_queries::context::OpContext;
use nexus_types::deployment::BlueprintDatasetsConfig;
use omicron_common::disk::DatasetsConfig;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use slog::info;
use slog::o;
use slog::warn;
use std::collections::BTreeMap;

/// Idempotently ensures that the specified datasets are deployed to the
/// corresponding sleds
pub(crate) async fn deploy_datasets(
    opctx: &OpContext,
    sleds_by_id: &BTreeMap<SledUuid, Sled>,
    sled_configs: &BTreeMap<SledUuid, BlueprintDatasetsConfig>,
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
                db_sled.sled_agent_address(),
                &log,
            );

            let config: DatasetsConfig = config.clone().into_in_service_datasets();
            let result =
                client.datasets_put(&config).await.with_context(
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
                            "Failed to deploy datasets for sled agent";
                            "successfully configured datasets" => successes.len(),
                            "failed dataset configurations" => errs.len(),
                        );
                        for err in &errs {
                            warn!(log, "{err:?}");
                        }
                        return Some(anyhow!(
                            "failure deploying datasets: {:?}",
                            errs
                        ));
                    }

                    info!(
                        log,
                        "Successfully deployed datasets for sled agent";
                        "successfully configured datasets" => successes.len(),
                    );
                    None
                }
            }
        })
        .collect()
        .await;

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_sled_agent_shared::inventory::SledRole;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::BlueprintDatasetConfig;
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use nexus_types::deployment::BlueprintDatasetsConfig;
    use nexus_types::deployment::id_map::IdMap;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::shared::DatasetKind;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::net::SocketAddr;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test]
    async fn test_deploy_datasets(cptestctx: &ControlPlaneTestContext) {
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

        // This is a fully fabricated dataset list for a simulated sled agent.
        //
        // We're testing the validity of the deployment calls here, not of any
        // blueprint.

        let sleds_by_id = BTreeMap::from([(
            sim_sled_agent.id,
            Sled::new(
                sim_sled_agent.id,
                sim_sled_agent_addr,
                SledRole::Scrimlet,
            ),
        )]);

        // Create two datasets which look like they came from the blueprint: One
        // which is in-service, and one which is expunged.
        //
        // During deployment, the in-service dataset should be deployed, but the
        // expunged dataset should be ignored.
        let dataset_id = DatasetUuid::new_v4();
        let expunged_dataset_id = DatasetUuid::new_v4();
        let mut datasets = IdMap::new();
        datasets.insert(BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id: dataset_id,
            pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
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

        let datasets_config =
            BlueprintDatasetsConfig { generation: Generation::new(), datasets };
        let sled_configs =
            BTreeMap::from([(sim_sled_agent.id, datasets_config.clone())]);

        // Give the simulated sled agent a configuration to deploy
        deploy_datasets(&opctx, &sleds_by_id, &sled_configs)
            .await
            .expect("Deploying datasets should have succeeded");

        // Observe the latest configuration stored on the simulated sled agent,
        // and verify that this output matches the "deploy_datasets" input.
        let observed_config = sim_sled_agent.datasets_config_list().unwrap();
        assert_eq!(observed_config, datasets_config.into_in_service_datasets());

        // We expect to see the single in-service dataset we supplied as input.
        assert_eq!(observed_config.datasets.len(), 1,);
        assert!(observed_config.datasets.contains_key(&dataset_id));
    }
}
