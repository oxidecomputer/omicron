// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures dataset records required by a given blueprint

use crate::Sled;

use anyhow::anyhow;
use anyhow::Context;
use futures::stream;
use futures::StreamExt;
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

            let config: DatasetsConfig = config.clone().into();
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

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}
