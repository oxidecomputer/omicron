// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manges deployment of Omicron zones to Sled Agents

use anyhow::Context;
use futures::stream;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::OmicronZonesConfig;
use sled_agent_client::Client as SledAgentClient;
use slog::info;
use slog::warn;
use slog::Logger;
use std::collections::BTreeMap;
use uuid::Uuid;

/// Idempotently ensure that the specified Omicron zones are deployed to the
/// corresponding sleds
pub async fn deploy_zones(
    log: &Logger,
    opctx: &OpContext,
    datastore: &DataStore,
    zones: &BTreeMap<Uuid, OmicronZonesConfig>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<_> = stream::iter(zones)
        .filter_map(|(sled_id, config)| async move {
            let client = match sled_client(opctx, datastore, *sled_id).await {
                Ok(client) => client,
                Err(err) => {
                    warn!(log, "{err:#}");
                    return Some(err);
                }
            };
            let result =
                client.omicron_zones_put(&config).await.with_context(|| {
                    format!("Failed to put {config:#?} to sled {sled_id}")
                });

            match result {
                Err(error) => {
                    warn!(log, "{error:#}");
                    Some(error)
                }
                Ok(_) => {
                    info!(
                        log,
                        "Successfully deployed zones for sled agent";
                        "sled_id" => %sled_id,
                        "generation" => config.generation.to_string()
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

// This is a modified copy of the functionality from `nexus/src/app/sled.rs`.
// There's no good way to access this functionality right now since it is a
// method on the `Nexus` type. We want to have a more constrained type we can
// pass into background tasks for this type of functionality, but for now we
// just copy the functionality.
async fn sled_client(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_id: Uuid,
) -> Result<SledAgentClient, anyhow::Error> {
    let (.., sled) = LookupPath::new(opctx, datastore)
        .sled_id(sled_id)
        .fetch()
        .await
        .with_context(|| {
            format!(
                "Failed to create sled_agent::Client for sled_id: {}",
                sled_id
            )
        })?;
    let dur = std::time::Duration::from_secs(60);
    let client = reqwest::ClientBuilder::new()
        .connect_timeout(dur)
        .timeout(dur)
        .build()
        .unwrap();
    Ok(SledAgentClient::new_with_client(
        &format!("http://{}", sled.address()),
        client,
        opctx.log.clone(),
    ))
}
