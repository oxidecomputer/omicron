// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect sled cubby information for support bundles

use crate::app::background::tasks::support_bundle::cache::Cache;
use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;

use anyhow::Context;
use anyhow::bail;
use camino::Utf8Path;
use gateway_client::Client as MgsClient;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpIgnition;
use gateway_types::component::SpType;
use nexus_db_model::Sled;
use omicron_uuid_kinds::GenericUuid;
use serde::Serialize;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use uuid::Uuid;

pub async fn collect(
    collection: &BundleCollection,
    cache: &Cache,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    let (log, request) = (collection.log(), collection.request());

    if !request.include_sled_cubby_info() {
        return Ok(CollectionStepOutput::Skipped);
    }

    let (mgs_client_option, nexus_sleds) = tokio::select! {
        _ = collection.cancelled() => return Ok(CollectionStepOutput::None),
        result = async {
            let mgs = cache.get_or_initialize_mgs_client(&collection).await;
            let sleds = cache.get_or_initialize_all_sleds(&collection).await;
            (mgs, sleds)
        } => result,
    };
    let nexus_sleds = nexus_sleds.map_or(&[][..], |v| v.as_slice());

    let Some(mgs_client) = mgs_client_option else {
        bail!("Could not initialize MGS client");
    };

    write_sled_cubby_info(collection, log, mgs_client, nexus_sleds, dir)
        .await?;

    Ok(CollectionStepOutput::None)
}

// Write a mapping of sled cubbies to serial numbers and UUIDs.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the filesystem via `tokio::fs`.
// HTTP requests to MGS are eagerly cancelled via `select!`.
async fn write_sled_cubby_info(
    collection: &BundleCollection,
    log: &Logger,
    mgs_client: &MgsClient,
    nexus_sleds: &[Sled],
    dir: &Utf8Path,
) -> anyhow::Result<()> {
    #[derive(Serialize)]
    struct SledInfo {
        cubby: Option<u16>,
        uuid: Option<Uuid>,
    }

    let sled_info_collection = async {
        let available_sps = get_available_sps(&mgs_client)
            .await
            .context("failed to get available SPs")?;

        let mut nexus_map: BTreeMap<_, _> = nexus_sleds
            .into_iter()
            .map(|sled| (sled.serial_number(), sled))
            .collect();

        let mut sled_info = BTreeMap::new();
        for sp in available_sps
            .into_iter()
            .filter(|sp| matches!(sp.type_, SpType::Sled))
        {
            match mgs_client.sp_get(&sp.type_, sp.slot).await {
                Ok(s) => {
                    let sp_state = s.into_inner();
                    if let Some(sled) =
                        nexus_map.remove(sp_state.serial_number.as_str())
                    {
                        sled_info.insert(
                            sp_state.serial_number.to_string(),
                            SledInfo {
                                cubby: Some(sp.slot),
                                uuid: Some(*sled.identity.id.as_untyped_uuid()),
                            },
                        );
                    } else {
                        sled_info.insert(
                            sp_state.serial_number.to_string(),
                            SledInfo { cubby: Some(sp.slot), uuid: None },
                        );
                    }
                }
                Err(e) => {
                    error!(log,
                        "Failed to get SP state for sled_info.json";
                        "cubby" => sp.slot,
                        "component" => %sp.type_,
                        "error" => InlineErrorChain::new(&e)
                    );
                }
            }
        }

        // Sleds not returned by MGS.
        for (serial, sled) in nexus_map {
            sled_info.insert(
                serial.to_string(),
                SledInfo {
                    cubby: None,
                    uuid: Some(*sled.identity.id.as_untyped_uuid()),
                },
            );
        }

        Ok::<_, anyhow::Error>(sled_info)
    };
    let sled_info = tokio::select! {
        _ = collection.cancelled() => return Ok(()),
        result = sled_info_collection => result?,
    };

    let json = serde_json::to_string_pretty(&sled_info)
        .context("failed to serialize sled info to JSON")?;
    tokio::fs::write(dir.join("sled_info.json"), json).await?;

    Ok(())
}

pub async fn get_available_sps(
    mgs_client: &MgsClient,
) -> anyhow::Result<Vec<SpIdentifier>> {
    let ignition_info = mgs_client
        .ignition_list()
        .await
        .context("failed to get ignition info from MGS")?
        .into_inner();

    let mut active_sps = Vec::new();
    for info in ignition_info {
        if let SpIgnition::Yes { power, flt_sp, .. } = info.details {
            // Only return SPs that are powered on and are not in a faulted state.
            if power && !flt_sp {
                active_sps.push(info.id);
            }
        }
    }

    Ok(active_sps)
}
