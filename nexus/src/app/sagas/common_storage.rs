// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions common to interacting with the Crucible agent in saga actions

use super::*;

use crate::Nexus;
use crucible_pantry_client::types::VolumeConstructionRequest;
use internal_dns::ServiceName;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::Error;
use omicron_common::retry_until_known_result;
use slog::Logger;
use std::net::SocketAddrV6;

// Common Pantry operations

pub(crate) async fn get_pantry_address(
    nexus: &Arc<Nexus>,
) -> Result<SocketAddrV6, ActionError> {
    nexus
        .resolver()
        .lookup_socket_v6(ServiceName::CruciblePantry)
        .await
        .map_err(|e| e.to_string())
        .map_err(ActionError::action_failed)
}

pub(crate) async fn call_pantry_attach_for_disk(
    log: &slog::Logger,
    opctx: &OpContext,
    nexus: &Arc<Nexus>,
    disk_id: Uuid,
    pantry_address: SocketAddrV6,
) -> Result<(), ActionError> {
    let endpoint = format!("http://{}", pantry_address);

    let (.., disk) = LookupPath::new(opctx, &nexus.datastore())
        .disk_id(disk_id)
        .fetch_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    let disk_volume = nexus
        .datastore()
        .volume_checkout(
            disk.volume_id,
            db::datastore::VolumeCheckoutReason::Pantry,
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        log,
        "sending attach for disk {disk_id} volume {} to endpoint {endpoint}",
        disk.volume_id,
    );

    let volume_construction_request: VolumeConstructionRequest =
        serde_json::from_str(&disk_volume.data()).map_err(|e| {
            ActionError::action_failed(Error::internal_error(&format!(
                "failed to deserialize disk {} volume data: {}",
                disk.id(),
                e,
            )))
        })?;

    let client = crucible_pantry_client::Client::new(&endpoint);

    let attach_request = crucible_pantry_client::types::AttachRequest {
        volume_construction_request,
    };

    retry_until_known_result(log, || async {
        client.attach(&disk_id.to_string(), &attach_request).await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(format!("pantry attach failed with {:?}", e))
    })?;

    Ok(())
}

pub(crate) async fn call_pantry_detach_for_disk(
    log: &slog::Logger,
    disk_id: Uuid,
    pantry_address: SocketAddrV6,
) -> Result<(), ActionError> {
    let endpoint = format!("http://{}", pantry_address);

    info!(log, "sending detach for disk {disk_id} to endpoint {endpoint}");

    let client = crucible_pantry_client::Client::new(&endpoint);

    retry_until_known_result(log, || async {
        client.detach(&disk_id.to_string()).await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(format!("pantry detach failed with {:?}", e))
    })?;

    Ok(())
}

pub(crate) fn find_only_new_region(
    log: &Logger,
    existing_datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
    new_datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
) -> Option<(db::model::Dataset, db::model::Region)> {
    // Only filter on whether or not a Region is in the existing list! Datasets
    // can change values (like size_used) if this saga interleaves with other
    // saga runs of the same type.
    let mut dataset_and_region: Vec<(db::model::Dataset, db::model::Region)> =
        new_datasets_and_regions
            .into_iter()
            .filter(|(_, r)| {
                !existing_datasets_and_regions.iter().any(|(_, er)| er == r)
            })
            .collect();

    if dataset_and_region.len() != 1 {
        error!(
            log,
            "find_only_new_region saw dataset_and_region len {}: {:?}",
            dataset_and_region.len(),
            dataset_and_region,
        );

        None
    } else {
        dataset_and_region.pop()
    }
}
