// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions common to interacting with the Crucible agent in saga actions

use super::*;

use crate::Nexus;
use crucible_agent_client::types::Region;
use crucible_agent_client::types::RegionId;
use crucible_agent_client::Client as CrucibleAgentClient;
use crucible_pantry_client::types::VolumeConstructionRequest;
use internal_dns::ServiceName;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::Error;
use omicron_common::retry_until_known_result;
use std::net::SocketAddrV6;

// Common Pantry operations

pub(crate) async fn get_pantry_address(
    nexus: &Arc<Nexus>,
) -> Result<SocketAddrV6, ActionError> {
    nexus
        .resolver()
        .await
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

/// Get a Crucible Agent's Region
pub(crate) async fn get_region_from_agent(
    agent_address: &SocketAddrV6,
    region_id: Uuid,
) -> Result<Region, Error> {
    let url = format!("http://{}", agent_address);
    let client = CrucibleAgentClient::new(&url);

    let result = client.region_get(&RegionId(region_id.to_string())).await;

    match result {
        Ok(v) => Ok(v.into_inner()),

        Err(e) => match e {
            crucible_agent_client::Error::ErrorResponse(rv) => {
                match rv.status() {
                    http::StatusCode::NOT_FOUND => {
                        Err(Error::non_resourcetype_not_found(format!(
                            "{region_id} not found"
                        )))
                    }

                    status if status.is_client_error() => {
                        Err(Error::invalid_request(&rv.message))
                    }

                    _ => Err(Error::internal_error(&rv.message)),
                }
            }

            _ => Err(Error::internal_error(
                "unexpected failure during `region_get`",
            )),
        },
    }
}
