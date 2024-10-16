// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions common to interacting with the Crucible agent in saga actions

use super::*;

use crate::Nexus;
use crucible_pantry_client::types::VolumeConstructionRequest;
use internal_dns_types::names::ServiceName;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::Error;
use omicron_common::progenitor_operation_retry::ProgenitorOperationRetry;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddrV6;

mod pantry_pool;

pub(crate) use pantry_pool::make_pantry_connection_pool;
pub(crate) use pantry_pool::PooledPantryClient;

// Common Pantry operations

pub(crate) async fn get_pantry_address(
    nexus: &Nexus,
) -> Result<SocketAddrV6, ActionError> {
    let client = nexus.pantry_connection_pool().claim().await.map_err(|e| {
        ActionError::action_failed(format!(
            "failed to claim pantry client from pool: {}",
            InlineErrorChain::new(&e)
        ))
    })?;
    Ok(client.address())
}

// Helper function for attach/detach below: we retry as long as the pantry isn't
// gone, and we detect "gone" by seeing whether the pantry address we've chosen
// is still present when we resolve all the crucible pantry records in DNS.
//
// This function never returns an error because it's expected to be used with
// `ProgenitorOperationRetry`, which treats an error in the "gone check" as a
// fatal error. We don't want to amplify failures: if something is wrong with
// DNS, we can't go back and choose another pantry anyway, so we'll just keep
// retrying until DNS comes back. All that to say: a failure to resolve DNS is
// treated as "the pantry is not gone".
pub(super) async fn is_pantry_gone(
    nexus: &Nexus,
    pantry_address: SocketAddrV6,
    log: &Logger,
) -> bool {
    let all_pantry_dns_entries = match nexus
        .resolver()
        .lookup_all_socket_v6(ServiceName::CruciblePantry)
        .await
    {
        Ok(entries) => entries,
        Err(err) => {
            warn!(
                log, "Failed to resolve Crucible pantry in DNS";
                InlineErrorChain::new(&err),
            );
            return false;
        }
    };
    !all_pantry_dns_entries.contains(&pantry_address)
}

pub(crate) async fn call_pantry_attach_for_disk(
    log: &slog::Logger,
    opctx: &OpContext,
    nexus: &Nexus,
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

    let attach_operation =
        || async { client.attach(&disk_id.to_string(), &attach_request).await };
    let gone_check =
        || async { Ok(is_pantry_gone(nexus, pantry_address, log).await) };

    ProgenitorOperationRetry::new(attach_operation, gone_check)
        .run(log)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "pantry attach failed: {}",
                InlineErrorChain::new(&e)
            ))
        })?;

    Ok(())
}

pub(crate) async fn call_pantry_detach_for_disk(
    nexus: &Nexus,
    log: &slog::Logger,
    disk_id: Uuid,
    pantry_address: SocketAddrV6,
) -> Result<(), ActionError> {
    let endpoint = format!("http://{}", pantry_address);

    info!(log, "sending detach for disk {disk_id} to endpoint {endpoint}");

    let client = crucible_pantry_client::Client::new(&endpoint);

    let detach_operation =
        || async { client.detach(&disk_id.to_string()).await };
    let gone_check =
        || async { Ok(is_pantry_gone(nexus, pantry_address, log).await) };

    ProgenitorOperationRetry::new(detach_operation, gone_check)
        .run(log)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "pantry detach failed: {}",
                InlineErrorChain::new(&e)
            ))
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
