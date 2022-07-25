// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus taking a snapshot is a perfect application for a saga. There are
//! several services that requests must be sent to, and the snapshot can only be
//! considered completed when all have returned OK and the appropriate database
//! records have been created.
//!
//! # Taking a snapshot of a disk currently attached to an instance
//!
//! A running instance has a corresponding zone that is running a propolis
//! server. That propolis server emulates a disk for the guest OS, and the
//! backend for that virtual disk is a crucible volume that is constructed out
//! of downstairs regions and optionally some sort of read only source (an
//! image, another snapshot, etc). The downstairs regions are concatenated
//! together under a "sub-volume" array over the span of the whole virtual disk
//! (this allows for one 1TB virtual disk to be composed of several smaller
//! downstairs regions for example). Below is an example of a volume where there
//! are two regions (where each region is backed by three downstairs) and a read
//! only parent that points to a URL:
//!
//! ```text
//! Volume {
//!   sub-volumes [
//!      region A {
//!        downstairs @ [fd00:1122:3344:0101::0500]:19000,
//!        downstairs @ [fd00:1122:3344:0101::0501]:19000,
//!        downstairs @ [fd00:1122:3344:0101::0502]:19000,
//!      },
//!      region B {
//!        downstairs @ [fd00:1122:3344:0101::0503]:19000,
//!        downstairs @ [fd00:1122:3344:0101::0504]:19000,
//!        downstairs @ [fd00:1122:3344:0101::0505]:19000,
//!      },
//!   ]
//!
//!   read only parent {
//!     image {
//!       url: "some.url/debian-11.img"
//!   }
//! }
//! ```
//!
//! When the guest OS writes to their disk, those blocks will be written to the
//! sub-volume regions only - no writes or flushes are sent to the read only
//! parent by the volume, only reads. A block read is served from the read only
//! parent if there hasn't been a write to that block in the sub volume,
//! otherwise the block is served from the sub volume. This means all modified
//! blocks for a volume will be written to sub-volume regions, and it's those
//! modifications we want to capture in a snapshot.
//!
//! First, this saga will send a snapshot request to the instance's propolis
//! server. Currently a snapshot request is implemented as a flush with an extra
//! parameter, and this is sent to the volume through the standard IO channels.
//! This flush will be processed by each downstairs in the same job order so
//! that each snapshot will contain the same information. A crucible snapshot is
//! created by taking a ZFS snapshot in each downstairs, so after this operation
//! there will be six new ZFS snapshots, one in each downstair's zfs dataset,
//! each with the same name.
//!
//! Next, this saga will valiate with the crucible agent that the snapshot was
//! created ok, and start a new read-only downstairs process for each snapshot.
//! The validation step isn't strictly required as the flush (with the extra
//! snapshot parameter) wouldn't have returned successfully if there was a
//! problem creating the snapshot, but it never hurts to check :) Once each
//! snapshot has a corresponding read-only downstairs process started for it,
//! the saga will record the addresses that those processes are listening on.
//!
//! The next step is to copy and modify the volume construction request for the
//! running volume in order to create the snapshot's volume construction
//! request. The read-only parent will stay the same, and the sub-volume's
//! region addresses will change to point to the new read-only downstairs
//! process' addresses. This is done by creating a map of old -> new addresses,
//! and passing that into a `replace_sockets` function. This new volume
//! construction request will be used as a read only parent when creating other
//! disks using this snapshot as a disk source.
//!
//! # Taking a snapshot of a detached disk
//!
//! TODO this is currently unimplemented
//!

use super::saga_generate_uuid;
use crate::context::OpContext;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::saga_interface::SagaContext;
use crate::{authn, authz, db};
use anyhow::anyhow;
use crucible_agent_client::{types::RegionId, Client as CrucibleAgentClient};
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::{
    DiskSnapshotRequestBody, InstanceIssueDiskSnapshotRequestBody,
    VolumeConstructionRequest,
};
use slog::warn;
use std::collections::BTreeMap;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

pub const SAGA_NAME: &'static str = "snapshot-create";

lazy_static! {
    pub static ref SAGA_TEMPLATE: Arc<SagaTemplate<SagaSnapshotCreate>> =
        Arc::new(saga_snapshot_create());
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub silo_id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub create_params: params::SnapshotCreate,
}

#[derive(Debug)]
pub struct SagaSnapshotCreate;
impl SagaType for SagaSnapshotCreate {
    type SagaParamsType = Arc<Params>;
    type ExecContextType = Arc<SagaContext>;
}

/// A no-op action used because if saga nodes fail their undo action isn't run!
async fn ssc_noop(
    _sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<(), ActionError> {
    Ok(())
}

fn saga_snapshot_create() -> SagaTemplate<SagaSnapshotCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    // Generate IDs
    template_builder.append(
        "snapshot_id",
        "GenerateSnapshotId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "volume_id",
        "GenerateVolumeId",
        new_action_noop_undo(saga_generate_uuid),
    );

    // Create the Snapshot DB object
    template_builder.append(
        "created_snapshot",
        "CreateSnapshotRecord",
        ActionFunc::new_action(
            ssc_create_snapshot_record,
            ssc_create_snapshot_record_undo,
        ),
    );

    // Send a snapshot request to propolis
    template_builder.append(
        "snapshot_request",
        "SendSnapshotRequest",
        ActionFunc::new_action(
            ssc_send_snapshot_request,
            ssc_send_snapshot_request_undo,
        ),
    );

    // The following saga action iterates over the datasets and regions for a
    // disk and make requests for each tuple, and this violates the saga's
    // mental model where actions should do one thing at a time and be
    // idempotent + atomic. If only a few of the requests succeed, the saga will
    // leave things in a partial state because the undo function of a node is
    // not run when the action fails.
    //
    // Use a noop action and an undo, followed by an action + no undo, to work
    // around this:
    //
    // - [noop, undo function]
    // - [action function, noop]
    //
    // With this, if the action function fails, the undo function will run.

    // Validate with crucible agent and start snapshot downstairs
    template_builder.append(
        "noop_for_replace_sockets_map",
        "NoopForStartRunningSnapshot",
        ActionFunc::new_action(ssc_noop, ssc_start_running_snapshot_undo),
    );
    template_builder.append(
        "replace_sockets_map",
        "StartRunningSnapshot",
        new_action_noop_undo(ssc_start_running_snapshot),
    );

    // Copy and modify the disk volume construction request to point to the new
    // running snapshot
    template_builder.append(
        "created_volume",
        "CreateVolumeRecord",
        ActionFunc::new_action(
            ssc_create_volume_record,
            ssc_create_volume_record_undo,
        ),
    );

    template_builder.append(
        "finalized_snapshot",
        "FinalizeSnapshotRecord",
        new_action_noop_undo(ssc_finalize_snapshot_record),
    );

    template_builder.build()
}

async fn ssc_create_snapshot_record(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<db::model::Snapshot, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    // We admittedly reference the volume before it has been allocated,
    // but this should be acceptable because the snapshot remains in a "Creating"
    // state until the saga has completed.
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;

    debug!(log, "grabbing disk by name {}", params.create_params.disk);

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .disk_name(&params.create_params.disk.clone().into())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    debug!(log, "creating snapshot {} from disk {}", snapshot_id, disk.id());

    let snapshot = db::model::Snapshot {
        identity: db::model::SnapshotIdentity::new(
            snapshot_id,
            params.create_params.identity.clone(),
        ),

        project_id: params.project_id,
        disk_id: disk.id(),
        volume_id,

        gen: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: disk.block_size,
        size: disk.size,
    };

    let (authz_silo, ..) = LookupPath::new(&opctx, &osagactx.datastore())
        .silo_id(params.silo_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let snapshot_created = osagactx
        .datastore()
        .project_create_snapshot(&opctx, &authz_silo, snapshot)
        .await
        .map_err(ActionError::action_failed)?;

    debug!(log, "created snapshot {} ok", snapshot_id);

    Ok(snapshot_created)
}

async fn ssc_create_snapshot_record_undo(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    debug!(log, "deleting snapshot {}", snapshot_id);

    let (.., authz_snapshot, db_snapshot) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .snapshot_id(snapshot_id)
            .fetch_for(authz::Action::Delete)
            .await
            .map_err(ActionError::action_failed)?;

    osagactx
        .datastore()
        .project_delete_snapshot(&opctx, &authz_snapshot, &db_snapshot)
        .await?;

    Ok(())
}

async fn ssc_send_snapshot_request(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    // Find if this disk is attached to an instance
    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .disk_name(&params.create_params.disk.clone().into())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    match disk.runtime().attach_instance_id {
        Some(instance_id) => {
            debug!(log, "disk {} instance is {}", disk.id(), instance_id);

            // Get the instance's sled agent client
            let (.., instance) = LookupPath::new(&opctx, &osagactx.datastore())
                .instance_id(instance_id)
                .fetch()
                .await
                .map_err(ActionError::action_failed)?;

            let sled_agent_client = osagactx
                .nexus()
                .instance_sled(&instance)
                .await
                .map_err(ActionError::action_failed)?;

            debug!(log, "instance {} sled agent created ok", instance_id);

            // Send a snapshot request to propolis through sled agent
            sled_agent_client
                .instance_issue_disk_snapshot_request(
                    &instance.id(),
                    &disk.id(),
                    &InstanceIssueDiskSnapshotRequestBody { snapshot_id },
                )
                .await
                .map_err(|e| e.to_string())
                .map_err(ActionError::action_failed)?;
        }

        None => {
            debug!(log, "disk {} not attached to an instance", disk.id());

            // Grab volume construction request for the disk
            let disk_volume = osagactx
                .datastore()
                .volume_get(disk.volume_id)
                .await
                .map_err(ActionError::action_failed)?;

            debug!(
                log,
                "disk volume construction request {}",
                disk_volume.data()
            );

            let disk_volume_construction_request: VolumeConstructionRequest =
                serde_json::from_str(&disk_volume.data()).map_err(|e| {
                    ActionError::action_failed(Error::internal_error(&format!(
                        "failed to deserialize disk {} volume data: {}",
                        disk.id(),
                        e,
                    )))
                })?;

            // Send the snapshot request to a random sled agent
            let sled_agent_client = osagactx
                .random_sled_client()
                .await
                .map_err(ActionError::action_failed)?
                .ok_or_else(|| {
                    "no sled found when looking for random sled?!".to_string()
                })
                .map_err(ActionError::action_failed)?;

            sled_agent_client
                .issue_disk_snapshot_request(
                    &disk.id(),
                    &DiskSnapshotRequestBody {
                        volume_construction_request:
                            disk_volume_construction_request.clone(),
                        snapshot_id,
                    },
                )
                .await
                .map_err(|e| e.to_string())
                .map_err(ActionError::action_failed)?;
        }
    }

    Ok(())
}

async fn ssc_send_snapshot_request_undo(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .disk_name(&params.create_params.disk.clone().into())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // Delete any snapshots created by this saga
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(disk.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    for (dataset, region) in datasets_and_regions {
        // Create a Crucible agent client
        let url = format!("http://{}", dataset.address());
        let client = CrucibleAgentClient::new(&url);

        // Delete snapshot, it was created by this saga
        debug!(log, "deleting snapshot {} {} {}", url, region.id(), snapshot_id);
        client
            .region_delete_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}

async fn ssc_start_running_snapshot(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<BTreeMap<String, String>, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .disk_name(&params.create_params.disk.clone().into())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // For each dataset and region that makes up the disk, create a map from the
    // region information to the new running snapshot information.
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(disk.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    let mut map: BTreeMap<String, String> = BTreeMap::new();

    for (dataset, region) in datasets_and_regions {
        // Create a Crucible agent client
        let url = format!("http://{}", dataset.address());
        let client = CrucibleAgentClient::new(&url);

        debug!(log, "dataset {:?} region {:?} url {}", dataset, region, url);

        // Validate with the Crucible agent that the snapshot exists
        let crucible_region = client
            .region_get(&RegionId(region.id().to_string()))
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;

        debug!(log, "crucible region {:?}", crucible_region);

        let crucible_snapshot = client
            .region_get_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;

        debug!(log, "crucible snapshot {:?}", crucible_snapshot);

        // Start the snapshot running
        let crucible_running_snapshot = client
            .region_run_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;

        debug!(log, "crucible running snapshot {:?}", crucible_running_snapshot);

        // Map from the region to the snapshot
        let region_addr = format!(
            "{}",
            dataset.address_with_port(crucible_region.port_number)
        );
        let snapshot_addr = format!(
            "{}",
            dataset.address_with_port(crucible_running_snapshot.port_number)
        );
        debug!(log, "map {} to {}", region_addr, snapshot_addr);
        map.insert(region_addr, snapshot_addr);
    }

    Ok(map)
}

async fn ssc_start_running_snapshot_undo(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .disk_name(&params.create_params.disk.clone().into())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // Delete any running snapshots created by this saga
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(disk.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    for (dataset, region) in datasets_and_regions {
        // Create a Crucible agent client
        let url = format!("http://{}", dataset.address());
        let client = CrucibleAgentClient::new(&url);

        // Delete running snapshot
        debug!(
            log,
            "deleting running snapshot {} {} {}",
            url,
            region.id(),
            snapshot_id
        );
        client
            .region_delete_running_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}

async fn ssc_create_volume_record(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<db::model::Volume, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;

    // For a snapshot, copy the volume construction request at the time the
    // snapshot was taken.
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .disk_name(&params.create_params.disk.clone().into())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let disk_volume = osagactx
        .datastore()
        .volume_get(disk.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    debug!(log, "disk volume construction request {}", disk_volume.data());

    let disk_volume_construction_request =
        serde_json::from_str(&disk_volume.data()).map_err(|e| {
            ActionError::action_failed(Error::internal_error(&format!(
                "failed to deserialize disk {} volume data: {}",
                disk.id(),
                e,
            )))
        })?;

    // The volume construction request must then be modified to point to the
    // read-only crucible agent downstairs (corresponding to this snapshot)
    // launched through this saga.
    let replace_sockets_map =
        sagactx.lookup::<BTreeMap<String, String>>("replace_sockets_map")?;
    let volume_construction_request: VolumeConstructionRequest =
        replace_sockets(
            &disk_volume_construction_request,
            &replace_sockets_map,
        )
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    // Create the volume record for this snapshot
    let volume_data: String =
        serde_json::to_string(&volume_construction_request).map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;
    debug!(log, "snapshot volume construction request {}", volume_data);
    let volume = db::model::Volume::new(volume_id, volume_data);

    let volume_created = osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    debug!(log, "volume {} created ok", volume_id);

    Ok(volume_created)
}

async fn ssc_create_volume_record_undo(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    debug!(log, "deleting volume {}", volume_id);
    osagactx.datastore().volume_delete(volume_id).await?;

    Ok(())
}

async fn ssc_finalize_snapshot_record(
    sagactx: ActionContext<SagaSnapshotCreate>,
) -> Result<db::model::Snapshot, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    debug!(log, "snapshot final lookup...");

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    let (.., authz_snapshot) = LookupPath::new(&opctx, &osagactx.datastore())
        .snapshot_id(snapshot_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    debug!(log, "snapshot final lookup ok");

    let snapshot = osagactx
        .datastore()
        .project_snapshot_update_state(
            &opctx,
            &authz_snapshot,
            db::model::Generation::new(),
            db::model::SnapshotState::Ready,
        )
        .await
        .map_err(ActionError::action_failed)?;

    debug!(log, "snapshot finalized!");

    Ok(snapshot)
}

fn replace_sockets(
    input: &VolumeConstructionRequest,
    map: &BTreeMap<String, String>,
) -> anyhow::Result<VolumeConstructionRequest> {
    match input {
        VolumeConstructionRequest::Volume {
            id,
            block_size,
            sub_volumes,
            read_only_parent,
        } => Ok(VolumeConstructionRequest::Volume {
            id: *id,
            block_size: *block_size,
            sub_volumes: sub_volumes
                .iter()
                .map(|subvol| -> anyhow::Result<VolumeConstructionRequest> {
                    replace_sockets(&subvol, map)
                })
                .collect::<anyhow::Result<Vec<VolumeConstructionRequest>>>()?,
            read_only_parent: read_only_parent.clone(),
        }),

        VolumeConstructionRequest::Url { id, block_size, url } => {
            Ok(VolumeConstructionRequest::Url {
                id: *id,
                block_size: *block_size,
                url: url.clone(),
            })
        }

        VolumeConstructionRequest::Region { block_size, opts, gen } => {
            let mut opts = opts.clone();

            for target in &mut opts.target {
                *target = map
                    .get(target)
                    .ok_or_else(|| {
                        anyhow!("target {} not found in map!", target)
                    })?
                    .clone();
            }

            Ok(VolumeConstructionRequest::Region {
                block_size: *block_size,
                opts,
                gen: *gen,
            })
        }

        VolumeConstructionRequest::File { id, block_size, path } => {
            Ok(VolumeConstructionRequest::File {
                id: *id,
                block_size: *block_size,
                path: path.clone(),
            })
        }
    }
}
