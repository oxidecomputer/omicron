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
//! Next, this saga will validate with the crucible agent that the snapshot was
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
//! and passing that into a `create_snapshot_from_disk` function. This new
//! volume construction request will be used as a read only parent when creating
//! other disks using this snapshot as a disk source.
//!
//! # Taking a snapshot of a detached disk
//!
//! TODO this is currently unimplemented
//!

use super::{
    common_storage::{
        delete_crucible_regions, ensure_all_datasets_and_regions,
    },
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::declare_saga_actions;
use crate::context::OpContext;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::{authn, authz, db};
use anyhow::anyhow;
use crucible_agent_client::{types::RegionId, Client as CrucibleAgentClient};
use omicron_common::api::external;
use omicron_common::api::external::Error;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::{
    CrucibleOpts, DiskSnapshotRequestBody,
    InstanceIssueDiskSnapshotRequestBody, VolumeConstructionRequest,
};
use slog::info;
use std::collections::BTreeMap;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// snapshot create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub silo_id: Uuid,
    pub project_id: Uuid,
    pub disk_id: Uuid,
    pub create_params: params::SnapshotCreate,
}

// snapshot create saga: actions

declare_saga_actions! {
    snapshot_create;
    REGIONS_ALLOC -> "datasets_and_regions" {
        + ssc_alloc_regions
        - ssc_alloc_regions_undo
    }
    REGIONS_ENSURE -> "regions_ensure" {
        + ssc_regions_ensure
        - ssc_regions_ensure_undo
    }
    CREATE_DESTINATION_VOLUME_RECORD -> "created_destination_volume" {
        + ssc_create_destination_volume_record
        - ssc_create_destination_volume_record_undo
    }
    CREATE_SNAPSHOT_RECORD -> "created_snapshot" {
        + ssc_create_snapshot_record
        - ssc_create_snapshot_record_undo
    }
    SPACE_ACCOUNT -> "no_result" {
        + ssc_account_space
        - ssc_account_space_undo
    }
    SEND_SNAPSHOT_REQUEST -> "snapshot_request" {
        + ssc_send_snapshot_request
        - ssc_send_snapshot_request_undo
    }
    START_RUNNING_SNAPSHOT -> "replace_sockets_map" {
        + ssc_start_running_snapshot
        - ssc_start_running_snapshot_undo
    }
    CREATE_VOLUME_RECORD -> "created_volume" {
        + ssc_create_volume_record
        - ssc_create_volume_record_undo
    }
    FINALIZE_SNAPSHOT_RECORD -> "finalized_snapshot" {
        + ssc_finalize_snapshot_record
    }
}

// snapshot create saga: definition

#[derive(Debug)]
pub struct SagaSnapshotCreate;
impl NexusSaga for SagaSnapshotCreate {
    const NAME: &'static str = "snapshot-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        snapshot_create_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        // Generate IDs
        builder.append(Node::action(
            "snapshot_id",
            "GenerateSnapshotId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "volume_id",
            "GenerateVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "destination_volume_id",
            "GenerateDestinationVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        // (DB) Allocate region space for snapshot to store blocks post-scrub
        builder.append(regions_alloc_action());
        // (Sleds) Reaches out to each dataset, and ensures the regions exist
        // for the destination volume
        builder.append(regions_ensure_action());
        // (DB) Creates a record of the destination volume in the DB
        builder.append(create_destination_volume_record_action());
        // (DB) Creates a record of the snapshot, referencing both the
        // original disk ID and the destination volume
        builder.append(create_snapshot_record_action());
        // (DB) Tracks virtual resource provisioning.
        builder.append(space_account_action());
        // (Sleds) Sends a request for the disk to create a ZFS snapshot
        builder.append(send_snapshot_request_action());
        // (Sleds + DB) Start snapshot downstairs, add an entry in the DB for
        // the dataset's snapshot.
        //
        // TODO: Should this be two separate saga steps?
        builder.append(start_running_snapshot_action());
        // (DB) Copy and modify the disk volume construction request to point
        // to the new running snapshot
        builder.append(create_volume_record_action());
        // (DB) Mark snapshot as "ready"
        builder.append(finalize_snapshot_record_action());

        Ok(builder.build()?)
    }
}

// snapshot create saga: action implementations

async fn ssc_alloc_regions(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::Dataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let destination_volume_id =
        sagactx.lookup::<Uuid>("destination_volume_id")?;

    // Ensure the destination volume is backed by appropriate regions.
    //
    // This allocates regions in the database, but the disk state is still
    // "creating" - the respective Crucible Agents must be instructed to
    // allocate the necessary regions before we can mark the disk as "ready to
    // be used".
    //
    // TODO: Depending on the result of
    // https://github.com/oxidecomputer/omicron/issues/613 , we
    // should consider using a paginated API to access regions, rather than
    // returning all of them at once.
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let datasets_and_regions = osagactx
        .datastore()
        .region_allocate(
            &opctx,
            destination_volume_id,
            &params::DiskSource::Blank {
                block_size: params::BlockSize::try_from(
                    disk.block_size.to_bytes(),
                )
                .map_err(|e| ActionError::action_failed(e.to_string()))?,
            },
            external::ByteCount::from(disk.size),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(datasets_and_regions)
}

async fn ssc_alloc_regions_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let region_ids = sagactx
        .lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?
        .into_iter()
        .map(|(_, region)| region.id())
        .collect::<Vec<Uuid>>();

    osagactx.datastore().regions_hard_delete(region_ids).await?;
    Ok(())
}

async fn ssc_regions_ensure(
    sagactx: NexusActionContext,
) -> Result<String, ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let destination_volume_id =
        sagactx.lookup::<Uuid>("destination_volume_id")?;

    let datasets_and_regions = ensure_all_datasets_and_regions(
        &log,
        sagactx.lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?,
    )
    .await?;

    let block_size = datasets_and_regions[0].1.block_size;
    let blocks_per_extent = datasets_and_regions[0].1.extent_size;
    let extent_count = datasets_and_regions[0].1.extent_count;

    // Create volume construction request
    let mut rng = StdRng::from_entropy();
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: destination_volume_id,
        block_size,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count: extent_count.try_into().unwrap(),
            gen: 1,
            opts: CrucibleOpts {
                id: destination_volume_id,
                target: datasets_and_regions
                    .iter()
                    .map(|(dataset, region)| {
                        dataset
                            .address_with_port(region.port_number)
                            .to_string()
                    })
                    .collect(),

                lossy: false,
                flush_timeout: None,

                // all downstairs will expect encrypted blocks
                key: Some(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    {
                        // TODO the current encryption key
                        // requirement is 32 bytes, what if that
                        // changes?
                        let mut random_bytes: [u8; 32] = [0; 32];
                        rng.fill_bytes(&mut random_bytes);
                        random_bytes
                    },
                )),

                // TODO TLS, which requires sending X509 stuff during
                // downstairs region allocation too.
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,

                control: None,

                // TODO while the transfer of blocks is occurring to the
                // destination volume, the opt here should be read-write. When
                // the transfer has completed, update the volume to make it
                // read-only.
                read_only: false,
            },
        }],
        read_only_parent: None,
    };

    let volume_data = serde_json::to_string(&volume_construction_request)
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    Ok(volume_data)
}

async fn ssc_regions_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    warn!(log, "ssc_regions_ensure_undo: Deleting crucible regions");
    delete_crucible_regions(
        sagactx.lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?,
    )
    .await?;
    info!(log, "ssc_regions_ensure_undo: Deleted crucible regions");
    Ok(())
}

async fn ssc_create_destination_volume_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Volume, ActionError> {
    let osagactx = sagactx.user_data();

    let destination_volume_id =
        sagactx.lookup::<Uuid>("destination_volume_id")?;
    let destination_volume_data = sagactx.lookup::<String>("regions_ensure")?;

    let volume =
        db::model::Volume::new(destination_volume_id, destination_volume_data);

    let volume_created = osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(volume_created)
}

async fn ssc_create_destination_volume_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let destination_volume_id =
        sagactx.lookup::<Uuid>("destination_volume_id")?;
    osagactx.nexus().volume_delete(&opctx, destination_volume_id).await?;

    Ok(())
}

async fn ssc_create_snapshot_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Snapshot, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    // We admittedly reference the volume(s) before they have been allocated,
    // but this should be acceptable because the snapshot remains in a
    // "Creating" state until the saga has completed.
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    let destination_volume_id =
        sagactx.lookup::<Uuid>("destination_volume_id")?;

    info!(log, "grabbing disk by name {}", params.create_params.disk);

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "creating snapshot {} from disk {}", snapshot_id, disk.id());

    let snapshot = db::model::Snapshot {
        identity: db::model::SnapshotIdentity::new(
            snapshot_id,
            params.create_params.identity.clone(),
        ),

        project_id: params.project_id,
        disk_id: disk.id(),
        volume_id,
        destination_volume_id: destination_volume_id,

        gen: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: disk.block_size,
        size: disk.size,
    };

    let (.., authz_project) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;

    let snapshot_created = osagactx
        .datastore()
        .project_ensure_snapshot(&opctx, &authz_project, snapshot)
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "created snapshot {} ok", snapshot_id);

    Ok(snapshot_created)
}

async fn ssc_create_snapshot_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    info!(log, "deleting snapshot {}", snapshot_id);

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

async fn ssc_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let snapshot_created =
        sagactx.lookup::<db::model::Snapshot>("created_snapshot")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_snapshot(
            &opctx,
            snapshot_created.id(),
            params.project_id,
            snapshot_created.size,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn ssc_account_space_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let snapshot_created =
        sagactx.lookup::<db::model::Snapshot>("created_snapshot")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_snapshot(
            &opctx,
            snapshot_created.id(),
            params.project_id,
            snapshot_created.size,
        )
        .await?;
    Ok(())
}

async fn ssc_send_snapshot_request(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    // Find if this disk is attached to an instance
    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    match disk.runtime().attach_instance_id {
        Some(instance_id) => {
            info!(log, "disk {} instance is {}", disk.id(), instance_id);

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

            info!(log, "instance {} sled agent created ok", instance_id);

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
            Ok(())
        }

        None => {
            info!(log, "disk {} not attached to an instance", disk.id());

            // Grab volume construction request for the disk
            let disk_volume = osagactx
                .datastore()
                .volume_checkout(disk.volume_id)
                .await
                .map_err(ActionError::action_failed)?;

            info!(
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
            let sled_id = osagactx
                .nexus()
                .random_sled_id()
                .await
                .map_err(ActionError::action_failed)?
                .ok_or_else(|| {
                    "no sled found when looking for random sled?!".to_string()
                })
                .map_err(ActionError::action_failed)?;
            let sled_agent_client = osagactx
                .nexus()
                .sled_client(&sled_id)
                .await
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

            Ok(())
        }
    }
}

async fn ssc_send_snapshot_request_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    info!(log, "Undoing snapshot request for {snapshot_id}");

    // Lookup the regions used by the source disk...
    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await?;
    let datasets_and_regions =
        osagactx.datastore().get_allocated_regions(disk.volume_id).await?;

    // ... and instruct each of those regions to delete the snapshot.
    for (dataset, region) in datasets_and_regions {
        let url = format!("http://{}", dataset.address());
        let client = CrucibleAgentClient::new(&url);

        client
            .region_delete_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await?;
    }
    Ok(())
}

async fn ssc_start_running_snapshot(
    sagactx: NexusActionContext,
) -> Result<BTreeMap<String, String>, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    info!(log, "starting running snapshot for {snapshot_id}");

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
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

        info!(log, "dataset {:?} region {:?} url {}", dataset, region, url);

        // Validate with the Crucible agent that the snapshot exists
        let crucible_region = client
            .region_get(&RegionId(region.id().to_string()))
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;

        info!(log, "crucible region {:?}", crucible_region);

        let crucible_snapshot = client
            .region_get_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;

        info!(log, "crucible snapshot {:?}", crucible_snapshot);

        // Start the snapshot running
        let crucible_running_snapshot = client
            .region_run_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;

        info!(log, "crucible running snapshot {:?}", crucible_running_snapshot);

        // Map from the region to the snapshot
        let region_addr = format!(
            "{}",
            dataset.address_with_port(crucible_region.port_number)
        );
        let snapshot_addr = format!(
            "{}",
            dataset.address_with_port(crucible_running_snapshot.port_number)
        );
        info!(log, "map {} to {}", region_addr, snapshot_addr);
        map.insert(region_addr, snapshot_addr.clone());

        // Once snapshot has been validated, and running snapshot has been
        // started, add an entry in the region_snapshot table to correspond to
        // these Crucible resources.
        osagactx
            .datastore()
            .region_snapshot_create(db::model::RegionSnapshot {
                dataset_id: dataset.id(),
                region_id: region.id(),
                snapshot_id,
                snapshot_addr,
                volume_references: 0, // to be filled later
            })
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(map)
}

async fn ssc_start_running_snapshot_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    info!(log, "Undoing snapshot start running request for {snapshot_id}");

    // Lookup the regions used by the source disk...
    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await?;
    let datasets_and_regions =
        osagactx.datastore().get_allocated_regions(disk.volume_id).await?;

    // ... and instruct each of those regions to delete the running snapshot.
    for (dataset, region) in datasets_and_regions {
        let url = format!("http://{}", dataset.address());
        let client = CrucibleAgentClient::new(&url);

        use crucible_agent_client::Error::ErrorResponse;
        use http::status::StatusCode;

        client
            .region_delete_running_snapshot(
                &RegionId(region.id().to_string()),
                &snapshot_id.to_string(),
            )
            .await
            .map(|_| ())
            // NOTE: If we later create a volume record and delete it, the
            // running snapshot may be deleted (see:
            // ssc_create_volume_record_undo).
            //
            // To cope, we treat "running snapshot not found" as "Ok", since it
            // may just be the result of the volume deletion steps completing.
            .or_else(|err| match err {
                ErrorResponse(r) if r.status() == StatusCode::NOT_FOUND => {
                    Ok(())
                }
                _ => Err(err),
            })?;
        osagactx
            .datastore()
            .region_snapshot_remove(dataset.id(), region.id(), snapshot_id)
            .await?;
    }
    Ok(())
}

async fn ssc_create_volume_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Volume, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;

    // For a snapshot, copy the volume construction request at the time the
    // snapshot was taken.
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let disk_volume = osagactx
        .datastore()
        .volume_checkout(disk.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "disk volume construction request {}", disk_volume.data());

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
    let snapshot_volume_construction_request: VolumeConstructionRequest =
        create_snapshot_from_disk(
            &disk_volume_construction_request,
            Some(&replace_sockets_map),
        )
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    // Create the volume record for this snapshot
    let volume_data: String = serde_json::to_string(
        &snapshot_volume_construction_request,
    )
    .map_err(|e| {
        ActionError::action_failed(Error::internal_error(&e.to_string()))
    })?;
    info!(log, "snapshot volume construction request {}", volume_data);
    let volume = db::model::Volume::new(volume_id, volume_data);

    // Insert volume record into the DB
    let volume_created = osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "volume {} created ok", volume_id);

    Ok(volume_created)
}

async fn ssc_create_volume_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;

    info!(log, "deleting volume {}", volume_id);
    osagactx.nexus().volume_delete(&opctx, volume_id).await?;

    Ok(())
}

async fn ssc_finalize_snapshot_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Snapshot, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    info!(log, "snapshot final lookup...");

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    let (.., authz_snapshot, db_snapshot) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .snapshot_id(snapshot_id)
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    info!(log, "snapshot final lookup ok");

    let snapshot = osagactx
        .datastore()
        .project_snapshot_update_state(
            &opctx,
            &authz_snapshot,
            db_snapshot.gen,
            db::model::SnapshotState::Ready,
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "snapshot finalized!");

    Ok(snapshot)
}

// helper functions

/// Create a Snapshot VolumeConstructionRequest by copying a disk's
/// VolumeConstructionRequest and modifying it accordingly.
fn create_snapshot_from_disk(
    disk: &VolumeConstructionRequest,
    socket_map: Option<&BTreeMap<String, String>>,
) -> anyhow::Result<VolumeConstructionRequest> {
    // When copying a disk's VolumeConstructionRequest to turn it into a
    // snapshot:
    //
    // - generate new IDs for each layer
    // - set read-only
    // - remove any control sockets

    match disk {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size,
            sub_volumes,
            read_only_parent,
        } => Ok(VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: *block_size,
            sub_volumes: sub_volumes
                .iter()
                .map(|subvol| -> anyhow::Result<VolumeConstructionRequest> {
                    create_snapshot_from_disk(&subvol, socket_map)
                })
                .collect::<anyhow::Result<Vec<VolumeConstructionRequest>>>()?,
            read_only_parent: if let Some(read_only_parent) = read_only_parent {
                Some(Box::new(create_snapshot_from_disk(
                    read_only_parent,
                    // no socket modification required for read-only parents
                    None,
                )?))
            } else {
                None
            },
        }),

        VolumeConstructionRequest::Url { id: _, block_size, url } => {
            Ok(VolumeConstructionRequest::Url {
                id: Uuid::new_v4(),
                block_size: *block_size,
                url: url.clone(),
            })
        }

        VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            opts,
            gen,
        } => {
            let mut opts = opts.clone();

            if let Some(socket_map) = socket_map {
                for target in &mut opts.target {
                    *target = socket_map
                        .get(target)
                        .ok_or_else(|| {
                            anyhow!("target {} not found in map!", target)
                        })?
                        .clone();
                }
            }

            opts.id = Uuid::new_v4();
            opts.read_only = true;
            opts.control = None;

            Ok(VolumeConstructionRequest::Region {
                block_size: *block_size,
                blocks_per_extent: *blocks_per_extent,
                extent_count: *extent_count,
                opts,
                gen: *gen,
            })
        }

        VolumeConstructionRequest::File { id: _, block_size, path } => {
            Ok(VolumeConstructionRequest::File {
                id: Uuid::new_v4(),
                block_size: *block_size,
                path: path.clone(),
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::app::saga::create_saga_dag;
    use crate::db::DataStore;
    use async_bb8_diesel::{AsyncRunQueryDsl, OptionalExtension};
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use dropshot::test_util::ClientTestContext;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_ip_pool;
    use nexus_test_utils::resource_helpers::create_organization;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::delete_disk;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use sled_agent_client::types::CrucibleOpts;
    use std::str::FromStr;

    #[test]
    fn test_create_snapshot_from_disk_modify_request() {
        let disk = VolumeConstructionRequest::Volume {
            block_size: 512,
            id: Uuid::new_v4(),
            read_only_parent: Some(
                Box::new(VolumeConstructionRequest::Volume {
                    block_size: 512,
                    id: Uuid::new_v4(),
                    read_only_parent: Some(
                        Box::new(VolumeConstructionRequest::Url {
                            id: Uuid::new_v4(),
                            block_size: 512,
                            url: "http://[fd01:1122:3344:101::15]/crucible-tester-sparse.img".into(),
                        })
                    ),
                    sub_volumes: vec![
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 20,
                            gen: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                key: Some("tkBksPOA519q11jvLCCX5P8t8+kCX4ZNzr+QP8M+TSg=".into()),
                                lossy: false,
                                read_only: true,
                                target: vec![
                                    "[fd00:1122:3344:101::8]:19001".into(),
                                    "[fd00:1122:3344:101::7]:19001".into(),
                                    "[fd00:1122:3344:101::6]:19001".into(),
                                ],
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                flush_timeout: None,
                                control: None,
                            }
                        },
                    ]
                }),
            ),
            sub_volumes: vec![
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 80,
                    gen: 100,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        key: Some("jVex5Zfm+avnFMyezI6nCVPRPs53EWwYMN844XETDBM=".into()),
                        lossy: false,
                        read_only: false,
                        target: vec![
                            "[fd00:1122:3344:101::8]:19002".into(),
                            "[fd00:1122:3344:101::7]:19002".into(),
                            "[fd00:1122:3344:101::6]:19002".into(),
                        ],
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        flush_timeout: None,
                        control: Some("127.0.0.1:12345".into()),
                    }
                },
            ],
        };

        let mut replace_sockets: BTreeMap<String, String> = BTreeMap::new();

        // Replacements for top level Region only
        replace_sockets.insert(
            "[fd00:1122:3344:101::6]:19002".into(),
            "[XXXX:1122:3344:101::6]:9000".into(),
        );
        replace_sockets.insert(
            "[fd00:1122:3344:101::7]:19002".into(),
            "[XXXX:1122:3344:101::7]:9000".into(),
        );
        replace_sockets.insert(
            "[fd00:1122:3344:101::8]:19002".into(),
            "[XXXX:1122:3344:101::8]:9000".into(),
        );

        let snapshot =
            create_snapshot_from_disk(&disk, Some(&replace_sockets)).unwrap();

        eprintln!("{:?}", serde_json::to_string(&snapshot).unwrap());

        // validate that each ID changed

        let snapshot_id_1 = match &snapshot {
            VolumeConstructionRequest::Volume { id, .. } => id,
            _ => panic!("enum changed shape!"),
        };

        let disk_id_1 = match &disk {
            VolumeConstructionRequest::Volume { id, .. } => id,
            _ => panic!("enum changed shape!"),
        };

        assert_ne!(snapshot_id_1, disk_id_1);

        let snapshot_first_opts = match &snapshot {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                match &sub_volumes[0] {
                    VolumeConstructionRequest::Region { opts, .. } => opts,
                    _ => panic!("enum changed shape!"),
                }
            }
            _ => panic!("enum changed shape!"),
        };

        let disk_first_opts = match &disk {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                match &sub_volumes[0] {
                    VolumeConstructionRequest::Region { opts, .. } => opts,
                    _ => panic!("enum changed shape!"),
                }
            }
            _ => panic!("enum changed shape!"),
        };

        assert_ne!(snapshot_first_opts.id, disk_first_opts.id);

        let snapshot_id_3 = match &snapshot {
            VolumeConstructionRequest::Volume { read_only_parent, .. } => {
                match read_only_parent.as_ref().unwrap().as_ref() {
                    VolumeConstructionRequest::Volume { id, .. } => id,
                    _ => panic!("enum changed shape!"),
                }
            }
            _ => panic!("enum changed shape!"),
        };

        let disk_id_3 = match &disk {
            VolumeConstructionRequest::Volume { read_only_parent, .. } => {
                match read_only_parent.as_ref().unwrap().as_ref() {
                    VolumeConstructionRequest::Volume { id, .. } => id,
                    _ => panic!("enum changed shape!"),
                }
            }
            _ => panic!("enum changed shape!"),
        };

        assert_ne!(snapshot_id_3, disk_id_3);

        let snapshot_second_opts = match &snapshot {
            VolumeConstructionRequest::Volume { read_only_parent, .. } => {
                match read_only_parent.as_ref().unwrap().as_ref() {
                    VolumeConstructionRequest::Volume {
                        sub_volumes, ..
                    } => match &sub_volumes[0] {
                        VolumeConstructionRequest::Region { opts, .. } => opts,
                        _ => panic!("enum changed shape!"),
                    },
                    _ => panic!("enum changed shape!"),
                }
            }
            _ => panic!("enum changed shape!"),
        };

        let disk_second_opts = match &disk {
            VolumeConstructionRequest::Volume { read_only_parent, .. } => {
                match read_only_parent.as_ref().unwrap().as_ref() {
                    VolumeConstructionRequest::Volume {
                        sub_volumes, ..
                    } => match &sub_volumes[0] {
                        VolumeConstructionRequest::Region { opts, .. } => opts,
                        _ => panic!("enum changed shape!"),
                    },
                    _ => panic!("enum changed shape!"),
                }
            }
            _ => panic!("enum changed shape!"),
        };

        assert_ne!(snapshot_second_opts.id, disk_second_opts.id);

        // validate only the top level targets were changed

        assert_ne!(snapshot_first_opts.target, disk_first_opts.target);
        assert_eq!(snapshot_second_opts.target, disk_second_opts.target);

        // validate that read_only was set correctly

        assert_eq!(snapshot_first_opts.read_only, true);
        assert_eq!(disk_first_opts.read_only, false);

        // validate control socket was removed

        assert!(snapshot_first_opts.control.is_none());
        assert!(disk_first_opts.control.is_some());
    }

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const ORG_NAME: &str = "test-org";
    const PROJECT_NAME: &str = "springfield-squidport";
    const DISK_NAME: &str = "disky-mcdiskface";

    async fn create_org_project_and_disk(client: &ClientTestContext) -> Uuid {
        create_ip_pool(&client, "p0", None).await;
        create_organization(&client, ORG_NAME).await;
        create_project(client, ORG_NAME, PROJECT_NAME).await;
        create_disk(client, ORG_NAME, PROJECT_NAME, DISK_NAME).await.identity.id
    }

    // Helper for creating snapshot create parameters
    fn new_test_params(
        opctx: &OpContext,
        silo_id: Uuid,
        project_id: Uuid,
        disk_id: Uuid,
        disk: Name,
    ) -> Params {
        Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id,
            project_id,
            disk_id,
            create_params: params::SnapshotCreate {
                identity: IdentityMetadataCreateParams {
                    name: "my-snapshot".parse().expect("Invalid disk name"),
                    description: "My snapshot".to_string(),
                },
                disk,
            },
        }
    }

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.apictx.nexus.datastore().clone(),
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx.nexus;
        let disk_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let (authz_silo, _authz_org, authz_project, _authz_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .lookup_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        let silo_id = authz_silo.id();
        let project_id = authz_project.id();

        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk_id,
            Name::from_str(DISK_NAME).unwrap().into(),
        );
        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        let output = nexus.run_saga(runnable_saga).await.unwrap();

        let snapshot = output
            .lookup_node_output::<crate::db::model::Snapshot>(
                "finalized_snapshot",
            )
            .unwrap();
        assert_eq!(snapshot.project_id, project_id);
    }

    async fn no_snapshot_records_exist(datastore: &DataStore) -> bool {
        use crate::db::model::Snapshot;
        use crate::db::schema::snapshot::dsl;

        dsl::snapshot
            .filter(dsl::time_deleted.is_null())
            .select(Snapshot::as_select())
            .first_async::<Snapshot>(datastore.pool_for_tests().await.unwrap())
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_region_snapshot_records_exist(datastore: &DataStore) -> bool {
        use crate::db::model::RegionSnapshot;
        use crate::db::schema::region_snapshot::dsl;

        dsl::region_snapshot
            .select(RegionSnapshot::as_select())
            .first_async::<RegionSnapshot>(
                datastore.pool_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        test: &DiskTest,
    ) {
        // Verifies:
        // - No disk records exist
        // - No volume records exist
        // - No region allocations exist
        // - No regions are ensured in the sled agent
        crate::app::sagas::disk_create::test::verify_clean_slate(
            cptestctx, test,
        )
        .await;

        // Verifies:
        // - No snapshot records exist
        // - No region snapshot records exist
        let datastore = cptestctx.server.apictx.nexus.datastore();
        assert!(no_snapshot_records_exist(datastore).await);
        assert!(no_region_snapshot_records_exist(datastore).await);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx.nexus;
        let mut disk_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let (authz_silo, _authz_org, authz_project, _authz_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .lookup_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        let silo_id = authz_silo.id();
        let project_id = authz_project.id();

        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk_id,
            Name::from_str(DISK_NAME).unwrap().into(),
        );
        let mut dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();

        // The saga's input parameters include a disk UUID, which makes sense,
        // since the snapshot is created from a disk.
        //
        // Unfortunately, for our idempotency checks, checking for a "clean
        // slate" gets more expensive when we need to compare region allocations
        // between the disk and the snapshot. If we can undo the snapshot
        // provisioning AND delete the disk together, these checks are much
        // simpler to write.
        //
        // So, in summary: We do some odd indexing here...
        // ... because we re-create the whole DAG on each iteration...
        // ... because we also delete the disk on each iteration, making the
        // parameters invalid...
        // ... because doing so provides a really easy-to-verify "clean slate"
        // for us to test against.
        let mut n: usize = 0;
        while let Some(node) = dag.get_nodes().nth(n) {
            n = n + 1;

            // Create a new saga for this node.
            info!(
                log,
                "Creating new saga which will fail at index {:?}", node.index();
                "node_name" => node.name().as_ref(),
                "label" => node.label(),
            );

            let runnable_saga =
                nexus.create_runnable_saga(dag.clone()).await.unwrap();

            // Inject an error instead of running the node.
            //
            // This should cause the saga to unwind.
            nexus
                .sec()
                .saga_inject_error(runnable_saga.id(), node.index())
                .await
                .unwrap();
            nexus
                .run_saga(runnable_saga)
                .await
                .expect_err("Saga should have failed");

            delete_disk(client, ORG_NAME, PROJECT_NAME, DISK_NAME).await;
            verify_clean_slate(cptestctx, &test).await;
            disk_id = create_disk(client, ORG_NAME, PROJECT_NAME, DISK_NAME)
                .await
                .identity
                .id;

            let params = new_test_params(
                &opctx,
                silo_id,
                project_id,
                disk_id,
                Name::from_str(DISK_NAME).unwrap().into(),
            );
            dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        }
    }
}
