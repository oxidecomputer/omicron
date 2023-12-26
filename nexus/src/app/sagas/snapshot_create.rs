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
//! This process is mostly the same as the process of taking a snapshot of a
//! disk that's attached to an instance, but if a disk is not currently attached
//! to an instance, there's no Upstairs to send the snapshot request to. The
//! Crucible Pantry is a service that will be launched on each Sled and will be
//! used for these types of maintenance tasks. In this case this saga will
//! attach the volume "to" a random Pantry by sending a volume construction
//! request, then send a snapshot request, then detach "from" that random
//! Pantry. Most of the rest of the saga is unchanged.
//!

use super::{
    common_storage::{
        call_pantry_attach_for_disk, call_pantry_detach_for_disk,
        delete_crucible_regions, delete_crucible_running_snapshot,
        delete_crucible_snapshot, ensure_all_datasets_and_regions,
        get_pantry_address,
    },
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::retry_until_known_result;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use anyhow::anyhow;
use crucible_agent_client::{types::RegionId, Client as CrucibleAgentClient};
use nexus_db_model::Generation;
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::CrucibleOpts;
use sled_agent_client::types::InstanceIssueDiskSnapshotRequestBody;
use sled_agent_client::types::VolumeConstructionRequest;
use slog::info;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// snapshot create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub silo_id: Uuid,
    pub project_id: Uuid,
    pub disk_id: Uuid,
    pub attached_instance_and_sled: Option<(Uuid, Uuid)>,
    pub create_params: params::SnapshotCreate,
}

// snapshot create saga: actions
declare_saga_actions! {
    snapshot_create;
    REGIONS_ALLOC -> "datasets_and_regions" {
        + ssc_alloc_regions
        - ssc_alloc_regions_undo
    }
    REGIONS_ENSURE_UNDO -> "regions_ensure_undo" {
        + ssc_noop
        - ssc_regions_ensure_undo
    }
    REGIONS_ENSURE -> "regions_ensure" {
        + ssc_regions_ensure
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
    SEND_SNAPSHOT_REQUEST_TO_SLED_AGENT -> "snapshot_request_to_sled_agent" {
        + ssc_send_snapshot_request_to_sled_agent
        - ssc_send_snapshot_request_to_sled_agent_undo
    }
    GET_PANTRY_ADDRESS -> "pantry_address" {
        + ssc_get_pantry_address
    }
    ATTACH_DISK_TO_PANTRY -> "disk_generation_number" {
        + ssc_attach_disk_to_pantry
        - ssc_attach_disk_to_pantry_undo
    }
    CALL_PANTRY_ATTACH_FOR_DISK -> "call_pantry_attach_for_disk" {
        + ssc_call_pantry_attach_for_disk
        - ssc_call_pantry_attach_for_disk_undo
    }
    CALL_PANTRY_SNAPSHOT_FOR_DISK -> "call_pantry_snapshot_for_disk" {
        + ssc_call_pantry_snapshot_for_disk
        - ssc_call_pantry_snapshot_for_disk_undo
    }
    CALL_PANTRY_DETACH_FOR_DISK -> "call_pantry_detach_for_disk" {
        + ssc_call_pantry_detach_for_disk
    }
    DETACH_DISK_FROM_PANTRY -> "detach_disk_from_pantry" {
        + ssc_detach_disk_from_pantry
    }

    START_RUNNING_SNAPSHOT_UNDO -> "ssc_not_used" {
        + ssc_noop
        - ssc_start_running_snapshot_undo
    }
    START_RUNNING_SNAPSHOT -> "replace_sockets_map" {
        + ssc_start_running_snapshot
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
pub(crate) struct SagaSnapshotCreate;
impl NexusSaga for SagaSnapshotCreate {
    const NAME: &'static str = "snapshot-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        snapshot_create_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
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
        builder.append(regions_ensure_undo_action());
        builder.append(regions_ensure_action());
        // (DB) Creates a record of the destination volume in the DB
        builder.append(create_destination_volume_record_action());
        // (DB) Creates a record of the snapshot, referencing both the
        // original disk ID and the destination volume
        builder.append(create_snapshot_record_action());
        // (DB) Tracks virtual resource provisioning.
        builder.append(space_account_action());

        let use_the_pantry = params.attached_instance_and_sled.is_none();
        if !use_the_pantry {
            // (Sleds) If the disk is attached to an instance, send a
            // snapshot request to sled-agent to create a ZFS snapshot.
            builder.append(send_snapshot_request_to_sled_agent_action());
        } else {
            // (Pantry) Record the address of a Pantry service
            builder.append(get_pantry_address_action());

            // (Pantry) If the disk is _not_ attached to an instance:
            // "attach" the disk to the pantry
            builder.append(attach_disk_to_pantry_action());

            // (Pantry) Call the Pantry's /attach
            builder.append(call_pantry_attach_for_disk_action());

            // (Pantry) Call the Pantry's /snapshot
            builder.append(call_pantry_snapshot_for_disk_action());

            // (Pantry) Call the Pantry's /detach
            builder.append(call_pantry_detach_for_disk_action());
        }

        // (Sleds + DB) Start snapshot downstairs, add an entry in the DB for
        // the dataset's snapshot.
        builder.append(start_running_snapshot_undo_action());
        builder.append(start_running_snapshot_action());
        // (DB) Copy and modify the disk volume construction request to point
        // to the new running snapshot
        builder.append(create_volume_record_action());
        // (DB) Mark snapshot as "ready"
        builder.append(finalize_snapshot_record_action());

        if use_the_pantry {
            // (Pantry) Set the state back to Detached
            //
            // This has to be the last saga node! Otherwise, concurrent
            // operation on this disk is possible.
            builder.append(detach_disk_from_pantry_action());
        }

        Ok(builder.build()?)
    }
}

// snapshot create saga: action implementations

async fn ssc_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
    Ok(())
}

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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let strategy = &osagactx.nexus().default_region_allocation_strategy;

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
            &strategy,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(datasets_and_regions)
}

async fn ssc_alloc_regions_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    let region_ids = sagactx
        .lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?
        .into_iter()
        .map(|(_, region)| region.id())
        .collect::<Vec<Uuid>>();

    osagactx.datastore().regions_hard_delete(log, region_ids).await?;
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
        log,
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
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let destination_volume_id =
        sagactx.lookup::<Uuid>("destination_volume_id")?;

    let destination_volume_data = sagactx.lookup::<String>("regions_ensure")?;

    let volume =
        db::model::Volume::new(destination_volume_id, destination_volume_data);

    osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn ssc_create_destination_volume_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let destination_volume_id =
        sagactx.lookup::<Uuid>("destination_volume_id")?;

    // This saga contains what is necessary to clean up the destination volume
    // resources. It's safe here to perform a volume hard delete without
    // decreasing the crucible resource count because the destination volume is
    // guaranteed to never have read only resources that require that
    // accounting.

    info!(log, "hard deleting volume {}", destination_volume_id,);

    osagactx.datastore().volume_hard_delete(destination_volume_id).await?;

    Ok(())
}

async fn ssc_create_snapshot_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Snapshot, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
        destination_volume_id,

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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
        .project_delete_snapshot(
            &opctx,
            &authz_snapshot,
            &db_snapshot,
            vec![
                db::model::SnapshotState::Creating,
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
            ],
        )
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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
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

async fn ssc_send_snapshot_request_to_sled_agent(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    // If this node was reached, the saga initiator thought the disk was
    // attached to an instance that was running on a specific sled. Contact that
    // sled and ask it to initiate a snapshot. Note that this is best-effort:
    // the instance may have stopped (or may be have stopped, had the disk
    // detached, and resumed running on the same sled) while the saga was
    // executing.
    let (instance_id, sled_id) =
        params.attached_instance_and_sled.ok_or_else(|| {
            ActionError::action_failed(Error::internal_error(
                "snapshot saga in send_snapshot_request_to_sled_agent but no \
                instance/sled pair was provided",
            ))
        })?;

    info!(log, "asking for disk snapshot from Propolis via sled agent";
          "disk_id" => %params.disk_id,
          "instance_id" => %instance_id,
          "sled_id" => %sled_id);

    let sled_agent_client = osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(ActionError::action_failed)?;

    retry_until_known_result(log, || async {
        sled_agent_client
            .instance_issue_disk_snapshot_request(
                &instance_id,
                &params.disk_id,
                &InstanceIssueDiskSnapshotRequestBody { snapshot_id },
            )
            .await
    })
    .await
    .map_err(|e| e.to_string())
    .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn ssc_send_snapshot_request_to_sled_agent_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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

        delete_crucible_snapshot(log, &client, region.id(), snapshot_id)
            .await?;
    }

    Ok(())
}

async fn ssc_get_pantry_address(
    sagactx: NexusActionContext,
) -> Result<(SocketAddrV6, bool), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // If the disk is already attached to a Pantry, use that, otherwise get a
    // random one. Return boolean indicating if additional saga nodes need to
    // attach this disk to that random pantry.
    let (.., disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let pantry_address = if let Some(pantry_address) = disk.pantry_address() {
        pantry_address
    } else {
        get_pantry_address(osagactx.nexus()).await?
    };

    let disk_already_attached_to_pantry = disk.pantry_address().is_some();

    info!(
        log,
        "using pantry at {}{}",
        pantry_address,
        if disk_already_attached_to_pantry {
            " (already attached)"
        } else {
            ""
        }
    );

    Ok((pantry_address, disk_already_attached_to_pantry))
}

async fn ssc_attach_disk_to_pantry(
    sagactx: NexusActionContext,
) -> Result<Generation, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .disk_id(params.disk_id)
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    // Query the fetched disk's runtime to see if changed after the saga
    // execution started. This can happen if it's attached to an instance, or if
    // it's undergoing other maintenance. If it was, then bail out. If it
    // wasn't, then try to update the runtime and attach it to the Pantry. In
    // the case where the disk is attached to an instance after this lookup, the
    // "runtime().maintenance(...)" call below should fail because the
    // generation number is too low.
    match db_disk.state().into() {
        external::DiskState::Detached => {
            info!(log, "setting state of {} to maintenance", params.disk_id);

            osagactx
                .datastore()
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &db_disk.runtime().maintenance(),
                )
                .await
                .map_err(ActionError::action_failed)?;
        }

        external::DiskState::Finalizing => {
            // This saga is a sub-saga of the finalize saga if the user has
            // specified an optional snapshot should be taken. No state change
            // is required.
            info!(log, "disk {} in state finalizing", params.disk_id);
        }

        _ => {
            // Return a 503 indicating that the user should retry
            return Err(ActionError::action_failed(
                Error::ServiceUnavailable {
                    internal_message: format!(
                        "disk is in state {:?}",
                        db_disk.state(),
                    ),
                },
            ));
        }
    }

    // Record the disk's new generation number as this saga node's output. It
    // will be important later to *only* transition this disk out of maintenance
    // if the generation number matches what *this* saga is doing.
    let (.., db_disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch_for(authz::Action::Read)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(db_disk.runtime().gen)
}

async fn ssc_attach_disk_to_pantry_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .disk_id(params.disk_id)
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    match db_disk.state().into() {
        external::DiskState::Maintenance => {
            info!(
                log,
                "undo: setting disk {} state from maintenance to detached",
                params.disk_id
            );

            osagactx
                .datastore()
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &db_disk.runtime().detach(),
                )
                .await
                .map_err(ActionError::action_failed)?;
        }

        external::DiskState::Detached => {
            info!(
                log,
                "undo: disk {} already in state detached", params.disk_id
            );
        }

        external::DiskState::Finalizing => {
            info!(
                log,
                "undo: disk {} already in state finalizing", params.disk_id
            );
        }

        _ => {
            warn!(log, "undo: disk is in state {:?}", db_disk.state());
        }
    }

    Ok(())
}

async fn ssc_call_pantry_attach_for_disk(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (pantry_address, disk_already_attached_to_pantry) =
        sagactx.lookup::<(SocketAddrV6, bool)>("pantry_address")?;

    if !disk_already_attached_to_pantry {
        info!(
            log,
            "attaching disk {:?} to pantry at {:?}",
            params.disk_id,
            pantry_address
        );

        call_pantry_attach_for_disk(
            &log,
            &opctx,
            &osagactx.nexus(),
            params.disk_id,
            pantry_address,
        )
        .await?;
    } else {
        info!(log, "disk {} already attached to a pantry", params.disk_id);
    }

    Ok(())
}

async fn ssc_call_pantry_attach_for_disk_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;

    let (pantry_address, disk_already_attached_to_pantry) =
        sagactx.lookup::<(SocketAddrV6, bool)>("pantry_address")?;

    // If the disk came into this saga attached to a pantry, don't detach it
    if !disk_already_attached_to_pantry {
        info!(
            log,
            "undo: detaching disk {:?} from pantry at {:?}",
            params.disk_id,
            pantry_address
        );

        call_pantry_detach_for_disk(&log, params.disk_id, pantry_address)
            .await?;
    } else {
        info!(
            log,
            "undo: not detaching disk {}, was already attached to a pantry",
            params.disk_id
        );
    }

    Ok(())
}

async fn ssc_call_pantry_snapshot_for_disk(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;

    let (pantry_address, _) =
        sagactx.lookup::<(SocketAddrV6, bool)>("pantry_address")?;
    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    let endpoint = format!("http://{}", pantry_address);

    info!(
        log,
        "sending snapshot request with id {} for disk {} to pantry endpoint {}",
        snapshot_id,
        params.disk_id,
        endpoint,
    );

    let client = crucible_pantry_client::Client::new(&endpoint);

    retry_until_known_result(log, || async {
        client
            .snapshot(
                &params.disk_id.to_string(),
                &crucible_pantry_client::types::SnapshotRequest {
                    snapshot_id: snapshot_id.to_string(),
                },
            )
            .await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(Error::internal_error(&e.to_string()))
    })?;

    Ok(())
}

async fn ssc_call_pantry_snapshot_for_disk_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    let params = sagactx.saga_params::<Params>()?;

    info!(log, "Undoing pantry snapshot request for {snapshot_id}");

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

        delete_crucible_snapshot(log, &client, region.id(), snapshot_id)
            .await?;
    }
    Ok(())
}

async fn ssc_call_pantry_detach_for_disk(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;

    let (pantry_address, disk_already_attached_to_pantry) =
        sagactx.lookup::<(SocketAddrV6, bool)>("pantry_address")?;

    // If the disk came into this saga attached to a pantry, don't detach it
    if !disk_already_attached_to_pantry {
        info!(
            log,
            "detaching disk {:?} from pantry at {:?}",
            params.disk_id,
            pantry_address
        );
        call_pantry_detach_for_disk(&log, params.disk_id, pantry_address)
            .await?;
    }

    Ok(())
}

async fn ssc_detach_disk_from_pantry(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .disk_id(params.disk_id)
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    match db_disk.state().into() {
        external::DiskState::Maintenance => {
            // A previous execution of this node in *this* saga may have already
            // transitioned this disk from maintenance to detached. Another saga
            // may have transitioned this disk *back* to the maintenance state.
            // If this saga crashed before marking this node as complete, upon
            // re-execution (absent any other checks) this node would
            // incorrectly attempt to transition this disk out of maintenance,
            // conflicting with the other currently executing saga.
            //
            // Check that the generation number matches what is expected for
            // this saga's execution.
            let expected_disk_generation_number =
                sagactx.lookup::<Generation>("disk_generation_number")?;
            if expected_disk_generation_number == db_disk.runtime().gen {
                info!(
                    log,
                    "setting disk {} state from maintenance to detached",
                    params.disk_id
                );

                osagactx
                    .datastore()
                    .disk_update_runtime(
                        &opctx,
                        &authz_disk,
                        &db_disk.runtime().detach(),
                    )
                    .await
                    .map_err(ActionError::action_failed)?;
            } else {
                info!(
                    log,
                    "disk {} has generation number {:?}, which doesn't match the expected {:?}: skip setting to detach",
                    params.disk_id,
                    db_disk.runtime().gen,
                    expected_disk_generation_number,
                );
            }
        }

        external::DiskState::Detached => {
            info!(log, "disk {} already in state detached", params.disk_id);
        }

        external::DiskState::Finalizing => {
            info!(log, "disk {} already in state finalizing", params.disk_id);
        }

        _ => {
            warn!(log, "disk is in state {:?}", db_disk.state());
        }
    }

    Ok(())
}

async fn ssc_start_running_snapshot(
    sagactx: NexusActionContext,
) -> Result<BTreeMap<String, String>, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
        let crucible_region = retry_until_known_result(log, || async {
            client.region_get(&RegionId(region.id().to_string())).await
        })
        .await
        .map_err(|e| e.to_string())
        .map_err(ActionError::action_failed)?;

        info!(log, "crucible region {:?}", crucible_region);

        let crucible_snapshot = retry_until_known_result(log, || async {
            client
                .region_get_snapshot(
                    &RegionId(region.id().to_string()),
                    &snapshot_id.to_string(),
                )
                .await
        })
        .await
        .map_err(|e| e.to_string())
        .map_err(ActionError::action_failed)?;

        info!(log, "crucible snapshot {:?}", crucible_snapshot);

        // Start the snapshot running
        let crucible_running_snapshot =
            retry_until_known_result(log, || async {
                client
                    .region_run_snapshot(
                        &RegionId(region.id().to_string()),
                        &snapshot_id.to_string(),
                    )
                    .await
            })
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
                deleting: false,
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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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

        delete_crucible_running_snapshot(
            &log,
            &client,
            region.id(),
            snapshot_id,
        )
        .await?;

        osagactx
            .datastore()
            .region_snapshot_remove(dataset.id(), region.id(), snapshot_id)
            .await?;
    }
    Ok(())
}

async fn ssc_create_volume_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;

    // For a snapshot, copy the volume construction request at the time the
    // snapshot was taken.
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
    osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "volume {} created ok", volume_id);

    Ok(())
}

async fn ssc_create_volume_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;

    // `volume_create` will increase the resource count for read only resources
    // in a volume, which there are guaranteed to be for snapshot volumes.
    // decreasing crucible resources is necessary as an undo step. Do not call
    // `volume_hard_delete` here: soft deleting volumes is necessary for
    // `find_deleted_volume_regions` to work.

    info!(
        log,
        "calling decrease crucible resource count for volume {}", volume_id
    );

    osagactx
        .datastore()
        .decrease_crucible_resource_count_and_soft_delete_volume(volume_id)
        .await?;

    Ok(())
}

async fn ssc_finalize_snapshot_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Snapshot, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
    use crate::app::sagas::test_helpers;
    use crate::external_api::shared::IpRange;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
    use nexus_db_queries::db::DataStore;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_ip_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::delete_disk;
    use nexus_test_utils::resource_helpers::object_create;
    use nexus_test_utils::resource_helpers::populate_ip_pool;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::params::InstanceDiskAttachment;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Instance;
    use omicron_common::api::external::InstanceCpuCount;
    use omicron_common::api::external::Name;
    use omicron_common::api::external::NameOrId;
    use sled_agent_client::types::CrucibleOpts;
    use sled_agent_client::TestInterfaces as SledAgentTestInterfaces;
    use std::net::Ipv4Addr;
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

    const PROJECT_NAME: &str = "springfield-squidport";
    const DISK_NAME: &str = "disky-mcdiskface";
    const INSTANCE_NAME: &str = "base-instance";

    async fn create_org_project_and_disk(client: &ClientTestContext) -> Uuid {
        create_ip_pool(&client, "p0", None, None).await;
        create_project(client, PROJECT_NAME).await;
        create_disk(client, PROJECT_NAME, DISK_NAME).await.identity.id
    }

    // Helper for creating snapshot create parameters
    fn new_test_params(
        opctx: &OpContext,
        silo_id: Uuid,
        project_id: Uuid,
        disk_id: Uuid,
        disk: NameOrId,
        instance_and_sled: Option<(Uuid, Uuid)>,
    ) -> Params {
        Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id,
            project_id,
            disk_id,
            attached_instance_and_sled: instance_and_sled,
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
            cptestctx.server.apictx().nexus.datastore().clone(),
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        // Basic snapshot test, create a snapshot of a disk that
        // is not attached to an instance.
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let disk_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let (authz_silo, authz_project, _authz_disk) =
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
            None,
        );
        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        let output = nexus.run_saga(runnable_saga).await.unwrap();

        let snapshot = output
            .lookup_node_output::<nexus_db_queries::db::model::Snapshot>(
                "finalized_snapshot",
            )
            .unwrap();
        assert_eq!(snapshot.project_id, project_id);
    }

    async fn no_snapshot_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Snapshot;
        use nexus_db_queries::db::schema::snapshot::dsl;

        dsl::snapshot
            .filter(dsl::time_deleted.is_null())
            .select(Snapshot::as_select())
            .first_async::<Snapshot>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_region_snapshot_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::RegionSnapshot;
        use nexus_db_queries::db::schema::region_snapshot::dsl;

        dsl::region_snapshot
            .select(RegionSnapshot::as_select())
            .first_async::<RegionSnapshot>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
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
        let datastore = cptestctx.server.apictx().nexus.datastore();
        assert!(no_snapshot_records_exist(datastore).await);
        assert!(no_region_snapshot_records_exist(datastore).await);
    }

    /// Creates an instance in the test project with the supplied disks attached
    /// and ensures the instance is started.
    async fn setup_test_instance(
        cptestctx: &ControlPlaneTestContext,
        client: &ClientTestContext,
        disks_to_attach: Vec<InstanceDiskAttachment>,
    ) -> InstanceAndActiveVmm {
        let instances_url = format!("/v1/instances?project={}", PROJECT_NAME,);
        let instance: Instance = object_create(
            client,
            &instances_url,
            &params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: INSTANCE_NAME.parse().unwrap(),
                    description: format!("instance {:?}", INSTANCE_NAME),
                },
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_gibibytes_u32(1),
                hostname: String::from("base_instance"),
                user_data:
                    b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                        .to_vec(),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::None,
                disks: disks_to_attach,
                external_ips: vec![],
                start: true,
            },
        )
        .await;

        // Read out the instance's assigned sled, then poke the instance to get
        // it from the Starting state to the Running state so the test disk can
        // be snapshotted.
        let nexus = &cptestctx.server.apictx().nexus;
        let opctx = test_opctx(&cptestctx);
        let (.., authz_instance) = LookupPath::new(&opctx, nexus.datastore())
            .instance_id(instance.identity.id)
            .lookup_for(authz::Action::Read)
            .await
            .unwrap();

        let instance_state = nexus
            .datastore()
            .instance_fetch_with_vmm(&opctx, &authz_instance)
            .await
            .unwrap();

        let sled_id = instance_state
            .sled_id()
            .expect("starting instance should have a sled");
        let sa = nexus.sled_client(&sled_id).await.unwrap();

        sa.instance_finish_transition(instance.identity.id).await;
        let instance_state = nexus
            .datastore()
            .instance_fetch_with_vmm(&opctx, &authz_instance)
            .await
            .unwrap();

        let new_state = instance_state
            .vmm()
            .as_ref()
            .expect("running instance should have a sled")
            .runtime
            .state
            .0;

        assert_eq!(
            new_state,
            omicron_common::api::external::InstanceState::Running
        );

        instance_state
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_no_pantry(
        cptestctx: &ControlPlaneTestContext,
    ) {
        test_action_failure_can_unwind_wrapper(cptestctx, false).await
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_pantry(
        cptestctx: &ControlPlaneTestContext,
    ) {
        test_action_failure_can_unwind_wrapper(cptestctx, true).await
    }

    async fn test_action_failure_can_unwind_wrapper(
        cptestctx: &ControlPlaneTestContext,
        use_the_pantry: bool,
    ) {
        let test = DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let disk_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let (authz_silo, authz_project, _authz_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .lookup_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        let silo_id = authz_silo.id();
        let project_id = authz_project.id();

        // As a concession to the test helper, make sure the disk is gone
        // before the first attempt to run the saga recreates it.
        delete_disk(client, PROJECT_NAME, DISK_NAME).await;

        // The no-pantry variant of the test needs to see the disk attached to
        // an instance. Set up an IP pool so that instances can be created
        // against it.
        if !use_the_pantry {
            populate_ip_pool(
                &client,
                "default",
                Some(
                    IpRange::try_from((
                        Ipv4Addr::new(10, 1, 0, 0),
                        Ipv4Addr::new(10, 1, 255, 255),
                    ))
                    .unwrap(),
                ),
            )
            .await;
        }

        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaSnapshotCreate,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin({
                    async {
                        let disk_id =
                            create_disk(client, PROJECT_NAME, DISK_NAME)
                                .await
                                .identity
                                .id;

                        // If the pantry isn't being used, make sure the disk is
                        // attached. Note that under normal circumstances, a
                        // disk can only be attached to a stopped instance, but
                        // since this is just a test, bypass the normal
                        // attachment machinery and just update the disk's
                        // database record directly.
                        let instance_and_sled = if !use_the_pantry {
                            let state = setup_test_instance(
                                cptestctx,
                                client,
                                vec![params::InstanceDiskAttachment::Attach(
                                    params::InstanceDiskAttach {
                                        name: Name::from_str(DISK_NAME)
                                            .unwrap(),
                                    },
                                )],
                            )
                            .await;

                            let sled_id = state
                                .sled_id()
                                .expect("running instance should have a vmm");

                            Some((state.instance().id(), sled_id))
                        } else {
                            None
                        };

                        new_test_params(
                            &opctx,
                            silo_id,
                            project_id,
                            disk_id,
                            Name::from_str(DISK_NAME).unwrap().into(),
                            instance_and_sled,
                        )
                    }
                })
            },
            || {
                Box::pin(async {
                    // If the pantry wasn't used, detach the disk before
                    // deleting it. Note that because each iteration creates a
                    // new disk ID, and that ID doesn't escape the closure that
                    // created it, the lookup needs to be done by name instead.
                    if !use_the_pantry {
                        let (.., authz_disk, db_disk) =
                            LookupPath::new(&opctx, nexus.datastore())
                                .project_id(project_id)
                                .disk_name(&db::model::Name(
                                    DISK_NAME.to_owned().try_into().unwrap(),
                                ))
                                .fetch_for(authz::Action::Read)
                                .await
                                .expect("Failed to look up created disk");

                        assert!(nexus
                            .datastore()
                            .disk_update_runtime(
                                &opctx,
                                &authz_disk,
                                &db_disk.runtime().detach(),
                            )
                            .await
                            .expect("failed to detach disk"));

                        // Stop and destroy the test instance to satisfy the
                        // clean-slate check.
                        test_helpers::instance_stop_by_name(
                            cptestctx,
                            INSTANCE_NAME,
                            PROJECT_NAME,
                        )
                        .await;
                        test_helpers::instance_simulate_by_name(
                            cptestctx,
                            INSTANCE_NAME,
                            PROJECT_NAME,
                        )
                        .await;
                        test_helpers::instance_delete_by_name(
                            cptestctx,
                            INSTANCE_NAME,
                            PROJECT_NAME,
                        )
                        .await;
                    }

                    delete_disk(client, PROJECT_NAME, DISK_NAME).await;
                    verify_clean_slate(cptestctx, &test).await;
                })
            },
            log,
        )
        .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_use_the_pantry_wrongly_set(
        cptestctx: &ControlPlaneTestContext,
    ) {
        // Test the correct handling of the following race: the user requests a
        // snapshot of a disk that isn't attached to anything (meaning
        // `use_the_pantry` is true, but before the saga starts executing
        // something else attaches the disk to an instance.
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let disk_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let (authz_silo, authz_project, _authz_disk) =
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
            // The disk isn't attached at this time, so don't supply a sled.
            None,
        );

        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Before running the saga, attach the disk to an instance!
        let (.., authz_disk, db_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .fetch_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        assert!(nexus
            .datastore()
            .disk_update_runtime(
                &opctx,
                &authz_disk,
                &db_disk.runtime().attach(Uuid::new_v4()),
            )
            .await
            .expect("failed to attach disk"));

        // Actually run the saga
        let output = nexus.run_saga(runnable_saga).await;

        // Expect to see 503
        match output {
            Err(e) => {
                assert!(matches!(e, Error::ServiceUnavailable { .. }));
            }

            Ok(_) => {
                assert!(false);
            }
        }

        // Detach the disk, then rerun the saga
        let (.., authz_disk, db_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .fetch_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        assert!(nexus
            .datastore()
            .disk_update_runtime(
                &opctx,
                &authz_disk,
                &db_disk.runtime().detach(),
            )
            .await
            .expect("failed to detach disk"));

        // Rerun the saga
        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk_id,
            Name::from_str(DISK_NAME).unwrap().into(),
            // The disk isn't attached at this time, so don't supply a sled.
            None,
        );

        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();
        let output = nexus.run_saga(runnable_saga).await;

        // Expect 200
        assert!(output.is_ok());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_use_the_pantry_wrongly_unset(
        cptestctx: &ControlPlaneTestContext,
    ) {
        // Test the correct handling of the following race: the user requests a
        // snapshot of a disk that is attached to an instance (meaning
        // `use_the_pantry` is false, but before the saga starts executing
        // something else detaches the disk.
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let disk_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let (authz_silo, authz_project, _authz_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .lookup_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        let silo_id = authz_silo.id();
        let project_id = authz_project.id();

        // Synthesize an instance ID to pass to the saga, but use the default
        // test sled ID. This will direct a snapshot request to the simulated
        // sled agent specifying an instance it knows nothing about, which is
        // equivalent to creating an instance, attaching the test disk, creating
        // the saga, stopping the instance, detaching the disk, and then letting
        // the saga run.
        let fake_instance_id = Uuid::new_v4();
        let fake_sled_id =
            Uuid::parse_str(nexus_test_utils::SLED_AGENT_UUID).unwrap();

        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk_id,
            Name::from_str(DISK_NAME).unwrap().into(),
            Some((fake_instance_id, fake_sled_id)),
        );

        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Before running the saga, detach the disk!
        let (.., authz_disk, db_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .fetch_for(authz::Action::Modify)
                .await
                .expect("Failed to look up created disk");

        assert!(nexus
            .datastore()
            .disk_update_runtime(
                &opctx,
                &authz_disk,
                &db_disk.runtime().detach(),
            )
            .await
            .expect("failed to detach disk"));

        // Actually run the saga. This should fail.
        let output = nexus.run_saga(runnable_saga).await;

        assert!(output.is_err());

        // Attach the disk to an instance, then rerun the saga
        populate_ip_pool(
            &client,
            "default",
            Some(
                IpRange::try_from((
                    Ipv4Addr::new(10, 1, 0, 0),
                    Ipv4Addr::new(10, 1, 255, 255),
                ))
                .unwrap(),
            ),
        )
        .await;

        let instance_state = setup_test_instance(
            cptestctx,
            client,
            vec![params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach {
                    name: Name::from_str(DISK_NAME).unwrap(),
                },
            )],
        )
        .await;

        let sled_id = instance_state
            .sled_id()
            .expect("running instance should have a vmm");

        // Rerun the saga
        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk_id,
            Name::from_str(DISK_NAME).unwrap().into(),
            Some((instance_state.instance().id(), sled_id)),
        );

        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();
        let output = nexus.run_saga(runnable_saga).await;

        // Expect 200
        assert!(output.is_ok());
    }
}
