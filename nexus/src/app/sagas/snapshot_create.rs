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
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
    common_storage::{
        call_pantry_attach_for_disk, call_pantry_detach, get_pantry_address,
        is_pantry_gone,
    },
};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use anyhow::anyhow;
use nexus_db_lookup::LookupPath;
use nexus_db_model::Generation;
use nexus_db_queries::db::identity::{Asset, Resource};
use omicron_common::api::external::Error;
use omicron_common::progenitor_operation_retry::ProgenitorOperationRetryError;
use omicron_common::{
    api::external, progenitor_operation_retry::ProgenitorOperationRetry,
};
use omicron_uuid_kinds::{GenericUuid, PropolisUuid, VolumeUuid};
use rand::{RngCore, SeedableRng, rngs::StdRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::CrucibleOpts;
use sled_agent_client::VolumeConstructionRequest;
use sled_agent_client::types::VmmIssueDiskSnapshotRequestBody;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

type ReplaceSocketsMap = BTreeMap<SocketAddrV6, SocketAddrV6>;

// snapshot create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub silo_id: Uuid,
    pub project_id: Uuid,
    pub disk: db::datastore::CrucibleDisk,
    pub attach_instance_id: Option<Uuid>,
    pub use_the_pantry: bool,
    pub create_params: params::SnapshotCreate,
}

// snapshot create saga: actions
declare_saga_actions! {
    snapshot_create;
    TAKE_VOLUME_LOCK -> "volume_lock" {
        + ssc_take_volume_lock
        - ssc_take_volume_lock_undo
    }
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
    RELEASE_VOLUME_LOCK -> "volume_unlock" {
        + ssc_release_volume_lock
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

        builder.append(Node::action(
            "lock_id",
            "GenerateLockId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(take_volume_lock_action());

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

        if !params.use_the_pantry {
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

        if params.use_the_pantry {
            // (Pantry) Set the state back to Detached
            //
            // This has to be the last saga node! Otherwise, concurrent
            // operation on this disk is possible.
            builder.append(detach_disk_from_pantry_action());
        }

        builder.append(release_volume_lock_action());

        Ok(builder.build()?)
    }
}

// snapshot create saga: action implementations

async fn ssc_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
    Ok(())
}

async fn ssc_take_volume_lock(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>("lock_id")?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Without a lock on the volume, this saga can race with the region
    // replacement saga, causing (at least!) two problems:
    //
    // - the request that this saga sends to a propolis to create a snapshot
    //   races with the vcr_replace request that the region replacement drive
    //   saga sends. Imagining that the region set for a disk's volume is [A, B,
    //   C] before either saga starts, then the following scenarios can occur:
    //
    //   1. the snapshot request lands before the vcr_replace request, meaning
    //      snapshots are created for the A, B, and C regions, but the region
    //      set was then modified to [A, B, D]: this means that two of the three
    //      regions have associated snapshots, and this will cause this saga to
    //      unwind when it checks that the associated region snapshots were
    //      created ok. as it's currently written, this saga could also error
    //      during unwind, as it's trying to clean up the snapshots for A, B,
    //      and C, and it could see a "Not Found" when querying C for the
    //      snapshot.
    //
    //   2. the vcr_replace request lands before the snapshot request, meaning
    //      snapshots would be created for the A, B, and D regions. this is a
    //      problem because D is new (having been allocated to replace C), and
    //      the upstairs could be performing live repair. taking a snapshot of
    //      an upstairs during live repair means either:
    //
    //      a. the Upstairs will reject it, causing this saga to unwind ok
    //
    //      b. the Upstairs will allow it, meaning any read-only Upstairs that
    //         uses the associated snapshots ([A', B', D']) will detect that
    //         reconciliation is required, not be able to perform reconciliation
    //         because it is read-only, and panic. note: accepting and
    //         performing a snapshot during live repair is almost certainly a
    //         bug in Crucible, not Nexus!
    //
    //      if the upstairs is _not_ performing live repair yet, then the
    //      snapshot could succeed. This means each of the A, B, and D regions
    //      will have an associated snapshot, but the D snapshot is of a blank
    //      region! the same problem will occur as 2b: a read-only Upstairs that
    //      uses those snapshots as targets will panic because the data doesn't
    //      match and it can't perform reconciliation.
    //
    // - get_allocated_regions will change during the execution of this saga,
    //   due to region replacement(s) occurring.
    //
    // With a lock on the volume, the snapshot creation and region replacement
    // drive sagas are serialized. Note this does mean that region replacement
    // is blocked by a snapshot being created, and snapshot creation is blocked
    // by region replacement.

    osagactx
        .datastore()
        .volume_repair_lock(&opctx, params.disk.volume_id(), lock_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn ssc_take_volume_lock_undo(
    sagactx: NexusActionContext,
) -> anyhow::Result<()> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>("lock_id")?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .volume_repair_unlock(&opctx, params.disk.volume_id(), lock_id)
        .await?;

    Ok(())
}

async fn ssc_alloc_regions(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::CrucibleDataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let destination_volume_id =
        sagactx.lookup::<VolumeUuid>("destination_volume_id")?;

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

    let (.., disk) = LookupPath::new(&opctx, osagactx.datastore())
        .disk_id(params.disk.id())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let strategy = &osagactx.nexus().default_region_allocation_strategy;

    let datasets_and_regions = osagactx
        .datastore()
        .disk_region_allocate(
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
        .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
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
) -> Result<VolumeConstructionRequest, ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let destination_volume_id =
        sagactx.lookup::<VolumeUuid>("destination_volume_id")?;

    let datasets_and_regions = osagactx
        .nexus()
        .ensure_all_datasets_and_regions(
            &log,
            sagactx
                .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
                    "datasets_and_regions",
                )?,
        )
        .await
        .map_err(ActionError::action_failed)?;

    let block_size = datasets_and_regions[0].1.block_size;
    let blocks_per_extent = datasets_and_regions[0].1.extent_size;
    let extent_count = datasets_and_regions[0].1.extent_count;

    // Create volume construction request
    let mut rng = StdRng::from_os_rng();
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *destination_volume_id.as_untyped_uuid(),
        block_size,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            generation: 1,
            opts: CrucibleOpts {
                id: *destination_volume_id.as_untyped_uuid(),
                target: datasets_and_regions
                    .iter()
                    .map(|(dataset, region)| {
                        SocketAddr::V6(
                            dataset.address_with_port(region.port_number),
                        )
                    })
                    .collect::<Vec<_>>(),

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

    Ok(volume_construction_request)
}

async fn ssc_regions_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    warn!(log, "ssc_regions_ensure_undo: Deleting crucible regions");
    osagactx
        .nexus()
        .delete_crucible_regions(
            log,
            sagactx
                .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
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
        sagactx.lookup::<VolumeUuid>("destination_volume_id")?;

    let destination_volume_data =
        sagactx.lookup::<VolumeConstructionRequest>("regions_ensure")?;

    osagactx
        .datastore()
        .volume_create(destination_volume_id, destination_volume_data)
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
        sagactx.lookup::<VolumeUuid>("destination_volume_id")?;

    // This saga contains what is necessary to clean up the destination volume
    // resources. It's safe here to perform a volume hard delete without
    // decreasing the crucible resource count because the destination volume is
    // guaranteed to never have read only resources that require that
    // accounting.

    info!(log, "hard deleting volume {}", destination_volume_id);

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
    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;
    let destination_volume_id =
        sagactx.lookup::<VolumeUuid>("destination_volume_id")?;

    info!(log, "grabbing disk by name {}", params.create_params.disk);

    let (.., disk) = LookupPath::new(&opctx, osagactx.datastore())
        .disk_id(params.disk.id())
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
        volume_id: volume_id.into(),
        destination_volume_id: destination_volume_id.into(),

        generation: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: disk.block_size,
        size: disk.size,
    };

    let (.., authz_project) = LookupPath::new(&opctx, osagactx.datastore())
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
        LookupPath::new(&opctx, osagactx.datastore())
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
    // attached to an instance that _may_ have a running Propolis. Contact that
    // Propolis and ask it to initiate a snapshot. Note that this is
    // best-effort: the instance may have stopped (or may be have stopped, had
    // the disk detached, and resumed running on the same sled) while the saga
    // was executing.
    let Some(attach_instance_id) = params.attach_instance_id else {
        return Err(ActionError::action_failed(Error::internal_error(
            "attach instance id is None!",
        )));
    };

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_instance) = LookupPath::new(&opctx, osagactx.datastore())
        .instance_id(attach_instance_id)
        .lookup_for(authz::Action::Read)
        .await
        .map_err(ActionError::action_failed)?;

    let instance_and_vmm = osagactx
        .datastore()
        .instance_fetch_with_vmm(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    let vmm = instance_and_vmm.vmm();

    // If this instance does not currently have a sled, we can't continue this
    // saga - the user will have to reissue the snapshot request and it will get
    // run on a Pantry.
    let Some((propolis_id, sled_id)) =
        vmm.as_ref().map(|vmm| (vmm.id, vmm.sled_id()))
    else {
        return Err(ActionError::action_failed(Error::unavail(
            "instance no longer has an active VMM!",
        )));
    };

    info!(log, "asking for disk snapshot from Propolis via sled agent";
          "disk_id" => %params.disk.id(),
          "instance_id" => %attach_instance_id,
          "propolis_id" => %propolis_id,
          "sled_id" => %sled_id);

    let sled_agent_client = osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(ActionError::action_failed)?;

    let snapshot_operation = || async {
        sled_agent_client
            .vmm_issue_disk_snapshot_request(
                &PropolisUuid::from_untyped_uuid(propolis_id),
                &params.disk.id(),
                &VmmIssueDiskSnapshotRequestBody { snapshot_id },
            )
            .await
    };
    let gone_check = || async {
        osagactx.datastore().check_sled_in_service(&opctx, sled_id).await?;
        // `check_sled_in_service` returns an error if the sled is no longer in
        // service; if it succeeds, the sled is not gone.
        Ok(false)
    };

    ProgenitorOperationRetry::new(snapshot_operation, gone_check)
        .run(log)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to issue VMM disk snapshot request: {}",
                InlineErrorChain::new(&e)
            ))
        })?;

    Ok(())
}

async fn ssc_send_snapshot_request_to_sled_agent_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    info!(log, "Undoing snapshot request for {snapshot_id}");

    // Lookup the regions used by the source disk...
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(params.disk.volume_id())
        .await?;

    // ... and instruct each of those regions to delete the snapshot.
    for (dataset, region) in datasets_and_regions {
        osagactx
            .nexus()
            .delete_crucible_snapshot(log, &dataset, region.id(), snapshot_id)
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
    // random one. The address that was passed in the disk param might be stale
    // so look it up again here.
    //
    // Return a boolean indicating if additional saga nodes need to attach this
    // disk to that random pantry.

    let disk = match osagactx
        .datastore()
        .disk_get(&opctx, params.disk.id())
        .await
        .map_err(ActionError::action_failed)?
    {
        db::datastore::Disk::Crucible(disk) => disk,
    };

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
        LookupPath::new(&opctx, osagactx.datastore())
            .disk_id(params.disk.id())
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
            info!(log, "setting state of {} to maintenance", params.disk.id());

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
            info!(log, "disk {} in state finalizing", params.disk.id());
        }

        external::DiskState::Attached(attach_instance_id) => {
            // No state change required
            info!(
                log,
                "disk {} in state attached to instance id {}",
                params.disk.id(),
                attach_instance_id
            );
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
    let (.., db_disk) = LookupPath::new(&opctx, osagactx.datastore())
        .disk_id(params.disk.id())
        .fetch_for(authz::Action::Read)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(db_disk.runtime().generation)
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
        LookupPath::new(&opctx, osagactx.datastore())
            .disk_id(params.disk.id())
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    match db_disk.state().into() {
        external::DiskState::Maintenance => {
            info!(
                log,
                "undo: setting disk {} state from maintenance to detached",
                params.disk.id()
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
                "undo: disk {} already in state detached",
                params.disk.id()
            );
        }

        external::DiskState::Finalizing => {
            info!(
                log,
                "undo: disk {} already in state finalizing",
                params.disk.id()
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
            params.disk.id(),
            pantry_address
        );

        call_pantry_attach_for_disk(
            &log,
            &opctx,
            &osagactx.nexus(),
            &params.disk,
            pantry_address,
        )
        .await?;
    } else {
        info!(log, "disk {} already attached to a pantry", params.disk.id());
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
            params.disk.id(),
            pantry_address
        );

        match call_pantry_detach(
            sagactx.user_data().nexus(),
            &log,
            params.disk.id(),
            pantry_address,
        )
        .await
        {
            // We can treat the pantry being permanently gone as success.
            Ok(()) | Err(ProgenitorOperationRetryError::Gone) => (),
            Err(err) => {
                return Err(anyhow!(
                    "failed to detach disk {} from pantry at {}: {}",
                    params.disk.id(),
                    pantry_address,
                    InlineErrorChain::new(&err)
                ));
            }
        }
    } else {
        info!(
            log,
            "undo: not detaching disk {}, was already attached to a pantry",
            params.disk.id()
        );
    }

    Ok(())
}

async fn ssc_call_pantry_snapshot_for_disk(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let nexus = sagactx.user_data().nexus();
    let params = sagactx.saga_params::<Params>()?;

    let (pantry_address, _) =
        sagactx.lookup::<(SocketAddrV6, bool)>("pantry_address")?;
    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    let endpoint = format!("http://{}", pantry_address);

    info!(
        log,
        "sending snapshot request with id {} for disk {} to pantry endpoint {}",
        snapshot_id,
        params.disk.id(),
        endpoint,
    );

    let client = crucible_pantry_client::Client::new(&endpoint);

    let snapshot_operation = || async {
        client
            .snapshot(
                &params.disk.id().to_string(),
                &crucible_pantry_client::types::SnapshotRequest {
                    snapshot_id: snapshot_id.to_string(),
                },
            )
            .await
    };
    let gone_check =
        || async { Ok(is_pantry_gone(nexus, pantry_address, log).await) };

    ProgenitorOperationRetry::new(snapshot_operation, gone_check)
        .run(log)
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
    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;

    info!(log, "Undoing pantry snapshot request for {snapshot_id}");

    // Lookup the regions used by the source disk...
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(params.disk.volume_id())
        .await?;

    // ... and instruct each of those regions to delete the snapshot.
    for (dataset, region) in datasets_and_regions {
        osagactx
            .nexus()
            .delete_crucible_snapshot(log, &dataset, region.id(), snapshot_id)
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
            params.disk.id(),
            pantry_address
        );
        call_pantry_detach(
            sagactx.user_data().nexus(),
            &log,
            params.disk.id(),
            pantry_address,
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "pantry detach failed: {}",
                InlineErrorChain::new(&e)
            ))
        })?;
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
        LookupPath::new(&opctx, osagactx.datastore())
            .disk_id(params.disk.id())
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
            if expected_disk_generation_number == db_disk.runtime().generation {
                info!(
                    log,
                    "setting disk {} state from maintenance to detached",
                    params.disk.id()
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
                    params.disk.id(),
                    db_disk.runtime().generation,
                    expected_disk_generation_number,
                );
            }
        }

        external::DiskState::Detached => {
            info!(log, "disk {} already in state detached", params.disk.id());
        }

        external::DiskState::Finalizing => {
            info!(log, "disk {} already in state finalizing", params.disk.id());
        }

        _ => {
            warn!(log, "disk is in state {:?}", db_disk.state());
        }
    }

    Ok(())
}

async fn ssc_start_running_snapshot(
    sagactx: NexusActionContext,
) -> Result<ReplaceSocketsMap, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    info!(log, "starting running snapshot"; "snapshot_id" => %snapshot_id);

    // For each dataset and region that makes up the disk, create a map from the
    // region information to the new running snapshot information.
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(params.disk.volume_id())
        .await
        .map_err(ActionError::action_failed)?;

    let mut map: ReplaceSocketsMap = BTreeMap::new();

    for (dataset, region) in datasets_and_regions {
        let dataset_addr = dataset.address();

        // Start the snapshot running
        let (crucible_region, _, crucible_running_snapshot) = osagactx
            .nexus()
            .ensure_crucible_running_snapshot(
                &log,
                &dataset,
                region.id(),
                snapshot_id,
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(ActionError::action_failed)?;

        info!(
            log,
            "successfully started running region snapshot";
            "running snapshot" => ?crucible_running_snapshot
        );

        // Map from the region to the snapshot
        let region_addr = SocketAddrV6::new(
            *dataset_addr.ip(),
            crucible_region.port_number,
            0,
            0,
        );

        let snapshot_addr = SocketAddrV6::new(
            *dataset_addr.ip(),
            crucible_running_snapshot.port_number,
            0,
            0,
        );

        info!(log, "map {} to {}", region_addr, snapshot_addr);
        map.insert(region_addr, snapshot_addr);

        // Once snapshot has been validated, and running snapshot has been
        // started, add an entry in the region_snapshot table to correspond to
        // these Crucible resources.
        osagactx
            .datastore()
            .region_snapshot_create(db::model::RegionSnapshot {
                dataset_id: dataset.id().into(),
                region_id: region.id(),
                snapshot_id,
                snapshot_addr: snapshot_addr.to_string(),
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

    let snapshot_id = sagactx.lookup::<Uuid>("snapshot_id")?;
    info!(log, "Undoing snapshot start running request for {snapshot_id}");

    // Lookup the regions used by the source disk...
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(params.disk.volume_id())
        .await?;

    // ... and instruct each of those regions to delete the running snapshot.
    for (dataset, region) in datasets_and_regions {
        osagactx
            .nexus()
            .delete_crucible_running_snapshot(
                &log,
                &dataset,
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

    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;

    // For a snapshot, copy the volume construction request at the time the
    // snapshot was taken.

    let disk_volume = osagactx
        .datastore()
        .volume_checkout(
            params.disk.volume_id(),
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "disk volume construction request {}", disk_volume.data());

    let disk_volume_construction_request =
        serde_json::from_str(&disk_volume.data()).map_err(|e| {
            ActionError::action_failed(Error::internal_error(&format!(
                "failed to deserialize disk {} volume data: {}",
                params.disk.id(),
                e,
            )))
        })?;

    // The volume construction request must then be modified to point to the
    // read-only crucible agent downstairs (corresponding to this snapshot)
    // launched through this saga.
    let replace_sockets_map =
        sagactx.lookup::<ReplaceSocketsMap>("replace_sockets_map")?;
    let snapshot_volume_construction_request: VolumeConstructionRequest =
        create_snapshot_from_disk(
            &disk_volume_construction_request,
            &replace_sockets_map,
        )
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    // Insert volume record into the DB

    info!(
        log,
        "snapshot volume construction request {:?}",
        snapshot_volume_construction_request
    );

    osagactx
        .datastore()
        .volume_create(volume_id, snapshot_volume_construction_request)
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
    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;

    // `volume_create` will increase the resource count for read only resources
    // in a volume, which there are guaranteed to be for snapshot volumes.
    // decreasing crucible resources is necessary as an undo step. Do not call
    // `volume_hard_delete` here: soft deleting volumes is necessary for
    // `find_deleted_volume_regions` to work.

    info!(log, "calling soft delete for volume {}", volume_id);

    osagactx.datastore().soft_delete_volume(volume_id).await?;

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
        LookupPath::new(&opctx, osagactx.datastore())
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
            db_snapshot.generation,
            db::model::SnapshotState::Ready,
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(log, "snapshot finalized!");

    Ok(snapshot)
}

async fn ssc_release_volume_lock(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>("lock_id")?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .volume_repair_unlock(&opctx, params.disk.volume_id(), lock_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

// helper functions

/// Create a Snapshot VolumeConstructionRequest by copying a disk's
/// VolumeConstructionRequest and modifying it accordingly.
fn create_snapshot_from_disk(
    disk: &VolumeConstructionRequest,
    socket_map: &ReplaceSocketsMap,
) -> anyhow::Result<VolumeConstructionRequest> {
    // When copying a disk's VolumeConstructionRequest to turn it into a
    // snapshot:
    //
    // - generate new IDs for each layer
    // - set read-only
    // - remove any control sockets

    let mut new_vcr = disk.clone();

    struct Work<'a> {
        vcr_part: &'a mut VolumeConstructionRequest,
        socket_modification_required: bool,
    }

    let mut parts: VecDeque<Work> = VecDeque::new();
    parts.push_back(Work {
        vcr_part: &mut new_vcr,
        socket_modification_required: true,
    });

    while let Some(work) = parts.pop_front() {
        match work.vcr_part {
            VolumeConstructionRequest::Volume {
                id,
                sub_volumes,
                read_only_parent,
                ..
            } => {
                *id = Uuid::new_v4();

                for sub_volume in sub_volumes {
                    parts.push_back(Work {
                        vcr_part: sub_volume,
                        // Inherit if socket modification is required from the
                        // parent layer
                        socket_modification_required: work
                            .socket_modification_required,
                    });
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(Work {
                        vcr_part: read_only_parent,
                        // no socket modification required for read-only parents
                        socket_modification_required: false,
                    });
                }
            }

            VolumeConstructionRequest::Url { id, .. } => {
                *id = Uuid::new_v4();
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                opts.id = Uuid::new_v4();
                opts.read_only = true;
                opts.control = None;

                if work.socket_modification_required {
                    for target in &mut opts.target {
                        let target = match target {
                            SocketAddr::V6(v6) => v6,
                            SocketAddr::V4(_) => {
                                anyhow::bail!(
                                    "unexpected IPv4 address in VCR: {:?}",
                                    work.vcr_part
                                )
                            }
                        };

                        *target = *socket_map.get(target).ok_or_else(|| {
                            anyhow!("target {} not found in map!", target)
                        })?;
                    }
                }
            }

            VolumeConstructionRequest::File { id, .. } => {
                *id = Uuid::new_v4();
            }
        }
    }

    Ok(new_vcr)
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::app::saga::create_saga_dag;
    use crate::app::sagas::test_helpers;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::DataStore;
    use nexus_db_queries::db::datastore::Disk;
    use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
    use nexus_test_utils::resource_helpers::create_default_ip_pool;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::delete_disk;
    use nexus_test_utils::resource_helpers::object_create;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::params::InstanceDiskAttachment;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Instance;
    use omicron_common::api::external::InstanceCpuCount;
    use omicron_common::api::external::Name;
    use omicron_common::api::external::NameOrId;
    use sled_agent_client::CrucibleOpts;
    use sled_agent_client::TestInterfaces as SledAgentTestInterfaces;
    use std::str::FromStr;

    type DiskTest<'a> =
        nexus_test_utils::resource_helpers::DiskTest<'a, crate::Server>;

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
                            generation: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                key: Some("tkBksPOA519q11jvLCCX5P8t8+kCX4ZNzr+QP8M+TSg=".into()),
                                lossy: false,
                                read_only: true,
                                target: vec![
                                    "[fd00:1122:3344:101::8]:19001".parse().unwrap(),
                                    "[fd00:1122:3344:101::7]:19001".parse().unwrap(),
                                    "[fd00:1122:3344:101::6]:19001".parse().unwrap(),
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
                    generation: 100,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        key: Some("jVex5Zfm+avnFMyezI6nCVPRPs53EWwYMN844XETDBM=".into()),
                        lossy: false,
                        read_only: false,
                        target: vec![
                            "[fd00:1122:3344:101::8]:19002".parse().unwrap(),
                            "[fd00:1122:3344:101::7]:19002".parse().unwrap(),
                            "[fd00:1122:3344:101::6]:19002".parse().unwrap(),
                        ],
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        flush_timeout: None,
                        control: Some("127.0.0.1:12345".parse().unwrap()),
                    }
                },
            ],
        };

        let mut replace_sockets = ReplaceSocketsMap::new();

        // Replacements for top level Region only
        replace_sockets.insert(
            "[fd00:1122:3344:101::6]:19002".parse().unwrap(),
            "[fd01:1122:3344:101::6]:9000".parse().unwrap(),
        );
        replace_sockets.insert(
            "[fd00:1122:3344:101::7]:19002".parse().unwrap(),
            "[fd01:1122:3344:101::7]:9000".parse().unwrap(),
        );
        replace_sockets.insert(
            "[fd00:1122:3344:101::8]:19002".parse().unwrap(),
            "[fd01:1122:3344:101::8]:9000".parse().unwrap(),
        );

        let snapshot =
            create_snapshot_from_disk(&disk, &replace_sockets).unwrap();

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

    async fn create_project_and_disk_and_pool(
        client: &ClientTestContext,
    ) -> Uuid {
        create_default_ip_pool(&client).await;
        create_project(client, PROJECT_NAME).await;
        create_disk(client, PROJECT_NAME, DISK_NAME).await.identity.id
    }

    // Helper for creating snapshot create parameters
    fn new_test_params(
        opctx: &OpContext,
        silo_id: Uuid,
        project_id: Uuid,
        db_disk: db::datastore::CrucibleDisk,
        disk: NameOrId,
        attach_instance_id: Option<Uuid>,
        use_the_pantry: bool,
    ) -> Params {
        Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id,
            project_id,
            disk: db_disk,
            attach_instance_id,
            use_the_pantry,
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
            cptestctx.server.server_context().nexus.datastore().clone(),
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
        let nexus = &cptestctx.server.server_context().nexus;
        let disk_id = create_project_and_disk_and_pool(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let (authz_silo, authz_project, _authz_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .lookup_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        let Disk::Crucible(disk) =
            nexus.datastore().disk_get(&opctx, disk_id).await.unwrap();

        let silo_id = authz_silo.id();
        let project_id = authz_project.id();

        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk,
            Name::from_str(DISK_NAME).unwrap().into(),
            None, // not attached to an instance
            true, // use the pantry
        );
        // Actually run the saga
        let output = nexus
            .sagas
            .saga_execute::<SagaSnapshotCreate>(params)
            .await
            .unwrap();

        let snapshot = output
            .lookup_node_output::<nexus_db_queries::db::model::Snapshot>(
                "finalized_snapshot",
            )
            .unwrap();
        assert_eq!(snapshot.project_id, project_id);
    }

    async fn no_snapshot_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Snapshot;
        use nexus_db_schema::schema::snapshot::dsl;

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
        use nexus_db_schema::schema::region_snapshot::dsl;

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
        test: &DiskTest<'_>,
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
        let datastore = cptestctx.server.server_context().nexus.datastore();
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

        let mut disks_iter = disks_to_attach.into_iter();
        let boot_disk = disks_iter.next();
        let data_disks: Vec<InstanceDiskAttachment> = disks_iter.collect();

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
                hostname: "base-instance".parse().unwrap(),
                user_data:
                    b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                        .to_vec(),
                ssh_public_keys:  Some(Vec::new()),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::None,
                boot_disk,
                cpu_platform: None,
                disks: data_disks,
                external_ips: vec![],
                start: true,
                auto_restart_policy: Default::default(),
                anti_affinity_groups: Vec::new(),
                multicast_groups: Vec::new(),
            },
        )
        .await;

        // Read out the instance's assigned sled, then poke the instance to get
        // it from the Starting state to the Running state so the test disk can
        // be snapshotted.
        let nexus = &cptestctx.server.server_context().nexus;
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

        let vmm_state = instance_state
            .vmm()
            .as_ref()
            .expect("starting instance should have a vmm");
        let propolis_id = PropolisUuid::from_untyped_uuid(vmm_state.id);
        let sled_id = vmm_state.sled_id();
        let sa = nexus.sled_client(&sled_id).await.unwrap();
        sa.vmm_finish_transition(propolis_id).await;

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
            .state;

        assert_eq!(new_state, nexus_db_model::VmmState::Running);

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
        let nexus = &cptestctx.server.server_context().nexus;
        let disk_id = create_project_and_disk_and_pool(&client).await;

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

                        let Disk::Crucible(disk) = nexus
                            .datastore()
                            .disk_get(&opctx, disk_id)
                            .await
                            .unwrap();

                        // If the pantry isn't being used, make sure the disk is
                        // attached. Note that under normal circumstances, a
                        // disk can only be attached to a stopped instance, but
                        // since this is just a test, bypass the normal
                        // attachment machinery and just update the disk's
                        // database record directly.
                        let attach_instance_id = if !use_the_pantry {
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

                            Some(state.instance().id())
                        } else {
                            None
                        };

                        new_test_params(
                            &opctx,
                            silo_id,
                            project_id,
                            disk,
                            Name::from_str(DISK_NAME).unwrap().into(),
                            attach_instance_id,
                            use_the_pantry,
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

                        assert!(
                            nexus
                                .datastore()
                                .disk_update_runtime(
                                    &opctx,
                                    &authz_disk,
                                    &db_disk.runtime().detach(),
                                )
                                .await
                                .expect("failed to detach disk")
                        );

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
                        // Wait until the instance has advanced to the `NoVmm`
                        // state before deleting it. This may not happen
                        // immediately, as the `Nexus::cpapi_instances_put` API
                        // endpoint simply writes the new VMM state to the
                        // database and *starts* an `instance-update` saga, and
                        // the instance record isn't updated until that saga
                        // completes.
                        test_helpers::instance_wait_for_state_by_name(
                            cptestctx,
                            INSTANCE_NAME,
                            PROJECT_NAME,
                            nexus_db_model::InstanceState::NoVmm,
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
        let nexus = &cptestctx.server.server_context().nexus;
        let disk_id = create_project_and_disk_and_pool(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let (authz_silo, authz_project, _authz_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .lookup_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        let Disk::Crucible(disk) =
            nexus.datastore().disk_get(&opctx, disk_id).await.unwrap();

        let silo_id = authz_silo.id();
        let project_id = authz_project.id();

        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk,
            Name::from_str(DISK_NAME).unwrap().into(),
            // The disk isn't attached at this time, so don't supply a sled.
            None,
            true, // use the pantry
        );

        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.sagas.saga_prepare(dag).await.unwrap();

        // Before running the saga, attach the disk to an instance!
        let _instance_and_vmm = setup_test_instance(
            &cptestctx,
            &client,
            vec![params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach {
                    name: Name::from_str(DISK_NAME).unwrap(),
                },
            )],
        )
        .await;

        // Actually run the saga
        let output = runnable_saga
            .run_to_completion()
            .await
            .unwrap()
            .into_omicron_result();

        // Expect to see 409
        match output {
            Err(e) => {
                assert!(matches!(e, Error::Conflict { .. }));
            }

            Ok(_) => {
                assert!(false);
            }
        }

        // Detach the disk, then rerun the saga
        let (.., authz_disk) = LookupPath::new(&opctx, nexus.datastore())
            .disk_id(disk_id)
            .lookup_for(authz::Action::Read)
            .await
            .expect("Failed to look up created disk");

        let Disk::Crucible(disk) =
            nexus.datastore().disk_get(&opctx, disk_id).await.unwrap();

        assert!(
            nexus
                .datastore()
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &disk.runtime().detach(),
                )
                .await
                .expect("failed to detach disk")
        );

        // Rerun the saga
        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk,
            Name::from_str(DISK_NAME).unwrap().into(),
            // The disk isn't attached at this time, so don't supply a sled.
            None,
            true, // use the pantry
        );

        let output =
            nexus.sagas.saga_execute::<SagaSnapshotCreate>(params).await;

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
        let nexus = &cptestctx.server.server_context().nexus;
        let disk_id = create_project_and_disk_and_pool(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let (authz_silo, authz_project, authz_disk) =
            LookupPath::new(&opctx, nexus.datastore())
                .disk_id(disk_id)
                .lookup_for(authz::Action::Read)
                .await
                .expect("Failed to look up created disk");

        let Disk::Crucible(disk) =
            nexus.datastore().disk_get(&opctx, disk_id).await.unwrap();

        let silo_id = authz_silo.id();
        let project_id = authz_project.id();

        // Synthesize an instance ID to pass to the saga, but use the default
        // test sled ID. This will direct a snapshot request to the simulated
        // sled agent specifying an instance it knows nothing about, which is
        // equivalent to creating an instance, attaching the test disk, creating
        // the saga, stopping the instance, detaching the disk, and then letting
        // the saga run.
        let fake_instance_id = Uuid::new_v4();

        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk.clone(),
            Name::from_str(DISK_NAME).unwrap().into(),
            Some(fake_instance_id),
            false, // use the pantry
        );

        let dag = create_saga_dag::<SagaSnapshotCreate>(params).unwrap();
        let runnable_saga = nexus.sagas.saga_prepare(dag).await.unwrap();

        // Before running the saga, detach the disk!

        assert!(
            nexus
                .datastore()
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &disk.runtime().detach(),
                )
                .await
                .expect("failed to detach disk")
        );

        // Actually run the saga. This should fail.
        let output = runnable_saga
            .run_to_completion()
            .await
            .unwrap()
            .into_omicron_result();

        assert!(output.is_err());

        // Attach the disk to an instance, then rerun the saga
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

        // Rerun the saga
        let params = new_test_params(
            &opctx,
            silo_id,
            project_id,
            disk,
            Name::from_str(DISK_NAME).unwrap().into(),
            Some(instance_state.instance().id()),
            false, // use the pantry
        );

        let output =
            nexus.sagas.saga_execute::<SagaSnapshotCreate>(params).await;

        // Expect 200
        assert!(output.is_ok());
    }
}
