// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
    common_storage::{
        call_pantry_attach_for_disk, call_pantry_detach, get_pantry_address,
    },
};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::db::identity::{Asset, Resource};
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::VolumeUuid;
use rand::{RngCore, SeedableRng, rngs::StdRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::{CrucibleOpts, VolumeConstructionRequest};
use std::convert::TryFrom;
use std::net::SocketAddrV6;
use std::{collections::VecDeque, net::SocketAddr};
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// disk create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub create_params: params::DiskCreate,
}

// disk create saga: actions

declare_saga_actions! {
    disk_create;
    CREATE_CRUCIBLE_DISK_RECORD -> "crucible_disk" {
        + sdc_create_crucible_disk_record
        - sdc_create_disk_record_undo
    }
    CREATE_LOCAL_STORAGE_DISK_RECORD -> "local_storage_disk" {
        + sdc_create_local_storage_disk_record
        - sdc_create_disk_record_undo
    }
    REGIONS_ALLOC -> "datasets_and_regions" {
        + sdc_alloc_regions
        - sdc_alloc_regions_undo
    }
    SPACE_ACCOUNT -> "no_result" {
        + sdc_account_space
        - sdc_account_space_undo
    }
    REGIONS_ENSURE_UNDO -> "regions_ensure_undo" {
        + sdc_noop
        - sdc_regions_ensure_undo
    }
    REGIONS_ENSURE -> "regions_ensure" {
        + sdc_regions_ensure
    }
    CREATE_VOLUME_RECORD -> "created_volume" {
        + sdc_create_volume_record
        - sdc_create_volume_record_undo
    }
    FINALIZE_DISK_RECORD -> "created_disk" {
        + sdc_finalize_disk_record
    }
    GET_PANTRY_ADDRESS -> "pantry_address" {
        + sdc_get_pantry_address
    }
    CALL_PANTRY_ATTACH_FOR_DISK -> "call_pantry_attach_for_disk" {
        + sdc_call_pantry_attach_for_disk
        - sdc_call_pantry_attach_for_disk_undo
    }
}

// disk create saga: definition

#[derive(Debug)]
pub(crate) struct SagaDiskCreate;
impl NexusSaga for SagaDiskCreate {
    const NAME: &'static str = "disk-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        disk_create_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            "disk_id",
            "GenerateDiskId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "volume_id",
            "GenerateVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(space_account_action());

        match &params.create_params {
            params::DiskCreate::Crucible { .. } => {
                builder.append(create_crucible_disk_record_action());
                builder.append(regions_alloc_action());
                builder.append(regions_ensure_undo_action());
                builder.append(regions_ensure_action());
                builder.append(create_volume_record_action());
            }

            params::DiskCreate::LocalStorage { .. } => {
                builder.append(create_local_storage_disk_record_action());
            }
        }

        builder.append(finalize_disk_record_action());

        match &params.create_params {
            params::DiskCreate::Crucible { disk_source, .. } => {
                match disk_source {
                    params::DiskSource::ImportingBlocks { .. } => {
                        builder.append(get_pantry_address_action());
                        builder.append(call_pantry_attach_for_disk_action());
                    }

                    _ => {}
                }
            }

            params::DiskCreate::LocalStorage { .. } => {
                // nothing to do!
            }
        }

        Ok(builder.build()?)
    }
}

// disk create saga: action implementations

async fn sdc_create_crucible_disk_record(
    sagactx: NexusActionContext,
) -> Result<db::datastore::CrucibleDisk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let disk_source = match &params.create_params {
        params::DiskCreate::Crucible { disk_source, .. } => disk_source,

        params::DiskCreate::LocalStorage { .. } => {
            // This should be unreachable given the match performed in
            // `make_saga_dag`!
            return Err(ActionError::action_failed(Error::internal_error(
                "wrong DiskCreate variant!",
            )));
        }
    };

    let block_size: db::model::BlockSize = match &disk_source {
        params::DiskSource::Blank { block_size } => {
            db::model::BlockSize::try_from(*block_size).map_err(|e| {
                ActionError::action_failed(Error::internal_error(
                    &e.to_string(),
                ))
            })?
        }
        params::DiskSource::Snapshot { snapshot_id } => {
            let (.., db_snapshot) =
                LookupPath::new(&opctx, osagactx.datastore())
                    .snapshot_id(*snapshot_id)
                    .fetch()
                    .await
                    .map_err(|e| {
                        ActionError::action_failed(Error::internal_error(
                            &e.to_string(),
                        ))
                    })?;

            db_snapshot.block_size
        }
        params::DiskSource::Image { image_id } => {
            let (.., image) = LookupPath::new(&opctx, osagactx.datastore())
                .image_id(*image_id)
                .fetch()
                .await
                .map_err(|e| {
                    ActionError::action_failed(Error::internal_error(
                        &e.to_string(),
                    ))
                })?;

            image.block_size
        }
        params::DiskSource::ImportingBlocks { block_size } => {
            db::model::BlockSize::try_from(*block_size).map_err(|e| {
                ActionError::action_failed(Error::internal_error(
                    &e.to_string(),
                ))
            })?
        }
    };

    let disk = db::model::Disk::new(
        disk_id,
        params.project_id,
        &params.create_params,
        block_size,
        db::model::DiskRuntimeState::new(),
        db::model::DiskType::Crucible,
    );

    let disk_type_crucible = db::model::DiskTypeCrucible::new(
        disk_id,
        // We admittedly reference the volume before it has been allocated, but
        // this should be acceptable because the disk remains in a "Creating"
        // state until the saga has completed.
        sagactx.lookup::<VolumeUuid>("volume_id")?,
        &disk_source,
    );

    let crucible_disk =
        db::datastore::CrucibleDisk { disk, disk_type_crucible };

    let (.., authz_project) = LookupPath::new(&opctx, osagactx.datastore())
        .project_id(params.project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .datastore()
        .project_create_disk(
            &opctx,
            &authz_project,
            db::datastore::Disk::Crucible(crucible_disk.clone()),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(crucible_disk)
}

async fn sdc_create_disk_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    osagactx
        .datastore()
        .project_delete_disk_no_auth(
            &disk_id,
            &[DiskState::Detached, DiskState::Faulted, DiskState::Creating],
        )
        .await?;

    Ok(())
}

async fn sdc_create_local_storage_disk_record(
    sagactx: NexusActionContext,
) -> Result<db::datastore::LocalStorageDisk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let block_size = match &params.create_params {
        params::DiskCreate::Crucible { .. } => {
            // This should be unreachable given the match performed in
            // `make_saga_dag`!
            return Err(ActionError::action_failed(Error::internal_error(
                "wrong DiskCreate variant!",
            )));
        }

        params::DiskCreate::LocalStorage { .. } => {
            // All LocalStorage disks have a block size of 4k
            db::model::BlockSize::AdvancedFormat
        }
    };

    let disk = db::model::Disk::new(
        disk_id,
        params.project_id,
        &params.create_params,
        block_size,
        db::model::DiskRuntimeState::new(),
        db::model::DiskType::LocalStorage,
    );

    let disk_type_local_storage = db::model::DiskTypeLocalStorage::new(
        disk_id,
        params.create_params.size(),
    )
    .map_err(ActionError::action_failed)?;

    let local_storage_disk =
        db::datastore::LocalStorageDisk::new(disk, disk_type_local_storage);

    let (.., authz_project) = LookupPath::new(&opctx, osagactx.datastore())
        .project_id(params.project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .datastore()
        .project_create_disk(
            &opctx,
            &authz_project,
            db::datastore::Disk::LocalStorage(local_storage_disk.clone()),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(local_storage_disk)
}

async fn sdc_alloc_regions(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::CrucibleDataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;

    // Ensure the disk is backed by appropriate regions.
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

    let strategy = &osagactx.nexus().default_region_allocation_strategy;

    let disk_source = match &params.create_params {
        params::DiskCreate::Crucible { disk_source, .. } => disk_source,

        params::DiskCreate::LocalStorage { .. } => {
            // This should be unreachable given the match performed in
            // `make_saga_dag`!
            return Err(ActionError::action_failed(Error::internal_error(
                "wrong DiskCreate variant!",
            )));
        }
    };

    let datasets_and_regions = osagactx
        .datastore()
        .disk_region_allocate(
            &opctx,
            volume_id,
            &disk_source,
            params.create_params.size(),
            &strategy,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(datasets_and_regions)
}

async fn sdc_alloc_regions_undo(
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

async fn sdc_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_disk(
            &opctx,
            disk_id,
            params.project_id,
            params.create_params.size().into(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdc_account_space_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_disk(
            &opctx,
            disk_id,
            params.project_id,
            params.create_params.size().into(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdc_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
    Ok(())
}

/// Call out to Crucible agent and perform region creation.
async fn sdc_regions_ensure(
    sagactx: NexusActionContext,
) -> Result<VolumeConstructionRequest, ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

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

    // If a disk source was requested, set the read-only parent of this disk.
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let disk_source = match &params.create_params {
        params::DiskCreate::Crucible { disk_source, .. } => disk_source,

        params::DiskCreate::LocalStorage { .. } => {
            // This should be unreachable given the match performed in
            // `make_saga_dag`!
            return Err(ActionError::action_failed(Error::internal_error(
                "wrong DiskCreate variant!",
            )));
        }
    };

    let mut read_only_parent: Option<Box<VolumeConstructionRequest>> =
        match disk_source {
            params::DiskSource::Blank { block_size: _ } => None,
            params::DiskSource::Snapshot { snapshot_id } => {
                debug!(log, "grabbing snapshot {}", snapshot_id);

                let (.., db_snapshot) =
                    LookupPath::new(&opctx, osagactx.datastore())
                        .snapshot_id(*snapshot_id)
                        .fetch()
                        .await
                        .map_err(ActionError::action_failed)?;

                debug!(
                    log,
                    "grabbing snapshot {} volume {}",
                    db_snapshot.id(),
                    db_snapshot.volume_id(),
                );

                let volume = osagactx
                    .datastore()
                    .volume_checkout(
                        db_snapshot.volume_id(),
                        db::datastore::VolumeCheckoutReason::ReadOnlyCopy,
                    )
                    .await
                    .map_err(ActionError::action_failed)?;

                debug!(
                    log,
                    "grabbed volume {}, with data {}",
                    volume.id(),
                    volume.data()
                );

                Some(Box::new(serde_json::from_str(volume.data()).map_err(
                    |e| {
                        ActionError::action_failed(Error::internal_error(
                            &format!(
                                "failed to deserialize volume data: {}",
                                e,
                            ),
                        ))
                    },
                )?))
            }
            params::DiskSource::Image { image_id } => {
                debug!(log, "grabbing image {}", image_id);

                let (.., image) = LookupPath::new(&opctx, osagactx.datastore())
                    .image_id(*image_id)
                    .fetch()
                    .await
                    .map_err(ActionError::action_failed)?;

                debug!(log, "retrieved project image {}", image.id());

                debug!(
                    log,
                    "grabbing image {} volume {}",
                    image.id(),
                    image.volume_id(),
                );

                let volume = osagactx
                    .datastore()
                    .volume_checkout(
                        image.volume_id(),
                        db::datastore::VolumeCheckoutReason::ReadOnlyCopy,
                    )
                    .await
                    .map_err(ActionError::action_failed)?;

                debug!(
                    log,
                    "grabbed volume {}, with data {}",
                    volume.id(),
                    volume.data()
                );

                Some(Box::new(serde_json::from_str(volume.data()).map_err(
                    |e| {
                        ActionError::action_failed(Error::internal_error(
                            &format!(
                                "failed to deserialize volume data: {}",
                                e,
                            ),
                        ))
                    },
                )?))
            }
            params::DiskSource::ImportingBlocks { block_size: _ } => None,
        };

    // Each ID should be unique to this disk
    if let Some(read_only_parent) = &mut read_only_parent {
        *read_only_parent = Box::new(
            randomize_volume_construction_request_ids(&read_only_parent)
                .map_err(|e| {
                    ActionError::action_failed(Error::internal_error(&format!(
                        "failed to randomize ids: {}",
                        e,
                    )))
                })?,
        );
    }

    // Create volume construction request for this disk
    let mut rng = StdRng::from_os_rng();
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: disk_id,
        block_size,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            generation: 1,
            opts: CrucibleOpts {
                id: disk_id,
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

                read_only: false,
            },
        }],
        read_only_parent,
    };

    Ok(volume_construction_request)
}

async fn sdc_regions_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    warn!(log, "sdc_regions_ensure_undo: Deleting crucible regions");

    let result = osagactx
        .nexus()
        .delete_crucible_regions(
            log,
            sagactx
                .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
                    "datasets_and_regions",
                )?,
        )
        .await;

    match result {
        Err(e) => {
            // If we cannot delete the regions, then returning an error will
            // cause the saga to stop unwinding and be stuck. This will leave
            // the disk that the saga created: change that disk's state to
            // Faulted here.

            error!(
                log,
                "sdc_regions_ensure_undo: Deleting crucible regions failed with {}",
                e
            );

            let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
            let (.., authz_disk, db_disk) = LookupPath::new(&opctx, datastore)
                .disk_id(disk_id)
                .fetch_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            datastore
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &db_disk.runtime().faulted(),
                )
                .await?;

            return Err(e.into());
        }

        Ok(()) => {
            info!(log, "sdc_regions_ensure_undo: Deleted crucible regions");
        }
    }

    Ok(())
}

async fn sdc_create_volume_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;
    let volume_data =
        sagactx.lookup::<VolumeConstructionRequest>("regions_ensure")?;

    osagactx
        .datastore()
        .volume_create(volume_id, volume_data)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sdc_create_volume_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;

    // Depending on the read only parent, there will some read only resources
    // used, however this saga tracks them all.
    osagactx.datastore().soft_delete_volume(volume_id).await?;

    osagactx.datastore().volume_hard_delete(volume_id).await?;

    Ok(())
}

async fn sdc_finalize_disk_record(
    sagactx: NexusActionContext,
) -> Result<db::datastore::Disk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    let (.., authz_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    // TODO-security Review whether further actions in this node can ever fail
    // an authz check.  We don't want this to ever fail the authz check here --
    // if it did, we would have wanted to catch that a lot sooner.  It wouldn't
    // make sense for it to fail anyway because we're modifying something that
    // *we* just created.  Right now, it's very unlikely that it would ever fail
    // because we checked Action::CreateChild on the Project before we created
    // this saga.  The only role that gets that permission is "project
    // collaborator", which also gets Action::Modify on Disks within the
    // Project.  So this shouldn't break in practice.  However, that's brittle.
    // It would be better if this were better guaranteed.

    match params.create_params {
        params::DiskCreate::Crucible { disk_source, .. } => {
            let disk_created = db::datastore::Disk::Crucible(
                sagactx
                    .lookup::<db::datastore::CrucibleDisk>("crucible_disk")?,
            );

            match disk_source {
                params::DiskSource::ImportingBlocks { .. } => {
                    datastore
                        .disk_update_runtime(
                            &opctx,
                            &authz_disk,
                            &disk_created.runtime().import_ready(),
                        )
                        .await
                        .map_err(ActionError::action_failed)?;
                }

                _ => {
                    datastore
                        .disk_update_runtime(
                            &opctx,
                            &authz_disk,
                            &disk_created.runtime().detach(),
                        )
                        .await
                        .map_err(ActionError::action_failed)?;
                }
            }

            Ok(disk_created)
        }

        params::DiskCreate::LocalStorage { .. } => {
            let disk_created = db::datastore::Disk::LocalStorage(
                sagactx.lookup::<db::datastore::LocalStorageDisk>(
                    "local_storage_disk",
                )?,
            );

            datastore
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &disk_created.runtime().detach(),
                )
                .await
                .map_err(ActionError::action_failed)?;

            Ok(disk_created)
        }
    }
}

async fn sdc_get_pantry_address(
    sagactx: NexusActionContext,
) -> Result<SocketAddrV6, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let datastore = osagactx.datastore();
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    // Pick a random Pantry and use it for this disk. This will be the
    // Pantry used for all subsequent import operations until the disk
    // is "finalized".
    let pantry_address = get_pantry_address(osagactx.nexus()).await?;

    info!(
        log,
        "using pantry at {} for importing to disk {}", pantry_address, disk_id
    );

    let (.., authz_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    datastore
        .disk_set_pantry(&opctx, &authz_disk, pantry_address)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(pantry_address)
}

async fn sdc_call_pantry_attach_for_disk(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let disk_created =
        sagactx.lookup::<db::datastore::CrucibleDisk>("crucible_disk")?;

    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;

    call_pantry_attach_for_disk(
        &log,
        &opctx,
        &osagactx.nexus(),
        &disk_created,
        pantry_address,
    )
    .await?;

    Ok(())
}

async fn sdc_call_pantry_attach_for_disk_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;

    call_pantry_detach(
        sagactx.user_data().nexus(),
        &log,
        disk_id,
        pantry_address,
    )
    .await?;

    Ok(())
}

// helper functions

/// Generate new IDs for each layer
fn randomize_volume_construction_request_ids(
    input: &VolumeConstructionRequest,
) -> anyhow::Result<VolumeConstructionRequest> {
    let mut new_vcr = input.clone();

    let mut parts: VecDeque<&mut VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(&mut new_vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume {
                id,
                sub_volumes,
                read_only_parent,
                ..
            } => {
                *id = Uuid::new_v4();

                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(read_only_parent);
                }
            }

            VolumeConstructionRequest::Url { id, .. } => {
                *id = Uuid::new_v4();
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                opts.id = Uuid::new_v4();
            }

            VolumeConstructionRequest::File { id, .. } => {
                *id = Uuid::new_v4();
            }
        }
    }

    Ok(new_vcr)
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_create::Params,
        app::sagas::disk_create::SagaDiskCreate, external_api::params,
    };
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db;
    use nexus_db_queries::db::datastore::DataStore;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_sled_agent::sim::SledAgent;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;
    type DiskTest<'a> =
        nexus_test_utils::resource_helpers::DiskTest<'a, crate::Server>;

    const DISK_NAME: &str = "my-disk";
    const PROJECT_NAME: &str = "springfield-squidport";

    pub fn new_disk_create_params() -> params::DiskCreate {
        params::DiskCreate::Crucible {
            identity: IdentityMetadataCreateParams {
                name: DISK_NAME.parse().expect("Invalid disk name"),
                description: "My disk".to_string(),
            },
            disk_source: params::DiskSource::Blank {
                block_size: params::BlockSize(512),
            },
            size: ByteCount::from_gibibytes_u32(1),
        }
    }

    // Helper for creating disk create parameters
    fn new_test_params(opctx: &OpContext, project_id: Uuid) -> Params {
        Params {
            serialized_authn: Serialized::for_opctx(opctx),
            project_id,
            create_params: new_disk_create_params(),
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
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        // Build the saga DAG with the provided test parameters and run it.
        let opctx = test_opctx(cptestctx);
        let params = new_test_params(&opctx, project_id);
        let output =
            nexus.sagas.saga_execute::<SagaDiskCreate>(params).await.unwrap();
        let disk = output
            .lookup_node_output::<db::datastore::Disk>("created_disk")
            .unwrap();
        assert_eq!(disk.project_id(), project_id);
    }

    async fn no_disk_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Disk;
        use nexus_db_schema::schema::disk::dsl;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .select(Disk::as_select())
            .first_async::<Disk>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_volume_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Volume;
        use nexus_db_schema::schema::volume::dsl;

        dsl::volume
            .filter(dsl::time_deleted.is_null())
            .select(Volume::as_select())
            .first_async::<Volume>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_volume_resource_usage_records_exist(
        datastore: &DataStore,
    ) -> bool {
        use nexus_db_schema::schema::volume_resource_usage::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let rows = datastore
            .transaction_retry_wrapper("no_volume_resource_usage_records_exist")
            .transaction(&conn, |conn| async move {
                conn.batch_execute_async(
                    nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL,
                )
                .await
                .unwrap();

                Ok(dsl::volume_resource_usage
                    .count()
                    .get_result_async::<i64>(&conn)
                    .await
                    .unwrap())
            })
            .await
            .unwrap();

        rows == 0
    }

    async fn no_virtual_provisioning_resource_records_exist(
        datastore: &DataStore,
    ) -> bool {
        use nexus_db_queries::db::model::VirtualProvisioningResource;
        use nexus_db_schema::schema::virtual_provisioning_resource::dsl;

        dsl::virtual_provisioning_resource
            .select(VirtualProvisioningResource::as_select())
            .first_async::<VirtualProvisioningResource>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_virtual_provisioning_collection_records_using_storage(
        datastore: &DataStore,
    ) -> bool {
        use nexus_db_queries::db::model::VirtualProvisioningCollection;
        use nexus_db_schema::schema::virtual_provisioning_collection::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .transaction_retry_wrapper(
                "no_virtual_provisioning_collection_records_using_storage",
            )
            .transaction(&conn, |conn| async move {
                conn.batch_execute_async(
                    nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL,
                )
                .await
                .unwrap();
                Ok(dsl::virtual_provisioning_collection
                    .filter(dsl::virtual_disk_bytes_provisioned.ne(0))
                    .select(VirtualProvisioningCollection::as_select())
                    .get_results_async::<VirtualProvisioningCollection>(&conn)
                    .await
                    .unwrap()
                    .is_empty())
            })
            .await
            .unwrap()
    }

    async fn no_region_allocations_exist(
        datastore: &DataStore,
        test: &DiskTest<'_>,
    ) -> bool {
        for zpool in test.zpools() {
            let dataset = zpool.crucible_dataset();
            if datastore.regions_total_reserved_size(dataset.id).await.unwrap()
                != 0
            {
                return false;
            }
        }
        true
    }

    fn no_regions_ensured(sled_agent: &SledAgent, test: &DiskTest<'_>) -> bool {
        for zpool in test.zpools() {
            let dataset = zpool.crucible_dataset();
            let crucible_dataset =
                sled_agent.get_crucible_dataset(zpool.id, dataset.id);
            if !crucible_dataset.is_empty() {
                return false;
            }
        }
        true
    }

    pub(crate) async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        test: &DiskTest<'_>,
    ) {
        let sled_agent = cptestctx.first_sled_agent();
        let datastore = cptestctx.server.server_context().nexus.datastore();

        crate::app::sagas::test_helpers::assert_no_failed_undo_steps(
            &cptestctx.logctx.log,
            datastore,
        )
        .await;
        assert!(no_disk_records_exist(datastore).await);
        assert!(no_volume_records_exist(datastore).await);
        assert!(no_volume_resource_usage_records_exist(datastore).await);
        assert!(
            no_virtual_provisioning_resource_records_exist(datastore).await
        );
        assert!(
            no_virtual_provisioning_collection_records_using_storage(datastore)
                .await
        );
        assert!(no_region_allocations_exist(datastore, &test).await);
        assert!(no_regions_ensured(&sled_agent, &test));

        assert!(test.crucible_resources_deleted().await);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;
        let opctx = test_opctx(cptestctx);

        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaDiskCreate,
            _,
            _,
        >(
            nexus,
            || Box::pin(async { new_test_params(&opctx, project_id) }),
            || {
                Box::pin(async {
                    verify_clean_slate(&cptestctx, &test).await;
                })
            },
            log,
        )
        .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;
        let opctx = test_opctx(&cptestctx);

        crate::app::sagas::test_helpers::action_failure_can_unwind_idempotently::<
            SagaDiskCreate,
            _,
            _
        >(
            nexus,
            || Box::pin(async { new_test_params(&opctx, project_id) }),
            || Box::pin(async { verify_clean_slate(&cptestctx, &test).await; }),
            log
        ).await;
    }

    async fn destroy_disk(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(&cptestctx);
        let disk_selector = params::DiskSelector {
            project: Some(
                Name::try_from(PROJECT_NAME.to_string()).unwrap().into(),
            ),
            disk: Name::try_from(DISK_NAME.to_string()).unwrap().into(),
        };
        let disk_lookup = nexus.disk_lookup(&opctx, disk_selector).unwrap();

        nexus
            .project_delete_disk(&opctx, &disk_lookup)
            .await
            .expect("Failed to delete disk");
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        let params = new_test_params(&opctx, project_id);
        let dag = create_saga_dag::<SagaDiskCreate>(params).unwrap();
        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;

        destroy_disk(&cptestctx).await;
        verify_clean_slate(&cptestctx, &test).await;
    }
}
