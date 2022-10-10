// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    common_storage::ensure_all_datasets_and_regions, ActionRegistry,
    NexusActionContext, NexusSaga, SagaInitError, ACTION_GENERATE_ID,
};
use crate::app::sagas::NexusAction;
use crate::context::OpContext;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::{authn, authz, db};
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::{CrucibleOpts, VolumeConstructionRequest};
use std::convert::TryFrom;
use std::sync::Arc;
use steno::ActionError;
use steno::ActionFunc;
use steno::{new_action_noop_undo, Node};
use uuid::Uuid;

// disk create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub create_params: params::DiskCreate,
}

// disk create saga: actions

lazy_static! {
    static ref CREATE_DISK_RECORD: NexusAction = ActionFunc::new_action(
        "disk-create.create-disk-record",
        sdc_create_disk_record,
        sdc_create_disk_record_undo
    );
    static ref REGIONS_ALLOC: NexusAction = ActionFunc::new_action(
        "disk-create.allocate-regions",
        sdc_alloc_regions,
        sdc_alloc_regions_undo,
    );
    static ref SPACE_ACCOUNT: NexusAction = ActionFunc::new_action(
        "disk-create.account-space",
        sdc_account_space,
        sdc_account_space_undo,
    );
    static ref REGIONS_ENSURE: NexusAction =
        new_action_noop_undo("disk-create.regions-ensure", sdc_regions_ensure,);
    static ref CREATE_VOLUME_RECORD: NexusAction = ActionFunc::new_action(
        "disk-create.create-volume-record",
        sdc_create_volume_record,
        sdc_create_volume_record_undo,
    );
    static ref FINALIZE_DISK_RECORD: NexusAction = new_action_noop_undo(
        "disk-create.finalize-disk-record",
        sdc_finalize_disk_record
    );
}

// disk create saga: definition

#[derive(Debug)]
pub struct SagaDiskCreate;
impl NexusSaga for SagaDiskCreate {
    const NAME: &'static str = "disk-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        registry.register(Arc::clone(&*CREATE_DISK_RECORD));
        registry.register(Arc::clone(&*REGIONS_ALLOC));
        registry.register(Arc::clone(&*SPACE_ACCOUNT));
        registry.register(Arc::clone(&*REGIONS_ENSURE));
        registry.register(Arc::clone(&*CREATE_VOLUME_RECORD));
        registry.register(Arc::clone(&*FINALIZE_DISK_RECORD));
    }

    fn make_saga_dag(
        _params: &Self::Params,
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

        builder.append(Node::action(
            "created_disk",
            "CreateDiskRecord",
            CREATE_DISK_RECORD.as_ref(),
        ));

        builder.append(Node::action(
            "datasets_and_regions",
            "RegionsAlloc",
            REGIONS_ALLOC.as_ref(),
        ));

        builder.append(Node::action(
            "no-result",
            "SpaceAccount",
            SPACE_ACCOUNT.as_ref(),
        ));

        builder.append(Node::action(
            "regions_ensure",
            "RegionsEnsure",
            REGIONS_ENSURE.as_ref(),
        ));

        builder.append(Node::action(
            "created_volume",
            "CreateVolumeRecord",
            CREATE_VOLUME_RECORD.as_ref(),
        ));

        builder.append(Node::action(
            "disk_runtime",
            "FinalizeDiskRecord",
            FINALIZE_DISK_RECORD.as_ref(),
        ));

        Ok(builder.build()?)
    }
}

// disk create saga: action implementations

async fn sdc_create_disk_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Disk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    // We admittedly reference the volume before it has been allocated,
    // but this should be acceptable because the disk remains in a "Creating"
    // state until the saga has completed.
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let block_size: db::model::BlockSize = match &params
        .create_params
        .disk_source
    {
        params::DiskSource::Blank { block_size } => {
            db::model::BlockSize::try_from(*block_size).map_err(|e| {
                ActionError::action_failed(Error::internal_error(
                    &e.to_string(),
                ))
            })?
        }
        params::DiskSource::Snapshot { snapshot_id } => {
            let (.., db_snapshot) =
                LookupPath::new(&opctx, &osagactx.datastore())
                    .snapshot_id(*snapshot_id)
                    .fetch()
                    .await
                    .map_err(ActionError::action_failed)?;

            db_snapshot.block_size
        }
        params::DiskSource::Image { image_id: _ } => {
            // Until we implement project images, do not allow disks to be
            // created from a project image.
            return Err(ActionError::action_failed(Error::InvalidValue {
                label: String::from("image"),
                message: String::from("project image are not yet supported"),
            }));
        }
        params::DiskSource::GlobalImage { image_id } => {
            let (.., global_image) =
                LookupPath::new(&opctx, &osagactx.datastore())
                    .global_image_id(*image_id)
                    .fetch()
                    .await
                    .map_err(ActionError::action_failed)?;

            global_image.block_size
        }
    };

    let disk = db::model::Disk::new(
        disk_id,
        params.project_id,
        volume_id,
        params.create_params.clone(),
        block_size,
        db::model::DiskRuntimeState::new(),
    )
    .map_err(|e| {
        ActionError::action_failed(Error::invalid_request(&e.to_string()))
    })?;

    let disk_created = osagactx
        .datastore()
        .project_create_disk(disk)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(disk_created)
}

async fn sdc_create_disk_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    osagactx.datastore().project_delete_disk_no_auth(&disk_id).await?;
    Ok(())
}

async fn sdc_alloc_regions(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::Dataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;

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
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let datasets_and_regions = osagactx
        .datastore()
        .region_allocate(
            &opctx,
            volume_id,
            &params.create_params.disk_source,
            params.create_params.size,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(datasets_and_regions)
}

async fn sdc_alloc_regions_undo(
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

// TODO: Not yet idempotent
async fn sdc_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk_created = sagactx.lookup::<db::model::Disk>("created_disk")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .virtual_resource_provisioning_update_disk(
            &opctx,
            params.project_id,
            i64::try_from(disk_created.size.to_bytes())
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "updating resource usage: {e}"
                    ))
                })
                .map_err(ActionError::action_failed)?,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

// TODO: Not yet idempotent
async fn sdc_account_space_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk_created = sagactx.lookup::<db::model::Disk>("created_disk")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .virtual_resource_provisioning_update_disk(
            &opctx,
            params.project_id,
            -i64::try_from(disk_created.size.to_bytes()).map_err(|e| {
                Error::internal_error(&format!("updating resource usage: {e}"))
            })?,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

/// Call out to Crucible agent and perform region creation.
async fn sdc_regions_ensure(
    sagactx: NexusActionContext,
) -> Result<String, ActionError> {
    let log = sagactx.user_data().log();
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    let datasets_and_regions = ensure_all_datasets_and_regions(
        &log,
        sagactx.lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?,
    )
    .await?;

    let block_size = datasets_and_regions[0].1.block_size;

    // If a disk source was requested, set the read-only parent of this disk.
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let log = osagactx.log();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let mut read_only_parent: Option<Box<VolumeConstructionRequest>> =
        match &params.create_params.disk_source {
            params::DiskSource::Blank { block_size: _ } => None,
            params::DiskSource::Snapshot { snapshot_id } => {
                debug!(log, "grabbing snapshot {}", snapshot_id);

                let (.., db_snapshot) =
                    LookupPath::new(&opctx, &osagactx.datastore())
                        .snapshot_id(*snapshot_id)
                        .fetch()
                        .await
                        .map_err(ActionError::action_failed)?;

                debug!(
                    log,
                    "grabbing snapshot {} volume {}",
                    db_snapshot.id(),
                    db_snapshot.volume_id,
                );

                let volume = osagactx
                    .datastore()
                    .volume_get(db_snapshot.volume_id)
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
            params::DiskSource::Image { image_id: _ } => {
                // Until we implement project images, do not allow disks to be
                // created from a project image.
                return Err(ActionError::action_failed(Error::InvalidValue {
                    label: String::from("image"),
                    message: String::from(
                        "project image are not yet supported",
                    ),
                }));
            }
            params::DiskSource::GlobalImage { image_id } => {
                debug!(log, "grabbing image {}", image_id);

                let (.., global_image) =
                    LookupPath::new(&opctx, &osagactx.datastore())
                        .global_image_id(*image_id)
                        .fetch()
                        .await
                        .map_err(ActionError::action_failed)?;

                debug!(log, "retrieved global image {}", global_image.id());

                debug!(
                    log,
                    "grabbing global image {} volume {}",
                    global_image.id(),
                    global_image.volume_id
                );

                let volume = osagactx
                    .datastore()
                    .volume_get(global_image.volume_id)
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
    let mut rng = StdRng::from_entropy();
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: disk_id,
        block_size,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size,
            // gen of 0 is here, these regions were just allocated.
            gen: 0,
            opts: CrucibleOpts {
                id: disk_id,
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
                key: Some(base64::encode({
                    // TODO the current encryption key
                    // requirement is 32 bytes, what if that
                    // changes?
                    let mut random_bytes: [u8; 32] = [0; 32];
                    rng.fill_bytes(&mut random_bytes);
                    random_bytes
                })),

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

    let volume_data = serde_json::to_string(&volume_construction_request)
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    Ok(volume_data)
}

async fn sdc_create_volume_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Volume, ActionError> {
    let osagactx = sagactx.user_data();

    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    let volume_data = sagactx.lookup::<String>("regions_ensure")?;

    let volume = db::model::Volume::new(volume_id, volume_data);

    let volume_created = osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(volume_created)
}

async fn sdc_create_volume_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx.nexus().volume_delete(&opctx, volume_id).await?;
    Ok(())
}

async fn sdc_finalize_disk_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    let disk_created = sagactx.lookup::<db::model::Disk>("created_disk")?;
    let (.., authz_disk) = LookupPath::new(&opctx, &datastore)
        .disk_id(disk_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    // TODO-security Review whether this can ever fail an authz check.  We don't
    // want this to ever fail the authz check here -- if it did, we would have
    // wanted to catch that a lot sooner.  It wouldn't make sense for it to fail
    // anyway because we're modifying something that *we* just created.  Right
    // now, it's very unlikely that it would ever fail because we checked
    // Action::CreateChild on the Project before we created this saga.  The only
    // role that gets that permission is "project collaborator", which also gets
    // Action::Modify on Disks within the Project.  So this shouldn't break in
    // practice.  However, that's brittle.  It would be better if this were
    // better guaranteed.
    datastore
        .disk_update_runtime(
            &opctx,
            &authz_disk,
            &disk_created.runtime().detach(),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

// helper functions

/// Generate new IDs for each layer
fn randomize_volume_construction_request_ids(
    input: &VolumeConstructionRequest,
) -> anyhow::Result<VolumeConstructionRequest> {
    match input {
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
                    randomize_volume_construction_request_ids(&subvol)
                })
                .collect::<anyhow::Result<Vec<VolumeConstructionRequest>>>()?,
            read_only_parent: if let Some(read_only_parent) = read_only_parent {
                Some(Box::new(randomize_volume_construction_request_ids(
                    read_only_parent,
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

        VolumeConstructionRequest::Region { block_size, opts, gen } => {
            let mut opts = opts.clone();
            opts.id = Uuid::new_v4();

            Ok(VolumeConstructionRequest::Region {
                block_size: *block_size,
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
