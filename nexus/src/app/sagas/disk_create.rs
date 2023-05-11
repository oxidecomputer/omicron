// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    common_storage::{
        call_pantry_attach_for_disk, call_pantry_detach_for_disk,
        delete_crucible_regions, ensure_all_datasets_and_regions,
        get_pantry_address,
    },
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::declare_saga_actions;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::{authn, authz, db};
use omicron_common::api::external::Error;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::{CrucibleOpts, VolumeConstructionRequest};
use std::convert::TryFrom;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// disk create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub create_params: params::DiskCreate,
}

// disk create saga: actions

declare_saga_actions! {
    disk_create;
    CREATE_DISK_RECORD -> "created_disk" {
        + sdc_create_disk_record
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
    REGIONS_ENSURE -> "regions_ensure" {
        + sdc_regions_ensure
        - sdc_regions_ensure_undo
    }
    CREATE_VOLUME_RECORD -> "created_volume" {
        + sdc_create_volume_record
        - sdc_create_volume_record_undo
    }
    FINALIZE_DISK_RECORD -> "disk_runtime" {
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
pub struct SagaDiskCreate;
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

        builder.append(create_disk_record_action());
        builder.append(regions_alloc_action());
        builder.append(space_account_action());
        builder.append(regions_ensure_action());
        builder.append(create_volume_record_action());
        builder.append(finalize_disk_record_action());

        match &params.create_params.disk_source {
            params::DiskSource::ImportingBlocks { .. } => {
                builder.append(get_pantry_address_action());
                builder.append(call_pantry_attach_for_disk_action());
            }

            _ => {}
        }

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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let block_size: db::model::BlockSize =
        match &params.create_params.disk_source {
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
                        .map_err(|e| {
                            ActionError::action_failed(Error::internal_error(
                                &e.to_string(),
                            ))
                        })?;

                db_snapshot.block_size
            }
            params::DiskSource::Image { image_id } => {
                let (.., image) =
                    LookupPath::new(&opctx, &osagactx.datastore())
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
        volume_id,
        params.create_params.clone(),
        block_size,
        db::model::DiskRuntimeState::new(),
    )
    .map_err(|e| {
        ActionError::action_failed(Error::invalid_request(&e.to_string()))
    })?;

    let (.., authz_project) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;

    let disk_created = osagactx
        .datastore()
        .project_create_disk(&opctx, &authz_project, disk)
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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
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

async fn sdc_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk_created = sagactx.lookup::<db::model::Disk>("created_disk")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_disk(
            &opctx,
            disk_created.id(),
            params.project_id,
            disk_created.size,
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

    let disk_created = sagactx.lookup::<db::model::Disk>("created_disk")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_disk(
            &opctx,
            disk_created.id(),
            params.project_id,
            disk_created.size,
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
    let blocks_per_extent = datasets_and_regions[0].1.extent_size;
    let extent_count = datasets_and_regions[0].1.extent_count;

    // If a disk source was requested, set the read-only parent of this disk.
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
                    .volume_checkout(db_snapshot.volume_id)
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

                let (.., image) =
                    LookupPath::new(&opctx, &osagactx.datastore())
                        .image_id(*image_id)
                        .fetch()
                        .await
                        .map_err(ActionError::action_failed)?;

                debug!(log, "retrieved project image {}", image.id());

                debug!(
                    log,
                    "grabbing image {} volume {}",
                    image.id(),
                    image.volume_id
                );

                let volume = osagactx
                    .datastore()
                    .volume_checkout(image.volume_id)
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
    let mut rng = StdRng::from_entropy();
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: disk_id,
        block_size,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count: extent_count.try_into().unwrap(),
            gen: 1,
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

    let volume_data = serde_json::to_string(&volume_construction_request)
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    Ok(volume_data)
}

async fn sdc_regions_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    warn!(log, "sdc_regions_ensure_undo: Deleting crucible regions");
    delete_crucible_regions(
        sagactx.lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?,
    )
    .await?;
    info!(log, "sdc_regions_ensure_undo: Deleted crucible regions");
    Ok(())
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

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
    match &params.create_params.disk_source {
        params::DiskSource::ImportingBlocks { block_size: _ } => {
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

    Ok(())
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

    let (.., authz_disk) = LookupPath::new(&opctx, &datastore)
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
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;

    call_pantry_attach_for_disk(
        &log,
        &opctx,
        &osagactx.nexus(),
        disk_id,
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

    call_pantry_detach_for_disk(&log, disk_id, pantry_address).await?;

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

        VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            opts,
            gen,
        } => {
            let mut opts = opts.clone();
            opts.id = Uuid::new_v4();

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
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_create::Params,
        app::sagas::disk_create::SagaDiskCreate, authn::saga::Serialized,
        db::datastore::DataStore, external_api::params,
    };
    use async_bb8_diesel::{
        AsyncConnection, AsyncRunQueryDsl, AsyncSimpleConnection,
        OptionalExtension,
    };
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_ip_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_sled_agent::sim::SledAgent;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const DISK_NAME: &str = "my-disk";
    const PROJECT_NAME: &str = "springfield-squidport";

    async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
        create_ip_pool(&client, "p0", None).await;
        let project = create_project(client, PROJECT_NAME).await;
        project.identity.id
    }

    pub fn new_disk_create_params() -> params::DiskCreate {
        params::DiskCreate {
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
            cptestctx.server.apictx().nexus.datastore().clone(),
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_and_project(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);
        let params = new_test_params(&opctx, project_id);
        let dag = create_saga_dag::<SagaDiskCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        let output = nexus.run_saga(runnable_saga).await.unwrap();

        let disk = output
            .lookup_node_output::<crate::db::model::Disk>("created_disk")
            .unwrap();
        assert_eq!(disk.project_id, project_id);
    }

    async fn no_disk_records_exist(datastore: &DataStore) -> bool {
        use crate::db::model::Disk;
        use crate::db::schema::disk::dsl;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .select(Disk::as_select())
            .first_async::<Disk>(datastore.pool_for_tests().await.unwrap())
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_volume_records_exist(datastore: &DataStore) -> bool {
        use crate::db::model::Volume;
        use crate::db::schema::volume::dsl;

        dsl::volume
            .filter(dsl::time_deleted.is_null())
            .select(Volume::as_select())
            .first_async::<Volume>(datastore.pool_for_tests().await.unwrap())
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_virtual_provisioning_resource_records_exist(
        datastore: &DataStore,
    ) -> bool {
        use crate::db::model::VirtualProvisioningResource;
        use crate::db::schema::virtual_provisioning_resource::dsl;

        dsl::virtual_provisioning_resource
            .select(VirtualProvisioningResource::as_select())
            .first_async::<VirtualProvisioningResource>(
                datastore.pool_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_virtual_provisioning_collection_records_using_storage(
        datastore: &DataStore,
    ) -> bool {
        use crate::db::model::VirtualProvisioningCollection;
        use crate::db::schema::virtual_provisioning_collection::dsl;

        datastore
            .pool_for_tests()
            .await
            .unwrap()
            .transaction_async(|conn| async move {
                conn.batch_execute_async(
                    nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL,
                )
                .await
                .unwrap();
                Ok::<_, crate::db::TransactionError<()>>(
                    dsl::virtual_provisioning_collection
                        .filter(dsl::virtual_disk_bytes_provisioned.ne(0))
                        .select(VirtualProvisioningCollection::as_select())
                        .get_results_async::<VirtualProvisioningCollection>(
                            &conn,
                        )
                        .await
                        .unwrap()
                        .is_empty(),
                )
            })
            .await
            .unwrap()
    }

    async fn no_region_allocations_exist(
        datastore: &DataStore,
        test: &DiskTest,
    ) -> bool {
        for zpool in &test.zpools {
            for dataset in &zpool.datasets {
                if datastore
                    .regions_total_occupied_size(dataset.id)
                    .await
                    .unwrap()
                    != 0
                {
                    return false;
                }
            }
        }
        true
    }

    async fn no_regions_ensured(
        sled_agent: &SledAgent,
        test: &DiskTest,
    ) -> bool {
        for zpool in &test.zpools {
            for dataset in &zpool.datasets {
                let crucible_dataset =
                    sled_agent.get_crucible_dataset(zpool.id, dataset.id).await;
                if !crucible_dataset.is_empty().await {
                    return false;
                }
            }
        }
        true
    }

    pub(crate) async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        test: &DiskTest,
    ) {
        let sled_agent = &cptestctx.sled_agent.sled_agent;
        let datastore = cptestctx.server.apictx().nexus.datastore();

        assert!(no_disk_records_exist(datastore).await);
        assert!(no_volume_records_exist(datastore).await);
        assert!(
            no_virtual_provisioning_resource_records_exist(datastore).await
        );
        assert!(
            no_virtual_provisioning_collection_records_using_storage(datastore)
                .await
        );
        assert!(no_region_allocations_exist(datastore, &test).await);
        assert!(no_regions_ensured(&sled_agent, &test).await);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_and_project(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let params = new_test_params(&opctx, project_id);
        let dag = create_saga_dag::<SagaDiskCreate>(params).unwrap();

        for node in dag.get_nodes() {
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

            // Check that no partial artifacts of disk creation exist:
            verify_clean_slate(&cptestctx, &test).await;
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx.nexus;
        let project_id = create_org_and_project(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        let params = new_test_params(&opctx, project_id);
        let dag = create_saga_dag::<SagaDiskCreate>(params).unwrap();

        // The "undo_node" should always be immediately preceding the
        // "error_node".
        for (undo_node, error_node) in
            dag.get_nodes().zip(dag.get_nodes().skip(1))
        {
            // Create a new saga for this node.
            info!(
                log,
                "Creating new saga which will fail at index {:?}", error_node.index();
                "node_name" => error_node.name().as_ref(),
                "label" => error_node.label(),
            );

            let runnable_saga =
                nexus.create_runnable_saga(dag.clone()).await.unwrap();

            // Inject an error instead of running the node.
            //
            // This should cause the saga to unwind.
            nexus
                .sec()
                .saga_inject_error(runnable_saga.id(), error_node.index())
                .await
                .unwrap();

            // Inject a repetition for the node being undone.
            //
            // This means it is executing twice while unwinding.
            nexus
                .sec()
                .saga_inject_repeat(
                    runnable_saga.id(),
                    undo_node.index(),
                    steno::RepeatInjected {
                        action: NonZeroU32::new(1).unwrap(),
                        undo: NonZeroU32::new(2).unwrap(),
                    },
                )
                .await
                .unwrap();

            nexus
                .run_saga(runnable_saga)
                .await
                .expect_err("Saga should have failed");

            verify_clean_slate(&cptestctx, &test).await;
        }
    }

    async fn destroy_disk(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx.nexus;
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
        let nexus = &cptestctx.server.apictx.nexus;
        let project_id = create_org_and_project(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        let params = new_test_params(&opctx, project_id);
        let dag = create_saga_dag::<SagaDiskCreate>(params).unwrap();

        let runnable_saga =
            nexus.create_runnable_saga(dag.clone()).await.unwrap();

        // Cause all actions to run twice. The saga should succeed regardless!
        for node in dag.get_nodes() {
            nexus
                .sec()
                .saga_inject_repeat(
                    runnable_saga.id(),
                    node.index(),
                    steno::RepeatInjected {
                        action: NonZeroU32::new(2).unwrap(),
                        undo: NonZeroU32::new(1).unwrap(),
                    },
                )
                .await
                .unwrap();
        }

        // Verify that the saga's execution succeeded.
        nexus
            .run_saga(runnable_saga)
            .await
            .expect("Saga should have succeeded");

        destroy_disk(&cptestctx).await;
        verify_clean_slate(&cptestctx, &test).await;
    }
}
