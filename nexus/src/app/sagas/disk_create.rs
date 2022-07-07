// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::saga_generate_uuid;
use crate::context::OpContext;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::saga_interface::SagaContext;
use crate::{authn, authz, db};
use anyhow::anyhow;
use crucible_agent_client::{
    types::{CreateRegion, RegionId, State as RegionState},
    Client as CrucibleAgentClient,
};
use futures::StreamExt;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::backoff::{self, BackoffError};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use slog::warn;
use slog::Logger;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

pub const SAGA_NAME: &'static str = "disk-create";

lazy_static! {
    pub static ref SAGA_TEMPLATE: Arc<SagaTemplate<SagaDiskCreate>> =
        Arc::new(saga_disk_create());
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub create_params: params::DiskCreate,
}

#[derive(Debug)]
pub struct SagaDiskCreate;
impl SagaType for SagaDiskCreate {
    type SagaParamsType = Arc<Params>;
    type ExecContextType = Arc<SagaContext>;
}

fn saga_disk_create() -> SagaTemplate<SagaDiskCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "disk_id",
        "GenerateDiskId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "volume_id",
        "GenerateVolumeId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "created_disk",
        "CreateDiskRecord",
        ActionFunc::new_action(
            sdc_create_disk_record,
            sdc_create_disk_record_undo,
        ),
    );

    template_builder.append(
        "datasets_and_regions",
        "AllocRegions",
        ActionFunc::new_action(sdc_alloc_regions, sdc_alloc_regions_undo),
    );

    template_builder.append(
        "regions_ensure",
        "RegionsEnsure",
        ActionFunc::new_action(sdc_regions_ensure, sdc_regions_ensure_undo),
    );

    template_builder.append(
        "created_volume",
        "CreateVolumeRecord",
        ActionFunc::new_action(
            sdc_create_volume_record,
            sdc_create_volume_record_undo,
        ),
    );

    template_builder.append(
        "disk_runtime",
        "FinalizeDiskRecord",
        new_action_noop_undo(sdc_finalize_disk_record),
    );

    template_builder.build()
}

async fn sdc_create_disk_record(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<db::model::Disk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

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
        params::DiskSource::Snapshot { snapshot_id: _ } => {
            // Until we implement snapshots, do not allow disks to be
            // created from a snapshot.
            return Err(ActionError::action_failed(Error::InvalidValue {
                label: String::from("snapshot"),
                message: String::from("snapshots are not yet supported"),
            }));
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
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    osagactx.datastore().project_delete_disk_no_auth(&disk_id).await?;
    Ok(())
}

async fn sdc_alloc_regions(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<Vec<(db::model::Dataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
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
        .region_allocate(&opctx, volume_id, &params.create_params)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(datasets_and_regions)
}

async fn sdc_alloc_regions_undo(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx.datastore().regions_hard_delete(volume_id).await?;
    Ok(())
}

async fn ensure_region_in_dataset(
    log: &Logger,
    dataset: &db::model::Dataset,
    region: &db::model::Region,
) -> Result<crucible_agent_client::types::Region, Error> {
    let url = format!("http://{}", dataset.address());
    let client = CrucibleAgentClient::new(&url);

    let region_request = CreateRegion {
        block_size: region.block_size().to_bytes(),
        extent_count: region.extent_count().try_into().unwrap(),
        extent_size: region.blocks_per_extent().try_into().unwrap(),
        // TODO: Can we avoid casting from UUID to string?
        // NOTE: This'll require updating the crucible agent client.
        id: RegionId(region.id().to_string()),
        encrypted: region.encrypted(),
        cert_pem: None,
        key_pem: None,
        root_pem: None,
    };

    let create_region = || async {
        let region = client
            .region_create(&region_request)
            .await
            .map_err(|e| BackoffError::Permanent(e.into()))?;
        match region.state {
            RegionState::Requested => Err(BackoffError::transient(anyhow!(
                "Region creation in progress"
            ))),
            RegionState::Created => Ok(region),
            _ => Err(BackoffError::Permanent(anyhow!(
                "Failed to create region, unexpected state: {:?}",
                region.state
            ))),
        }
    };

    let log_create_failure = |_, delay| {
        warn!(
            log,
            "Region requested, not yet created. Retrying in {:?}", delay
        );
    };

    let region = backoff::retry_notify(
        backoff::internal_service_policy(),
        create_region,
        log_create_failure,
    )
    .await
    .map_err(|e| Error::internal_error(&e.to_string()))?;

    Ok(region.into_inner())
}

// Arbitrary limit on concurrency, for operations issued
// on multiple regions within a disk at the same time.
const MAX_CONCURRENT_REGION_REQUESTS: usize = 3;

async fn sdc_regions_ensure(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<String, ActionError> {
    let log = sagactx.user_data().log();
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    let datasets_and_regions = sagactx
        .lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?;

    let request_count = datasets_and_regions.len();

    // Allocate regions, and additionally return the dataset that the region was
    // allocated in.
    let datasets_and_regions: Vec<(
        db::model::Dataset,
        crucible_agent_client::types::Region,
    )> = futures::stream::iter(datasets_and_regions)
        .map(|(dataset, region)| async move {
            match ensure_region_in_dataset(log, &dataset, &region).await {
                Ok(result) => Ok((dataset, result)),
                Err(e) => Err(e),
            }
        })
        // Execute the allocation requests concurrently.
        .buffer_unordered(std::cmp::min(
            request_count,
            MAX_CONCURRENT_REGION_REQUESTS,
        ))
        .collect::<Vec<
            Result<
                (db::model::Dataset, crucible_agent_client::types::Region),
                Error,
            >,
        >>()
        .await
        .into_iter()
        .collect::<Result<
            Vec<(db::model::Dataset, crucible_agent_client::types::Region)>,
            Error,
        >>()
        .map_err(ActionError::action_failed)?;

    // Assert each region has the same block size, otherwise Volume creation
    // will fail.
    let all_region_have_same_block_size = datasets_and_regions
        .windows(2)
        .all(|w| w[0].1.block_size == w[1].1.block_size);

    if !all_region_have_same_block_size {
        return Err(ActionError::action_failed(Error::internal_error(
            "volume creation will fail due to block size mismatch",
        )));
    }

    let block_size = datasets_and_regions[0].1.block_size;

    // If requested, back disk by image
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let log = osagactx.log();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let read_only_parent: Option<
        Box<sled_agent_client::types::VolumeConstructionRequest>,
    > = match &params.create_params.disk_source {
        params::DiskSource::Blank { block_size: _ } => None,
        params::DiskSource::Snapshot { snapshot_id: _ } => {
            // Until we implement snapshots, do not allow disks to be
            // created from a snapshot.
            return Err(ActionError::action_failed(Error::InvalidValue {
                label: String::from("snapshot"),
                message: String::from("snapshots are not yet supported"),
            }));
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
            warn!(log, "grabbing image {}", image_id);

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
                    ActionError::action_failed(Error::internal_error(&format!(
                        "failed to deserialize volume data: {}",
                        e,
                    )))
                },
            )?))
        }
    };

    // Store volume details in db
    let mut rng = StdRng::from_entropy();
    let volume_construction_request =
        sled_agent_client::types::VolumeConstructionRequest::Volume {
            id: disk_id,
            block_size,
            sub_volumes: vec![
                sled_agent_client::types::VolumeConstructionRequest::Region {
                    block_size,
                    // gen of 0 is here, these regions were just allocated.
                    gen: 0,
                    opts: sled_agent_client::types::CrucibleOpts {
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

                        // TODO open a control socket for the whole volume, not
                        // in the sub volumes
                        control: None,
                        metric_register: None,
                        metric_collect: None,
                    },
                },
            ],
            read_only_parent,
        };

    let volume_data = serde_json::to_string(&volume_construction_request)
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    Ok(volume_data)
}

pub(super) async fn delete_regions(
    datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
) -> Result<(), Error> {
    let request_count = datasets_and_regions.len();
    futures::stream::iter(datasets_and_regions)
        .map(|(dataset, region)| async move {
            let url = format!("http://{}", dataset.address());
            let client = CrucibleAgentClient::new(&url);
            let id = RegionId(region.id().to_string());
            client.region_delete(&id).await.map_err(|e| match e {
                crucible_agent_client::Error::ErrorResponse(rv) => {
                    match rv.status() {
                        http::StatusCode::SERVICE_UNAVAILABLE => {
                            Error::unavail(&rv.message)
                        }
                        status if status.is_client_error() => {
                            Error::invalid_request(&rv.message)
                        }
                        _ => Error::internal_error(&rv.message),
                    }
                }
                _ => Error::internal_error(
                    "unexpected failure during `region_delete`",
                ),
            })
        })
        // Execute the allocation requests concurrently.
        .buffer_unordered(std::cmp::min(
            request_count,
            MAX_CONCURRENT_REGION_REQUESTS,
        ))
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(())
}

async fn sdc_regions_ensure_undo(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<(), anyhow::Error> {
    let datasets_and_regions = sagactx
        .lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?;
    delete_regions(datasets_and_regions).await?;
    Ok(())
}

async fn sdc_create_volume_record(
    sagactx: ActionContext<SagaDiskCreate>,
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
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx.datastore().volume_delete(volume_id).await?;
    Ok(())
}

async fn sdc_finalize_disk_record(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
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
