// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Saga actions, undo actions, and saga constructors used in Nexus.
 */

/*
 * NOTE: We want to be careful about what interfaces we expose to saga actions.
 * In the future, we expect to mock these out for comprehensive testing of
 * correctness, idempotence, etc.  The more constrained this interface is, the
 * easier it will be to test, version, and update in deployed systems.
 */

use crate::db;
use crate::db::identity::{Asset, Resource};
use crate::external_api::params;
use crate::saga_interface::SagaContext;
use anyhow::anyhow;
use chrono::Utc;
use crucible_agent_client::{
    types::{CreateRegion, RegionId, State as RegionState},
    Client as CrucibleAgentClient,
};
use futures::StreamExt;
use lazy_static::lazy_static;
use omicron_common::api::external::Generation;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::external::NetworkInterface;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::InstanceHardware;
use omicron_common::backoff::{self, BackoffError};
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaTemplateGeneric;
use steno::SagaType;
use slog::Logger;
use uuid::Uuid;

/*
 * We'll need a richer mechanism for registering sagas, but this works for now.
 */
pub const SAGA_INSTANCE_CREATE_NAME: &'static str = "instance-create";
pub const SAGA_DISK_CREATE_NAME: &'static str = "disk-create";
lazy_static! {
    pub static ref SAGA_INSTANCE_CREATE_TEMPLATE: Arc<SagaTemplate<SagaInstanceCreate>> =
        Arc::new(saga_instance_create());
    pub static ref SAGA_DISK_CREATE_TEMPLATE: Arc<SagaTemplate<SagaDiskCreate>> =
        Arc::new(saga_disk_create());
}

lazy_static! {
    pub static ref ALL_TEMPLATES: BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>> =
        all_templates();
}

fn all_templates(
) -> BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>> {
    vec![
        (
            SAGA_INSTANCE_CREATE_NAME,
            Arc::clone(&SAGA_INSTANCE_CREATE_TEMPLATE)
                as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
        ),
        (
            SAGA_DISK_CREATE_NAME,
            Arc::clone(&SAGA_DISK_CREATE_TEMPLATE)
                as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
        ),
    ]
    .into_iter()
    .collect()
}

/*
 * "Create Instance" saga template
 */

#[derive(Debug, Deserialize, Serialize)]
pub struct ParamsInstanceCreate {
    pub project_id: Uuid,
    pub create_params: params::InstanceCreate,
}

#[derive(Debug)]
pub struct SagaInstanceCreate;
impl SagaType for SagaInstanceCreate {
    type SagaParamsType = Arc<ParamsInstanceCreate>;
    type ExecContextType = Arc<SagaContext>;
}

pub fn saga_instance_create() -> SagaTemplate<SagaInstanceCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "instance_id",
        "GenerateInstanceId",
        new_action_noop_undo(sic_generate_uuid),
    );

    template_builder.append(
        "propolis_id",
        "GeneratePropolisId",
        new_action_noop_undo(sic_generate_uuid),
    );

    template_builder.append(
        "server_id",
        "AllocServer",
        // TODO-robustness This still needs an undo action, and we should really
        // keep track of resources and reservations, etc.  See the comment on
        // SagaContext::alloc_server()
        new_action_noop_undo(sic_alloc_server),
    );

    template_builder.append(
        "network_interface",
        "CreateNetworkInterface",
        ActionFunc::new_action(
            sic_create_network_interface,
            sic_create_network_interface_undo,
        ),
    );

    template_builder.append(
        "initial_runtime",
        "CreateInstanceRecord",
        new_action_noop_undo(sic_create_instance_record),
    );

    template_builder.append(
        "instance_ensure",
        "InstanceEnsure",
        new_action_noop_undo(sic_instance_ensure),
    );

    template_builder.build()
}

async fn sic_generate_uuid(
    _: ActionContext<SagaInstanceCreate>,
) -> Result<Uuid, ActionError> {
    Ok(Uuid::new_v4())
}

async fn sic_alloc_server(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    osagactx
        .alloc_server(&params.create_params)
        .await
        .map_err(ActionError::action_failed)
}

async fn sic_create_network_interface(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<NetworkInterface, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;

    let default_name =
        db::model::Name(Name::try_from("default".to_string()).unwrap());
    let vpc = osagactx
        .datastore()
        .vpc_fetch_by_name(&params.project_id, &default_name)
        .await
        .map_err(ActionError::action_failed)?;
    let subnet = osagactx
        .datastore()
        .vpc_subnet_fetch_by_name(&vpc.id(), &default_name)
        .await
        .map_err(ActionError::action_failed)?;

    let mac = db::model::MacAddr::new().map_err(ActionError::action_failed)?;
    let interface_id = Uuid::new_v4();
    // Request an allocation
    let ip = None;
    let interface = db::model::IncompleteNetworkInterface::new(
        interface_id,
        instance_id,
        // TODO-correctness: vpc_id here is used for name uniqueness. Should
        // interface names be unique to the subnet's VPC or to the
        // VPC associated with the instance's default interface?
        vpc.id(),
        subnet,
        mac,
        ip,
        params::NetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                // By naming the interface after the instance id, we should
                // avoid name conflicts on creation.
                name: format!("default-{}", instance_id).parse().unwrap(),
                description: format!(
                    "default interface for {}",
                    params.create_params.identity.name
                ),
            },
        },
    );

    let interface = osagactx
        .datastore()
        .instance_create_network_interface(interface)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(interface.into())
}

async fn sic_create_network_interface_undo(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let network_interface =
        sagactx.lookup::<NetworkInterface>("network_interface")?;

    osagactx
        .datastore()
        .instance_delete_network_interface(&network_interface.identity.id)
        .await?;
    Ok(())
}

async fn sic_create_instance_record(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<InstanceHardware, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let sled_uuid = sagactx.lookup::<Uuid>("server_id");
    let instance_id = sagactx.lookup::<Uuid>("instance_id");
    let propolis_uuid = sagactx.lookup::<Uuid>("propolis_id");
    let network_interface =
        sagactx.lookup::<NetworkInterface>("network_interface")?;

    let runtime = InstanceRuntimeState {
        run_state: InstanceState::Creating,
        sled_uuid: sled_uuid?,
        propolis_uuid: propolis_uuid?,
        hostname: params.create_params.hostname.clone(),
        memory: params.create_params.memory,
        ncpus: params.create_params.ncpus,
        gen: Generation::new(),
        time_updated: Utc::now(),
    };

    let new_instance = db::model::Instance::new(
        instance_id?,
        params.project_id,
        &params.create_params,
        runtime.into(),
    );

    let instance = osagactx
        .datastore()
        .project_create_instance(new_instance)
        .await
        .map_err(ActionError::action_failed)?;

    // See also: instance_set_runtime in nexus.rs for a similar construction.
    Ok(InstanceHardware {
        runtime: instance.runtime().clone().into(),
        nics: vec![network_interface],
    })
}

async fn sic_instance_ensure(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), ActionError> {
    /*
     * TODO-correctness is this idempotent?
     */
    let osagactx = sagactx.user_data();
    let runtime_params =
        sled_agent_client::types::InstanceRuntimeStateRequested {
            run_state:
                sled_agent_client::types::InstanceStateRequested::Running,
        };
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let sled_uuid = sagactx.lookup::<Uuid>("server_id")?;
    let initial_runtime =
        sagactx.lookup::<InstanceHardware>("initial_runtime")?;
    let sa = osagactx
        .sled_client(&sled_uuid)
        .await
        .map_err(ActionError::action_failed)?;

    /*
     * Ask the sled agent to begin the state change.  Then update the database
     * to reflect the new intermediate state.  If this update is not the newest
     * one, that's fine.  That might just mean the sled agent beat us to it.
     */
    let new_runtime_state = sa
        .instance_put(
            &instance_id,
            &sled_agent_client::types::InstanceEnsureBody {
                initial: sled_agent_client::types::InstanceHardware::from(
                    initial_runtime,
                ),
                target: runtime_params,
            },
        )
        .await
        .map_err(omicron_common::api::external::Error::from)
        .map_err(ActionError::action_failed)?;

    let new_runtime_state: InstanceRuntimeState = new_runtime_state.into();

    osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime_state.into())
        .await
        .map(|_| ())
        .map_err(ActionError::action_failed)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ParamsDiskCreate {
    pub project_id: Uuid,
    pub create_params: params::DiskCreate,
}

#[derive(Debug)]
pub struct SagaDiskCreate;
impl SagaType for SagaDiskCreate {
    type SagaParamsType = Arc<ParamsDiskCreate>;
    type ExecContextType = Arc<SagaContext>;
}

pub fn saga_disk_create() -> SagaTemplate<SagaDiskCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "disk_id",
        "GenerateDiskId",
        new_action_noop_undo(sdc_generate_uuid),
    );

    template_builder.append(
        "created_disk",
        "CreateDiskRecord",
        // TODO: Needs undo action.
        new_action_noop_undo(sdc_create_disk_record),
    );

    template_builder.append(
        "datasets_and_regions",
        "AllocRegions",
        // TODO: Needs undo action.
        new_action_noop_undo(sdc_alloc_regions),
    );

    template_builder.append(
        "regions_ensure",
        "RegionsEnsure",
        // TODO: Needs undo action.
        new_action_noop_undo(sdc_regions_ensure),
    );

    template_builder.append(
        "disk_runtime",
        "FinalizeDiskRecord",
        // TODO: Needs undo action.
        new_action_noop_undo(sdc_finalize_disk_record),
    );

    template_builder.build()
}

async fn sdc_generate_uuid(
    _: ActionContext<SagaDiskCreate>,
) -> Result<Uuid, ActionError> {
    Ok(Uuid::new_v4())
}

async fn sdc_create_disk_record(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<db::model::Disk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;

    // NOTE: This could be done in a txn with region allocation?
    //
    // Unclear if it's a problem to let this disk exist without any backing
    // regions for a brief period of time.
    let disk = db::model::Disk::new(
        disk_id,
        params.project_id,
        params.create_params.clone(),
        db::model::DiskRuntimeState::new(),
    );
    let disk_created = osagactx
        .datastore()
        .project_create_disk(disk)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(disk_created)
}

async fn sdc_alloc_regions(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<Vec<(db::model::Dataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    // Ensure the disk is backed by appropriate regions.
    //
    // This allocates regions in the database, but the disk state is still
    // "creating" - the respective Crucible Agents must be instructed to
    // allocate the necessary regions before we can mark the disk as "ready to
    // be used".
    let datasets_and_regions = osagactx
        .datastore()
        .region_allocate(disk_id, &params.create_params)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(datasets_and_regions)
}

async fn allocate_region_from_dataset(
    log: &Logger,
    dataset: &db::model::Dataset,
    region: &db::model::Region,
) -> Result<crucible_agent_client::types::Region, ActionError> {
    let url = format!("http://{}", dataset.address());
    let client = CrucibleAgentClient::new(&url);

    let region_request = CreateRegion {
        block_size: region.block_size(),
        extent_count: region.extent_count(),
        extent_size: region.extent_size(),
        // TODO: Can we avoid casting from UUID to string?
        // NOTE: This'll require updating the crucible agent client.
        id: RegionId(region.id().to_string()),
        volume_id: region.disk_id().to_string(),
    };

    let create_region = || async {
        let region = client
            .region_create(&region_request)
            .await
            .map_err(|e| BackoffError::Permanent(e))?;
        match region.state {
            RegionState::Requested => Err(BackoffError::Transient(anyhow!(
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
        warn!(log, "Region requested, not yet created. Retrying in {:?}", delay);
    };

    let region = backoff::retry_notify(
        backoff::internal_service_policy(),
        create_region,
        log_create_failure,
    )
    .await
    .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(region)
}

async fn sdc_regions_ensure(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let datasets_and_regions = sagactx
        .lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
            "datasets_and_regions",
        )?;
    let request_count = datasets_and_regions.len();
    futures::stream::iter(datasets_and_regions)
        .map(|(dataset, region)| async move {
            allocate_region_from_dataset(log, &dataset, &region).await
        })
        // Execute the allocation requests concurrently.
        .buffer_unordered(request_count)
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    // TODO: Region has a port value, we could store this in the DB?

    Ok(())
}

async fn sdc_finalize_disk_record(
    sagactx: ActionContext<SagaDiskCreate>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let _params = sagactx.saga_params();

    let disk_id = sagactx.lookup::<Uuid>("disk_id")?;
    let disk_created = sagactx.lookup::<db::model::Disk>("disk_created")?;
    osagactx
        .datastore()
        .disk_update_runtime(&disk_id, &disk_created.runtime().detach())
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
