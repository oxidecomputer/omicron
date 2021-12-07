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
use crate::db::identity::Resource;
use crate::external_api::params;
use crate::saga_interface::SagaContext;
use chrono::Utc;
use lazy_static::lazy_static;
use omicron_common::api::external::Generation;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::InstanceHardware;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaTemplateGeneric;
use steno::SagaType;
use uuid::Uuid;

/*
 * We'll need a richer mechanism for registering sagas, but this works for now.
 */
pub const SAGA_INSTANCE_CREATE_NAME: &'static str = "instance-create";
lazy_static! {
    pub static ref SAGA_INSTANCE_CREATE_TEMPLATE: Arc<SagaTemplate<SagaInstanceCreate>> =
        Arc::new(saga_instance_create());
}

lazy_static! {
    pub static ref ALL_TEMPLATES: BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>> =
        all_templates();
}

fn all_templates(
) -> BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>> {
    vec![(
        SAGA_INSTANCE_CREATE_NAME,
        Arc::clone(&SAGA_INSTANCE_CREATE_TEMPLATE)
            as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
    )]
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

async fn sic_create_instance_record(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<InstanceHardware, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let sled_uuid = sagactx.lookup::<Uuid>("server_id");
    let instance_id = sagactx.lookup::<Uuid>("instance_id");
    let propolis_uuid = sagactx.lookup::<Uuid>("propolis_id");

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

    let default_name =
        db::model::Name(Name::try_from("default".to_string()).unwrap());

    let vpc = osagactx
        .datastore()
        .vpc_fetch_by_name(&instance.project_id, &default_name)
        .await
        .map_err(ActionError::action_failed)?;
    let subnet = osagactx
        .datastore()
        .vpc_subnet_fetch_by_name(&vpc.id(), &default_name)
        .await
        .map_err(ActionError::action_failed)?;

    let mac = osagactx
        .datastore()
        .generate_mac_address()
        .map_err(ActionError::action_failed)?;
    let interface_id = Uuid::new_v4();
    // Request an allocation
    let ip = None;
    let interface = db::model::IncompleteNetworkInterface::new(
        interface_id,
        instance.id(),
        // TODO-correctness: vpc_id here is used for name uniqueness. Should
        // interface names be unique to the subnet's VPC or to the
        // VPC associated with the instance's default interface?
        vpc.id(),
        subnet,
        mac,
        ip,
        params::NetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                // TODO: Generate a unique name here, since we're not guaranteed
                // this interface name is available
                name: instance.name().0.clone(),
                description: "default interface".to_string(),
            },
        },
    );
    let interface = osagactx
        .datastore()
        .instance_create_network_interface(interface)
        .await
        .map_err(ActionError::action_failed)?;

    // See also: instance_set_runtime in nexus.rs for a similar construction.
    Ok(InstanceHardware {
        runtime: instance.runtime().clone().into(),
        nics: vec![interface.into()],
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
