/*!
 * Saga actions, undo actions, and saga constructors used in Nexus.
 */

/*
 * NOTE: We want to be careful about what interfaces we expose to saga actions.
 * In the future, we expect to mock these out for comprehensive testing of
 * correctness, idempotence, etc.  The more constrained this interface is, the
 * easier it will be to test, version, and update in deployed systems.
 */

use crate::saga_interface::SagaContext;
use chrono::Utc;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InstanceCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::CrucibleDiskInfo;
use omicron_common::api::internal::sled_agent::InstanceHardware;
use omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested;
use omicron_common::api::internal::sled_agent::InstanceStateRequested;
use serde::Deserialize;
use serde::Serialize;
use slog::error;
use std::collections::BTreeMap;
use std::net::SocketAddr;
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
    pub create_params: InstanceCreateParams,
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
        new_action_noop_undo(sic_generate_instance_id),
    );

    template_builder.append(
        "server_id",
        "AllocServer",
        // TODO-robustness This still needs an undo action, and we should really
        // keep track of resources and reservations, etc.  See the comment on
        // SagaContext::alloc_server()
        new_action_noop_undo(sic_alloc_server),
    );

    template_builder.append_parallel(vec![
        (
            "crucible0id",
            "AllocCrucible0",
            new_action_noop_undo(sic_alloc_crucible),
        ),
        (
            "crucible1id",
            "AllocCrucible1",
            new_action_noop_undo(sic_alloc_crucible),
        ),
        (
            "crucible2id",
            "AllocCrucible2",
            new_action_noop_undo(sic_alloc_crucible),
        ),
    ]);

    template_builder.append_parallel(vec![
        (
            "crucible0address",
            "EnsureCrucible0",
            new_action_noop_undo(sic_ensure_crucible_region),
        ),
        (
            "crucible1address",
            "EnsureCrucible1",
            new_action_noop_undo(sic_ensure_crucible_region),
        ),
        (
            "crucible2address",
            "EnsureCrucible2",
            new_action_noop_undo(sic_ensure_crucible_region),
        ),
    ]);

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

async fn sic_generate_instance_id(
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

async fn sic_alloc_crucible(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();

    /*
     * XXX This is obviously deeply unfortunate!
     */
    let index: usize =
        sagactx.node_label().replace("AllocCrucible", "").parse().unwrap();

    osagactx.alloc_crucible(index).await.map_err(ActionError::action_failed)
}

async fn sic_ensure_crucible_region(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<SocketAddr, ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.logger(); /* XXX */

    use crucible_agent_client::types;

    /*
     * XXX This is obviously deeply unfortunate!
     */
    let i: usize =
        sagactx.node_label().replace("EnsureCrucible", "").parse().unwrap();
    let crucible_id = sagactx.lookup::<Uuid>(&format!("crucible{}id", i))?;
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;

    let ca = osagactx
        .crucible_client(&crucible_id)
        .await
        .map_err(ActionError::action_failed)?;

    let block_size = 512;
    let extent_size = 10 * 1024 * 1024 / block_size; /* 10 MiB */

    let req = types::CreateRegion {
        block_size,
        extent_size,
        extent_count: 1000, /* ~10GB */
        /*
         * XXX For now, use the instance ID as the volume ID.
         */
        volume_id: instance_id.to_string(),
        /*
         * XXX This will eventually go in a database record, presumably.
         */
        id: types::RegionId(Uuid::new_v4().to_string()),
    };

    let mut tries: u32 = 10;
    loop {
        /*
         * The region create request is idempotent.  We post the same request
         * over and over until the server tells us the region was created.
         */
        let dur = match ca.region_create(&req).await {
            Ok(r) => {
                use types::State::*;

                /*
                 * If we have received a reply from the server, reset our
                 * failure counter.
                 */
                tries = 10;

                match &r.state {
                    Requested => (),
                    Created => {
                        return Ok(SocketAddr::new(
                            ca.addr.ip(),
                            r.port_number as u16,
                        ));
                    }
                    Tombstoned | Destroyed | Failed => {
                        /*
                         * XXX How should we return this error?!
                         */
                        return Err(ActionError::action_failed(
                            Error::InternalError {
                                message:
                                    "crucible region entered unexpected state"
                                        .to_string(),
                            },
                        ));
                    }
                }

                std::time::Duration::from_millis(100)
            }
            Err(e) => {
                error!(log, "crucible client error: {:?}", e);
                if tries == 0 {
                    /*
                     * XXX How should we return this error?!
                     */
                    return Err(ActionError::action_failed(
                        Error::InternalError {
                            message: "too many crucible agent failures"
                                .to_string(),
                        },
                    ));
                }
                tries -= 1;
                std::time::Duration::from_secs(1)
            }
        };

        tokio::time::sleep(dur).await;
    }
}

async fn sic_create_instance_record(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<InstanceHardware, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let sled_uuid = sagactx.lookup::<Uuid>("server_id");
    let instance_id = sagactx.lookup::<Uuid>("instance_id");
    let crucibles = (0..=2)
        .map(|n| sagactx.lookup(&format!("crucible{}address", n)))
        .collect::<Result<Vec<_>, _>>()?;

    let runtime = InstanceRuntimeState {
        run_state: InstanceState::Creating,
        sled_uuid: sled_uuid?,
        hostname: params.create_params.hostname.clone(),
        memory: params.create_params.memory,
        ncpus: params.create_params.ncpus,
        gen: Generation::new(),
        time_updated: Utc::now(),
    };

    let instance = osagactx
        .datastore()
        .project_create_instance(
            &instance_id?,
            &params.project_id,
            &params.create_params,
            &runtime.into(),
            crucibles.clone(),
        )
        .await
        .map_err(ActionError::action_failed)?;

    // TODO: Populate this with an appropriate NIC.
    // See also: instance_set_runtime in nexus.rs for a similar construction.
    Ok(InstanceHardware {
        runtime: instance.runtime().into(),
        nics: vec![],
        disks: vec![CrucibleDiskInfo {
            address: crucibles,
            // TODO: Avoid hard-coding this slot number.
            slot: 0,
            read_only: false,
        }],
    })
}

async fn sic_instance_ensure(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), ActionError> {
    /*
     * TODO-correctness is this idempotent?
     */
    let osagactx = sagactx.user_data();
    let runtime_params = InstanceRuntimeStateRequested {
        run_state: InstanceStateRequested::Running,
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
     * XXX Get the list of socket addresses for crucibles so that we can pass it
     * in the instance request as a disk, when that is plumbed through to
     * Propolis.
     */
    // let crucibles = (0..=2)
    //     .map(|n| sagactx.lookup(&format!("crucible{}address", n)))
    //     .collect::<Result<Vec<_>, _>>()?;

    /*
     * Ask the sled agent to begin the state change.  Then update the database
     * to reflect the new intermediate state.  If this update is not the newest
     * one, that's fine.  That might just mean the sled agent beat us to it.
     */
    let new_runtime_state = sa
        .instance_ensure(instance_id, initial_runtime, runtime_params)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime_state.into())
        .await
        .map(|_| ())
        .map_err(ActionError::action_failed)
}
