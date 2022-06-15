// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    impl_authenticated_saga_params, saga_generate_uuid, AuthenticatedSagaParams,
};
use crate::app::{MAX_DISKS_PER_INSTANCE, MAX_NICS_PER_INSTANCE};
use crate::context::OpContext;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::queries::network_interface::InsertError as InsertNicError;
use crate::defaults::DEFAULT_PRIMARY_NIC_NAME;
use crate::external_api::params;
use crate::saga_interface::SagaContext;
use crate::{authn, authz, db};
use chrono::Utc;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use slog::warn;
use std::convert::{TryFrom, TryInto};
use std::net::Ipv6Addr;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

pub const SAGA_NAME: &'static str = "instance-create";

lazy_static! {
    pub static ref SAGA_TEMPLATE: Arc<SagaTemplate<SagaInstanceCreate>> =
        Arc::new(saga_instance_create());
}

// "Create Instance" saga template

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub organization_name: Name,
    pub project_name: Name,
    pub project_id: Uuid,
    pub create_params: params::InstanceCreate,
}

#[derive(Debug)]
pub struct SagaInstanceCreate;
impl SagaType for SagaInstanceCreate {
    type SagaParamsType = Arc<Params>;
    type ExecContextType = Arc<SagaContext>;
}
impl_authenticated_saga_params!(SagaInstanceCreate);

fn saga_instance_create() -> SagaTemplate<SagaInstanceCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "instance_id",
        "GenerateInstanceId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "propolis_id",
        "GeneratePropolisId",
        new_action_noop_undo(saga_generate_uuid),
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
        "propolis_ip",
        "AllocatePropolisIp",
        new_action_noop_undo(sic_allocate_propolis_ip),
    );

    template_builder.append(
        "instance_name",
        "CreateInstanceRecord",
        ActionFunc::new_action(
            sic_create_instance_record,
            sic_delete_instance_record,
        ),
    );

    // NOTE: The separation of the ID-allocation and NIC creation nodes is
    // intentional.
    //
    // The Nexus API supports creating multiple network interfaces at the time
    // an instance is provisioned. However, each NIC creation is independent,
    // and each can fail. For example, someone might specify multiple NICs with
    // the same IP address. The first will be created successfully, but the
    // later ones will fail. We need to handle this case gracefully, and always
    // delete any NICs we create, even if the NIC creation node itself fails.
    //
    // To do that, we create an action that only allocates the UUIDs for each
    // interface. This has an undo action that actually deletes any NICs for the
    // instance to be provisioned. The forward action is infallible, so this
    // undo action will always run, even (and especially) if the NIC creation
    // action fails.
    //
    // It's also important that we allocate the UUIDs first. It's possible that
    // we crash partway through the NIC creation action. In this case, the saga
    // recovery machinery will pick it up where it left off, without first
    // destroying the NICs we created before crashing. By allocating the UUIDs
    // first, we can make the insertion idempotent, by ignoring conflicts on the
    // UUID.
    template_builder.append(
        "network_interface_ids",
        "NetworkInterfaceIds",
        ActionFunc::new_action(
            sic_allocate_network_interface_ids,
            sic_create_network_interfaces_undo,
        ),
    );

    template_builder.append(
        "network_interfaces",
        "CreateNetworkInterfaces",
        new_action_noop_undo(sic_create_network_interfaces),
    );

    // Saga actions must be atomic - they have to fully complete or fully abort.
    // This is because Steno assumes that the saga actions are atomic and
    // therefore undo actions are *not* run for the failing node.
    //
    // For this reason, each disk is created and attached with a separate saga
    // node. If a saga node had a loop to attach or detach all disks, and one
    // failed, any disks that were attached would not be detached because the
    // corresponding undo action would not be run. Separate each disk create and
    // attach to their own saga node and ensure that each function behaves
    // atomically.
    //
    // Currently, instances can have a maximum of 8 disks attached. Create two
    // saga nodes for each disk that will unconditionally run but contain
    // conditional logic depending on if that disk index is going to be used.
    // Steno does not currently support the saga node graph changing shape.
    for i in 0..MAX_DISKS_PER_INSTANCE {
        template_builder.append(
            &format!("create_disks{}", i),
            "CreateDisksForInstance",
            ActionFunc::new_action(
                async move |sagactx| {
                    sic_create_disks_for_instance(sagactx, i as usize).await
                },
                async move |sagactx| {
                    sic_create_disks_for_instance_undo(sagactx, i as usize)
                        .await
                },
            ),
        );

        template_builder.append(
            &format!("attach_disks{}", i),
            "AttachDisksToInstance",
            ActionFunc::new_action(
                async move |sagactx| {
                    sic_attach_disks_to_instance(sagactx, i as usize).await
                },
                async move |sagactx| {
                    sic_attach_disks_to_instance_undo(sagactx, i as usize).await
                },
            ),
        );
    }

    template_builder.append(
        "instance_ensure",
        "InstanceEnsure",
        new_action_noop_undo(sic_instance_ensure),
    );

    template_builder.build()
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

async fn sic_allocate_network_interface_ids(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<Vec<Uuid>, ActionError> {
    match sagactx.saga_params().create_params.network_interfaces {
        params::InstanceNetworkInterfaceAttachment::None => Ok(vec![]),
        params::InstanceNetworkInterfaceAttachment::Default => {
            Ok(vec![Uuid::new_v4()])
        }
        params::InstanceNetworkInterfaceAttachment::Create(
            ref create_params,
        ) => {
            if create_params.len() > MAX_NICS_PER_INSTANCE.try_into().unwrap() {
                return Err(ActionError::action_failed(
                    Error::invalid_request(
                        format!(
                            "Instances may not have more than {}
                            network interfaces",
                            MAX_NICS_PER_INSTANCE
                        )
                        .as_str(),
                    ),
                ));
            }
            let mut ids = Vec::with_capacity(create_params.len());
            for _ in 0..create_params.len() {
                ids.push(Uuid::new_v4());
            }
            Ok(ids)
        }
    }
}

async fn sic_create_network_interfaces(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), ActionError> {
    match sagactx.saga_params().create_params.network_interfaces {
        params::InstanceNetworkInterfaceAttachment::None => Ok(()),
        params::InstanceNetworkInterfaceAttachment::Default => {
            sic_create_default_primary_network_interface(&sagactx).await
        }
        params::InstanceNetworkInterfaceAttachment::Create(
            ref create_params,
        ) => {
            sic_create_custom_network_interfaces(&sagactx, &create_params).await
        }
    }
}

/// Create one or more custom (non-default) network interfaces for the provided
/// instance.
async fn sic_create_custom_network_interfaces(
    sagactx: &ActionContext<SagaInstanceCreate>,
    interface_params: &[params::NetworkInterfaceCreate],
) -> Result<(), ActionError> {
    if interface_params.is_empty() {
        return Ok(());
    }

    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let ids = sagactx.lookup::<Vec<Uuid>>("network_interface_ids")?;

    // Lookup authz objects, used in the call to create the NIC itself.
    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;
    let (.., authz_vpc, db_vpc) = LookupPath::new(&opctx, &datastore)
        .project_id(saga_params.project_id)
        .vpc_name(&db::model::Name::from(interface_params[0].vpc_name.clone()))
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // Check that all VPC names are the same.
    //
    // This isn't strictly necessary, as the queries would fail below, but it's
    // easier to handle here.
    if interface_params.iter().any(|p| p.vpc_name != db_vpc.name().0) {
        return Err(ActionError::action_failed(Error::invalid_request(
            "All interfaces must be in the same VPC",
        )));
    }

    if ids.len() != interface_params.len() {
        return Err(ActionError::action_failed(Error::internal_error(
            "found differing number of network interface IDs and interface \
            parameters",
        )));
    }
    for (interface_id, params) in ids.into_iter().zip(interface_params.iter()) {
        // TODO-correctness: It seems racy to fetch the subnet and create the
        // interface in separate requests, but outside of a transaction. This
        // should probably either be in a transaction, or the
        // `instance_create_network_interface` function/query needs some JOIN
        // on the `vpc_subnet` table.
        let (.., authz_subnet, db_subnet) = LookupPath::new(&opctx, &datastore)
            .vpc_id(authz_vpc.id())
            .vpc_subnet_name(&db::model::Name::from(params.subnet_name.clone()))
            .fetch()
            .await
            .map_err(ActionError::action_failed)?;
        let interface = db::model::IncompleteNetworkInterface::new(
            interface_id,
            instance_id,
            authz_vpc.id(),
            db_subnet.clone(),
            params.identity.clone(),
            params.ip,
        )
        .map_err(ActionError::action_failed)?;
        let result = datastore
            .instance_create_network_interface(
                &opctx,
                &authz_subnet,
                &authz_instance,
                interface,
            )
            .await;

        match result {
            Ok(_) => Ok(()),

            // Detect the specific error arising from this node being partially
            // completed.
            //
            // The query used to insert network interfaces first checks for an
            // existing record with the same primary key. It will attempt to
            // insert that record if it exists, which obviously fails with a
            // primary key violation. (If the record does _not_ exist, one will
            // be inserted as usual, see
            // `db::queries::network_interface::InsertQuery` for details).
            //
            // In this one specific case, we're asserting that any primary key
            // duplicate arises because this saga node ran partway and then
            // crashed. The saga recovery machinery will replay just this node,
            // without first unwinding it, so any previously-inserted interfaces
            // will still exist. This is expected.
            Err(InsertNicError::DuplicatePrimaryKey(_)) => {
                // TODO-observability: We should bump a counter here.
                let log = osagactx.log();
                warn!(
                    log,
                    "Detected duplicate primary key during saga to \
                    create network interfaces for instance '{}'. \
                    This likely occurred because \
                    the saga action 'sic_create_custom_network_interfaces' \
                    crashed and has been recovered.",
                    instance_id;
                    "primary_key" => interface_id.to_string(),
                );
                Ok(())
            }
            Err(e) => Err(e.into_external()),
        }
        .map_err(ActionError::action_failed)?;
    }
    Ok(())
}

/// Create a default primary network interface for an instance during the create
/// saga.
async fn sic_create_default_primary_network_interface(
    sagactx: &ActionContext<SagaInstanceCreate>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;

    // The literal name "default" is currently used for the VPC and VPC Subnet,
    // when not specified in the client request.
    // TODO-completeness: We'd like to select these from Project-level defaults.
    // See https://github.com/oxidecomputer/omicron/issues/1015.
    let default_name = Name::try_from("default".to_string()).unwrap();
    let internal_default_name = db::model::Name::from(default_name.clone());

    // The name of the default primary interface.
    let iface_name =
        Name::try_from(DEFAULT_PRIMARY_NIC_NAME.to_string()).unwrap();

    let interface_params = params::NetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: iface_name.clone(),
            description: format!(
                "default primary interface for {}",
                saga_params.create_params.identity.name,
            ),
        },
        vpc_name: default_name.clone(),
        subnet_name: default_name.clone(),
        ip: None, // Request an IP address allocation
    };

    // Lookup authz objects, used in the call to actually create the NIC.
    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;
    let (.., authz_vpc, authz_subnet, db_subnet) =
        LookupPath::new(&opctx, &datastore)
            .project_id(saga_params.project_id)
            .vpc_name(&internal_default_name)
            .vpc_subnet_name(&internal_default_name)
            .fetch()
            .await
            .map_err(ActionError::action_failed)?;

    let interface_id = Uuid::new_v4();
    let interface = db::model::IncompleteNetworkInterface::new(
        interface_id,
        instance_id,
        authz_vpc.id(),
        db_subnet.clone(),
        interface_params.identity.clone(),
        interface_params.ip,
    )
    .map_err(ActionError::action_failed)?;
    datastore
        .instance_create_network_interface(
            &opctx,
            &authz_subnet,
            &authz_instance,
            interface,
        )
        .await
        .map_err(InsertNicError::into_external)
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sic_create_network_interfaces_undo(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), anyhow::Error> {
    // We issue a request to delete any interfaces associated with this instance.
    // In the case we failed partway through allocating interfaces, we need to
    // clean up any previously-created interface records from the database.
    // Just delete every interface that exists, even if there are zero such
    // records.
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;
    datastore
        .instance_delete_all_network_interfaces(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

/// Create disks during instance creation, and return a list of disk names
// TODO implement
async fn sic_create_disks_for_instance(
    sagactx: ActionContext<SagaInstanceCreate>,
    disk_index: usize,
) -> Result<Option<String>, ActionError> {
    let saga_params = sagactx.saga_params();
    let saga_disks = &saga_params.create_params.disks;

    if disk_index >= saga_disks.len() {
        return Ok(None);
    }

    let disk = &saga_disks[disk_index];

    match disk {
        params::InstanceDiskAttachment::Create(_create_params) => {
            return Err(ActionError::action_failed(
                "Creating disk during instance create unsupported!".to_string(),
            ));
        }

        _ => {}
    }

    Ok(None)
}

/// Undo disks created during instance creation
// TODO implement
async fn sic_create_disks_for_instance_undo(
    _sagactx: ActionContext<SagaInstanceCreate>,
    _disk_index: usize,
) -> Result<(), anyhow::Error> {
    Ok(())
}

async fn sic_attach_disks_to_instance(
    sagactx: ActionContext<SagaInstanceCreate>,
    disk_index: usize,
) -> Result<(), ActionError> {
    ensure_instance_disk_attach_state(sagactx, disk_index, true).await
}

async fn sic_attach_disks_to_instance_undo(
    sagactx: ActionContext<SagaInstanceCreate>,
    disk_index: usize,
) -> Result<(), anyhow::Error> {
    Ok(ensure_instance_disk_attach_state(sagactx, disk_index, false).await?)
}

async fn ensure_instance_disk_attach_state(
    sagactx: ActionContext<SagaInstanceCreate>,
    disk_index: usize,
    attached: bool,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);

    let saga_disks = &saga_params.create_params.disks;
    let instance_name = sagactx.lookup::<db::model::Name>("instance_name")?;

    if disk_index >= saga_disks.len() {
        return Ok(());
    }

    let disk = &saga_disks[disk_index];

    let organization_name: db::model::Name =
        saga_params.organization_name.clone().into();
    let project_name: db::model::Name = saga_params.project_name.clone().into();

    match disk {
        params::InstanceDiskAttachment::Create(_) => {
            // TODO grab disks created in sic_create_disks_for_instance
            return Err(ActionError::action_failed(Error::invalid_request(
                "creating disks while creating an instance not supported",
            )));
        }
        params::InstanceDiskAttachment::Attach(instance_disk_attach) => {
            let disk_name: db::model::Name =
                instance_disk_attach.name.clone().into();

            if attached {
                osagactx
                    .nexus()
                    .instance_attach_disk(
                        &opctx,
                        &organization_name,
                        &project_name,
                        &instance_name,
                        &disk_name,
                    )
                    .await
            } else {
                osagactx
                    .nexus()
                    .instance_detach_disk(
                        &opctx,
                        &organization_name,
                        &project_name,
                        &instance_name,
                        &disk_name,
                    )
                    .await
            }
            .map_err(ActionError::action_failed)?;
        }
    }

    Ok(())
}

/// Helper function to allocate a new IPv6 address for an Oxide service running
/// on the provided sled.
///
/// `sled_id_name` is the name of the serialized output containing the UUID for
/// the targeted sled.
pub(super) async fn allocate_sled_ipv6<T>(
    sagactx: ActionContext<T>,
    sled_id_name: &str,
) -> Result<Ipv6Addr, ActionError>
where
    T: SagaType<ExecContextType = Arc<SagaContext>>,
    T::SagaParamsType: AuthenticatedSagaParams,
{
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, params.serialized_authn());
    let sled_uuid = sagactx.lookup::<Uuid>(sled_id_name)?;
    osagactx
        .datastore()
        .next_ipv6_address(&opctx, sled_uuid)
        .await
        .map_err(ActionError::action_failed)
}

// Allocate an IP address on the destination sled for the Propolis server
async fn sic_allocate_propolis_ip(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<Ipv6Addr, ActionError> {
    allocate_sled_ipv6(sagactx, "server_id").await
}

async fn sic_create_instance_record(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<db::model::Name, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let sled_uuid = sagactx.lookup::<Uuid>("server_id")?;
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let propolis_uuid = sagactx.lookup::<Uuid>("propolis_id")?;
    let propolis_addr = sagactx.lookup::<Ipv6Addr>("propolis_ip")?;

    let runtime = InstanceRuntimeState {
        run_state: InstanceState::Creating,
        sled_id: sled_uuid,
        propolis_id: propolis_uuid,
        dst_propolis_id: None,
        propolis_addr: Some(std::net::SocketAddr::new(
            propolis_addr.into(),
            12400,
        )),
        migration_id: None,
        hostname: params.create_params.hostname.clone(),
        memory: params.create_params.memory,
        ncpus: params.create_params.ncpus,
        gen: Generation::new(),
        time_updated: Utc::now(),
    };

    let new_instance = db::model::VmInstance::new(
        instance_id,
        params.project_id,
        &params.create_params,
        runtime.into(),
    );

    let instance = osagactx
        .datastore()
        .project_create_instance(new_instance)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(instance.name().clone())
}

async fn sic_delete_instance_record(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let datastore = osagactx.datastore();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let instance_name = sagactx.lookup::<db::model::Name>("instance_name")?;

    // We currently only support deleting an instance if it is stopped or
    // failed, so update the state accordingly to allow deletion.
    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .project_id(params.project_id)
        .vm_instance_name(&instance_name)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let runtime_state = db::model::InstanceRuntimeState {
        state: db::model::InstanceState::new(InstanceState::Failed),
        // Must update the generation, or the database query will fail.
        //
        // The runtime state of the instance record is only changed as a result
        // of the successful completion of the saga, or in this action during
        // saga unwinding. So we're guaranteed that the cached generation in the
        // saga log is the most recent in the database.
        gen: db::model::Generation::from(db_instance.runtime_state.gen.next()),
        ..db_instance.runtime_state
    };

    let updated = datastore
        .instance_update_runtime(&instance_id, &runtime_state)
        .await
        .map_err(ActionError::action_failed)?;

    if !updated {
        warn!(
            osagactx.log(),
            "failed to update instance runtime state from creating to failed",
        );
    }

    // Actually delete the record.
    datastore
        .project_delete_instance(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sic_instance_ensure(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), ActionError> {
    // TODO-correctness is this idempotent?
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let datastore = osagactx.datastore();
    let runtime_params = InstanceRuntimeStateRequested {
        run_state: InstanceStateRequested::Running,
        migration_params: None,
    };

    let instance_name = sagactx.lookup::<db::model::Name>("instance_name")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .project_id(params.project_id)
        .vm_instance_name(&instance_name)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_set_runtime(
            &opctx,
            &authz_instance,
            &db_instance,
            runtime_params,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
