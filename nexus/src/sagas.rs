// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga actions, undo actions, and saga constructors used in Nexus.

// NOTE: We want to be careful about what interfaces we expose to saga actions.
// In the future, we expect to mock these out for comprehensive testing of
// correctness, idempotence, etc.  The more constrained this interface is, the
// easier it will be to test, version, and update in deployed systems.

use crate::context::OpContext;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::saga_interface::SagaContext;
use crate::{authn, authz, db};
use anyhow::anyhow;
use chrono::Utc;
use crucible_agent_client::{
    types::{CreateRegion, RegionId, State as RegionState},
    Client as CrucibleAgentClient,
};
use futures::StreamExt;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::external::NetworkInterface;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::backoff::{self, BackoffError};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::InstanceEnsureBody;
use sled_agent_client::types::InstanceHardware;
use sled_agent_client::types::InstanceMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use slog::warn;
use slog::Logger;
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::net::Ipv6Addr;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaTemplateGeneric;
use steno::SagaType;
use uuid::Uuid;

// We'll need a richer mechanism for registering sagas, but this works for now.
pub const SAGA_INSTANCE_CREATE_NAME: &'static str = "instance-create";
pub const SAGA_INSTANCE_MIGRATE_NAME: &'static str = "instance-migrate";
pub const SAGA_DISK_CREATE_NAME: &'static str = "disk-create";
pub const SAGA_DISK_DELETE_NAME: &'static str = "disk-delete";
lazy_static! {
    pub static ref SAGA_INSTANCE_CREATE_TEMPLATE: Arc<SagaTemplate<SagaInstanceCreate>> =
        Arc::new(saga_instance_create());
    pub static ref SAGA_INSTANCE_MIGRATE_TEMPLATE: Arc<SagaTemplate<SagaInstanceMigrate>> =
        Arc::new(saga_instance_migrate());
    pub static ref SAGA_DISK_CREATE_TEMPLATE: Arc<SagaTemplate<SagaDiskCreate>> =
        Arc::new(saga_disk_create());
    pub static ref SAGA_DISK_DELETE_TEMPLATE: Arc<SagaTemplate<SagaDiskDelete>> =
        Arc::new(saga_disk_delete());
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
            SAGA_INSTANCE_MIGRATE_NAME,
            Arc::clone(&SAGA_INSTANCE_MIGRATE_TEMPLATE)
                as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
        ),
        (
            SAGA_DISK_CREATE_NAME,
            Arc::clone(&SAGA_DISK_CREATE_TEMPLATE)
                as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
        ),
        (
            SAGA_DISK_DELETE_NAME,
            Arc::clone(&SAGA_DISK_DELETE_TEMPLATE)
                as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
        ),
    ]
    .into_iter()
    .collect()
}

async fn saga_generate_uuid<UserType: SagaType>(
    _: ActionContext<UserType>,
) -> Result<Uuid, ActionError> {
    Ok(Uuid::new_v4())
}

/// A trait for sagas with serialized authentication information.
///
/// This allows sharing code in different sagas which rely on some
/// authentication information, for example when doing database lookups.
trait AuthenticatedSagaParams {
    fn serialized_authn(&self) -> &authn::saga::Serialized;
}

/// A helper macro which implements the `AuthenticatedSagaParams` trait for saga
/// parameter types which have a field called `serialized_authn`.
macro_rules! impl_authenticated_saga_params {
    ($typ:ty) => {
        impl AuthenticatedSagaParams for <$typ as SagaType>::SagaParamsType {
            fn serialized_authn(&self) -> &authn::saga::Serialized {
                &self.serialized_authn
            }
        }
    };
}

// "Create Instance" saga template

#[derive(Debug, Deserialize, Serialize)]
pub struct ParamsInstanceCreate {
    pub serialized_authn: authn::saga::Serialized,
    pub organization_name: Name,
    pub project_name: Name,
    pub project_id: Uuid,
    pub create_params: params::InstanceCreate,
}

#[derive(Debug)]
pub struct SagaInstanceCreate;
impl SagaType for SagaInstanceCreate {
    type SagaParamsType = Arc<ParamsInstanceCreate>;
    type ExecContextType = Arc<SagaContext>;
}
impl_authenticated_saga_params!(SagaInstanceCreate);

pub fn saga_instance_create() -> SagaTemplate<SagaInstanceCreate> {
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
    for i in 0..crate::nexus::MAX_DISKS_PER_INSTANCE {
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
            if create_params.len()
                > crate::nexus::MAX_NICS_PER_INSTANCE.try_into().unwrap()
            {
                return Err(ActionError::action_failed(
                    Error::invalid_request(
                        format!(
                            "Instances may not have more than {}
                            network interfaces",
                            crate::nexus::MAX_NICS_PER_INSTANCE
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
) -> Result<Option<Vec<NetworkInterface>>, ActionError> {
    match sagactx.saga_params().create_params.network_interfaces {
        params::InstanceNetworkInterfaceAttachment::None => Ok(None),
        params::InstanceNetworkInterfaceAttachment::Default => {
            sic_create_default_network_interface(&sagactx).await
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
) -> Result<Option<Vec<NetworkInterface>>, ActionError> {
    if interface_params.is_empty() {
        return Ok(Some(vec![]));
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

    let mut interfaces = Vec::with_capacity(interface_params.len());
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
        // `subnet_create_network_interface` function/query needs some JOIN
        // on the `vpc_subnet` table.
        let (.., authz_subnet, db_subnet) = LookupPath::new(&opctx, &datastore)
            .vpc_id(authz_vpc.id())
            .vpc_subnet_name(&db::model::Name::from(params.subnet_name.clone()))
            .fetch()
            .await
            .map_err(ActionError::action_failed)?;
        let mac =
            db::model::MacAddr::new().map_err(ActionError::action_failed)?;
        let interface = db::model::IncompleteNetworkInterface::new(
            interface_id,
            instance_id,
            authz_vpc.id(),
            db_subnet,
            mac,
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

        use crate::db::subnet_allocation::NetworkInterfaceError;
        let interface = match result {
            Ok(interface) => Ok(interface),

            // Detect the specific error arising from this node being partially
            // completed.
            //
            // The query used to insert network interfaces first checks for an
            // existing record with the same primary key. It will attempt to
            // insert that record if it exists, which obviously fails with a
            // primary key violation. (If the record does _not_ exist, one will
            // be inserted as usual, see
            // `db::subnet_name::InsertNetworkInterfaceQuery` for details).
            //
            // In this one specific case, we're asserting that any primary key
            // duplicate arises because this saga node ran partway and then
            // crashed. The saga recovery machinery will replay just this node,
            // without first unwinding it, so any previously-inserted interfaces
            // will still exist. This is expected.
            Err(NetworkInterfaceError::DuplicatePrimaryKey(_)) => {
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

                // Refetch the interface itself, to serialize it for the next
                // saga node.
                LookupPath::new(&opctx, &datastore)
                    .instance_id(authz_instance.id())
                    .network_interface_name(&db::model::Name(
                        params.identity.name.clone(),
                    ))
                    .fetch()
                    .await
                    .map(|(.., db_interface)| db_interface)
            }
            Err(e) => Err(e.into_external()),
        }
        .map_err(ActionError::action_failed)?;
        interfaces.push(NetworkInterface::from(interface))
    }
    Ok(Some(interfaces))
}

/// Create the default network interface for an instance during the create saga
async fn sic_create_default_network_interface(
    sagactx: &ActionContext<SagaInstanceCreate>,
) -> Result<Option<Vec<NetworkInterface>>, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let default_name = Name::try_from("default".to_string()).unwrap();
    let internal_default_name = db::model::Name::from(default_name.clone());
    let interface_params = params::NetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: default_name.clone(),
            description: format!(
                "default interface for {}",
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

    let mac = db::model::MacAddr::new().map_err(ActionError::action_failed)?;
    let interface_id = Uuid::new_v4();
    let interface = db::model::IncompleteNetworkInterface::new(
        interface_id,
        instance_id,
        authz_vpc.id(),
        db_subnet,
        mac,
        interface_params.identity.clone(),
        interface_params.ip,
    )
    .map_err(ActionError::action_failed)?;
    let interface = datastore
        .instance_create_network_interface(
            &opctx,
            &authz_subnet,
            &authz_instance,
            interface,
        )
        .await
        .map_err(db::subnet_allocation::NetworkInterfaceError::into_external)
        .map_err(ActionError::action_failed)?;
    Ok(Some(vec![interface.into()]))
}

async fn sic_create_network_interfaces_undo(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), anyhow::Error> {
    // We issue a request to delete any interfaces associated with this
    // instance.  In the case we failed partway through allocating interfaces,
    // we won't have cached the interface records in the saga log, but they're
    // definitely still in the database. Just delete every interface that
    // exists, even if there are zero such records.
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
async fn allocate_sled_ipv6<T>(
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
        sled_uuid,
        propolis_uuid,
        dst_propolis_uuid: None,
        propolis_addr: Some(std::net::SocketAddr::new(
            propolis_addr.into(),
            12400,
        )),
        migration_uuid: None,
        hostname: params.create_params.hostname.clone(),
        memory: params.create_params.memory,
        ncpus: params.create_params.ncpus,
        gen: Generation::new(),
        time_updated: Utc::now(),
    };

    let new_instance = db::model::Instance::new(
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
        .instance_name(&instance_name)
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
        .instance_name(&instance_name)
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

// "Migrate Instance" saga template
#[derive(Debug, Deserialize, Serialize)]
pub struct ParamsInstanceMigrate {
    pub serialized_authn: authn::saga::Serialized,
    pub instance_id: Uuid,
    pub migrate_params: params::InstanceMigrate,
}

#[derive(Debug)]
pub struct SagaInstanceMigrate;
impl SagaType for SagaInstanceMigrate {
    type SagaParamsType = Arc<ParamsInstanceMigrate>;
    type ExecContextType = Arc<SagaContext>;
}
impl_authenticated_saga_params!(SagaInstanceMigrate);

pub fn saga_instance_migrate() -> SagaTemplate<SagaInstanceMigrate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "migrate_id",
        "GenerateMigrateId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "dst_propolis_id",
        "GeneratePropolisId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "dst_propolis_ip",
        "AllocatePropolisIp",
        new_action_noop_undo(sim_allocate_propolis_ip),
    );

    template_builder.append(
        "migrate_instance",
        "MigratePrep",
        new_action_noop_undo(sim_migrate_prep),
    );

    template_builder.append(
        "instance_migrate",
        "InstanceMigrate",
        // TODO robustness: This needs an undo action
        new_action_noop_undo(sim_instance_migrate),
    );

    template_builder.append(
        "cleanup_source",
        "CleanupSource",
        // TODO robustness: This needs an undo action. Is it even possible
        // to undo at this point?
        new_action_noop_undo(sim_cleanup_source),
    );

    template_builder.build()
}

async fn sim_migrate_prep(
    sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<(Uuid, InstanceRuntimeState), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let migrate_uuid = sagactx.lookup::<Uuid>("migrate_id")?;
    let dst_propolis_uuid = sagactx.lookup::<Uuid>("dst_propolis_id")?;

    // We have sled-agent (via Nexus) attempt to place
    // the instance in a "Migrating" state w/ the given
    // migration id. This will also update the instance
    // state in the db
    let instance = osagactx
        .nexus()
        .instance_start_migrate(
            &opctx,
            params.instance_id,
            migrate_uuid,
            dst_propolis_uuid,
        )
        .await
        .map_err(ActionError::action_failed)?;
    let instance_id = instance.id();

    Ok((instance_id, instance.runtime_state.into()))
}

// Allocate an IP address on the destination sled for the Propolis server.
async fn sim_allocate_propolis_ip(
    sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<Ipv6Addr, ActionError> {
    allocate_sled_ipv6(sagactx, "dst_sled_uuid").await
}

async fn sim_instance_migrate(
    sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let migration_id = sagactx.lookup::<Uuid>("migrate_id")?;
    let dst_sled_uuid = params.migrate_params.dst_sled_uuid;
    let dst_propolis_uuid = sagactx.lookup::<Uuid>("dst_propolis_id")?;
    let (instance_id, old_runtime) =
        sagactx.lookup::<(Uuid, InstanceRuntimeState)>("migrate_instance")?;

    // Allocate an IP address the destination sled for the new Propolis server.
    let propolis_addr = osagactx
        .datastore()
        .next_ipv6_address(&opctx, dst_sled_uuid)
        .await
        .map_err(ActionError::action_failed)?;

    let runtime = InstanceRuntimeState {
        sled_uuid: dst_sled_uuid,
        propolis_uuid: dst_propolis_uuid,
        propolis_addr: Some(std::net::SocketAddr::new(
            propolis_addr.into(),
            12400,
        )),
        ..old_runtime
    };
    let instance_hardware = InstanceHardware {
        runtime: runtime.into(),
        // TODO: populate NICs
        nics: vec![],
        // TODO: populate disks
        disks: vec![],
    };
    let target = InstanceRuntimeStateRequested {
        run_state: InstanceStateRequested::Migrating,
        migration_params: Some(InstanceRuntimeStateMigrateParams {
            migration_id,
            dst_propolis_id: dst_propolis_uuid,
        }),
    };

    let src_propolis_uuid = old_runtime.propolis_uuid;
    let src_propolis_addr = old_runtime.propolis_addr.ok_or_else(|| {
        ActionError::action_failed(Error::invalid_request(
            "expected source propolis-addr",
        ))
    })?;

    let dst_sa = osagactx
        .sled_client(&dst_sled_uuid)
        .await
        .map_err(ActionError::action_failed)?;

    let new_runtime_state: InstanceRuntimeState = dst_sa
        .instance_put(
            &instance_id,
            &InstanceEnsureBody {
                initial: instance_hardware,
                target,
                migrate: Some(InstanceMigrateParams {
                    src_propolis_addr: src_propolis_addr.to_string(),
                    src_propolis_uuid,
                }),
            },
        )
        .await
        .map_err(omicron_common::api::external::Error::from)
        .map_err(ActionError::action_failed)?
        .into_inner()
        .into();

    osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime_state.into())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sim_cleanup_source(
    _sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<(), ActionError> {
    // TODO: clean up the previous instance whether it's on the same sled or a different one
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ParamsDiskCreate {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub create_params: params::DiskCreate,
}

#[derive(Debug)]
pub struct SagaDiskCreate;
impl SagaType for SagaDiskCreate {
    type SagaParamsType = Arc<ParamsDiskCreate>;
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

    let disk = db::model::Disk::new(
        disk_id,
        params.project_id,
        volume_id,
        params.create_params.clone(),
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
    let datasets_and_regions = osagactx
        .datastore()
        .region_allocate(volume_id, &params.create_params)
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
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let read_only_parent: Option<
        Box<sled_agent_client::types::VolumeConstructionRequest>,
    > = if let Some(image_id) = params.create_params.image_id {
        let log = osagactx.log();
        warn!(log, "grabbing image {}", image_id);

        // TODO: support project images too
        let (.., global_image) = LookupPath::new(&opctx, &osagactx.datastore())
            .global_image_id(image_id)
            .fetch()
            .await
            .map_err(ActionError::action_failed)?;

        debug!(log, "retrieved global image {}", global_image.id());

        // If the block size of the requested disk doesn't match the image,
        // return an error
        if global_image.block_size.to_bytes() as u64
            != params.create_params.block_size().to_bytes()
        {
            return Err(ActionError::action_failed(Error::invalid_request(
                &format!(
                    "image block size {} does not match disk block size {}",
                    global_image.block_size.to_bytes(),
                    params.create_params.block_size().to_bytes()
                ),
            )));
        }

        // If the size of the image is greater than the size of the disk,
        // return an error.
        if global_image.size.to_bytes() > params.create_params.size.to_bytes() {
            return Err(ActionError::action_failed(Error::invalid_request(
                &format!(
                    "image size {} is greater than disk size {}",
                    global_image.size.to_bytes(),
                    params.create_params.size.to_bytes()
                ),
            )));
        }

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
            .map_err(|e| {
                ActionError::action_failed(Error::internal_error(
                    &e.to_string(),
                ))
            })?;

        debug!(
            log,
            "grabbed volume {}, with data {}",
            volume.id(),
            volume.data()
        );

        Some(Box::new(serde_json::from_str(volume.data()).map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?))
    } else {
        None
    };

    // Store volume details in db
    let mut rng = StdRng::from_entropy();
    let volume_construction_request =
        sled_agent_client::types::VolumeConstructionRequest::Volume {
            block_size,
            sub_volumes: vec![
                sled_agent_client::types::VolumeConstructionRequest::Region {
                    block_size,
                    // gen of 0 is here, these regions were just allocated.
                    gen: 0,
                    opts: sled_agent_client::types::CrucibleOpts {
                        target: datasets_and_regions
                            .iter()
                            .map(|(dataset, region)| {
                                dataset
                                    .address_with_port(region.port_number)
                                    .to_string()
                            })
                            .collect(),

                        lossy: false,

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

async fn delete_regions(
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

#[derive(Debug, Deserialize, Serialize)]
pub struct ParamsDiskDelete {
    pub disk_id: Uuid,
}

#[derive(Debug)]
pub struct SagaDiskDelete;
impl SagaType for SagaDiskDelete {
    type SagaParamsType = Arc<ParamsDiskDelete>;
    type ExecContextType = Arc<SagaContext>;
}

fn saga_disk_delete() -> SagaTemplate<SagaDiskDelete> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "volume_id",
        "DeleteDiskRecord",
        // TODO: See the comment on the "DeleteRegions" step,
        // we may want to un-delete the disk if we cannot remove
        // underlying regions.
        new_action_noop_undo(sdd_delete_disk_record),
    );

    template_builder.append(
        "no_result",
        "DeleteRegions",
        // TODO(https://github.com/oxidecomputer/omicron/issues/612):
        // We need a way to deal with this operation failing, aside from
        // propagating the error to the user.
        //
        // What if the Sled goes offline? Nexus must ultimately be
        // responsible for reconciling this scenario.
        //
        // The current behavior causes the disk deletion saga to
        // fail, but still marks the disk as destroyed.
        new_action_noop_undo(sdd_delete_regions),
    );

    template_builder.append(
        "no_result",
        "DeleteRegionRecords",
        new_action_noop_undo(sdd_delete_region_records),
    );

    template_builder.append(
        "no_result",
        "DeleteVolumeRecord",
        new_action_noop_undo(sdd_delete_volume_record),
    );

    template_builder.build()
}

async fn sdd_delete_disk_record(
    sagactx: ActionContext<SagaDiskDelete>,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

    let volume_id = osagactx
        .datastore()
        .project_delete_disk_no_auth(&params.disk_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(volume_id)
}

async fn sdd_delete_regions(
    sagactx: ActionContext<SagaDiskDelete>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    delete_regions(datasets_and_regions)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdd_delete_region_records(
    sagactx: ActionContext<SagaDiskDelete>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx
        .datastore()
        .regions_hard_delete(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdd_delete_volume_record(
    sagactx: ActionContext<SagaDiskDelete>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx
        .datastore()
        .volume_delete(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
