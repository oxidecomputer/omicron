// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    impl_authenticated_saga_params, saga_generate_uuid, AuthenticatedSagaParams,
};
use crate::app::{
    MAX_DISKS_PER_INSTANCE, MAX_EXTERNAL_IPS_PER_INSTANCE,
    MAX_NICS_PER_INSTANCE,
};
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
use std::convert::TryFrom;
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

    // Similar to disks (see block comment below), create a sequence of
    // action-undo pairs for each requested network interface, to make sure we
    // always unwind after failures.
    for i in 0..MAX_NICS_PER_INSTANCE {
        template_builder.append(
            format!("network_interface_id{i}").as_str(),
            "CreateNetworkInterfaceId",
            new_action_noop_undo(saga_generate_uuid),
        );

        template_builder.append(
            format!("network_interface{i}").as_str(),
            "CreateNetworkInterface",
            ActionFunc::new_action(
                async move |sagactx| {
                    sic_create_network_interface(sagactx, i).await
                },
                async move |sagactx| {
                    sic_create_network_interface_undo(sagactx, i).await
                },
            ),
        );
    }

    // Allocate an external IP address for the default outbound connectivity
    template_builder.append(
        "snat_ip_id",
        "CreateSnatIpId",
        new_action_noop_undo(saga_generate_uuid),
    );
    template_builder.append(
        "snat_ip",
        "CreateSnatIp",
        ActionFunc::new_action(
            sic_allocate_instance_snat_ip,
            sic_allocate_instance_snat_ip_undo,
        ),
    );

    // Similar to disks (see block comment below), create a sequence of
    // action-undo pairs for each requested external IP, to make sure we always
    // unwind after failures.
    for i in 0..MAX_EXTERNAL_IPS_PER_INSTANCE {
        template_builder.append(
            format!("external_ip_id{i}").as_str(),
            "CreateExternalIpId",
            new_action_noop_undo(saga_generate_uuid),
        );

        template_builder.append(
            format!("external_ip{i}").as_str(),
            "CreateExternalIp",
            ActionFunc::new_action(
                async move |sagactx| {
                    sic_allocate_instance_external_ip(sagactx, i).await
                },
                async move |sagactx| {
                    sic_allocate_instance_external_ip_undo(sagactx, i).await
                },
            ),
        );
    }

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

/// Create a network interface for an instance, using the parameters at index
/// `nic_index`, returning the UUID for the NIC (or None).
async fn sic_create_network_interface(
    sagactx: ActionContext<SagaInstanceCreate>,
    nic_index: usize,
) -> Result<(), ActionError> {
    let saga_params = sagactx.saga_params();
    let interface_params = &saga_params.create_params.network_interfaces;
    match interface_params {
        params::InstanceNetworkInterfaceAttachment::None => Ok(()),
        params::InstanceNetworkInterfaceAttachment::Default => {
            sic_create_default_primary_network_interface(&sagactx, nic_index)
                .await
        }
        params::InstanceNetworkInterfaceAttachment::Create(
            ref create_params,
        ) => match create_params.get(nic_index) {
            None => Ok(()),
            Some(ref prs) => {
                sic_create_custom_network_interface(&sagactx, nic_index, prs)
                    .await
            }
        },
    }
}

/// Delete one network interface, by index.
async fn sic_create_network_interface_undo(
    sagactx: ActionContext<SagaInstanceCreate>,
    nic_index: usize,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let interface_id = sagactx.lookup::<Option<Uuid>>(
        format!("network_interface_id{nic_index}").as_str(),
    )?;
    match interface_id {
        None => Ok(()),
        Some(id) => {
            let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
            let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
                .instance_id(instance_id)
                .lookup_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;
            match LookupPath::new(&opctx, &datastore)
                .network_interface_id(id)
                .lookup_for(authz::Action::Delete)
                .await
            {
                Ok((.., authz_interface)) => {
                    datastore
                        .instance_delete_network_interface(
                            &opctx,
                            &authz_instance,
                            &authz_interface,
                        )
                        .await
                        .map_err(|e| e.into_external())?;
                    Ok(())
                }
                Err(Error::ObjectNotFound { .. }) => {
                    // The saga is attempting to delete the NIC by the ID cached
                    // in the saga log. If we're running this, the NIC already
                    // appears to be gone, which is odd, but not exactly an
                    // error. Swallowing the error allows the saga to continue,
                    // but this is another place we might want to consider
                    // bumping a counter or otherwise tracking things.
                    warn!(
                        osagactx.log(),
                        "During saga unwind, NIC already appears deleted";
                        "interface_id" => %id,
                    );
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        }
    }
}

/// Create one custom (non-default) network interfaces for the provided instance.
async fn sic_create_custom_network_interface(
    sagactx: &ActionContext<SagaInstanceCreate>,
    nic_index: usize,
    interface_params: &params::NetworkInterfaceCreate,
) -> Result<(), ActionError> {
    let interface_id = match sagactx.lookup::<Option<Uuid>>(
        format!("network_interface_id{nic_index}").as_str(),
    )? {
        None => return Ok(()),
        Some(id) => id,
    };

    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;

    // Lookup authz objects, used in the call to create the NIC itself.
    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;
    let (.., authz_vpc) = LookupPath::new(&opctx, &datastore)
        .project_id(saga_params.project_id)
        .vpc_name(&db::model::Name::from(interface_params.vpc_name.clone()))
        .lookup_for(authz::Action::Read)
        .await
        .map_err(ActionError::action_failed)?;

    // TODO-correctness: It seems racy to fetch the subnet and create the
    // interface in separate requests, but outside of a transaction. This
    // should probably either be in a transaction, or the
    // `instance_create_network_interface` function/query needs some JOIN
    // on the `vpc_subnet` table.
    let (.., authz_subnet, db_subnet) = LookupPath::new(&opctx, &datastore)
        .vpc_id(authz_vpc.id())
        .vpc_subnet_name(&db::model::Name::from(
            interface_params.subnet_name.clone(),
        ))
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;
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
        .map_err(|e| ActionError::action_failed(e.into_external()))?;
    Ok(())
}

/// Create a default primary network interface for an instance during the create
/// saga.
async fn sic_create_default_primary_network_interface(
    sagactx: &ActionContext<SagaInstanceCreate>,
    nic_index: usize,
) -> Result<(), ActionError> {
    // We're statically creating up to MAX_NICS_PER_INSTANCE saga nodes, but
    // this method only applies to the case where there's exactly one parameter
    // of type `InstanceNetworkInterfaceAttachment::Default`, so ignore any
    // later calls.
    if nic_index > 0 {
        return Ok(());
    }
    let interface_id =
        match sagactx.lookup::<Option<Uuid>>("network_interface_id0")? {
            None => return Ok(()),
            Some(id) => id,
        };

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

/// Create an external IP address for instance source NAT.
async fn sic_allocate_instance_snat_ip(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let ip_id = sagactx.lookup::<Uuid>("snat_ip_id")?;
    datastore
        .allocate_instance_snat_ip(
            &opctx,
            ip_id,
            saga_params.project_id,
            instance_id,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

/// Destroy an allocated SNAT IP address for the instance.
async fn sic_allocate_instance_snat_ip_undo(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let ip_id = sagactx.lookup::<Uuid>("snat_ip_id")?;
    datastore
        .deallocate_instance_external_ip(&opctx, ip_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

/// Create an external IPs for the instance, using the request parameters at
/// index `ip_index`, and return its ID if one is created (or None).
async fn sic_allocate_instance_external_ip(
    sagactx: ActionContext<SagaInstanceCreate>,
    ip_index: usize,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    let ip_params = saga_params.create_params.external_ips.get(ip_index);
    let ip_params = match ip_params {
        None => {
            return Ok(());
        }
        Some(ref prs) => prs,
    };
    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let name = format!("external_ip_id{ip_index}");
    let ip_id = sagactx.lookup::<Option<Uuid>>(&name)?.ok_or_else(|| {
        ActionError::action_failed(Error::internal_error(
            "Expected a UUID for instance external IP",
        ))
    })?;

    // Collect the possible pool name for this IP address
    let pool_name = match ip_params {
        params::ExternalIpCreate::Ephemeral { ref pool_name } => {
            pool_name.as_ref().map(|name| db::model::Name(name.clone()))
        }
    };
    datastore
        .allocate_instance_ephemeral_ip(
            &opctx,
            ip_id,
            saga_params.project_id,
            instance_id,
            pool_name,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sic_allocate_instance_external_ip_undo(
    sagactx: ActionContext<SagaInstanceCreate>,
    ip_index: usize,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params();
    if ip_index >= saga_params.create_params.external_ips.len() {
        return Ok(());
    }

    let opctx =
        OpContext::for_saga_action(&sagactx, &saga_params.serialized_authn);
    let name = format!("external_ip_id{ip_index}");
    let ip_id =
        sagactx.lookup::<Option<Uuid>>(name.as_str())?.ok_or_else(|| {
            ActionError::action_failed(Error::internal_error(
                "Expected a UUID for instance external IP",
            ))
        })?;
    datastore
        .deallocate_instance_external_ip(&opctx, ip_id)
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
