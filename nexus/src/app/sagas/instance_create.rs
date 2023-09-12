// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NexusSaga, SagaInitError, ACTION_GENERATE_ID};
use crate::app::instance::WriteBackUpdatedInstance;
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::disk_create::{self, SagaDiskCreate};
use crate::app::sagas::retry_until_known_result;
use crate::app::{
    MAX_DISKS_PER_INSTANCE, MAX_EXTERNAL_IPS_PER_INSTANCE,
    MAX_NICS_PER_INSTANCE,
};
use crate::external_api::params;
use chrono::Utc;
use nexus_db_model::NetworkInterfaceKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::ByteCount as DbByteCount;
use nexus_db_queries::db::queries::network_interface::InsertError as InsertNicError;
use nexus_db_queries::{authn, authz, db};
use nexus_defaults::DEFAULT_PRIMARY_NIC_NAME;
use nexus_types::external_api::params::InstanceDiskAttachment;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::shared::SwitchLocation;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::InstanceStateRequested;
use slog::warn;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::net::Ipv6Addr;
use steno::ActionError;
use steno::Node;
use steno::{DagBuilder, SagaName};
use uuid::Uuid;

// instance create saga: input parameters

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub create_params: params::InstanceCreate,
    pub boundary_switches: HashSet<SwitchLocation>,
}

// Several nodes in this saga are wrapped in their own subsaga so that they can
// have a parameter that denotes which node they are (e.g., which NIC or which
// external IP).  They also need the outer saga's parameters.
#[derive(Debug, Deserialize, Serialize)]
struct NetParams {
    saga_params: Params,
    which: usize,
    instance_id: Uuid,
    new_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
struct NetworkConfigParams {
    saga_params: Params,
    instance_id: Uuid,
    which: usize,
    switch_location: SwitchLocation,
}

#[derive(Debug, Deserialize, Serialize)]
struct DiskAttachParams {
    serialized_authn: authn::saga::Serialized,
    project_id: Uuid,
    instance_id: Uuid,
    attach_params: InstanceDiskAttachment,
}

// instance create saga: actions

declare_saga_actions! {
    instance_create;
    ALLOC_SERVER -> "server_id" {
        + sic_alloc_server
        - sic_alloc_server_undo
    }
    VIRTUAL_RESOURCES_ACCOUNT -> "no_result" {
        + sic_account_virtual_resources
        - sic_account_virtual_resources_undo
    }
    ALLOC_PROPOLIS_IP -> "propolis_ip" {
        + sic_allocate_propolis_ip
    }
    CREATE_INSTANCE_RECORD -> "instance_name" {
        + sic_create_instance_record
        - sic_delete_instance_record
    }
    CREATE_NETWORK_INTERFACE -> "output" {
        + sic_create_network_interface
        - sic_create_network_interface_undo
    }
    CREATE_SNAT_IP -> "snat_ip" {
        + sic_allocate_instance_snat_ip
        - sic_allocate_instance_snat_ip_undo
    }
    CREATE_EXTERNAL_IP -> "output" {
        + sic_allocate_instance_external_ip
        - sic_allocate_instance_external_ip_undo
    }
    ATTACH_DISKS_TO_INSTANCE -> "attach_disk_output" {
        + sic_attach_disk_to_instance
        - sic_attach_disk_to_instance_undo
    }
    CONFIGURE_ASIC -> "configure_asic" {
        + sic_add_network_config
        - sic_remove_network_config
    }
    V2P_ENSURE_UNDO -> "v2p_ensure_undo" {
        + sic_noop
        - sic_v2p_ensure_undo
    }
    V2P_ENSURE -> "v2p_ensure" {
        + sic_v2p_ensure
    }
    INSTANCE_ENSURE_REGISTERED -> "instance_ensure_registered" {
        + sic_instance_ensure_registered
        - sic_instance_ensure_registered_undo
    }
    INSTANCE_ENSURE_RUNNING -> "instance_ensure_running" {
        + sic_instance_ensure_running
    }
}

// instance create saga: definition

#[derive(Debug)]
pub(crate) struct SagaInstanceCreate;
impl NexusSaga for SagaInstanceCreate {
    const NAME: &'static str = "instance-create";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        instance_create_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        // Pre-create the instance ID so that it can be supplied as a constant
        // parameter to the subsagas that create and attach devices.
        let instance_id = Uuid::new_v4();

        builder.append(Node::constant(
            "instance_id",
            serde_json::to_value(&instance_id).map_err(|e| {
                SagaInitError::SerializeError(String::from("instance_id"), e)
            })?,
        ));

        builder.append(Node::action(
            "propolis_id",
            "GeneratePropolisId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(alloc_server_action());
        builder.append(virtual_resources_account_action());
        builder.append(alloc_propolis_ip_action());
        builder.append(create_instance_record_action());

        // Helper function for appending subsagas to our parent saga.
        fn subsaga_append<S: Serialize>(
            node_basename: String,
            subsaga_dag: steno::Dag,
            parent_builder: &mut steno::DagBuilder,
            params: S,
            which: usize,
        ) -> Result<(), SagaInitError> {
            // The "parameter" node is a constant node that goes into the outer
            // saga.  Its value becomes the parameters for the one-node subsaga
            // (defined below) that actually creates each NIC.
            let params_node_name = format!("{}_params{}", node_basename, which);
            parent_builder.append(Node::constant(
                &params_node_name,
                serde_json::to_value(&params).map_err(|e| {
                    SagaInitError::SerializeError(params_node_name.clone(), e)
                })?,
            ));

            let output_name = format!("{}{}", node_basename, which);
            parent_builder.append(Node::subsaga(
                output_name.as_str(),
                subsaga_dag,
                params_node_name,
            ));
            Ok(())
        }

        // We use a similar pattern here for NICs, external IPs and disks.  We
        // want one saga action per item (i.e., per NIC, per external IP, or per
        // disk).  That makes it much easier to make actions and undo actions
        // idempotent.  But the user may ask for a variable number of these
        // items.  Previous versions of Steno required the saga DAG to be fixed
        // for all runs of a saga.  To address this, we put a static limit on
        // the number of NICs, external IPs, or disks that you can request.
        // Here, where we're building the saga DAG, we always add that maximum
        // number of nodes and we just have the extra nodes do nothing.
        //
        // An easy way to pass this kind of information to an action node is to
        // wrap it in a subsaga and put that information into the subsaga
        // parameters.  That's what we do below.  subsaga_append() (defined
        // above) handles much of the details.
        //
        // TODO-cleanup More recent Steno versions support more flexibility
        // here.  Instead of always creating MAX_NICS_PER_INSTANCE and ignoring
        // many of them if we've got a default config or fewer than
        // MAX_NICS_PER_INSTANCE NICs, we could just create the DAG with the
        // right number of the right nodes.  We could also put the correct
        // config into each subsaga's params node so that we don't have to pass
        // the index around.
        for i in 0..MAX_NICS_PER_INSTANCE {
            let repeat_params = NetParams {
                saga_params: params.clone(),
                which: i,
                instance_id,
                new_id: Uuid::new_v4(),
            };
            let subsaga_name =
                SagaName::new(&format!("instance-create-nic{i}"));
            let mut subsaga_builder = DagBuilder::new(subsaga_name);
            subsaga_builder.append(Node::action(
                "output",
                format!("CreateNetworkInterface{i}").as_str(),
                CREATE_NETWORK_INTERFACE.as_ref(),
            ));
            subsaga_append(
                "network_interface".into(),
                subsaga_builder.build()?,
                &mut builder,
                repeat_params,
                i,
            )?;
        }

        // Allocate an external IP address for the default outbound connectivity
        builder.append(Node::action(
            "snat_ip_id",
            "CreateSnatIpId",
            ACTION_GENERATE_ID.as_ref(),
        ));
        builder.append(create_snat_ip_action());

        // See the comment above where we add nodes for creating NICs.  We use
        // the same pattern here.
        for i in 0..MAX_EXTERNAL_IPS_PER_INSTANCE {
            let repeat_params = NetParams {
                saga_params: params.clone(),
                which: i,
                instance_id,
                new_id: Uuid::new_v4(),
            };
            let subsaga_name =
                SagaName::new(&format!("instance-create-external-ip{i}"));
            let mut subsaga_builder = DagBuilder::new(subsaga_name);
            subsaga_builder.append(Node::action(
                "output",
                format!("CreateExternalIp{i}").as_str(),
                CREATE_EXTERNAL_IP.as_ref(),
            ));
            subsaga_append(
                "external_ip".into(),
                subsaga_builder.build()?,
                &mut builder,
                repeat_params,
                i,
            )?;
        }

        // Appends the disk create saga as a subsaga directly to the instance create builder.
        for (i, disk) in params.create_params.disks.iter().enumerate() {
            if let InstanceDiskAttachment::Create(create_disk) = disk {
                let subsaga_name =
                    SagaName::new(&format!("instance-create-disk-{i}"));
                let subsaga_builder = DagBuilder::new(subsaga_name);
                let params = disk_create::Params {
                    serialized_authn: params.serialized_authn.clone(),
                    project_id: params.project_id,
                    create_params: create_disk.clone(),
                };
                subsaga_append(
                    "create_disk".into(),
                    SagaDiskCreate::make_saga_dag(&params, subsaga_builder)?,
                    &mut builder,
                    params,
                    i,
                )?;
            }
        }

        // Attaches all disks included in the instance create request, including those which were previously created
        // by the disk create subsagas.
        for (i, disk_attach) in params.create_params.disks.iter().enumerate() {
            let subsaga_name =
                SagaName::new(&format!("instance-attach-disk-{i}"));
            let mut subsaga_builder = DagBuilder::new(subsaga_name);
            subsaga_builder.append(Node::action(
                "attach_disk_output",
                format!("AttachDisksToInstance-{i}").as_str(),
                ATTACH_DISKS_TO_INSTANCE.as_ref(),
            ));
            let params = DiskAttachParams {
                serialized_authn: params.serialized_authn.clone(),
                project_id: params.project_id,
                instance_id,
                attach_params: disk_attach.clone(),
            };
            subsaga_append(
                "attach_disk".into(),
                subsaga_builder.build()?,
                &mut builder,
                params,
                i,
            )?;
        }

        // If a primary NIC exists, create a NAT entry for the default external IP,
        // as well as additional NAT entries for each requested ephemeral IP
        for i in 0..(params.create_params.external_ips.len() + 1) {
            for &switch_location in &params.boundary_switches {
                let subsaga_name = SagaName::new(&format!(
                    "instance-configure-nat-{i}-{switch_location}"
                ));
                let mut subsaga_builder = DagBuilder::new(subsaga_name);

                let basename = format!("ConfigureAsic-{i}-{switch_location}");
                subsaga_builder.append(Node::action(
                    "configure_asic",
                    &basename,
                    CONFIGURE_ASIC.as_ref(),
                ));
                let net_params = NetworkConfigParams {
                    saga_params: params.clone(),
                    instance_id,
                    which: i,
                    switch_location,
                };
                subsaga_append(
                    basename,
                    subsaga_builder.build()?,
                    &mut builder,
                    net_params,
                    i,
                )?;
            }
        }

        // creating instance v2p mappings is not atomic - there are many calls
        // to different sled agents that occur. for this to unwind correctly
        // given a partial success of the ensure node, the undo node must be
        // prior to the ensure node as a separate action.
        builder.append(v2p_ensure_undo_action());
        builder.append(v2p_ensure_action());

        builder.append(instance_ensure_registered_action());
        if params.create_params.start {
            builder.append(instance_ensure_running_action());
        }
        Ok(builder.build()?)
    }
}

async fn sic_add_network_config(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let net_params = sagactx.saga_params::<NetworkConfigParams>()?;
    let which = net_params.which;
    let instance_id = net_params.instance_id;
    let params = net_params.saga_params;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let switch = net_params.switch_location;
    let dpd_client =
        osagactx.nexus().dpd_clients.get(&switch).ok_or_else(|| {
            ActionError::action_failed(Error::internal_error(&format!(
                "unable to find client for switch {switch}"
            )))
        })?;

    let (.., db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // Read the sled record from the database. This needs to use the instance-
    // create context (and not the regular saga context) to leverage its fleet-
    // read permissions.
    let sled_uuid = db_instance.runtime_state.sled_id;
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, &datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // Set up Dendrite configuration using the saga context, which supplies
    // access to the instance's device configuration.
    osagactx
        .nexus()
        .instance_ensure_dpd_config(
            &opctx,
            instance_id,
            &sled.address(),
            Some(which),
            dpd_client,
        )
        .await
        .map_err(ActionError::action_failed)
}

async fn sic_remove_network_config(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let net_params = sagactx.saga_params::<NetworkConfigParams>()?;
    let which = net_params.which;
    let instance_id = net_params.instance_id;
    let switch = net_params.switch_location;
    let params = net_params.saga_params;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let osagactx = sagactx.user_data();
    let dpd_client =
        osagactx.nexus().dpd_clients.get(&switch).ok_or_else(|| {
            Error::internal_error(&format!(
                "unable to find client for switch {switch}"
            ))
        })?;
    let datastore = &osagactx.datastore();
    let log = sagactx.user_data().log();

    debug!(log, "fetching external ip addresses");

    let target_ip = &datastore
        .instance_lookup_external_ips(&opctx, instance_id)
        .await
        .map_err(ActionError::action_failed)?
        .get(which)
        .ok_or_else(|| {
            ActionError::action_failed(Error::internal_error(&format!(
                "failed to find external ip address at index: {which}"
            )))
        })?
        .to_owned();

    debug!(log, "deleting nat mapping for entry: {target_ip:#?}");

    let result = retry_until_known_result(log, || async {
        dpd_client
            .ensure_nat_entry_deleted(log, target_ip.ip, *target_ip.first_port)
            .await
    })
    .await;

    match result {
        Ok(_) => {
            debug!(log, "deletion of nat entry successful for: {target_ip:#?}");
            Ok(())
        }
        Err(e) => Err(Error::internal_error(&format!(
            "failed to delete nat entry via dpd: {e}"
        ))),
    }?;

    Ok(())
}

async fn sic_alloc_server(
    sagactx: NexusActionContext,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();

    // ALLOCATION POLICY
    //
    // NOTE: This policy can - and should! - be changed.
    //
    // See https://rfd.shared.oxide.computer/rfd/0205 for a more complete
    // discussion.
    //
    // Right now, allocate an instance to any random sled agent. This has a few
    // problems:
    //
    // - There's no consideration for "health of the sled" here, other than
    //   "time_deleted = Null". If the sled is rebooting, in a known unhealthy
    //   state, etc, we'd currently provision it here. I don't think this is a
    //   trivial fix, but it's work we'll need to account for eventually.
    //
    // - This is selecting a random sled from all sleds in the cluster. For
    //   multi-rack, this is going to fling the sled to an arbitrary system.
    //   Maybe that's okay, but worth knowing about explicitly.
    //
    // - This doesn't take into account anti-affinity - users will want to
    //   schedule instances that belong to a cluster on different failure
    //   domains. See https://github.com/oxidecomputer/omicron/issues/1705.

    // TODO: Fix these values. They're wrong now, but they let us move
    // forward with plumbing.
    let params = sagactx.saga_params::<Params>()?;
    let hardware_threads = params.create_params.ncpus.0;
    let rss_ram = params.create_params.memory;
    let reservoir_ram = omicron_common::api::external::ByteCount::from(0);

    // Use the instance's Propolis ID as its resource key, since each unique
    // Propolis consumes its own resources, and an instance can have multiple
    // Propolises during a live migration.
    let propolis_id = sagactx.lookup::<Uuid>("propolis_id")?;
    let resources = db::model::Resources::new(
        hardware_threads.into(),
        rss_ram.into(),
        reservoir_ram.into(),
    );

    let resource = osagactx
        .nexus()
        .reserve_on_random_sled(
            propolis_id,
            db::model::SledResourceKind::Instance,
            resources,
            db::model::SledReservationConstraints::none(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(resource.sled_id)
}

async fn sic_alloc_server_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let propolis_id = sagactx.lookup::<Uuid>("propolis_id")?;

    osagactx.nexus().delete_sled_reservation(propolis_id).await?;
    Ok(())
}

/// Create a network interface for an instance, using the parameters at index
/// `nic_index`, returning the UUID for the NIC (or None).
async fn sic_create_network_interface(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let repeat_saga_params = sagactx.saga_params::<NetParams>()?;
    let saga_params = repeat_saga_params.saga_params;
    let nic_index = repeat_saga_params.which;
    let instance_id = repeat_saga_params.instance_id;
    let interface_id = repeat_saga_params.new_id;
    let interface_params = &saga_params.create_params.network_interfaces;
    match interface_params {
        params::InstanceNetworkInterfaceAttachment::None => Ok(()),
        params::InstanceNetworkInterfaceAttachment::Default => {
            create_default_primary_network_interface(
                &sagactx,
                &saga_params,
                nic_index,
                instance_id,
                interface_id,
            )
            .await
        }
        params::InstanceNetworkInterfaceAttachment::Create(
            ref create_params,
        ) => match create_params.get(nic_index) {
            None => Ok(()),
            Some(ref prs) => {
                create_custom_network_interface(
                    &sagactx,
                    &saga_params,
                    instance_id,
                    interface_id,
                    prs,
                )
                .await
            }
        },
    }
}

/// Delete one network interface, by interface id.
async fn sic_create_network_interface_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let repeat_saga_params = sagactx.saga_params::<NetParams>()?;
    let instance_id = repeat_saga_params.instance_id;
    let saga_params = repeat_saga_params.saga_params;
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &saga_params.serialized_authn,
    );
    let interface_id = repeat_saga_params.new_id;
    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;
    match LookupPath::new(&opctx, &datastore)
        .instance_network_interface_id(interface_id)
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
                "interface_id" => %interface_id,
            );
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// Create one custom (non-default) network interface for the provided instance.
async fn create_custom_network_interface(
    sagactx: &NexusActionContext,
    saga_params: &Params,
    instance_id: Uuid,
    interface_id: Uuid,
    interface_params: &params::InstanceNetworkInterfaceCreate,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &saga_params.serialized_authn,
    );

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
    let interface = db::model::IncompleteNetworkInterface::new_instance(
        interface_id,
        instance_id,
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
        .map(|_| ())
        .or_else(|err| {
            match err {
                // Necessary for idempotency
                InsertNicError::InterfaceAlreadyExists(
                    _,
                    NetworkInterfaceKind::Instance,
                ) => Ok(()),
                _ => Err(err),
            }
        })
        .map_err(InsertNicError::into_external)
        .map_err(ActionError::action_failed)?;
    Ok(())
}

/// Create a default primary network interface for an instance during the create
/// saga.
async fn create_default_primary_network_interface(
    sagactx: &NexusActionContext,
    saga_params: &Params,
    nic_index: usize,
    instance_id: Uuid,
    interface_id: Uuid,
) -> Result<(), ActionError> {
    // We're statically creating up to MAX_NICS_PER_INSTANCE saga nodes, but
    // this method only applies to the case where there's exactly one parameter
    // of type `InstanceNetworkInterfaceAttachment::Default`, so ignore any
    // later calls.
    if nic_index > 0 {
        return Ok(());
    }

    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &saga_params.serialized_authn,
    );

    // The literal name "default" is currently used for the VPC and VPC Subnet,
    // when not specified in the client request.
    // TODO-completeness: We'd like to select these from Project-level defaults.
    // See https://github.com/oxidecomputer/omicron/issues/1015.
    let default_name = Name::try_from("default".to_string()).unwrap();
    let internal_default_name = db::model::Name::from(default_name.clone());

    // The name of the default primary interface.
    let iface_name =
        Name::try_from(DEFAULT_PRIMARY_NIC_NAME.to_string()).unwrap();

    let interface_params = params::InstanceNetworkInterfaceCreate {
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
    let (.., authz_subnet, db_subnet) = LookupPath::new(&opctx, &datastore)
        .project_id(saga_params.project_id)
        .vpc_name(&internal_default_name)
        .vpc_subnet_name(&internal_default_name)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let interface = db::model::IncompleteNetworkInterface::new_instance(
        interface_id,
        instance_id,
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
        .map(|_| ())
        .or_else(|err| {
            match err {
                // Necessary for idempotency
                InsertNicError::InterfaceAlreadyExists(
                    _,
                    NetworkInterfaceKind::Instance,
                ) => Ok(()),
                _ => Err(err),
            }
        })
        .map_err(InsertNicError::into_external)
        .map_err(ActionError::action_failed)?;
    Ok(())
}

/// Create an external IP address for instance source NAT.
async fn sic_allocate_instance_snat_ip(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &saga_params.serialized_authn,
    );
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let ip_id = sagactx.lookup::<Uuid>("snat_ip_id")?;

    let pool = datastore
        .ip_pools_fetch_default(&opctx)
        .await
        .map_err(ActionError::action_failed)?;
    let pool_id = pool.identity.id;

    datastore
        .allocate_instance_snat_ip(&opctx, ip_id, instance_id, pool_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

/// Destroy an allocated SNAT IP address for the instance.
async fn sic_allocate_instance_snat_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let saga_params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &saga_params.serialized_authn,
    );
    let ip_id = sagactx.lookup::<Uuid>("snat_ip_id")?;
    datastore.deallocate_external_ip(&opctx, ip_id).await?;
    Ok(())
}

/// Create an external IPs for the instance, using the request parameters at
/// index `ip_index`, and return its ID if one is created (or None).
async fn sic_allocate_instance_external_ip(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let repeat_saga_params = sagactx.saga_params::<NetParams>()?;
    let saga_params = repeat_saga_params.saga_params;
    let ip_index = repeat_saga_params.which;
    let ip_params = saga_params.create_params.external_ips.get(ip_index);
    let ip_params = match ip_params {
        None => {
            return Ok(());
        }
        Some(ref prs) => prs,
    };
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &saga_params.serialized_authn,
    );
    let instance_id = repeat_saga_params.instance_id;
    let ip_id = repeat_saga_params.new_id;

    // Collect the possible pool name for this IP address
    let pool_name = match ip_params {
        params::ExternalIpCreate::Ephemeral { ref pool_name } => {
            pool_name.as_ref().map(|name| db::model::Name(name.clone()))
        }
    };
    datastore
        .allocate_instance_ephemeral_ip(&opctx, ip_id, instance_id, pool_name)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sic_allocate_instance_external_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let repeat_saga_params = sagactx.saga_params::<NetParams>()?;
    let saga_params = repeat_saga_params.saga_params;
    let ip_index = repeat_saga_params.which;
    if ip_index >= saga_params.create_params.external_ips.len() {
        return Ok(());
    }

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &saga_params.serialized_authn,
    );
    let ip_id = repeat_saga_params.new_id;
    datastore.deallocate_external_ip(&opctx, ip_id).await?;
    Ok(())
}

async fn sic_attach_disk_to_instance(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    ensure_instance_disk_attach_state(sagactx, true).await
}

async fn sic_attach_disk_to_instance_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    Ok(ensure_instance_disk_attach_state(sagactx, false).await?)
}

async fn ensure_instance_disk_attach_state(
    sagactx: NexusActionContext,
    attached: bool,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<DiskAttachParams>()?;
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let instance_id = params.instance_id;
    let project_id = params.project_id;

    let disk_name = match params.attach_params {
        InstanceDiskAttachment::Create(create_params) => {
            db::model::Name(create_params.identity.name)
        }
        InstanceDiskAttachment::Attach(attach_params) => {
            db::model::Name(attach_params.name)
        }
    };

    let (.., authz_instance, _db_instance) =
        LookupPath::new(&opctx, &datastore)
            .instance_id(instance_id)
            .fetch()
            .await
            .map_err(ActionError::action_failed)?;

    // TODO-correctness TODO-security It's not correct to re-resolve the
    // disk name now.  See oxidecomputer/omicron#1536.
    let (.., authz_disk, _db_disk) = LookupPath::new(&opctx, &datastore)
        .project_id(project_id)
        .disk_name(&disk_name)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    if attached {
        datastore
            .instance_attach_disk(
                &opctx,
                &authz_instance,
                &authz_disk,
                MAX_DISKS_PER_INSTANCE,
            )
            .await
            .map_err(ActionError::action_failed)?;
    } else {
        datastore
            .instance_detach_disk(&opctx, &authz_instance, &authz_disk)
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}

/// Helper function to allocate a new IPv6 address for an Oxide service running
/// on the provided sled.
///
/// `sled_id_name` is the name of the serialized output containing the UUID for
/// the targeted sled.
pub(super) async fn allocate_sled_ipv6(
    opctx: &OpContext,
    sagactx: NexusActionContext,
    sled_uuid: Uuid,
) -> Result<Ipv6Addr, ActionError> {
    let osagactx = sagactx.user_data();
    osagactx
        .datastore()
        .next_ipv6_address(opctx, sled_uuid)
        .await
        .map_err(ActionError::action_failed)
}

async fn sic_account_virtual_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_instance(
            &opctx,
            instance_id,
            params.project_id,
            i64::from(params.create_params.ncpus.0),
            DbByteCount(params.create_params.memory),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sic_account_virtual_resources_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance_id,
            params.project_id,
            i64::from(params.create_params.ncpus.0),
            DbByteCount(params.create_params.memory),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

// Allocate an IP address on the destination sled for the Propolis server
async fn sic_allocate_propolis_ip(
    sagactx: NexusActionContext,
) -> Result<Ipv6Addr, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let sled_uuid = sagactx.lookup::<Uuid>("server_id")?;
    allocate_sled_ipv6(&opctx, sagactx, sled_uuid).await
}

async fn sic_create_instance_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Name, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
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
        propolis_gen: Generation::new(),
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

    let (.., authz_project) = LookupPath::new(&opctx, &osagactx.datastore())
        .project_id(params.project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;

    let instance = osagactx
        .datastore()
        .project_create_instance(&opctx, &authz_project, new_instance)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(instance.name().clone().into())
}

async fn sic_delete_instance_record(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let instance_name = sagactx.lookup::<db::model::Name>("instance_name")?;

    // We currently only support deleting an instance if it is stopped or
    // failed, so update the state accordingly to allow deletion.
    // TODO-correctness TODO-security It's not correct to re-resolve the
    // instance name now.  See oxidecomputer/omicron#1536.
    let result = LookupPath::new(&opctx, &datastore)
        .project_id(params.project_id)
        .instance_name(&instance_name)
        .fetch()
        .await;

    // Although, as mentioned in the comment above, we should not be doing the
    // lookup by name here, we do want this operation to be idempotent.
    //
    // As such, if the instance has already been deleted, we should return with
    // a no-op.
    let (authz_instance, db_instance) = match result {
        Ok((.., authz_instance, db_instance)) => (authz_instance, db_instance),
        Err(err) => match err {
            Error::ObjectNotFound { .. } => return Ok(()),
            _ => return Err(err.into()),
        },
    };

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

    let updated =
        datastore.instance_update_runtime(&instance_id, &runtime_state).await?;

    if !updated {
        warn!(
            osagactx.log(),
            "failed to update instance runtime state from creating to failed",
        );
    }

    // Actually delete the record.
    datastore.project_delete_instance(&opctx, &authz_instance).await?;

    Ok(())
}

async fn sic_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
    Ok(())
}

/// Ensure that the necessary v2p mappings exist for this instance
async fn sic_v2p_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let sled_id = sagactx.lookup::<Uuid>("server_id")?;

    osagactx
        .nexus()
        .create_instance_v2p_mappings(&opctx, instance_id, sled_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sic_v2p_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;

    osagactx
        .nexus()
        .delete_instance_v2p_mappings(&opctx, instance_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sic_instance_ensure_registered(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();

    // TODO-correctness TODO-security It's not correct to re-resolve the
    // instance name now.  See oxidecomputer/omicron#1536.
    let instance_name = sagactx.lookup::<db::model::Name>("instance_name")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .project_id(params.project_id)
        .instance_name(&instance_name)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    if !params.create_params.start {
        let instance_id = db_instance.id();
        // If we don't need to start the instance, we can skip the ensure
        // and just update the instance runtime state to `Stopped`.
        //
        // TODO-correctness: This is dangerous if this step is replayed, since
        // a user can discover this instance and ask to start it in between
        // attempts to run this step. One way to fix this is to avoid refetching
        // the previous runtime state each time this step is taken, such that
        // once this update is applied once, subsequent attempts to apply it
        // will have an already-used generation number.
        let runtime_state = db::model::InstanceRuntimeState {
            state: db::model::InstanceState::new(InstanceState::Stopped),
            // Must update the generation, or the database query will fail.
            //
            // The runtime state of the instance record is only changed as a
            // result of the successful completion of the saga (i.e. after
            // ensure which we're skipping in this case) or during saga
            // unwinding. So we're guaranteed that the cached generation in the
            // saga log is the most recent in the database.
            gen: db::model::Generation::from(
                db_instance.runtime_state.gen.next(),
            ),
            ..db_instance.runtime_state
        };

        let updated = datastore
            .instance_update_runtime(&instance_id, &runtime_state)
            .await
            .map_err(ActionError::action_failed)?;

        if !updated {
            warn!(
                osagactx.log(),
                "failed to update instance runtime state from creating to stopped",
            );
        }
    } else {
        osagactx
            .nexus()
            .instance_ensure_registered(&opctx, &authz_instance, &db_instance)
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}

async fn sic_instance_ensure_registered_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_unregistered(
            &opctx,
            &authz_instance,
            &db_instance,
            WriteBackUpdatedInstance::WriteBack,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sic_instance_ensure_running(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_request_state(
            &opctx,
            &authz_instance,
            &db_instance,
            InstanceStateRequested::Running,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
pub mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::instance_create::Params,
        app::sagas::instance_create::SagaInstanceCreate,
        app::sagas::test_helpers, external_api::params,
    };
    use async_bb8_diesel::{
        AsyncConnection, AsyncRunQueryDsl, AsyncSimpleConnection,
        OptionalExtension,
    };
    use diesel::{
        BoolExpressionMethods, ExpressionMethods, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::DataStore;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::populate_ip_pool;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };
    use omicron_common::api::internal::shared::SwitchLocation;
    use omicron_sled_agent::sim::SledAgent;
    use std::collections::HashSet;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const INSTANCE_NAME: &str = "my-instance";
    const PROJECT_NAME: &str = "springfield-squidport";
    const DISK_NAME: &str = "my-disk";

    async fn create_org_project_and_disk(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
        let project = create_project(client, PROJECT_NAME).await;
        create_disk(&client, PROJECT_NAME, DISK_NAME).await;
        project.identity.id
    }

    // Helper for creating instance create parameters
    fn new_test_params(opctx: &OpContext, project_id: Uuid) -> Params {
        Params {
            serialized_authn: Serialized::for_opctx(opctx),
            project_id,
            create_params: params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: INSTANCE_NAME.parse().unwrap(),
                    description: "My instance".to_string(),
                },
                ncpus: InstanceCpuCount::try_from(2).unwrap(),
                memory: ByteCount::from_gibibytes_u32(4),
                hostname: String::from("inst"),
                user_data: vec![],
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::Default,
                external_ips: vec![params::ExternalIpCreate::Ephemeral {
                    pool_name: None,
                }],
                disks: vec![params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: DISK_NAME.parse().unwrap(),
                    },
                )],
                start: false,
            },
            boundary_switches: HashSet::from([SwitchLocation::Switch0]),
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_helpers::test_opctx(&cptestctx);
        let params = new_test_params(&opctx, project_id);
        let dag = create_saga_dag::<SagaInstanceCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        nexus
            .run_saga(runnable_saga)
            .await
            .expect("Saga should have succeeded");
    }

    async fn no_instance_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Instance;
        use nexus_db_queries::db::schema::instance::dsl;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .select(Instance::as_select())
            .first_async::<Instance>(datastore.pool_for_tests().await.unwrap())
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_network_interface_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::NetworkInterface;
        use nexus_db_queries::db::model::NetworkInterfaceKind;
        use nexus_db_queries::db::schema::network_interface::dsl;

        dsl::network_interface
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::kind.eq(NetworkInterfaceKind::Instance))
            .select(NetworkInterface::as_select())
            .first_async::<NetworkInterface>(
                datastore.pool_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_external_ip_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::ExternalIp;
        use nexus_db_queries::db::schema::external_ip::dsl;

        dsl::external_ip
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::is_service.eq(false))
            .select(ExternalIp::as_select())
            .first_async::<ExternalIp>(
                datastore.pool_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    async fn no_sled_resource_instance_records_exist(
        datastore: &DataStore,
    ) -> bool {
        use nexus_db_queries::db::model::SledResource;
        use nexus_db_queries::db::schema::sled_resource::dsl;

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

                Ok::<_, nexus_db_queries::db::TransactionError<()>>(
                    dsl::sled_resource
                        .filter(
                            dsl::kind.eq(
                                nexus_db_queries::db::model::SledResourceKind::Instance,
                            ),
                        )
                        .select(SledResource::as_select())
                        .get_results_async::<SledResource>(&conn)
                        .await
                        .unwrap()
                        .is_empty(),
                )
            })
            .await
            .unwrap()
    }

    async fn no_virtual_provisioning_resource_records_exist(
        datastore: &DataStore,
    ) -> bool {
        use nexus_db_queries::db::model::VirtualProvisioningResource;
        use nexus_db_queries::db::schema::virtual_provisioning_resource::dsl;

        datastore.pool_for_tests()
            .await
            .unwrap()
            .transaction_async(|conn| async move {
                conn
                    .batch_execute_async(nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL)
                    .await
                    .unwrap();

                Ok::<_, nexus_db_queries::db::TransactionError<()>>(
                    dsl::virtual_provisioning_resource
                        .filter(dsl::resource_type.eq(nexus_db_queries::db::model::ResourceTypeProvisioned::Instance.to_string()))
                        .select(VirtualProvisioningResource::as_select())
                        .get_results_async::<VirtualProvisioningResource>(&conn)
                        .await
                        .unwrap()
                        .is_empty()
                )
            }).await.unwrap()
    }

    async fn no_virtual_provisioning_collection_records_using_instances(
        datastore: &DataStore,
    ) -> bool {
        use nexus_db_queries::db::model::VirtualProvisioningCollection;
        use nexus_db_queries::db::schema::virtual_provisioning_collection::dsl;

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
                Ok::<_, nexus_db_queries::db::TransactionError<()>>(
                    dsl::virtual_provisioning_collection
                        .filter(
                            dsl::cpus_provisioned
                                .ne(0)
                                .or(dsl::ram_provisioned.ne(0)),
                        )
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

    async fn disk_is_detached(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Disk;
        use nexus_db_queries::db::schema::disk::dsl;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(DISK_NAME))
            .select(Disk::as_select())
            .first_async::<Disk>(datastore.pool_for_tests().await.unwrap())
            .await
            .unwrap()
            .runtime_state
            .disk_state
            == "detached"
    }

    async fn no_instances_or_disks_on_sled(sled_agent: &SledAgent) -> bool {
        sled_agent.instance_count().await == 0
            && sled_agent.disk_count().await == 0
    }

    pub(crate) async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let sled_agent = &cptestctx.sled_agent.sled_agent;
        let datastore = cptestctx.server.apictx().nexus.datastore();

        // Check that no partial artifacts of instance creation exist
        assert!(no_instance_records_exist(datastore).await);
        assert!(no_network_interface_records_exist(datastore).await);
        assert!(no_external_ip_records_exist(datastore).await);
        assert!(no_sled_resource_instance_records_exist(datastore).await);
        assert!(
            no_virtual_provisioning_resource_records_exist(datastore).await
        );
        assert!(
            no_virtual_provisioning_collection_records_using_instances(
                datastore
            )
            .await
        );
        assert!(disk_is_detached(datastore).await);
        assert!(no_instances_or_disks_on_sled(&sled_agent).await);

        let v2p_mappings = &*sled_agent.v2p_mappings.lock().await;
        for (_nic_id, mappings) in v2p_mappings {
            assert!(mappings.is_empty());
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_helpers::test_opctx(&cptestctx);

        test_helpers::action_failure_can_unwind::<SagaInstanceCreate, _, _>(
            nexus,
            || Box::pin(async { new_test_params(&opctx, project_id) }),
            || {
                Box::pin({
                    async {
                        verify_clean_slate(&cptestctx).await;
                    }
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
        DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_project_and_disk(&client).await;
        let opctx = test_helpers::test_opctx(&cptestctx);

        test_helpers::action_failure_can_unwind_idempotently::<
            SagaInstanceCreate,
            _,
            _,
        >(
            nexus,
            || Box::pin(async { new_test_params(&opctx, project_id) }),
            || Box::pin(async { verify_clean_slate(&cptestctx).await }),
            log,
        )
        .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_helpers::test_opctx(&cptestctx);

        let params = new_test_params(&opctx, project_id);
        let dag = create_saga_dag::<SagaInstanceCreate>(params).unwrap();
        test_helpers::actions_succeed_idempotently(nexus, dag).await;

        // Verify that if the instance is destroyed, no detritus remains.
        // This is important to ensure that our original saga didn't
        // double-allocate during repeated actions.
        test_helpers::instance_delete_by_name(
            &cptestctx,
            INSTANCE_NAME,
            PROJECT_NAME,
        )
        .await;
        verify_clean_slate(&cptestctx).await;
    }
}
