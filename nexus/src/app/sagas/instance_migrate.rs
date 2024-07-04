// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NexusSaga, ACTION_GENERATE_ID};
use crate::app::instance::{
    InstanceRegisterReason, InstanceStateChangeError,
    InstanceStateChangeRequest,
};
use crate::app::sagas::{
    declare_saga_actions, instance_common::allocate_vmm_ipv6,
};
use crate::external_api::params;
use nexus_db_queries::db::{identity::Resource, lookup::LookupPath};
use nexus_db_queries::{authn, authz, db};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid, SledUuid};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::InstanceMigrationTargetParams;
use slog::warn;
use std::net::{Ipv6Addr, SocketAddr};
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// instance migrate saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub instance: db::model::Instance,
    pub src_vmm: db::model::Vmm,
    pub migrate_params: params::InstanceMigrate,
}

// The migration saga is similar to the instance start saga: get a destination
// sled, allocate a Propolis process on it, and send that Propolis a request to
// initialize via migration, then wait (outside the saga) for this to resolve.

declare_saga_actions! {
    instance_migrate;

    GENERATE_PROPOLIS_ID -> "dst_propolis_id" {
        + sim_generate_propolis_id
    }

    // In order to set up migration, the saga needs to construct the following:
    //
    // - A migration ID and destination Propolis ID (added to the DAG inline as
    //   ACTION_GENERATE_ID actions)
    // - A sled ID
    // - An IP address for the destination Propolis server
    //
    // The latter two pieces of information are used to create a VMM record for
    // the new Propolis, which can then be written into the instance as a
    // migration target.
    RESERVE_RESOURCES -> "dst_sled_id" {
        + sim_reserve_sled_resources
        - sim_release_sled_resources
    }

    ALLOCATE_PROPOLIS_IP -> "dst_propolis_ip" {
        + sim_allocate_propolis_ip
    }

    CREATE_VMM_RECORD -> "dst_vmm_record" {
        + sim_create_vmm_record
        - sim_destroy_vmm_record
    }

    CREATE_MIGRATION_RECORD -> "migration_record" {
        + sim_create_migration_record
        - sim_fail_migration_record
    }

    // This step the instance's migration ID and destination Propolis ID
    // fields in the database.
    //
    // If the instance's migration ID has already been set when we attempt to
    // set ours, that means we have probably raced with another migrate saga for
    // the same instance. If this is the case, this action will fail and the
    // saga will unwind.
    //
    // Yes, it's a bit unfortunate that our attempt to compare-and-swap in a
    // migration ID happens only after we've created VMM and migration records,
    // and that we'll have to destroy them as we unwind. However, the
    // alternative, setting the migration IDs *before* records for the target
    // VMM and the migration are created, would mean that there is a period of
    // time during which the instance record contains foreign keys into the
    // `vmm` and `migration` tables that don't have corresponding records to
    // those tables. Because the `instance` table is queried in the public API,
    // we take care to ensure that it doesn't have "dangling pointers" to
    // records in the `vmm` and `migration` tables that don't exist yet.
    SET_MIGRATION_IDS -> "set_migration_ids" {
        + sim_set_migration_ids
        - sim_clear_migration_ids
    }

    // This step registers the instance with the destination sled. Care is
    // needed at this point because there are two sleds that can send updates
    // that affect the same instance record (though they have separate VMMs that
    // update independently), and if the saga unwinds they need to ensure they
    // cooperate to return the instance to the correct pre-migration state.
    ENSURE_DESTINATION_PROPOLIS -> "ensure_destination" {
        + sim_ensure_destination_propolis
        - sim_ensure_destination_propolis_undo
    }

    // Finally, this step requests migration by sending a "migrate in" request
    // to the destination sled. It does not wait for migration to finish and
    // cannot be allowed to unwind (if a migration has already started, it
    // cannot be canceled and indeed may have completed by the time the undo
    // step runs).
    INSTANCE_MIGRATE -> "instance_migrate" {
        + sim_instance_migrate
    }
}

#[derive(Debug)]
pub struct SagaInstanceMigrate;
impl NexusSaga for SagaInstanceMigrate {
    const NAME: &'static str = "instance-migrate";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        instance_migrate_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(Node::action(
            "migrate_id",
            "GenerateMigrateId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(generate_propolis_id_action());
        builder.append(reserve_resources_action());
        builder.append(allocate_propolis_ip_action());
        builder.append(create_vmm_record_action());
        builder.append(create_migration_record_action());
        builder.append(set_migration_ids_action());
        builder.append(ensure_destination_propolis_action());
        builder.append(instance_migrate_action());

        Ok(builder.build()?)
    }
}

async fn sim_generate_propolis_id(
    _sagactx: NexusActionContext,
) -> Result<PropolisUuid, ActionError> {
    Ok(PropolisUuid::new_v4())
}

/// Reserves resources for the destination on the specified target sled.
async fn sim_reserve_sled_resources(
    sagactx: NexusActionContext,
) -> Result<SledUuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;

    // Add a constraint that requires the allocator to reserve on the
    // migration's destination sled instead of a random sled.
    let constraints = db::model::SledReservationConstraintBuilder::new()
        .must_select_from(&[params.migrate_params.dst_sled_id])
        .build();

    let resource = super::instance_common::reserve_vmm_resources(
        osagactx.nexus(),
        propolis_id,
        u32::from(params.instance.ncpus.0 .0),
        params.instance.memory,
        constraints,
    )
    .await?;

    Ok(SledUuid::from_untyped_uuid(resource.sled_id))
}

async fn sim_release_sled_resources(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;

    osagactx
        .nexus()
        .delete_sled_reservation(propolis_id.into_untyped_uuid())
        .await?;
    Ok(())
}

/// Allocates an IP address on the destination sled for the Propolis server.
async fn sim_allocate_propolis_ip(
    sagactx: NexusActionContext,
) -> Result<Ipv6Addr, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    allocate_vmm_ipv6(
        &opctx,
        sagactx.user_data().datastore(),
        SledUuid::from_untyped_uuid(params.migrate_params.dst_sled_id),
    )
    .await
}

async fn sim_create_migration_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Migration, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let source_propolis_id = params.src_vmm.id;
    let migration_id = sagactx.lookup::<Uuid>("migrate_id")?;
    let target_propolis_id = sagactx.lookup::<Uuid>("dst_propolis_id")?;

    info!(osagactx.log(), "creating migration record";
          "migration_id" => %migration_id,
          "source_propolis_id" => %source_propolis_id,
          "target_propolis_id" => %target_propolis_id);

    osagactx
        .datastore()
        .migration_insert(
            &opctx,
            db::model::Migration::new(
                migration_id,
                InstanceUuid::from_untyped_uuid(params.instance.id()),
                source_propolis_id,
                target_propolis_id,
            ),
        )
        .await
        .map_err(ActionError::action_failed)
}

async fn sim_fail_migration_record(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx: &std::sync::Arc<crate::saga_interface::SagaContext> =
        sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let migration_id = sagactx.lookup::<Uuid>("migrate_id")?;

    info!(
        osagactx.log(),
        "migration saga unwinding, marking migration record as failed";
        "instance_id" => %params.instance.id(),
        "migration_id" => %migration_id,
    );
    // If the migration record wasn't updated, this means it's already deleted,
    // which...seems weird, but isn't worth getting the whole saga unwind stuck over.
    if let Err(e) =
        osagactx.datastore().migration_mark_deleted(&opctx, migration_id).await
    {
        warn!(osagactx.log(),
              "Error marking migration record as failed during rollback";
              "instance_id" => %params.instance.id(),
              "migration_id" => %migration_id,
              "error" => ?e);
    }

    Ok(())
}

async fn sim_create_vmm_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Vmm, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let instance_id = params.instance.id();
    let propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;
    let sled_id = sagactx.lookup::<SledUuid>("dst_sled_id")?;
    let propolis_ip = sagactx.lookup::<Ipv6Addr>("dst_propolis_ip")?;

    info!(osagactx.log(), "creating vmm record for migration destination";
          "instance_id" => %instance_id,
          "propolis_id" => %propolis_id,
          "sled_id" => %sled_id);

    super::instance_common::create_and_insert_vmm_record(
        osagactx.datastore(),
        &opctx,
        InstanceUuid::from_untyped_uuid(instance_id),
        propolis_id,
        sled_id,
        propolis_ip,
        nexus_db_model::VmmInitialState::Migrating,
    )
    .await
}

async fn sim_destroy_vmm_record(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let vmm = sagactx.lookup::<db::model::Vmm>("dst_vmm_record")?;
    info!(osagactx.log(), "destroying vmm record for migration unwind";
          "propolis_id" => %vmm.id);

    super::instance_common::unwind_vmm_record(
        osagactx.datastore(),
        &opctx,
        &vmm,
    )
    .await
}

async fn sim_set_migration_ids(
    sagactx: NexusActionContext,
) -> Result<db::model::Instance, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let db_instance = &params.instance;
    let instance_id = InstanceUuid::from_untyped_uuid(db_instance.id());
    let src_propolis_id = PropolisUuid::from_untyped_uuid(params.src_vmm.id);
    let migration_id = sagactx.lookup::<Uuid>("migrate_id")?;
    let dst_propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;

    info!(osagactx.log(), "setting migration IDs on migration source sled";
          "instance_id" => %db_instance.id(),
          "migration_id" => %migration_id,
          "src_propolis_id" => %src_propolis_id,
          "dst_propolis_id" => %dst_propolis_id,
          "prev_runtime_state" => ?db_instance.runtime());

    osagactx
        .datastore()
        .instance_set_migration_ids(
            &opctx,
            instance_id,
            src_propolis_id,
            migration_id,
            dst_propolis_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    // Refetch the instance to make sure we have the correct thing to send to
    // sled-agents.
    // TODO(eliza): we *could* probably just munge the previous
    // `InstanceRuntimeState` to have the migration IDs set, but...that feels
    // sketchy. Doing another db query here to get the latest state is kinda sad
    // but whatever.
    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(db_instance.id())
        .lookup_for(authz::Action::Read)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .datastore()
        .instance_refetch(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)
}

async fn sim_clear_migration_ids(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let db_instance = params.instance;
    let instance_id = InstanceUuid::from_untyped_uuid(db_instance.id());
    let src_propolis_id = PropolisUuid::from_untyped_uuid(params.src_vmm.id);
    let migration_id = sagactx.lookup::<Uuid>("migrate_id")?;
    let dst_propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;

    info!(osagactx.log(), "clearing migration IDs for saga unwind";
          "instance_id" => %db_instance.id(),
          "migration_id" => %migration_id,
          "src_propolis_id" => %src_propolis_id,
          "dst_propolis_id" => %dst_propolis_id);

    if let Err(e) = osagactx
        .datastore()
        .instance_unset_migration_ids(
            &opctx,
            instance_id,
            migration_id,
            dst_propolis_id,
        )
        .await
    {
        warn!(osagactx.log(),
              "Error clearing migration IDs during rollback";
              "instance_id" => %instance_id,
              "src_propolis_id" => %src_propolis_id,
              "dst_propolis_id" => %dst_propolis_id,
              "error" => ?e);
    }

    Ok(())
}

async fn sim_ensure_destination_propolis(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let vmm = sagactx.lookup::<db::model::Vmm>("dst_vmm_record")?;
    let db_instance =
        sagactx.lookup::<db::model::Instance>("set_migration_ids")?;

    info!(osagactx.log(), "ensuring migration destination vmm exists";
          "instance_id" => %db_instance.id(),
          "dst_propolis_id" => %vmm.id,
          "dst_vmm_state" => ?vmm);

    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(db_instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    let src_propolis_id = PropolisUuid::from_untyped_uuid(params.src_vmm.id);
    let dst_propolis_id = PropolisUuid::from_untyped_uuid(vmm.id);
    osagactx
        .nexus()
        .instance_ensure_registered(
            &opctx,
            &authz_instance,
            &db_instance,
            &dst_propolis_id,
            &vmm,
            InstanceRegisterReason::Migrate {
                vmm_id: src_propolis_id,
                target_vmm_id: dst_propolis_id,
            },
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sim_ensure_destination_propolis_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let dst_sled_id = sagactx.lookup::<SledUuid>("dst_sled_id")?;
    let db_instance =
        sagactx.lookup::<db::model::Instance>("set_migration_ids")?;
    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(db_instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    info!(osagactx.log(), "unregistering destination vmm for migration unwind";
          "instance_id" => %db_instance.id(),
          "sled_id" => %dst_sled_id,
          "prev_runtime_state" => ?db_instance.runtime());

    // Ensure that the destination sled has no Propolis matching the description
    // the saga previously generated. If this succeeds, or if it fails because
    // the destination sled no longer knows about this instance, allow the rest
    // of unwind to take care of cleaning up the migration IDs in the instance
    // record. Otherwise the unwind has failed and manual intervention is
    // needed.
    match osagactx
        .nexus()
        .instance_ensure_unregistered(&opctx, &authz_instance, &dst_sled_id)
        .await
    {
        Ok(_) => Ok(()),
        Err(InstanceStateChangeError::SledAgent(inner)) => {
            if !inner.instance_unhealthy() {
                Ok(())
            } else {
                Err(inner.0.into())
            }
        }
        Err(e) => Err(e.into()),
    }
}

async fn sim_instance_migrate(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let db_instance =
        sagactx.lookup::<db::model::Instance>("set_migration_ids")?;

    let src_vmm_addr = SocketAddr::new(
        params.src_vmm.propolis_ip.ip(),
        params.src_vmm.propolis_port.into(),
    );

    let src_propolis_id = db_instance.runtime().propolis_id.unwrap();
    let dst_vmm = sagactx.lookup::<db::model::Vmm>("dst_vmm_record")?;
    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(db_instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    info!(osagactx.log(), "initiating migration from destination sled";
          "instance_id" => %db_instance.id(),
          "dst_vmm_record" => ?dst_vmm,
          "src_propolis_id" => %src_propolis_id);

    // TODO-correctness: This needs to be retried if a transient error occurs to
    // avoid a problem like the following:
    //
    // 1. The saga executor runs this step and successfully starts migration.
    // 2. The executor crashes.
    // 3. Migration completes.
    // 4. The executor restarts, runs this step, encounters a transient error,
    //    and then tries to unwind the saga.
    //
    // Now the "ensure destination" undo step will tear down the (running)
    // migration target.
    //
    // Possibly sled agent can help with this by using state or Propolis
    // generation numbers to filter out stale destruction requests.
    match osagactx
        .nexus()
        .instance_request_state(
            &opctx,
            &authz_instance,
            &db_instance,
            &Some(dst_vmm),
            InstanceStateChangeRequest::Migrate(
                InstanceMigrationTargetParams {
                    src_propolis_addr: src_vmm_addr.to_string(),
                    src_propolis_id,
                },
            ),
        )
        .await
    {
        Ok(_) => Ok(()),
        // Failure to initiate migration to a specific target doesn't entail
        // that the entire instance has failed, so handle errors by unwinding
        // the saga without otherwise touching the instance's state.
        Err(InstanceStateChangeError::SledAgent(inner)) => {
            info!(osagactx.log(),
                      "migration saga: sled agent failed to start migration";
                      "instance_id" => %db_instance.id(),
                      "error" => ?inner);

            Err(ActionError::action_failed(
                omicron_common::api::external::Error::from(inner),
            ))
        }
        Err(InstanceStateChangeError::Other(inner)) => {
            info!(osagactx.log(),
                      "migration saga: internal error changing instance state";
                      "instance_id" => %db_instance.id(),
                      "error" => ?inner);

            Err(ActionError::action_failed(inner))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::app::db::datastore::InstanceAndActiveVmm;
    use crate::app::sagas::test_helpers;
    use camino::Utf8Path;
    use dropshot::test_util::ClientTestContext;
    use nexus_test_interface::NexusServer;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project, object_create,
    };
    use nexus_test_utils::start_sled_agent;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };
    use omicron_sled_agent::sim::Server;
    use omicron_test_utils::dev::poll;
    use std::time::Duration;

    use super::*;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "test-project";
    const INSTANCE_NAME: &str = "test-instance";

    async fn setup_test_project(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(&client).await;
        let project = create_project(&client, PROJECT_NAME).await;
        project.identity.id
    }

    async fn add_sleds(
        cptestctx: &ControlPlaneTestContext,
        num_sleds: usize,
    ) -> Vec<(SledUuid, Server)> {
        let mut sas = Vec::with_capacity(num_sleds);
        for _ in 0..num_sleds {
            let sa_id = SledUuid::new_v4();
            let log =
                cptestctx.logctx.log.new(o!("sled_id" => sa_id.to_string()));
            let addr =
                cptestctx.server.get_http_server_internal_address().await;

            info!(&cptestctx.logctx.log, "Adding simulated sled"; "sled_id" => %sa_id);
            let update_dir = Utf8Path::new("/should/be/unused");
            let sa = start_sled_agent(
                log,
                addr,
                sa_id,
                &update_dir,
                omicron_sled_agent::sim::SimMode::Explicit,
            )
            .await
            .unwrap();
            sas.push((sa_id, sa));
        }

        sas
    }

    async fn create_instance(
        client: &ClientTestContext,
    ) -> omicron_common::api::external::Instance {
        let instances_url = format!("/v1/instances?project={}", PROJECT_NAME);
        object_create(
            client,
            &instances_url,
            &params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: INSTANCE_NAME.parse().unwrap(),
                    description: format!("instance {:?}", INSTANCE_NAME),
                },
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_gibibytes_u32(2),
                hostname: INSTANCE_NAME.parse().unwrap(),
                user_data: b"#cloud-config".to_vec(),
                ssh_public_keys: Some(Vec::new()),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::None,
                external_ips: vec![],
                disks: vec![],
                start: true,
            },
        )
        .await
    }

    fn select_first_alternate_sled(
        db_vmm: &db::model::Vmm,
        other_sleds: &[(SledUuid, Server)],
    ) -> SledUuid {
        let default_sled_uuid: SledUuid =
            nexus_test_utils::SLED_AGENT_UUID.parse().unwrap();
        if other_sleds.is_empty() {
            panic!("need at least one other sled");
        }

        if other_sleds.iter().any(|sled| sled.0 == default_sled_uuid) {
            panic!("default test sled agent was in other_sleds");
        }

        if db_vmm.sled_id == default_sled_uuid.into_untyped_uuid() {
            other_sleds[0].0
        } else {
            default_sled_uuid
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let other_sleds = add_sleds(cptestctx, 1).await;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let _project_id = setup_test_project(&client).await;

        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

        // Poke the instance to get it into the Running state.
        test_helpers::instance_simulate(cptestctx, &instance_id).await;

        let state = test_helpers::instance_fetch(cptestctx, instance_id).await;
        let vmm = state.vmm().as_ref().unwrap();
        let dst_sled_id = select_first_alternate_sled(vmm, &other_sleds);
        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            instance: state.instance().clone(),
            src_vmm: vmm.clone(),
            migrate_params: params::InstanceMigrate {
                dst_sled_id: dst_sled_id.into_untyped_uuid(),
            },
        };

        nexus
            .sagas
            .saga_execute::<SagaInstanceMigrate>(params)
            .await
            .expect("Migration saga should succeed");

        // Merely running the migration saga (without simulating any completion
        // steps in the simulated agents) should not change where the instance
        // is running.
        let new_state =
            test_helpers::instance_fetch(cptestctx, instance_id).await;

        assert_eq!(
            new_state.instance().runtime().propolis_id,
            state.instance().runtime().propolis_id
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let other_sleds = add_sleds(cptestctx, 1).await;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let _project_id = setup_test_project(&client).await;

        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

        // Poke the instance to get it into the Running state.
        test_helpers::instance_simulate(cptestctx, &instance_id).await;

        let make_params = || -> futures::future::BoxFuture<'_, Params> {
            Box::pin({
                async {
                    let old_state =
                        test_helpers::instance_fetch(cptestctx, instance_id)
                            .await;

                    let old_instance = old_state.instance();
                    let old_vmm = old_state
                        .vmm()
                        .as_ref()
                        .expect("instance should have a vmm before migrating");

                    let dst_sled_id =
                        select_first_alternate_sled(old_vmm, &other_sleds);

                    info!(log, "setting up new migration saga";
                          "old_instance" => ?old_instance,
                          "src_vmm" => ?old_vmm,
                          "dst_sled_id" => %dst_sled_id);

                    Params {
                        serialized_authn: authn::saga::Serialized::for_opctx(
                            &opctx,
                        ),
                        instance: old_instance.clone(),
                        src_vmm: old_vmm.clone(),
                        migrate_params: params::InstanceMigrate {
                            dst_sled_id: dst_sled_id.into_untyped_uuid(),
                        },
                    }
                }
            })
        };

        let after_saga = || -> futures::future::BoxFuture<'_, ()> {
            Box::pin({
                async {
                    // Unwinding at any step should clear the migration IDs from
                    // the instance record and leave the instance's location
                    // otherwise untouched.
                    let new_state =
                        test_helpers::instance_fetch(cptestctx, instance_id)
                            .await;

                    let new_instance = new_state.instance();
                    let new_vmm =
                        new_state.vmm().as_ref().expect("vmm should be active");

                    assert!(new_instance.runtime().migration_id.is_none());
                    assert!(new_instance.runtime().dst_propolis_id.is_none());
                    assert_eq!(
                        new_instance.runtime().propolis_id.unwrap(),
                        new_vmm.id
                    );

                    info!(
                        &log,
                        "migration saga unwind: stopping instance after failed \
                        saga"
                    );

                    // Ensure the instance can stop. This helps to check that
                    // destroying the migration destination (if one was ensured)
                    // doesn't advance the Propolis ID generation in a way that
                    // prevents the source from issuing further state updates.
                    test_helpers::instance_stop(cptestctx, &instance_id).await;
                    test_helpers::instance_simulate(cptestctx, &instance_id)
                        .await;

                    // Wait until the instance has advanced to the `NoVmm`
                    // state. This may not happen immediately, as the
                    // `Nexus::cpapi_instances_put` API endpoint simply
                    // writes the new VMM state to the database and *starts*
                    // an `instance-update` saga, and the instance record
                    // isn't updated until that saga completes.
                    let new_state = poll::wait_for_condition(
                        || async {
                            let new_state = test_helpers::instance_fetch(
                                cptestctx,
                                instance_id,
                            )
                            .await;
                            if new_state.instance().runtime().nexus_state == nexus_db_model::InstanceState::Vmm {
                                Err(poll::CondCheckError::<InstanceAndActiveVmm>::NotYet)
                            } else {
                                Ok(new_state)
                            }
                        },
                        &Duration::from_secs(5),
                        &Duration::from_secs(300),
                    )
                    .await.expect("instance did not transition to NoVmm state after 300 seconds");

                    let new_instance = new_state.instance();
                    let new_vmm = new_state.vmm().as_ref();
                    assert_eq!(
                        new_instance.runtime().nexus_state,
                        nexus_db_model::InstanceState::NoVmm,
                    );
                    assert!(new_instance.runtime().propolis_id.is_none());
                    assert!(new_vmm.is_none());

                    // Restart the instance for the next iteration.
                    info!(
                        &log,
                        "migration saga unwind: restarting instance after \
                         failed saga"
                    );
                    test_helpers::instance_start(cptestctx, &instance_id).await;
                    test_helpers::instance_simulate(cptestctx, &instance_id)
                        .await;
                }
            })
        };

        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaInstanceMigrate,
            _,
            _,
        >(nexus, make_params, after_saga, log)
        .await;
    }
}
