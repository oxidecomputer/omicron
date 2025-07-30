// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ACTION_GENERATE_ID, NexusActionContext, NexusSaga};
use crate::app::instance::{
    InstanceEnsureRegisteredApiResources, InstanceRegisterReason,
    InstanceStateChangeError, InstanceStateChangeRequest,
};
use crate::app::sagas::{
    declare_saga_actions, instance_common::allocate_vmm_ipv6,
};
use nexus_db_lookup::LookupPath;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::{authn, authz, db};
use nexus_types::internal_api::params::InstanceMigrateRequest;
use omicron_common::api::external::Error;
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
    pub migrate_params: InstanceMigrateRequest,
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
    //
    // Note that unwinding this action does *not* clear the migration IDs from
    // the instance record. This is to avoid a potential race with the instance
    // update saga where:
    //
    // - a `instance-migrate` saga sets the migration IDs at instance state
    //   generation  _N_
    // - an `instance-update` saga increments the instance's state generation to
    //   _N_ + 1
    // - the `instance-migrate` saga unwinds and attempts to clear the migration
    //   IDs, but can't, because the state generation has advanced.
    //
    // Instead, we leave the migration IDs in place and rely on setting the VMM
    // state to `SagaUnwound` to indicate to other future `instance-migrate`
    // sagas that it's okay to start a new migration.
    SET_MIGRATION_IDS -> "set_migration_ids" {
        + sim_set_migration_ids
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
        InstanceUuid::from_untyped_uuid(params.instance.id()),
        propolis_id,
        u32::from(params.instance.ncpus.0.0),
        params.instance.memory,
        constraints,
    )
    .await?;

    Ok(resource.sled_id.into())
}

async fn sim_release_sled_resources(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;

    osagactx.nexus().delete_sled_reservation(propolis_id).await?;
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
        osagactx.datastore().migration_mark_failed(&opctx, migration_id).await
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
        params.src_vmm.cpu_platform,
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

    let propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;
    info!(
        osagactx.log(),
        "destroying vmm record for migration unwind";
        "propolis_id" => %propolis_id,
    );

    osagactx.datastore().vmm_mark_saga_unwound(&opctx, &propolis_id).await?;
    Ok(())
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

    info!(osagactx.log(), "setting instance migration IDs";
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
        .map_err(ActionError::action_failed)
}

async fn sim_ensure_destination_propolis(
    sagactx: NexusActionContext,
) -> Result<db::model::Vmm, ActionError> {
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

    let (authz_silo, authz_project, authz_instance) =
        LookupPath::new(&opctx, osagactx.datastore())
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
            &InstanceEnsureRegisteredApiResources {
                authz_silo,
                authz_project,
                authz_instance,
            },
            &db_instance,
            &dst_propolis_id,
            &vmm,
            InstanceRegisterReason::Migrate {
                vmm_id: src_propolis_id,
                target_vmm_id: dst_propolis_id,
            },
        )
        .await
        .map_err(|err| match err {
            InstanceStateChangeError::SledAgent(inner) => {
                info!(
                    osagactx.log(),
                    "migration saga: sled agent failed to register instance";
                    "instance_id" => %db_instance.id(),
                    "dst_propolis_id" => %dst_propolis_id,
                    "error" => ?inner,
                );

                // Don't set the instance to Failed in this case. Instead, allow
                // the saga to unwind, marking the VMM as SagaUnwound.
                ActionError::action_failed(Error::from(inner))
            }
            InstanceStateChangeError::Other(inner) => {
                info!(
                    osagactx.log(),
                    "migration saga: internal error registering instance";
                    "instance_id" => %db_instance.id(),
                    "dst_propolis_id" => %dst_propolis_id,
                    "error" => ?inner,
                );
                ActionError::action_failed(inner)
            }
        })
}

async fn sim_ensure_destination_propolis_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let dst_propolis_id = sagactx.lookup::<PropolisUuid>("dst_propolis_id")?;
    let dst_sled_id = sagactx.lookup::<SledUuid>("dst_sled_id")?;
    let db_instance =
        sagactx.lookup::<db::model::Instance>("set_migration_ids")?;

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
        .instance_ensure_unregistered(&dst_propolis_id, &dst_sled_id)
        .await
    {
        Ok(_) => Ok(()),
        Err(InstanceStateChangeError::SledAgent(inner)) => {
            if !inner.vmm_gone() { Ok(()) } else { Err(inner.0.into()) }
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
    let dst_vmm = sagactx.lookup::<db::model::Vmm>("ensure_destination")?;
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
            &db_instance,
            &Some(dst_vmm),
            InstanceStateChangeRequest::Migrate(
                InstanceMigrationTargetParams {
                    src_propolis_addr: src_vmm_addr.to_string(),
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
    use super::*;
    use crate::app::sagas::test_helpers;
    use crate::external_api::params;
    use dropshot::test_util::ClientTestContext;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project, object_create,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "test-project";
    const INSTANCE_NAME: &str = "test-instance";

    async fn setup_test_project(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(&client).await;
        let project = create_project(&client, PROJECT_NAME).await;
        project.identity.id
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
                boot_disk: None,
                cpu_platform: None,
                start: true,
                auto_restart_policy: Default::default(),
                anti_affinity_groups: Vec::new(),
            },
        )
        .await
    }

    #[nexus_test(server = crate::Server, extra_sled_agents = 1)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
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
        let dst_sled_id = cptestctx
            .find_sled_agent(vmm.sled_id)
            .expect("need at least one other sled");
        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            instance: state.instance().clone(),
            src_vmm: vmm.clone(),
            migrate_params: InstanceMigrateRequest {
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

    #[nexus_test(server = crate::Server, extra_sled_agents = 1)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
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

                    let dst_sled_id = cptestctx
                        .find_sled_agent(old_vmm.sled_id)
                        .expect("need at least one other sled");

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
                        migrate_params: InstanceMigrateRequest {
                            dst_sled_id: dst_sled_id.into_untyped_uuid(),
                        },
                    }
                }
            })
        };

        let after_saga = || -> futures::future::BoxFuture<'_, ()> {
            Box::pin({
                async {
                    let new_state = test_helpers::instance_fetch_all(
                        cptestctx,
                        instance_id,
                    )
                    .await;

                    let new_instance = new_state.instance;
                    let new_vmm = new_state
                        .active_vmm
                        .as_ref()
                        .expect("vmm should be active");

                    assert_eq!(
                        new_instance.runtime().propolis_id.unwrap(),
                        new_vmm.id
                    );

                    // If the instance has had migration IDs set, then both
                    // sides of the migration should be marked as failed.
                    if let Some(migration) = new_state.migration {
                        assert_eq!(
                            migration.source_state,
                            db::model::MigrationState::FAILED
                        );
                        assert_eq!(
                            migration.target_state,
                            db::model::MigrationState::FAILED
                        );
                    }
                    // If the instance has a target VMM ID left behind by the
                    // unwinding saga, that VMM must be in the `SagaUnwound` state.
                    if let Some(target_vmm) = new_state.target_vmm {
                        assert_eq!(
                            target_vmm.runtime.state,
                            db::model::VmmState::SagaUnwound
                        );
                    }

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
                    // state. This may not happen immediately, as an
                    // instance-update saga must complete to update the
                    // instance's state.
                    let new_state = test_helpers::instance_wait_for_state(
                        cptestctx,
                        instance_id,
                        nexus_db_model::InstanceState::NoVmm,
                    )
                    .await;

                    let new_instance = new_state.instance();
                    let new_vmm = new_state.vmm().as_ref();
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
