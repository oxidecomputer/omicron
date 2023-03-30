// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_create::allocate_sled_ipv6;
use super::{NexusActionContext, NexusSaga, ACTION_GENERATE_ID};
use crate::app::sagas::declare_saga_actions;
use crate::db::{identity::Resource, lookup::LookupPath};
use crate::external_api::params;
use crate::{authn, authz, db};
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::{
    InstanceMigrationSourceParams, InstanceMigrationTargetParams,
    InstanceStateRequested,
};
use std::net::Ipv6Addr;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// instance migrate saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub instance: db::model::Instance,
    pub migrate_params: params::InstanceMigrate,
}

// The migration saga is similar to the instance creation saga: get a
// destination sled, allocate a Propolis process on it, and send it a request to
// initialize via migration, then wait (outside the saga) for this to resolve.
//
// Most of the complexity in this saga comes from the fact that during
// migration, there are two sleds with their own instance runtime states, and
// both the saga and the work that happen after it have to specify carefully
// which of the two participating VMMs is actually running the VM once the
// migration is over.
declare_saga_actions! {
    instance_migrate;
    ALLOCATE_PROPOLIS_IP -> "dst_propolis_ip" {
        + sim_allocate_propolis_ip
    }

    // This step sets the migration ID and destination Propolis ID fields in
    // CRDB by asking the instance's current sled to paste them into its runtime
    // state. Sled agent provides the synchronization here: while this operation
    // is idempotent for any single transition between IDs, sled agent ensures
    // that if multiple concurrent sagas try to set migration IDs at the same
    // Propolis generation, then only one will win and get to proceed through
    // the saga.
    SET_MIGRATION_IDS -> "set_migration_ids" {
        + sim_set_migration_ids
    }

    // The instance state on the destination looks like the instance state on
    // the source, except that it bears all of the destination's "location"
    // information--its Propolis ID, sled ID, and Propolis IP--with the same
    // Propolis generation number as the source set in the previous step.
    // Consider an example:
    //
    // - Before the saga begins, the instance has Propolis generation 10 and no
    //   migration IDs.
    // - The previous step sets Propolis generation 11 and sets the instance's
    //   migration ID.
    // - This step synthesizes a record with Propolis generation 11 and the same
    //   migration IDs, but with the Propolis identifiers set to the
    //   destination's IDs.
    //
    // When the migration resolves one way or the other, one or both of the
    // participating sleds will update CRDB with Propolis generation 12 and the
    // Propolis ID set to whichever Propolis ended up running the instance.
    //
    // This step must be infallible and does not have an undo action for reasons
    // described below.
    CREATE_DESTINATION_STATE -> "dst_runtime_state" {
        + sim_create_destination_state
    }

    // The next three steps are organized so that if the saga unwinds after the
    // destination Propolis is created, the "set migration IDs" step is undone
    // before the "ensure destination Propolis" step. Consider again the example
    // above, but this time, let the saga unwind after the destination Propolis
    // starts. If sled agent changes an instance's Propolis generation when a
    // Propolis is destroyed, and the undo steps are specified in the
    // traditional way, the following can occur:
    //
    // - After the steps above, the instance is registered with the source and
    //   target sleds, both at Propolis generation 11.
    // - Undoing the "ensure destination Propolis" step sets the Propolis
    //   generation to 12, but with the location information from the target!
    // - When the "set migration IDs" step is undone, the source will publish
    //   Propolis generation 12, but this will be dropped because the generation
    //   was already incremented.
    //
    // Now CRDB is pointing at the wrong Propolis, and instance state updates
    // from the former migration source will be ignored. Oops.
    //
    // Instead of asking sled agent to reason about what steps in a migration
    // have and haven't been undertaken, the following steps are arranged so
    // that the update that clears migration IDs happens before the one that
    // destroys the destination Propolis, which unwinds the saga to the expected
    // state.
    //
    // Note that this implies that `sim_ensure_destination_propolis_undo` must
    // be able to succeed even if the "ensure destination Propolis" step was
    // never reached.
    ENSURE_DESTINATION_PROPOLIS_UNDO -> "unused_ensure_destination_undo" {
        + sim_noop
        - sim_ensure_destination_propolis_undo
    }
    SET_MIGRATION_IDS_UNDO -> "unused_set_ids_undo" {
        + sim_noop
        - sim_clear_migration_ids
    }
    ENSURE_DESTINATION_PROPOLIS -> "ensure_destination" {
        + sim_ensure_destination_propolis
    }

    // Note that this step only requests migration by sending a "migrate in"
    // request to the destination sled. It does not wait for migration to
    // finish. It cannot be unwound, either, because there is no way to cancel
    // an in-progress migration (indeed, a requested migration might have
    // finished entirely by the time the undo step runs).
    INSTANCE_MIGRATE -> "instance_migrate" {
        + sim_instance_migrate
    }
}

// instance migrate saga: definition

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

        builder.append(Node::action(
            "dst_propolis_id",
            "GeneratePropolisId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(allocate_propolis_ip_action());
        builder.append(set_migration_ids_action());
        builder.append(create_destination_state_action());
        builder.append(ensure_destination_propolis_undo_action());
        builder.append(set_migration_ids_undo_action());
        builder.append(ensure_destination_propolis_action());
        builder.append(instance_migrate_action());

        Ok(builder.build()?)
    }
}

/// A no-op forward action.
async fn sim_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
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
    allocate_sled_ipv6(&opctx, sagactx, params.migrate_params.dst_sled_id).await
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
    let migration_id = sagactx.lookup::<Uuid>("migrate_id")?;
    let dst_propolis_id = sagactx.lookup::<Uuid>("dst_propolis_id")?;
    let updated_record = osagactx
        .nexus()
        .instance_set_migration_ids(
            &opctx,
            db_instance.id(),
            db_instance,
            Some(InstanceMigrationSourceParams {
                dst_propolis_id,
                migration_id,
            }),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(updated_record)
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
    let db_instance =
        sagactx.lookup::<db::model::Instance>("set_migration_ids")?;

    // The instance may have moved to another Propolis generation by this point
    // (if e.g. it was stopped and restarted), so this is not guaranteed to
    // succeed.
    //
    // REVIEW(gjc): This callee has enough information to distinguish "outdated
    // generation" failures from other errors. What is the right way to handle
    // that class of other errors? For example, suppose CRDB is totally
    // unavailable right now, but becomes available again later. The migration
    // IDs in this step need to be cleared so that the instance can try to
    // migrate again. How should this step achieve that?
    let _ = osagactx
        .nexus()
        .instance_set_migration_ids(
            &opctx,
            db_instance.id(),
            &db_instance,
            None,
        )
        .await;

    Ok(())
}

async fn sim_create_destination_state(
    sagactx: NexusActionContext,
) -> Result<db::model::Instance, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let mut db_instance =
        sagactx.lookup::<db::model::Instance>("set_migration_ids")?;
    let dst_propolis_id = sagactx.lookup::<Uuid>("dst_propolis_id")?;
    let dst_propolis_ip = sagactx.lookup::<Ipv6Addr>("dst_propolis_ip")?;

    // Update the runtime state to refer to the new Propolis.
    let new_runtime = db::model::InstanceRuntimeState {
        state: db::model::InstanceState::new(InstanceState::Creating),
        sled_id: params.migrate_params.dst_sled_id,
        propolis_id: dst_propolis_id,
        propolis_ip: Some(ipnetwork::Ipv6Network::from(dst_propolis_ip).into()),
        ..db_instance.runtime_state
    };

    db_instance.runtime_state = new_runtime;
    Ok(db_instance)
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
    let db_instance =
        sagactx.lookup::<db::model::Instance>("dst_runtime_state")?;
    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(db_instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_registered(&opctx, &authz_instance, &db_instance)
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
    let db_instance =
        sagactx.lookup::<db::model::Instance>("dst_runtime_state")?;
    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(db_instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    // Ensure that the destination sled has no Propolis matching the description
    // the saga previously generated.
    //
    // Note that this step can run before the instance was actually ensured.
    // This is OK; sled agent will quietly succeed if asked to unregister an
    // unregistered instance.
    osagactx
        .nexus()
        .instance_ensure_unregistered(&opctx, &authz_instance, &db_instance)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
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
    let src_runtime: InstanceRuntimeState = sagactx
        .lookup::<db::model::Instance>("set_migration_ids")?
        .runtime()
        .clone()
        .into();
    let dst_db_instance =
        sagactx.lookup::<db::model::Instance>("dst_runtime_state")?;
    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(dst_db_instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

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
    osagactx
        .nexus()
        .instance_request_state(
            &opctx,
            &authz_instance,
            &dst_db_instance,
            InstanceStateRequested::MigrationTarget(
                InstanceMigrationTargetParams {
                    src_propolis_addr: src_runtime
                        .propolis_addr
                        .unwrap()
                        .to_string(),
                    src_propolis_id: src_runtime.propolis_id,
                },
            ),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        app::{saga::create_saga_dag, sagas::instance_create},
        Nexus, TestInterfaces as _,
    };

    use dropshot::test_util::ClientTestContext;
    use http::{method::Method, StatusCode};
    use nexus_test_interface::NexusServer;
    use nexus_test_utils::{
        http_testing::{AuthnMode, NexusRequest, RequestBuilder},
        resource_helpers::{create_project, object_create, populate_ip_pool},
        start_sled_agent,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };
    use omicron_sled_agent::sim::Server;
    use sled_agent_client::TestInterfaces as _;

    use super::*;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "test-project";
    const INSTANCE_NAME: &str = "test-instance";

    async fn setup_test_project(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
        let project = create_project(&client, PROJECT_NAME).await;
        project.identity.id
    }

    async fn add_sleds(
        cptestctx: &ControlPlaneTestContext,
        num_sleds: usize,
    ) -> Vec<(Uuid, Server)> {
        let mut sas = Vec::with_capacity(num_sleds);
        for _ in 0..num_sleds {
            let sa_id = Uuid::new_v4();
            let log =
                cptestctx.logctx.log.new(o!("sled_id" => sa_id.to_string()));
            let addr =
                cptestctx.server.get_http_server_internal_address().await;

            info!(&cptestctx.logctx.log, "Adding simulated sled"; "sled_id" => %sa_id);
            let update_dir = std::path::Path::new("/should/be/unused");
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
                hostname: String::from(INSTANCE_NAME),
                user_data: b"#cloud-config".to_vec(),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::None,
                external_ips: vec![],
                disks: vec![],
                start: true,
            },
        )
        .await
    }

    async fn instance_simulate(
        cptestctx: &ControlPlaneTestContext,
        nexus: &Arc<Nexus>,
        instance_id: &Uuid,
    ) {
        info!(&cptestctx.logctx.log, "Poking simulated instance";
              "instance_id" => %instance_id);
        let sa = nexus.instance_sled_by_id(instance_id).await.unwrap();
        sa.instance_finish_transition(*instance_id).await;
    }

    async fn fetch_db_instance(
        cptestctx: &ControlPlaneTestContext,
        opctx: &nexus_db_queries::context::OpContext,
        id: Uuid,
    ) -> nexus_db_model::Instance {
        let datastore = cptestctx.server.apictx().nexus.datastore().clone();
        let (.., db_instance) = LookupPath::new(&opctx, &datastore)
            .instance_id(id)
            .fetch()
            .await
            .expect("test instance should be present in datastore");

        info!(&cptestctx.logctx.log, "refetched instance from db";
              "instance" => ?db_instance);

        db_instance
    }

    async fn instance_start(cptestctx: &ControlPlaneTestContext, id: &Uuid) {
        let client = &cptestctx.external_client;
        let instance_stop_url = format!("/v1/instances/{}/start", id);
        NexusRequest::new(
            RequestBuilder::new(client, Method::POST, &instance_stop_url)
                .body(None as Option<&serde_json::Value>)
                .expect_status(Some(StatusCode::ACCEPTED)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to start instance");
    }

    async fn instance_stop(cptestctx: &ControlPlaneTestContext, id: &Uuid) {
        let client = &cptestctx.external_client;
        let instance_stop_url = format!("/v1/instances/{}/stop", id);
        NexusRequest::new(
            RequestBuilder::new(client, Method::POST, &instance_stop_url)
                .body(None as Option<&serde_json::Value>)
                .expect_status(Some(StatusCode::ACCEPTED)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to stop instance");
    }

    fn select_first_alternate_sled(
        db_instance: &db::model::Instance,
        other_sleds: &[(Uuid, Server)],
    ) -> Uuid {
        let default_sled_uuid =
            Uuid::parse_str(nexus_test_utils::SLED_AGENT_UUID).unwrap();
        if other_sleds.is_empty() {
            panic!("need at least one other sled");
        }

        if other_sleds.iter().any(|sled| sled.0 == default_sled_uuid) {
            panic!("default test sled agent was in other_sleds");
        }

        if db_instance.runtime().sled_id == default_sled_uuid {
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
        let nexus = &cptestctx.server.apictx().nexus;
        let _project_id = setup_test_project(&client).await;

        let opctx = instance_create::test::test_opctx(cptestctx);
        let instance = create_instance(client).await;

        // Poke the instance to get it into the Running state.
        instance_simulate(cptestctx, nexus, &instance.identity.id).await;

        let db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;
        let old_runtime = db_instance.runtime().clone();
        let dst_sled_id =
            select_first_alternate_sled(&db_instance, &other_sleds);
        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            instance: db_instance,
            migrate_params: params::InstanceMigrate { dst_sled_id },
        };

        let dag = create_saga_dag::<SagaInstanceMigrate>(params).unwrap();
        let saga = nexus.create_runnable_saga(dag).await.unwrap();
        nexus.run_saga(saga).await.expect("Migration saga should succeed");

        // Merely running the migration saga (without simulating any completion
        // steps in the simulated agents) should not change where the instance
        // is running.
        let new_db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;
        assert_eq!(new_db_instance.runtime().sled_id, old_runtime.sled_id);
        assert_eq!(
            new_db_instance.runtime().propolis_id,
            old_runtime.propolis_id
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let other_sleds = add_sleds(cptestctx, 1).await;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let _project_id = setup_test_project(&client).await;

        let opctx = instance_create::test::test_opctx(cptestctx);
        let instance = create_instance(client).await;

        // Poke the instance to get it into the Running state.
        instance_simulate(cptestctx, nexus, &instance.identity.id).await;

        let db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;
        let old_runtime = db_instance.runtime().clone();
        let dst_sled_id =
            select_first_alternate_sled(&db_instance, &other_sleds);
        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            instance: db_instance,
            migrate_params: params::InstanceMigrate { dst_sled_id },
        };

        let dag = create_saga_dag::<SagaInstanceMigrate>(params).unwrap();

        // Some of the nodes in the DAG are expected to be infallible to allow
        // certain undo steps to be reordered. Specify these nodes here, then
        // verify that every infallible node actually appears in the DAG (to try
        // to detect drift between the saga specification and the test).
        let infallible_nodes = vec![
            "dst_runtime_state",
            "unused_ensure_destination_undo",
            "unused_set_ids_undo",
        ];
        let dag_node_names: Vec<String> =
            dag.get_nodes().map(|n| n.name().as_ref().to_owned()).collect();
        assert!(infallible_nodes
            .iter()
            .all(|n| dag_node_names.iter().any(|d| d == n)));

        for node in dag.get_nodes() {
            if infallible_nodes.contains(&node.name().as_ref()) {
                info!(log, "Skipping infallible node";
                      "node_name" => node.name().as_ref());
                continue;
            }

            info!(
                log,
                "Creating new saga which will fail at index {:?}", node.index();
                "node_name" => node.name().as_ref(),
                "label" => node.label(),
            );

            let runnable_saga =
                nexus.create_runnable_saga(dag.clone()).await.unwrap();
            nexus
                .sec()
                .saga_inject_error(runnable_saga.id(), node.index())
                .await
                .unwrap();
            nexus
                .run_saga(runnable_saga)
                .await
                .expect_err("Saga should have failed");

            // Unwinding at any step should clear the migration IDs from the
            // instance record and leave the instance's location otherwise
            // untouched.
            let new_db_instance =
                fetch_db_instance(cptestctx, &opctx, instance.identity.id)
                    .await;

            assert!(new_db_instance.runtime().migration_id.is_none());
            assert!(new_db_instance.runtime().dst_propolis_id.is_none());
            assert_eq!(new_db_instance.runtime().sled_id, old_runtime.sled_id);
            assert_eq!(
                new_db_instance.runtime().propolis_id,
                old_runtime.propolis_id
            );

            // Ensure the instance can stop. This helps to check that destroying
            // the migration destination (if one was ensured) doesn't advance
            // the Propolis ID generation in a way that prevents the source from
            // issuing further state updates.
            instance_stop(cptestctx, &instance.identity.id).await;
            instance_simulate(cptestctx, nexus, &instance.identity.id).await;
            let new_db_instance =
                fetch_db_instance(cptestctx, &opctx, instance.identity.id)
                    .await;
            assert_eq!(
                new_db_instance.runtime().state.0,
                InstanceState::Stopped
            );

            // Restart the instance for the next iteration.
            instance_start(cptestctx, &instance.identity.id).await;
            instance_simulate(cptestctx, nexus, &instance.identity.id).await;
        }
    }
}
