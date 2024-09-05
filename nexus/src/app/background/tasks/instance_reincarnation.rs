// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatically restarting failed instances.

use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas::instance_start;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use nexus_db_queries::authn;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::InstanceReincarnationStatus;
use omicron_common::api::external::Error;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::task::JoinSet;

pub struct InstanceReincarnation {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

const BATCH_SIZE: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl BackgroundTask for InstanceReincarnation {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let mut status = InstanceReincarnationStatus::default();
            self.actually_activate(opctx, &mut status).await;
            if !status.restart_errors.is_empty() || status.query_error.is_some()
            {
                error!(
                    &opctx.log,
                    "instance reincarnation completed with errors!";
                    "instances_found" => status.instances_found,
                    "instances_reincarnated" => status.instances_reincarnated.len(),
                    "already_reincarnated" => status.already_reincarnated.len(),
                    "query_error" => ?status.query_error,
                    "restart_errors" => status.restart_errors.len(),
                );
            } else if !status.instances_reincarnated.is_empty() {
                info!(
                    &opctx.log,
                    "instance reincarnation completed";
                    "instances_found" => status.instances_found,
                    "instances_reincarnated" => status.instances_reincarnated.len(),
                    "already_reincarnated" => status.already_reincarnated.len(),
                );
            } else {
                debug!(
                    &opctx.log,
                    "instance reincarnation completed; no instances \
                     in need of reincarnation";
                    "instances_found" => status.instances_found,
                    "already_reincarnated" => status.already_reincarnated.len(),
                );
            };
            serde_json::json!(status)
        })
    }
}

impl InstanceReincarnation {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        sagas: Arc<dyn StartSaga>,
    ) -> Self {
        Self { datastore, sagas }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        status: &mut InstanceReincarnationStatus,
    ) {
        let mut tasks = JoinSet::new();
        let mut paginator = Paginator::new(BATCH_SIZE);

        while let Some(p) = paginator.next() {
            let maybe_batch = self
                .datastore
                .find_reincarnatable_instances(opctx, &p.current_pagparams())
                .await;
            let batch = match maybe_batch {
                Ok(batch) => batch,
                Err(error) => {
                    error!(
                        opctx.log,
                        "failed to list instances in need of reincarnation";
                        "error" => &error,
                    );
                    status.query_error = Some(error.to_string());
                    break;
                }
            };

            paginator = p.found_batch(&batch, &|instance| instance.id());

            let found = batch.len();
            if found == 0 {
                debug!(
                    opctx.log,
                    "no more instances in need of reincarnation";
                    "total_found" => status.instances_found,
                );
                break;
            }

            let prev_sagas_started = tasks.len();
            status.instances_found += found;

            let serialized_authn = authn::saga::Serialized::for_opctx(opctx);
            for db_instance in batch {
                let instance_id = db_instance.id();
                let prepared_saga = instance_start::SagaInstanceStart::prepare(
                    &instance_start::Params {
                        db_instance,
                        serialized_authn: serialized_authn.clone(),
                        reason: instance_start::Reason::AutoRestart,
                    },
                );
                match prepared_saga {
                    Ok(saga) => {
                        let start_saga = self.sagas.clone();
                        tasks.spawn(async move {
                            start_saga
                                .saga_start(saga)
                                .await
                                .map_err(|e| (instance_id, e))?;
                            Ok(instance_id)
                        });
                    }
                    Err(error) => {
                        const ERR_MSG: &'static str =
                            "failed to prepare instance-start saga";
                        error!(
                            opctx.log,
                            "{ERR_MSG} for {instance_id}";
                            "instance_id" => %instance_id,
                            "error" => %error,
                        );
                        status
                            .restart_errors
                            .push((instance_id, format!("{ERR_MSG}: {error}")))
                    }
                };
            }

            let total_sagas_started = tasks.len();
            debug!(
                opctx.log,
                "found instance in need of reincarnation";
                "instances_found" => found,
                "total_found" => status.instances_found,
                "sagas_started" => total_sagas_started - prev_sagas_started,
                "total_sagas_started" => total_sagas_started,
            );
        }

        // All sagas started, wait for them to come back...
        while let Some(saga_result) = tasks.join_next().await {
            match saga_result {
                // Start saga completed successfully
                Ok(Ok(instance_id)) => {
                    debug!(
                        opctx.log,
                        "welcome back to the realm of the living, {instance_id}!";
                        "instance_id" => %instance_id,
                    );
                    status.instances_reincarnated.push(instance_id);
                }
                // The instance was restarted by another saga, that's fine...
                Ok(Err((instance_id, Error::Conflict { message })))
                    if message.external_message()
                        == instance_start::ALREADY_STARTING_ERROR =>
                {
                    debug!(
                        opctx.log,
                        "instance {instance_id} was already reincarnated";
                        "instance_id" => %instance_id,
                    );
                    status.already_reincarnated.push(instance_id);
                }
                // Start saga failed
                Ok(Err((instance_id, error))) => {
                    const ERR_MSG: &'static str = "instance-start saga failed";
                    warn!(opctx.log,
                        "{ERR_MSG}";
                        "instance_id" => %instance_id,
                        "error" => %error,
                    );
                    status
                        .restart_errors
                        .push((instance_id, format!("{ERR_MSG}: {error}")));
                }
                Err(e) => {
                    const JOIN_ERR_MSG: &'static str =
                        "tasks spawned on the JoinSet should never return a \
                        JoinError, as nexus is compiled with panic=\"abort\", \
                        and we never cancel them...";
                    error!(opctx.log, "{JOIN_ERR_MSG}"; "error" => %e);
                    if cfg!(debug_assertions) {
                        unreachable!("{JOIN_ERR_MSG} but, I saw {e}!",)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::init::test::NoopStartSaga;
    use crate::external_api::params;
    use chrono::Utc;
    use nexus_db_model::Instance;
    use nexus_db_model::InstanceAutoRestart;
    use nexus_db_model::InstanceRuntimeState;
    use nexus_db_model::InstanceState;
    use nexus_db_queries::authz;
    use nexus_db_queries::db::lookup::LookupPath;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "reincarnation-station";

    async fn setup_test_project(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
    ) -> authz::Project {
        create_default_ip_pool(&cptestctx.external_client).await;
        let project =
            create_project(&cptestctx.external_client, PROJECT_NAME).await;

        let datastore = cptestctx.server.server_context().nexus.datastore();
        let (_, authz_project) = LookupPath::new(opctx, datastore)
            .project_id(project.identity.id)
            .lookup_for(authz::Action::CreateChild)
            .await
            .expect("project must exist");
        authz_project
    }

    async fn create_instance(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
        authz_project: &authz::Project,
        restart_policy: InstanceAutoRestart,
        state: InstanceState,
    ) -> InstanceUuid {
        let id = InstanceUuid::from_untyped_uuid(Uuid::new_v4());
        // Use the first chunk of the UUID as the name, to avoid conflicts.
        // Start with a lower ascii character to satisfy the name constraints.
        let name = format!("instance-{id}").parse().unwrap();
        let mut instance = Instance::new(
            id,
            authz_project.id(),
            &params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name,
                    description: "It's an instance".into(),
                },
                ncpus: 2i64.try_into().unwrap(),
                memory: ByteCount::from_gibibytes_u32(16),
                hostname: "myhostname".try_into().unwrap(),
                user_data: Vec::new(),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::None,
                external_ips: Vec::new(),
                disks: Vec::new(),
                ssh_public_keys: None,
                start: false,
                auto_restart_policy: Some(restart_policy),
            },
        );
        let datastore = cptestctx.server.server_context().nexus.datastore();

        let instance = datastore
            .project_create_instance(opctx, authz_project, instance)
            .await
            .expect("test instance should be created successfully");
        let prev_state = instance.runtime_state;
        datastore
            .instance_update_runtime(
                &id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    nexus_state: state,
                    r#gen: nexus_db_model::Generation(prev_state.r#gen.next()),
                    ..prev_state
                },
            )
            .await
            .expect("instance runtime state should update");
        eprintln!("instance {id}: policy={restart_policy:?}; state={state:?}");
        id
    }

    #[nexus_test(server = crate::Server)]
    async fn test_reincarnates_failed_instances(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let authz_project = setup_test_project(&cptestctx, &opctx).await;

        let starter = Arc::new(NoopStartSaga::new());
        let mut task =
            InstanceReincarnation::new(datastore.clone(), starter.clone());

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            serde_json::json!(InstanceReincarnationStatus::default())
        );
        assert_eq!(starter.count_reset(), 0);

        // Create an instance in the `Failed` state that's eligible to be
        // restarted.
        let instance_id = create_instance(
            &cptestctx,
            &opctx,
            &authz_project,
            InstanceAutoRestart::AllFailures,
            InstanceState::Failed,
        )
        .await;

        // Activate the task again, and check that our instance had an
        // instance-start saga  started.
        let result = task.activate(&opctx).await;
        let status =
            serde_json::from_value::<InstanceReincarnationStatus>(result)
                .expect("JSON must be correctly shaped");
        eprintln!("activation: {status:#?}");

        assert_eq!(starter.count_reset(), 1);
        assert_eq!(status.instances_found, 1);
        assert_eq!(
            status.instances_reincarnated,
            vec![instance_id.into_untyped_uuid()]
        );
        assert_eq!(status.already_reincarnated, Vec::new());
        assert_eq!(status.query_error, None);
        assert_eq!(status.restart_errors, Vec::new());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_only_reincarnates_eligible_instances(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let authz_project = setup_test_project(&cptestctx, &opctx).await;

        let starter = Arc::new(NoopStartSaga::new());
        let mut task =
            InstanceReincarnation::new(datastore.clone(), starter.clone());

        // Create instances in the `Failed` state that are eligible to be
        // restarted.
        let mut will_reincarnate = std::collections::BTreeSet::new();
        for _ in 0..3 {
            let id = create_instance(
                &cptestctx,
                &opctx,
                &authz_project,
                InstanceAutoRestart::AllFailures,
                InstanceState::Failed,
            )
            .await;
            will_reincarnate.insert(id.into_untyped_uuid());
        }

        // Create some instances that will not reicnarnate.
        let mut will_not_reincarnate = std::collections::BTreeSet::new();
        // Some instances which are `Failed`` but don't have policies permitting
        // them to be reincarnated.
        for policy in
            [InstanceAutoRestart::Never, InstanceAutoRestart::SledFailuresOnly]
        {
            let id = create_instance(
                &cptestctx,
                &opctx,
                &authz_project,
                policy,
                InstanceState::Failed,
            )
            .await;
            will_not_reincarnate.insert(id.into_untyped_uuid());
        }

        // Some instances with policies permitting them to be reincarnated, but
        // which are not `Failed`.
        for _ in 0..2 {
            let id = create_instance(
                &cptestctx,
                &opctx,
                &authz_project,
                InstanceAutoRestart::AllFailures,
                InstanceState::NoVmm,
            )
            .await;
            will_not_reincarnate.insert(id.into_untyped_uuid());
        }

        // Activate the task again, and check that our instance had an
        // instance-start saga started.
        let result = task.activate(&opctx).await;
        let status =
            serde_json::from_value::<InstanceReincarnationStatus>(result)
                .expect("JSON must be correctly shaped");
        eprintln!("activation: {status:#?}");

        assert_eq!(starter.count_reset(), will_reincarnate.len() as u64);
        assert_eq!(status.instances_found, will_reincarnate.len());
        assert_eq!(status.already_reincarnated, Vec::new());
        assert_eq!(status.query_error, None);
        assert_eq!(status.restart_errors, Vec::new());

        for id in &status.instances_reincarnated {
            eprintln!("instance {id} reincarnated");
            assert!(
                !will_not_reincarnate.contains(id),
                "expected {id} not to reincarnate! reincarnated: {:?}",
                status.instances_reincarnated
            );
        }

        for id in will_reincarnate {
            assert!(
                status.instances_reincarnated.contains(&id),
                "expected {id} to have reincarnated! reincarnated: {:?}",
                status.instances_reincarnated
            )
        }
    }
}
