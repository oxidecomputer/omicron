// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::InlineErrorChain;
use crate::app::sagas::SagaInitError;
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::sled_out_of_service_gone_check;
use crate::app::sagas::volume_delete;
use crate::app::sagas::zpool_out_of_service_gone_check;
use nexus_db_queries::authn;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore;
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::backoff::backon_retry_policy_internal_service;
use progenitor_extras::retry::{
    GoneCheckResult, retry_operation_while_indefinitely,
};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::LocalStorageDatasetDeleteRequest;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// disk delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub disk: datastore::Disk,
}

// disk delete saga: actions

declare_saga_actions! {
    disk_delete;
    DELETE_DISK_RECORD -> "deleted_disk" {
        + sdd_delete_disk_record
        - sdd_delete_disk_record_undo
    }
    SPACE_ACCOUNT -> "no_result1" {
        + sdd_account_space
        - sdd_account_space_undo
    }
    DELETE_LOCAL_STORAGE -> "delete_local_storage" {
        + sdd_delete_local_storage
    }
    DEALLOCATE_LOCAL_STORAGE -> "deallocate_local_storage" {
        + sdd_deallocate_local_storage
    }
}

// disk delete saga: definition

#[derive(Debug)]
pub(crate) struct SagaDiskDelete;
impl NexusSaga for SagaDiskDelete {
    const NAME: &'static str = "disk-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        disk_delete_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_disk_record_action());
        builder.append(space_account_action());

        match &params.disk {
            datastore::Disk::Crucible(disk) => {
                let subsaga_params = volume_delete::Params {
                    serialized_authn: params.serialized_authn.clone(),
                    volume_id: disk.volume_id(),
                };

                let subsaga_dag = {
                    let subsaga_builder =
                        steno::DagBuilder::new(steno::SagaName::new(
                            volume_delete::SagaVolumeDelete::NAME,
                        ));
                    volume_delete::SagaVolumeDelete::make_saga_dag(
                        &subsaga_params,
                        subsaga_builder,
                    )?
                };

                builder.append(Node::constant(
                    "params_for_volume_delete_subsaga",
                    serde_json::to_value(&subsaga_params).map_err(|e| {
                        SagaInitError::SerializeError(
                            "params_for_volume_delete_subsaga".to_string(),
                            e,
                        )
                    })?,
                ));

                builder.append(Node::subsaga(
                    "volume_delete_subsaga_no_result",
                    subsaga_dag,
                    "params_for_volume_delete_subsaga",
                ));
            }

            datastore::Disk::LocalStorage(_) => {
                // Attempt deleting the local storage before removing the
                // database record. If the delete does not succeed, at least the
                // user can re-request the deletion.

                builder.append(delete_local_storage_action());
                builder.append(deallocate_local_storage_action());
            }
        }

        Ok(builder.build()?)
    }
}

// disk delete saga: action implementations

async fn sdd_delete_disk_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Disk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk = osagactx
        .datastore()
        .project_delete_disk_no_auth(
            &params.disk.id(),
            &[DiskState::Detached, DiskState::Faulted],
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(disk)
}

async fn sdd_delete_disk_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    osagactx
        .datastore()
        .project_undelete_disk_set_faulted_no_auth(&params.disk.id())
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

async fn sdd_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let deleted_disk = sagactx.lookup::<db::model::Disk>("deleted_disk")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_disk(
            &opctx,
            deleted_disk.id(),
            params.project_id,
            deleted_disk.size,
        )
        .await
        .map_err(saga_action_failed)?;
    Ok(())
}

async fn sdd_account_space_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let deleted_disk = sagactx.lookup::<db::model::Disk>("deleted_disk")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_disk(
            &opctx,
            deleted_disk.id(),
            params.project_id,
            deleted_disk.size,
        )
        .await
        .map_err(saga_action_failed)?;
    Ok(())
}

async fn sdd_delete_local_storage(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let datastore::Disk::LocalStorage(disk) = params.disk else {
        unreachable!(
            "check during `make_saga_dag` should have ensured disk type is \
            local storage"
        );
    };

    let Some(allocation) = disk.local_storage_dataset_allocation else {
        // Nothing to do!
        return Ok(());
    };

    let sled_id = allocation.sled_id();
    let zpool_id = allocation.pool_id().upcast();

    let request = LocalStorageDatasetDeleteRequest {
        zpool_id: allocation.pool_id(),
        dataset_id: allocation.id(),
        encrypted_at_rest: allocation.encrypted_at_rest(),
    };

    // Get a sled agent client

    let sled_agent_client = osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(saga_action_failed)?;

    // Ensure that the local storage is deleted

    let delete_operation = || async {
        sled_agent_client.local_storage_dataset_delete(&request).await
    };

    // Bail out of the retry loop if either the disk or sled is no longer
    // in-service.
    let gone_check = || async {
        match sled_out_of_service_gone_check(
            osagactx.datastore(),
            &opctx,
            sled_id,
        )
        .await?
        {
            GoneCheckResult::StillAvailable => {
                // proceed to zpool check
            }

            GoneCheckResult::Gone => {
                return Ok(GoneCheckResult::Gone);
            }
        }

        zpool_out_of_service_gone_check(osagactx.datastore(), &opctx, zpool_id)
            .await
    };

    let log = osagactx.log().clone();
    let result = retry_operation_while_indefinitely(
        backon_retry_policy_internal_service(),
        delete_operation,
        gone_check,
        |notification| {
            slog::warn!(
                log,
                "failed to delete local storage dataset, retrying in {:?}",
                notification.delay;
                InlineErrorChain::new(&notification.error),
            );
        },
    )
    .await;

    match result {
        Ok(_) => Ok(()),

        // In this case, if the particular disk hosting this local storage was
        // expunged, or if the sled was expunged, then proceed with the rest of
        // the saga.
        Err(e) if e.is_gone() => Ok(()),

        Err(e) => Err(saga_action_failed(Error::internal_error(&format!(
            "failed to delete local storage: {}",
            InlineErrorChain::new(&e)
        )))),
    }
}

async fn sdd_deallocate_local_storage(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let datastore::Disk::LocalStorage(disk) = params.disk else {
        unreachable!(
            "check during `make_saga_dag` should have ensured disk type is \
            local storage"
        );
    };

    osagactx
        .datastore()
        .delete_local_storage_dataset_allocations(&opctx, &disk)
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_delete::Params,
        app::sagas::disk_delete::SagaDiskDelete,
    };
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::LocalStorageUnencryptedDatasetAllocation;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::RendezvousLocalStorageUnencryptedDataset;
    use nexus_db_model::to_db_typed_uuid;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::authz;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::Disk;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::disk;
    use nexus_types::external_api::project;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::ExternalZpoolUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "test-project";

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    pub fn new_disk_create_params() -> disk::DiskCreate {
        disk::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "distributed".parse().expect("Invalid disk name"),
                description: "My disk".to_string(),
            },
            disk_backend: disk::DiskBackend::Distributed {
                disk_source: disk::DiskSource::Blank {
                    block_size: disk::BlockSize(512),
                },
            },
            size: ByteCount::from_gibibytes_u32(1),
        }
    }

    pub fn new_local_disk_create_params() -> disk::DiskCreate {
        disk::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "local".parse().expect("Invalid disk name"),
                description: "My disk".to_string(),
            },
            disk_backend: disk::DiskBackend::Local {},
            size: ByteCount::from_gibibytes_u32(1),
        }
    }

    pub async fn create_disk<F>(
        cptestctx: &ControlPlaneTestContext,
        create_params: F,
    ) -> Disk
    where
        F: Fn() -> disk::DiskCreate,
    {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(&cptestctx);

        let project_selector = project::ProjectSelector {
            project: Name::try_from(PROJECT_NAME.to_string()).unwrap().into(),
        };

        let project_lookup =
            nexus.project_lookup(&opctx, project_selector).unwrap();

        nexus
            .project_create_disk(&opctx, &project_lookup, &create_params())
            .await
            .expect("Failed to create disk")
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id = create_project(client, PROJECT_NAME).await.identity.id;
        let disk = create_disk(&cptestctx, new_disk_create_params).await;

        // Build the saga DAG with the provided test parameters and run it.
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk,
        };

        nexus.sagas.saga_execute::<SagaDiskDelete>(params).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id = create_project(client, PROJECT_NAME).await.identity.id;
        let disk = create_disk(&cptestctx, new_disk_create_params).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk,
        };

        let dag = create_saga_dag::<SagaDiskDelete>(params).unwrap();
        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;

        crate::app::sagas::disk_create::test::verify_clean_slate(
            &cptestctx, &test,
        )
        .await;
    }

    pub struct ExpungeTestHarness<'a> {
        cptestctx: &'a ControlPlaneTestContext,
        zpool_id: ZpoolUuid,
        allocation: LocalStorageUnencryptedDatasetAllocation,
    }

    impl<'a> ExpungeTestHarness<'a> {
        pub async fn setup(
            cptestctx: &'a ControlPlaneTestContext,
            disk_id: Uuid,
            zpool_id: ZpoolUuid,
        ) -> Self {
            // TODO normally these allocations are only performed during sled
            // reservation so manually inserting records bypasses the need for
            // also creating an instance in this test. Further, Nexus created
            // for unit and integration tests using the `nexus_test` does _not_
            // perform reconfigurator execution, which means local storage
            // datasets are not created, which necessitates extra manual steps
            // here to create the relevant rendezvous table entries. Once this
            // occurs, this test (and others!) will need to be updated.

            let nexus = &cptestctx.server.server_context().nexus;
            let datastore = nexus.datastore();

            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            // Manually insert into rendezvous table

            let local_storage_dataset = datastore
                .local_storage_unencrypted_dataset_insert_if_not_exists(
                    &opctx,
                    RendezvousLocalStorageUnencryptedDataset::new(
                        DatasetUuid::new_v4(),
                        zpool_id,
                        BlueprintUuid::new_v4(),
                    ),
                )
                .await
                .unwrap()
                .unwrap();

            // Manually populate both the `disk_type_local_storage` table's
            // allocation id, and the allocation table.

            let (.., db_zpool) = LookupPath::new(&opctx, datastore)
                .zpool_id(zpool_id)
                .fetch()
                .await
                .unwrap();

            let allocation =
                LocalStorageUnencryptedDatasetAllocation::new_for_tests_only(
                    DatasetUuid::new_v4(),
                    Utc::now(),
                    local_storage_dataset.id(),
                    ExternalZpoolUuid::from_untyped_uuid(
                        db_zpool.id().into_untyped_uuid(),
                    ),
                    db_zpool.sled_id(),
                    ByteCount::from_gibibytes_u32(1).into(),
                );

            {
                let conn = datastore.pool_connection_for_tests().await.unwrap();

                use nexus_db_schema::schema::disk_type_local_storage::dsl;

                diesel::update(dsl::disk_type_local_storage)
                    .filter(dsl::disk_id.eq(disk_id))
                    .set(
                        dsl::local_storage_unencrypted_dataset_allocation_id
                            .eq(to_db_typed_uuid(allocation.id())),
                    )
                    .execute_async(&*conn)
                    .await
                    .unwrap();
            }

            {
                let conn = datastore.pool_connection_for_tests().await.unwrap();

                use nexus_db_schema::schema::local_storage_unencrypted_dataset_allocation::dsl;

                diesel::insert_into(
                    dsl::local_storage_unencrypted_dataset_allocation,
                )
                .values(allocation.clone())
                .execute_async(&*conn)
                .await
                .unwrap();
            }

            ExpungeTestHarness { cptestctx, zpool_id, allocation }
        }

        pub async fn expunge_sled(&self) {
            // Set the sled backing the zpool's policy to expunged

            let nexus = &self.cptestctx.server.server_context().nexus;
            let datastore = nexus.datastore();

            let opctx = OpContext::for_tests(
                self.cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            let (.., db_zpool) = LookupPath::new(&opctx, datastore)
                .zpool_id(self.zpool_id)
                .fetch()
                .await
                .unwrap();

            let (.., authz_sled) = LookupPath::new(&opctx, datastore)
                .sled_id(db_zpool.sled_id())
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();

            datastore
                .sled_set_policy_to_expunged(&opctx, &authz_sled)
                .await
                .unwrap();
        }

        pub async fn expunge_disk(&self) {
            // Set the physical disk backing the zpool's policy to expunged

            let nexus = &self.cptestctx.server.server_context().nexus;
            let datastore = nexus.datastore();

            let opctx = OpContext::for_tests(
                self.cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            let (.., db_zpool) = LookupPath::new(&opctx, datastore)
                .zpool_id(self.zpool_id)
                .fetch()
                .await
                .unwrap();

            datastore
                .physical_disk_update_policy(
                    &opctx,
                    db_zpool.physical_disk_id(),
                    PhysicalDiskPolicy::Expunged,
                )
                .await
                .unwrap();
        }

        pub async fn validate_allocation_deleted(&self) {
            let nexus = &self.cptestctx.server.server_context().nexus;
            let datastore = nexus.datastore();

            let conn = datastore.pool_connection_for_tests().await.unwrap();

            use nexus_db_schema::schema::local_storage_unencrypted_dataset_allocation::dsl;

            let allocation = dsl::local_storage_unencrypted_dataset_allocation
                .filter(dsl::id.eq(to_db_typed_uuid(self.allocation.id())))
                .select(LocalStorageUnencryptedDatasetAllocation::as_select())
                .get_result_async(&*conn)
                .await
                .unwrap();

            assert!(allocation.time_deleted.is_some());
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_delete_local_disk_backed_by_expunged_sled(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let disk_test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );

        let project_id = create_project(client, PROJECT_NAME).await.identity.id;
        let disk = create_disk(&cptestctx, new_local_disk_create_params).await;

        // Create a local storage disk, manually create an allocation for that
        // local storage on a pool, then expunge the sled backing the pool.

        let zpool = disk_test.zpools().next().unwrap();

        let harness =
            ExpungeTestHarness::setup(cptestctx, disk.id(), zpool.id).await;

        harness.expunge_sled().await;

        // Now that the disk' local storage specific information has been
        // populated, re-fetch it.

        let disk = datastore.disk_get(&opctx, disk.id()).await.unwrap();

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk,
        };

        nexus.sagas.saga_execute::<SagaDiskDelete>(params).await.unwrap();

        harness.validate_allocation_deleted().await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_delete_local_disk_backed_by_expunged_zpool(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let disk_test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );

        let project_id = create_project(client, PROJECT_NAME).await.identity.id;
        let disk = create_disk(&cptestctx, new_local_disk_create_params).await;

        // Create a local storage disk, manually create an allocation for that
        // local storage on a pool, then expunge the sled backing the pool.

        let zpool = disk_test.zpools().next().unwrap();

        let harness =
            ExpungeTestHarness::setup(cptestctx, disk.id(), zpool.id).await;

        harness.expunge_disk().await;

        // Now that the disk' local storage specific information has been
        // populated, re-fetch it.

        let disk = datastore.disk_get(&opctx, disk.id()).await.unwrap();

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk,
        };

        nexus.sagas.saga_execute::<SagaDiskDelete>(params).await.unwrap();

        harness.validate_allocation_deleted().await;
    }
}
