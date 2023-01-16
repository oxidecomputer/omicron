// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// disk delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub disk_id: Uuid,
}

// disk delete saga: actions

declare_saga_actions! {
    disk_delete;
    DELETE_DISK_RECORD -> "volume_id" {
        // TODO: See the comment on the "DeleteRegions" step,
        // we may want to un-delete the disk if we cannot remove
        // underlying regions.
        + sdd_delete_disk_record
    }
    DELETE_VOLUME -> "no_result" {
        + sdd_delete_volume
    }
}

// disk delete saga: definition

#[derive(Debug)]
pub struct SagaDiskDelete;
impl NexusSaga for SagaDiskDelete {
    const NAME: &'static str = "disk-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        disk_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_disk_record_action());
        builder.append(delete_volume_action());
        Ok(builder.build()?)
    }
}

// disk delete saga: action implementations

async fn sdd_delete_disk_record(
    sagactx: NexusActionContext,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let volume_id = osagactx
        .datastore()
        .project_delete_disk_no_auth(&params.disk_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(volume_id)
}

async fn sdd_delete_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx
        .nexus()
        .volume_delete(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_delete::Params,
        app::sagas::disk_delete::SagaDiskDelete, context::OpContext, db,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_test_utils::resource_helpers::create_ip_pool;
    use nexus_test_utils::resource_helpers::create_organization;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::Name;
    use ref_cast::RefCast;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const ORG_NAME: &str = "test-org";
    const PROJECT_NAME: &str = "springfield-squidport";

    async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
        create_ip_pool(&client, "p0", None).await;
        create_organization(&client, ORG_NAME).await;
        let project = create_project(client, ORG_NAME, PROJECT_NAME).await;
        project.identity.id
    }

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.apictx.nexus.datastore().clone(),
        )
    }

    async fn create_disk(cptestctx: &ControlPlaneTestContext) -> Uuid {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        nexus
            .project_create_disk(
                &opctx,
                db::model::Name::ref_cast(
                    &Name::try_from(ORG_NAME.to_string()).unwrap(),
                ),
                db::model::Name::ref_cast(
                    &Name::try_from(PROJECT_NAME.to_string()).unwrap(),
                ),
                &crate::app::sagas::disk_create::test::new_disk_create_params(),
            )
            .await
            .expect("Failed to create disk")
            .id()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx.nexus;
        create_org_and_project(&client).await;
        let disk_id = create_disk(&cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let params = Params { disk_id };
        let dag = create_saga_dag::<SagaDiskDelete>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        nexus.run_saga(runnable_saga).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx.nexus;
        create_org_and_project(&client).await;
        let disk_id = create_disk(&cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let params = Params { disk_id };
        let dag = create_saga_dag::<SagaDiskDelete>(params).unwrap();

        let runnable_saga =
            nexus.create_runnable_saga(dag.clone()).await.unwrap();

        // Cause all actions to run twice. The saga should succeed regardless!
        for node in dag.get_nodes() {
            nexus
                .sec()
                .saga_inject_repeat(
                    runnable_saga.id(),
                    node.index(),
                    steno::RepeatInjected {
                        action: NonZeroU32::new(2).unwrap(),
                        undo: NonZeroU32::new(1).unwrap(),
                    },
                )
                .await
                .unwrap();
        }

        // Verify that the saga's execution succeeded.
        nexus
            .run_saga(runnable_saga)
            .await
            .expect("Saga should have succeeded");

        crate::app::sagas::disk_create::test::verify_clean_slate(
            &cptestctx, &test,
        )
        .await;
    }
}
