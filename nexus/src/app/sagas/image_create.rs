// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use nexus_db_lookup::LookupPath;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// image create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum ImageType {
    Project { authz_silo: authz::Silo, authz_project: authz::Project },

    Silo { authz_silo: authz::Silo },
}

impl ImageType {
    fn silo_id(&self) -> Uuid {
        match self {
            ImageType::Project { authz_silo, .. }
            | ImageType::Silo { authz_silo } => authz_silo.id(),
        }
    }

    fn project_id(&self) -> Option<Uuid> {
        match self {
            ImageType::Project { authz_project, .. } => {
                Some(authz_project.id())
            }

            ImageType::Silo { .. } => None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub image_type: ImageType,
    pub create_params: params::ImageCreate,
}

// image create saga: actions

declare_saga_actions! {
    image_create;
    GET_SOURCE_VOLUME -> "source_volume" {
        + simc_get_source_volume
        - simc_get_source_volume_undo
    }
    CREATE_IMAGE_RECORD -> "created_image" {
        + simc_create_image_record
        - simc_create_image_record_undo
    }
}

// image create saga: definition

#[derive(Debug)]
pub(crate) struct SagaImageCreate;
impl NexusSaga for SagaImageCreate {
    const NAME: &'static str = "image-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        image_create_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            "image_id",
            "GenerateImageId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "dest_volume_id",
            "GenerateDestVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        match &params.create_params.source {
            params::ImageSource::Snapshot { .. } => {}

            params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine => {
                builder.append(Node::action(
                    "alpine_volume_id",
                    "GenerateAlpineVolumeId",
                    ACTION_GENERATE_ID.as_ref(),
                ));
            }
        }

        builder.append(get_source_volume_action());
        builder.append(create_image_record_action());

        Ok(builder.build()?)
    }
}

// image create saga: action implementations

#[derive(Debug, Deserialize, Serialize)]
struct SourceVolume {
    volume_id: VolumeUuid,
    block_size: db::model::BlockSize,
    size: external::ByteCount,
}

async fn simc_get_source_volume(
    sagactx: NexusActionContext,
) -> Result<SourceVolume, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    match &params.create_params.source {
        params::ImageSource::Snapshot { id } => {
            let (.., db_snapshot) =
                LookupPath::new(&opctx, osagactx.datastore())
                    .snapshot_id(*id)
                    .fetch()
                    .await
                    .map_err(ActionError::action_failed)?;

            if let ImageType::Project { authz_project, .. } = &params.image_type
            {
                if db_snapshot.project_id != authz_project.id() {
                    return Err(ActionError::action_failed(
                        Error::invalid_request(
                            "snapshot does not belong to this project",
                        ),
                    ));
                }
            }

            // Copy the Volume data for this snapshot with randomized ids - this
            // is safe because the snapshot is read-only, and even though
            // volume_checkout will bump the gen numbers multiple Upstairs can
            // connect to read-only downstairs without kicking each other out.

            let dest_volume_id =
                sagactx.lookup::<VolumeUuid>("dest_volume_id")?;

            osagactx
                .datastore()
                .volume_checkout_randomize_ids(
                    db::datastore::SourceVolume(db_snapshot.volume_id()),
                    db::datastore::DestVolume(dest_volume_id),
                    db::datastore::VolumeCheckoutReason::ReadOnlyCopy,
                )
                .await
                .map_err(ActionError::action_failed)?;

            Ok(SourceVolume {
                volume_id: dest_volume_id,
                block_size: db_snapshot.block_size,
                size: db_snapshot.size.into(),
            })
        }

        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine => {
            let alpine_volume_id =
                sagactx.lookup::<VolumeUuid>("alpine_volume_id")?;

            // Each Propolis zone ships with an alpine.iso (it's part of the
            // package-manifest.toml blobs), and for development purposes allow
            // users to boot that. This should go away when that blob does.
            let block_size = db::model::BlockSize::Traditional;

            let volume_construction_request =
                sled_agent_client::VolumeConstructionRequest::File {
                    id: alpine_volume_id.into_untyped_uuid(),
                    block_size: u64::from(block_size.to_bytes()),
                    path: "/opt/oxide/propolis-server/blob/alpine.iso".into(),
                };

            osagactx
                .datastore()
                .volume_create(alpine_volume_id, volume_construction_request)
                .await
                .map_err(ActionError::action_failed)?;

            // Nexus runs in its own zone so we can't ask the propolis zone
            // image tar file for size of alpine.iso. Conservatively set the
            // size to 100M (at the time of this comment, it's 41M). Any disk
            // created from this image has to be larger than it, and our
            // smallest disk size is 1G, so this is valid.
            let size: u64 = 100 * 1024 * 1024;
            let size: external::ByteCount = size.try_into().map_err(|e| {
                ActionError::action_failed(Error::invalid_value(
                    "size",
                    format!("size is invalid: {}", e),
                ))
            })?;

            Ok(SourceVolume { volume_id: alpine_volume_id, block_size, size })
        }
    }
}

async fn simc_get_source_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    let source_volume = sagactx.lookup::<SourceVolume>("source_volume")?;

    osagactx.datastore().soft_delete_volume(source_volume.volume_id).await?;

    Ok(())
}

async fn simc_create_image_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Image, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let image_id = sagactx.lookup::<Uuid>("image_id")?;
    let source_volume = sagactx.lookup::<SourceVolume>("source_volume")?;

    let record = match &params.create_params.source {
        params::ImageSource::Snapshot { .. } => {
            db::model::Image {
                identity: db::model::ImageIdentity::new(
                    image_id,
                    params.create_params.identity.clone(),
                ),
                silo_id: params.image_type.silo_id(),
                project_id: params.image_type.project_id(),
                volume_id: source_volume.volume_id.into(),
                url: None,
                os: params.create_params.os.clone(),
                version: params.create_params.version.clone(),
                digest: None, // TODO
                block_size: source_volume.block_size,
                size: source_volume.size.into(),
            }
        }

        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine => {
            db::model::Image {
                identity: db::model::ImageIdentity::new(
                    image_id,
                    params.create_params.identity.clone(),
                ),
                silo_id: params.image_type.silo_id(),
                project_id: params.image_type.project_id(),
                volume_id: source_volume.volume_id.into(),
                url: None,
                os: "alpine".into(),
                version: "propolis-blob".into(),
                digest: None,
                block_size: source_volume.block_size,
                size: source_volume.size.into(),
            }
        }
    };

    match &params.image_type {
        ImageType::Project { authz_project, .. } => osagactx
            .datastore()
            .project_image_create(
                &opctx,
                &authz_project,
                record.try_into().map_err(ActionError::action_failed)?,
            )
            .await
            .map_err(ActionError::action_failed),

        ImageType::Silo { authz_silo, .. } => osagactx
            .datastore()
            .silo_image_create(
                &opctx,
                &authz_silo,
                record.try_into().map_err(ActionError::action_failed)?,
            )
            .await
            .map_err(ActionError::action_failed),
    }
}

async fn simc_create_image_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let image_id = sagactx.lookup::<Uuid>("image_id")?;

    match &params.image_type {
        ImageType::Project { .. } => {
            let (.., authz_image, db_image) =
                LookupPath::new(&opctx, osagactx.datastore())
                    .project_image_id(image_id)
                    .fetch()
                    .await?;

            osagactx
                .datastore()
                .project_image_delete(&opctx, &authz_image, db_image)
                .await?;
        }

        ImageType::Silo { .. } => {
            let (.., authz_image, db_image) =
                LookupPath::new(&opctx, osagactx.datastore())
                    .silo_image_id(image_id)
                    .fetch()
                    .await?;

            osagactx
                .datastore()
                .silo_image_delete(&opctx, &authz_image, db_image)
                .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{app::saga::create_saga_dag, external_api::params};
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::{authn::saga::Serialized, db::datastore::DataStore};
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_snapshot;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;
    type DiskTest<'a> =
        nexus_test_utils::resource_helpers::DiskTest<'a, crate::Server>;

    const DISK_NAME: &str = "my-disk";
    const PROJECT_NAME: &str = "springfield-squidport";

    async fn new_test_params(
        cptestctx: &ControlPlaneTestContext,
        project_id: Uuid,
    ) -> Params {
        let opctx = test_opctx(cptestctx);
        let datastore = cptestctx.server.server_context().nexus.datastore();

        let (.., authz_silo, _authz_project) =
            LookupPath::new(&opctx, datastore)
                .project_id(project_id)
                .lookup_for(nexus_db_queries::authz::Action::Modify)
                .await
                .unwrap();

        Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            image_type: ImageType::Silo { authz_silo },
            create_params: params::ImageCreate {
                identity: IdentityMetadataCreateParams {
                    name: "image".parse().unwrap(),
                    description: String::from("description"),
                },
                source:
                    params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
                os: "debian".to_string(),
                version: "12".to_string(),
            },
        }
    }

    fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds_project(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();

        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        create_disk(&client, PROJECT_NAME, DISK_NAME).await;

        let snapshot =
            create_snapshot(&client, PROJECT_NAME, DISK_NAME, "snapshot").await;

        // Build the saga DAG with the provided test parameters and run it.
        let opctx = test_opctx(cptestctx);

        let (.., authz_silo, authz_project) =
            LookupPath::new(&opctx, datastore)
                .project_id(project_id)
                .lookup_for(nexus_db_queries::authz::Action::Modify)
                .await
                .unwrap();

        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            image_type: ImageType::Project { authz_silo, authz_project },
            create_params: params::ImageCreate {
                identity: IdentityMetadataCreateParams {
                    name: "image".parse().unwrap(),
                    description: String::from("description"),
                },
                source: params::ImageSource::Snapshot {
                    id: snapshot.identity.id,
                },
                os: "debian".to_string(),
                version: "12".to_string(),
            },
        };

        let output =
            nexus.sagas.saga_execute::<SagaImageCreate>(params).await.unwrap();

        output
            .lookup_node_output::<nexus_db_queries::db::model::Image>(
                "created_image",
            )
            .unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds_silo(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;

        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        // Build the saga DAG with the provided test parameters and run it.

        let params = new_test_params(&cptestctx, project_id).await;

        let output =
            nexus.sagas.saga_execute::<SagaImageCreate>(params).await.unwrap();

        output
            .lookup_node_output::<nexus_db_queries::db::model::Image>(
                "created_image",
            )
            .unwrap();
    }

    async fn no_image_records_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Image;
        use nexus_db_schema::schema::image::dsl;

        // `new_test_params` creates a silo image, so search for those
        dsl::image
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.is_null())
            .select(Image::as_select())
            .first_async::<Image>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .is_none()
    }

    pub(crate) async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let datastore = cptestctx.server.server_context().nexus.datastore();

        crate::app::sagas::test_helpers::assert_no_failed_undo_steps(
            &cptestctx.logctx.log,
            datastore,
        )
        .await;

        assert!(no_image_records_exist(datastore).await);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaImageCreate,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin(async {
                    new_test_params(&cptestctx, project_id).await
                })
            },
            || {
                Box::pin(async {
                    verify_clean_slate(&cptestctx).await;
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
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        crate::app::sagas::test_helpers::action_failure_can_unwind_idempotently::<
            SagaImageCreate,
            _,
            _
        >(
            nexus,
            || Box::pin(async {
                new_test_params(&cptestctx, project_id).await
            }),
            || Box::pin(async { verify_clean_slate(&cptestctx).await; }),
            log
        ).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        let params = new_test_params(&cptestctx, project_id).await;
        let dag = create_saga_dag::<SagaImageCreate>(params).unwrap();
        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;

        // Delete the (silo) image, and verify clean slate

        let image_selector = params::ImageSelector {
            project: None,
            image: Name::try_from("image".to_string()).unwrap().into(),
        };
        let image_lookup =
            nexus.image_lookup(&opctx, image_selector).await.unwrap();

        nexus.image_delete(&opctx, &image_lookup).await.unwrap();

        verify_clean_slate(&cptestctx).await;
    }
}
