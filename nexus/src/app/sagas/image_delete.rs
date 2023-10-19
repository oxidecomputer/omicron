// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz, db};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum ImageParam {
    Project { authz_image: authz::ProjectImage, image: db::model::ProjectImage },

    Silo { authz_image: authz::SiloImage, image: db::model::SiloImage },
}

impl ImageParam {
    fn volume_id(&self) -> Uuid {
        match self {
            ImageParam::Project { image, .. } => image.volume_id,

            ImageParam::Silo { image, .. } => image.volume_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub image_param: ImageParam,
}

declare_saga_actions! {
    image_delete;
    DELETE_IMAGE_RECORD -> "no_result1" {
        + sid_delete_image_record
    }
}

#[derive(Debug)]
pub(crate) struct SagaImageDelete;
impl NexusSaga for SagaImageDelete {
    const NAME: &'static str = "image-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        image_delete_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_image_record_action());

        const DELETE_VOLUME_PARAMS: &'static str = "delete_volume_params";

        let volume_delete_params = sagas::volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.image_param.volume_id(),
        };
        builder.append(Node::constant(
            DELETE_VOLUME_PARAMS,
            serde_json::to_value(&volume_delete_params).map_err(|e| {
                super::SagaInitError::SerializeError(
                    String::from("volume_id"),
                    e,
                )
            })?,
        ));

        let make_volume_delete_dag = || {
            let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
                sagas::volume_delete::SagaVolumeDelete::NAME,
            ));
            sagas::volume_delete::create_dag(subsaga_builder)
        };
        builder.append(steno::Node::subsaga(
            "delete_volume",
            make_volume_delete_dag()?,
            DELETE_VOLUME_PARAMS,
        ));

        Ok(builder.build()?)
    }
}

// image delete saga: action implementations

async fn sid_delete_image_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    match params.image_param {
        ImageParam::Project { authz_image, image } => {
            osagactx
                .datastore()
                .project_image_delete(&opctx, &authz_image, image)
                .await
                .map_err(ActionError::action_failed)?;
        }

        ImageParam::Silo { authz_image, image } => {
            osagactx
                .datastore()
                .silo_image_delete(&opctx, &authz_image, image)
                .await
                .map_err(ActionError::action_failed)?;
        }
    }

    Ok(())
}
