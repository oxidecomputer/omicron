// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz, db};
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum ImageParam {
    Project { authz_image: authz::ProjectImage, image: db::model::ProjectImage },

    Silo { authz_image: authz::SiloImage, image: db::model::SiloImage },
}

impl ImageParam {
    fn volume_id(&self) -> VolumeUuid {
        match self {
            ImageParam::Project { image, .. } => image.volume_id(),

            ImageParam::Silo { image, .. } => image.volume_id(),
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
    SOFT_DELETE_VOLUME -> "soft_delete_volume" {
        + sid_soft_delete_volume
    }
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
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_image_record_action());
        builder.append(soft_delete_volume_action());

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
                .map_err(saga_action_failed)?;
        }

        ImageParam::Silo { authz_image, image } => {
            osagactx
                .datastore()
                .silo_image_delete(&opctx, &authz_image, image)
                .await
                .map_err(saga_action_failed)?;
        }
    }

    Ok(())
}

async fn sid_soft_delete_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();

    osagactx
        .datastore()
        .soft_delete_volume(params.image_param.volume_id())
        .await
        .map_err(|e| {
            saga_action_failed(Error::internal_error(&format!(
                "failed to soft_delete_volume: {:?}",
                e,
            )))
        })?;

    Ok(())
}
