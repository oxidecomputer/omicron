// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::saga_generate_uuid;
use crate::context::OpContext;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::external_api::shared;
use crate::saga_interface::SagaContext;
use crate::{authn, authz, db};
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

pub const SAGA_NAME: &'static str = "silo-create";

lazy_static! {
    pub static ref SAGA_TEMPLATE: Arc<SagaTemplate<SagaSiloCreate>> =
        Arc::new(saga_silo_create());
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub create_params: params::SiloCreate,
}

#[derive(Debug)]
pub struct SagaSiloCreate;

impl SagaType for SagaSiloCreate {
    type SagaParamsType = Arc<Params>;
    type ExecContextType = Arc<SagaContext>;
}

fn saga_silo_create() -> SagaTemplate<SagaSiloCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "silo_id",
        "GenerateSiloId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "silo_admin_group_id",
        "GenerateSiloAdminGroupId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "created_silo",
        "CreateSiloRecord",
        ActionFunc::new_action(
            saga_create_silo_record,
            saga_create_silo_record_undo,
        ),
    );

    template_builder.append(
        "created_silo_admin_group",
        "CreateSiloAdminGroup",
        ActionFunc::new_action(
            saga_create_silo_admin_group,
            saga_create_silo_admin_group_undo,
        ),
    );

    template_builder.build()
}

async fn saga_create_silo_record(
    sagactx: ActionContext<SagaSiloCreate>,
) -> Result<db::model::Silo, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

    let silo_id = sagactx.lookup::<Uuid>("silo_id")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let silo = osagactx
        .datastore()
        .silo_create(
            &opctx,
            db::model::Silo::new_with_id(silo_id, params.create_params.clone()),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(silo)
}

async fn saga_create_silo_record_undo(
    sagactx: ActionContext<SagaSiloCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let nexus = osagactx.nexus();

    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let silo_id = sagactx.lookup::<Uuid>("silo_id")?;

    nexus.silo_delete_by_id(&opctx, silo_id).await?;

    Ok(())
}

async fn saga_create_silo_admin_group(
    sagactx: ActionContext<SagaSiloCreate>,
) -> Result<Option<db::model::SiloGroup>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

    if let Some(admin_group_name) = &params.create_params.admin_group_name {
        let opctx =
            OpContext::for_saga_action(&sagactx, &params.serialized_authn);

        let silo = sagactx.lookup::<db::model::Silo>("created_silo")?;
        let silo_admin_group_id =
            sagactx.lookup::<Uuid>("silo_admin_group_id")?;

        let silo_group = osagactx.datastore().silo_group_create(
            &opctx,
            db::model::SiloGroup::new(
                silo_admin_group_id,
                IdentityMetadataCreateParams {
                    name: admin_group_name.parse().map_err(|_|
                        ActionError::action_failed(
                            Error::invalid_request(&format!(
                                "could not parse admin group name {} during silo_create",
                                admin_group_name,
                            ))
                        )
                    )?,
                    description: "".into(),
                },
                silo.id(),
            )
        )
        .await
        .map_err(ActionError::action_failed)?;

        // Grant silo admin role for members of the admin group.
        let policy = shared::Policy {
            role_assignments: vec![shared::RoleAssignment {
                identity_type: shared::IdentityType::SiloGroup,
                identity_id: silo_group.id(),
                role_name: authz::SiloRole::Admin,
            }],
        };

        osagactx
            .nexus()
            .silo_update_policy(&opctx, silo.name(), &policy)
            .await
            .map_err(ActionError::action_failed)?;

        Ok(Some(silo_group))
    } else {
        Ok(None)
    }
}

async fn saga_create_silo_admin_group_undo(
    sagactx: ActionContext<SagaSiloCreate>,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

    let silo_admin_group_id = sagactx.lookup::<Uuid>("silo_admin_group_id")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let (_authz_silo, authz_silo_group, _db_silo_group) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .silo_group_id(silo_admin_group_id)
            .fetch_for(authz::Action::Delete)
            .await
            .map_err(ActionError::action_failed)?;

    osagactx.datastore().silo_group_delete(&opctx, &authz_silo_group).await?;

    Ok(())
}
