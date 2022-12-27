// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas;
use crate::app::sagas::declare_saga_actions;
use crate::context::OpContext;
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::{authn, authz, db};
use nexus_defaults as defaults;
use nexus_types::identity::Resource;
use omicron_common::api::external::IdentityMetadataCreateParams;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// project create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_create: params::ProjectCreate,
    pub authz_org: authz::Organization,
}

// project create saga: actions

declare_saga_actions! {
    project_create;
    PROJECT_CREATE_RECORD -> "project" {
        + spc_create_record
    }
    PROJECT_CREATE_VPC_PARAMS -> "vpc_create_params" {
        + spc_create_vpc_params
    }
}

// project create saga: definition

#[derive(Debug)]
pub struct SagaProjectCreate;
impl NexusSaga for SagaProjectCreate {
    const NAME: &'static str = "project-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        project_create_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(project_create_record_action());
        builder.append(project_create_vpc_params_action());

        let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
            sagas::vpc_create::SagaVpcCreate::NAME,
        ));
        builder.append(steno::Node::subsaga(
            "vpc",
            sagas::vpc_create::create_dag(subsaga_builder)?,
            "vpc_create_params",
        ));
        Ok(builder.build()?)
    }
}

// project create saga: action implementations

async fn spc_create_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Project, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let db_project =
        db::model::Project::new(params.authz_org.id(), params.project_create);
    osagactx
        .datastore()
        .project_create(&opctx, &params.authz_org, db_project)
        .await
        .map_err(ActionError::action_failed)
}

async fn spc_create_vpc_params(
    sagactx: NexusActionContext,
) -> Result<sagas::vpc_create::Params, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let project_id = sagactx.lookup::<db::model::Project>("project")?.id();
    let ipv6_prefix = Some(
        defaults::random_vpc_ipv6_prefix()
            .map_err(ActionError::action_failed)?,
    );

    let (.., authz_project) = LookupPath::new(&opctx, osagactx.datastore())
        .project_id(project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(ActionError::action_failed)?;

    let vpc_create = params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: "default".parse().unwrap(),
            description: "Default VPC".to_string(),
        },
        ipv6_prefix,
        // TODO-robustness this will need to be None if we decide to
        // handle the logic around name and dns_name by making
        // dns_name optional
        dns_name: "default".parse().unwrap(),
    };
    let saga_params = sagas::vpc_create::Params {
        serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
        vpc_create,
        authz_project,
    };
    Ok(saga_params)
}
