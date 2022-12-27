// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::ACTION_GENERATE_ID;
use crate::app::sagas::declare_saga_actions;
use crate::context::OpContext;
use crate::db::model::VpcRouterKind;
use crate::db::queries::vpc_subnet::SubnetError;
use crate::external_api::params;
use crate::{authn, authz, db};
use nexus_defaults as defaults;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// vpc create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub vpc_create: params::VpcCreate,
    pub authz_project: authz::Project,
}

// vpc create saga: actions

declare_saga_actions! {
    vpc_create;
    VPC_CREATE_VPC -> "vpc" {
        + svc_create_vpc
    }
    VPC_CREATE_ROUTER -> "router" {
        + svc_create_router
    }
    VPC_CREATE_ROUTE -> "route" {
        + svc_create_route
    }
    VPC_CREATE_SUBNET -> "subnet" {
        + svc_create_subnet
    }
    VPC_UPDATE_FIREWALL -> "firewall" {
        + svc_update_firewall
    }
    VPC_NOTIFY_SLEDS -> "no_result" {
        + svc_notify_sleds
    }
}

// vpc create saga: definition

/// Identical to [SagaVpcCreate::make_saga_dag], but using types
/// to identify that parameters do not need to be supplied as input.
pub fn create_dag(
    mut builder: steno::DagBuilder,
) -> Result<steno::Dag, super::SagaInitError> {
    builder.append(Node::action(
        "vpc_id",
        "GenerateVpcId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(Node::action(
        "system_router_id",
        "GenerateSystemRouterId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(Node::action(
        "default_route_id",
        "GenerateDefaultRouteId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(Node::action(
        "default_subnet_id",
        "GenerateDefaultSubnetId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(vpc_create_vpc_action());
    builder.append(vpc_create_router_action());
    builder.append(vpc_create_route_action());
    builder.append(vpc_create_subnet_action());
    builder.append(vpc_update_firewall_action());
    builder.append(vpc_notify_sleds_action());

    Ok(builder.build()?)
}

#[derive(Debug)]
pub struct SagaVpcCreate;
impl NexusSaga for SagaVpcCreate {
    const NAME: &'static str = "vpc-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        vpc_create_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        create_dag(builder)
    }
}

// vpc create saga: action implementations

async fn svc_create_vpc(
    sagactx: NexusActionContext,
) -> Result<(authz::Vpc, db::model::Vpc), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let vpc_id = sagactx.lookup::<Uuid>("vpc_id")?;
    let system_router_id = sagactx.lookup::<Uuid>("system_router_id")?;

    // TODO: This is both fake and utter nonsense. It should be eventually
    // replaced with the proper behavior for creating the default route
    // which may not even happen here. Creating the vpc, its system router,
    // and that routers default route should all be a part of the same
    // transaction.
    let vpc = db::model::IncompleteVpc::new(
        vpc_id,
        params.authz_project.id(),
        system_router_id,
        params.vpc_create.clone(),
    )
    .map_err(ActionError::action_failed)?;
    let (authz_vpc, db_vpc) = osagactx
        .datastore()
        .project_create_vpc(&opctx, &params.authz_project, vpc)
        .await
        .map_err(ActionError::action_failed)?;
    Ok((authz_vpc, db_vpc))
}

async fn svc_create_router(
    sagactx: NexusActionContext,
) -> Result<authz::VpcRouter, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let vpc_id = sagactx.lookup::<Uuid>("vpc_id")?;
    let system_router_id = sagactx.lookup::<Uuid>("system_router_id")?;
    let (authz_vpc, _) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;

    // TODO: Ultimately when the VPC is created a system router w/ an
    // appropriate setup should also be created.  Given that the underlying
    // systems aren't wired up yet this is a naive implementation to
    // populate the database with a starting router.
    let router = db::model::VpcRouter::new(
        system_router_id,
        vpc_id,
        VpcRouterKind::System,
        params::VpcRouterCreate {
            identity: IdentityMetadataCreateParams {
                name: "system".parse().unwrap(),
                description: "Routes are automatically added to this \
                    router as vpc subnets are created"
                    .into(),
            },
        },
    );
    let (authz_router, _) = osagactx
        .datastore()
        .vpc_create_router(&opctx, &authz_vpc, router)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(authz_router)
}

async fn svc_create_route(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let default_route_id = sagactx.lookup::<Uuid>("default_route_id")?;
    let system_router_id = sagactx.lookup::<Uuid>("system_router_id")?;
    let authz_router = sagactx.lookup::<authz::VpcRouter>("router")?;

    let route = db::model::RouterRoute::new(
        default_route_id,
        system_router_id,
        RouterRouteKind::Default,
        RouterRouteCreateParams {
            identity: IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
                description: "The default route of a vpc".to_string(),
            },
            target: RouteTarget::InternetGateway("outbound".parse().unwrap()),
            destination: RouteDestination::Vpc(
                params.vpc_create.identity.name.clone(),
            ),
        },
    );

    osagactx
        .datastore()
        .router_create_route(&opctx, &authz_router, route)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn svc_create_subnet(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let vpc_id = sagactx.lookup::<Uuid>("vpc_id")?;
    let (authz_vpc, db_vpc) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    let default_subnet_id = sagactx.lookup::<Uuid>("default_subnet_id")?;

    // Allocate the first /64 sub-range from the requested or created
    // prefix.
    let ipv6_block = external::Ipv6Net(
        ipnetwork::Ipv6Network::new(db_vpc.ipv6_prefix.network(), 64)
            .map_err(|_| {
                external::Error::internal_error(
                    "Failed to allocate default IPv6 subnet",
                )
            })
            .map_err(ActionError::action_failed)?,
    );

    let subnet = db::model::VpcSubnet::new(
        default_subnet_id,
        vpc_id,
        IdentityMetadataCreateParams {
            name: "default".parse().unwrap(),
            description: format!(
                "The default subnet for {}",
                params.vpc_create.identity.name
            ),
        },
        *defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK,
        ipv6_block,
    );

    // Create the subnet record in the database. Overlapping IP ranges
    // should be translated into an internal error. That implies that
    // there's already an existing VPC Subnet, but we're explicitly creating
    // the _first_ VPC in the project. Something is wrong, and likely a bug
    // in our code.
    osagactx
        .datastore()
        .vpc_create_subnet(&opctx, &authz_vpc, subnet)
        .await
        .map_err(|err| match err {
            SubnetError::OverlappingIpRange(ip) => {
                let ipv4_block = &defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK;
                let log = sagactx.user_data().log();
                error!(
                    log,
                    concat!(
                        "failed to create default VPC Subnet, IP address ",
                        "range '{}' overlaps with existing",
                    ),
                    ip;
                    "vpc_id" => ?vpc_id,
                    "subnet_id" => ?default_subnet_id,
                    "ipv4_block" => ?**ipv4_block,
                    "ipv6_block" => ?ipv6_block,
                );
                external::Error::internal_error(
                    "Failed to create default VPC Subnet, \
                        found overlapping IP address ranges",
                )
            }
            SubnetError::External(e) => e,
        })
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn svc_update_firewall(
    sagactx: NexusActionContext,
) -> Result<Vec<db::model::VpcFirewallRule>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let (authz_vpc, _) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    let rules = osagactx
        .nexus()
        .default_firewall_rules_for_vpc(
            authz_vpc.id(),
            params.vpc_create.identity.name.clone().into(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    osagactx
        .datastore()
        .vpc_update_firewall_rules(&opctx, &authz_vpc, rules.clone())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(rules)
}

async fn svc_notify_sleds(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let (_, db_vpc) = sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    let rules =
        sagactx.lookup::<Vec<db::model::VpcFirewallRule>>("firewall")?;

    osagactx
        .nexus()
        .send_sled_agents_firewall_rules(&opctx, &db_vpc, &rules)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
