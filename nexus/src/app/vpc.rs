// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPCs and firewall rules

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::VpcRouterKind;
use crate::db::queries::vpc_subnet::SubnetError;
use crate::defaults;
use crate::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use uuid::Uuid;

impl super::Nexus {
    // VPCs

    pub async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::VpcCreate,
    ) -> CreateResult<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let vpc_id = Uuid::new_v4();
        let system_router_id = Uuid::new_v4();
        let default_route_id = Uuid::new_v4();
        let default_subnet_id = Uuid::new_v4();

        // TODO: This is both fake and utter nonsense. It should be eventually
        // replaced with the proper behavior for creating the default route
        // which may not even happen here. Creating the vpc, its system router,
        // and that routers default route should all be a part of the same
        // transaction.
        let vpc = db::model::IncompleteVpc::new(
            vpc_id,
            authz_project.id(),
            system_router_id,
            params.clone(),
        )?;
        let (authz_vpc, db_vpc) = self
            .db_datastore
            .project_create_vpc(opctx, &authz_project, vpc)
            .await?;

        // TODO: Ultimately when the VPC is created a system router w/ an
        // appropriate setup should also be created.  Given that the underlying
        // systems aren't wired up yet this is a naive implementation to
        // populate the database with a starting router. Eventually this code
        // should be replaced with a saga that'll handle creating the VPC and
        // its underlying system
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
        let (authz_router, _) = self
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        let route = db::model::RouterRoute::new(
            default_route_id,
            system_router_id,
            RouterRouteKind::Default,
            RouterRouteCreateParams {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description: "The default route of a vpc".to_string(),
                },
                target: RouteTarget::InternetGateway(
                    "outbound".parse().unwrap(),
                ),
                destination: RouteDestination::Vpc(
                    params.identity.name.clone(),
                ),
            },
        );

        self.db_datastore
            .router_create_route(opctx, &authz_router, route)
            .await?;

        // Allocate the first /64 sub-range from the requested or created
        // prefix.
        let ipv6_block = external::Ipv6Net(
            ipnetwork::Ipv6Network::new(db_vpc.ipv6_prefix.network(), 64)
                .map_err(|_| {
                    external::Error::internal_error(
                        "Failed to allocate default IPv6 subnet",
                    )
                })?,
        );

        // TODO: batch this up with everything above
        let subnet = db::model::VpcSubnet::new(
            default_subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
                description: format!(
                    "The default subnet for {}",
                    params.identity.name
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
        self.db_datastore
            .vpc_create_subnet(opctx, &authz_vpc, subnet)
            .await
            .map_err(|err| match err {
                SubnetError::OverlappingIpRange(ip) => {
                    let ipv4_block = &defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK;
                    error!(
                        self.log,
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
            })?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            defaults::DEFAULT_FIREWALL_RULES.clone(),
        );
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await?;
        Ok(db_vpc)
    }

    pub async fn project_list_vpcs(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_vpcs(&opctx, &authz_project, pagparams)
            .await
    }

    pub async fn vpc_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> LookupResult<db::model::Vpc> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    pub async fn vpc_fetch_by_id(
        &self,
        opctx: &OpContext,
        vpc_id: &Uuid,
    ) -> LookupResult<db::model::Vpc> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .vpc_id(*vpc_id)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    pub async fn project_update_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcUpdate,
    ) -> UpdateResult<db::model::Vpc> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .project_update_vpc(opctx, &authz_vpc, params.clone().into())
            .await
    }

    pub async fn project_delete_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> DeleteResult {
        let (.., authz_vpc, db_vpc) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .fetch()
                .await?;

        let authz_vpc_router = authz::VpcRouter::new(
            authz_vpc.clone(),
            db_vpc.system_router_id,
            LookupType::ById(db_vpc.system_router_id),
        );

        // TODO: This should eventually use a saga to call the
        // networking subsystem to have it clean up the networking resources
        self.db_datastore.vpc_delete_router(&opctx, &authz_vpc_router).await?;
        self.db_datastore.project_delete_vpc(opctx, &authz_vpc).await?;

        // Delete all firewall rules after deleting the VPC, to ensure no
        // firewall rules get added between rules deletion and VPC deletion.
        self.db_datastore
            .vpc_delete_all_firewall_rules(&opctx, &authz_vpc)
            .await
    }

    // Firewall rules

    pub async fn vpc_list_firewall_rules(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> ListResultVec<db::model::VpcFirewallRule> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Read)
            .await?;
        let rules = self
            .db_datastore
            .vpc_list_firewall_rules(&opctx, &authz_vpc)
            .await?;
        Ok(rules)
    }

    pub async fn vpc_update_firewall_rules(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &VpcFirewallRuleUpdateParams,
    ) -> UpdateResult<Vec<db::model::VpcFirewallRule>> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            params.clone(),
        );
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await
    }
}
