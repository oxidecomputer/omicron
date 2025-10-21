// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC Subnets and their network interfaces

use super::sagas;
use crate::external_api::params;
use nexus_auth::authn;
use nexus_config::MIN_VPC_IPV4_SUBNET_PREFIX;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::model::VpcSubnet;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::Ipv6NetExt;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;

impl super::Nexus {
    pub fn vpc_subnet_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        subnet_selector: params::SubnetSelector,
    ) -> LookupResult<lookup::VpcSubnet<'a>> {
        match subnet_selector {
            params::SubnetSelector {
                subnet: NameOrId::Id(id),
                vpc: None,
                project: None,
            } => {
                let subnet = LookupPath::new(opctx, &self.db_datastore)
                    .vpc_subnet_id(id);
                Ok(subnet)
            }
            params::SubnetSelector {
                subnet: NameOrId::Name(name),
                vpc: Some(vpc),
                project,
            } => {
                let subnet = self
                    .vpc_lookup(opctx, params::VpcSelector { project, vpc })?
                    .vpc_subnet_name_owned(name.into());
                Ok(subnet)
            }
            params::SubnetSelector {
                subnet: NameOrId::Id(_),
                vpc: _,
                project: _,
            } => Err(Error::invalid_request(
                "when providing subnet as an ID, vpc and project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "subnet should either be an ID or vpc should be specified",
            )),
        }
    }

    pub(crate) async fn vpc_create_subnet(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        params: &params::VpcSubnetCreate,
    ) -> CreateResult<db::model::VpcSubnet> {
        let (.., authz_vpc, db_vpc) = vpc_lookup.fetch().await?;
        let (.., authz_system_router) =
            LookupPath::new(opctx, self.datastore())
                .vpc_router_id(db_vpc.system_router_id)
                .lookup_for(authz::Action::CreateChild)
                .await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can create VPC subnets
        self.check_networking_restrictions(opctx).await?;
        let custom_router = match &params.custom_router {
            Some(k) => Some(
                self.vpc_router_lookup_for_attach(opctx, k, &authz_vpc).await?,
            ),
            None => None,
        };

        // Validate IPv4 range
        if !params.ipv4_block.prefix().is_private() {
            return Err(external::Error::invalid_request(
                "VPC Subnet IPv4 address ranges must be from a private range",
            ));
        }
        if params.ipv4_block.width() < MIN_VPC_IPV4_SUBNET_PREFIX
            || params.ipv4_block.width()
                > self.tunables.max_vpc_ipv4_subnet_prefix
        {
            return Err(external::Error::invalid_request(&format!(
                "VPC Subnet IPv4 address ranges must have prefix \
                length between {} and {}, inclusive",
                MIN_VPC_IPV4_SUBNET_PREFIX,
                self.tunables.max_vpc_ipv4_subnet_prefix,
            )));
        }

        // If the client provided an IPv6 range, we try to insert that or fail
        // with a conflict error.
        //
        // If they did _not_, we randomly generate a subnet valid for the VPC's
        // prefix, and the insert that. There's a small retry loop if we get
        // unlucky and conflict with an existing IPv6 range. In the case we
        // cannot find a subnet within a small number of retries, we fail the
        // request with a 503.
        //
        // TODO-robustness: We'd really prefer to allocate deterministically.
        // See <https://github.com/oxidecomputer/omicron/issues/685> for
        // details.
        let potential_ipv6_blocks = if let Some(ipv6_block) = params.ipv6_block
        {
            if !ipv6_block.is_vpc_subnet(&db_vpc.ipv6_prefix) {
                return Err(external::Error::invalid_request(&format!(
                    "VPC Subnet IPv6 address range '{}' is not valid for \
                    VPC with IPv6 prefix '{}'",
                    ipv6_block, db_vpc.ipv6_prefix.0,
                )));
            }
            vec![ipv6_block]
        } else {
            const NUM_RETRIES: usize = 2;
            (0..=NUM_RETRIES)
                .map(|_| {
                    db_vpc
                        .ipv6_prefix
                        .random_subnet(
                            oxnet::Ipv6Net::VPC_SUBNET_IPV6_PREFIX_LENGTH,
                        )
                        .map(|block| block.0)
                        .ok_or_else(|| {
                            external::Error::internal_error(
                                "Failed to create random IPv6 subnet",
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let saga_params = sagas::vpc_subnet_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            subnet_create: params.clone(),
            potential_ipv6_blocks,
            custom_router,
            authz_vpc,
            authz_system_router,
        };

        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::vpc_subnet_create::SagaVpcSubnetCreate>(
                saga_params,
            )
            .await?;

        let out = saga_outputs
            .lookup_node_output::<VpcSubnet>("output")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from vpc create saga")?;

        self.vpc_needed_notify_sleds();

        Ok(out)
    }

    pub(crate) async fn vpc_subnet_list(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::VpcSubnet> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore.vpc_subnet_list(opctx, &authz_vpc, pagparams).await
    }

    pub(crate) async fn vpc_update_subnet(
        &self,
        opctx: &OpContext,
        vpc_subnet_lookup: &lookup::VpcSubnet<'_>,
        params: &params::VpcSubnetUpdate,
    ) -> UpdateResult<VpcSubnet> {
        let (.., authz_vpc, authz_subnet) =
            vpc_subnet_lookup.lookup_for(authz::Action::Modify).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can update VPC subnets
        self.check_networking_restrictions(opctx).await?;

        let custom_router = match &params.custom_router {
            Some(k) => Some(
                self.vpc_router_lookup_for_attach(opctx, k, &authz_vpc).await?,
            ),
            None => None,
        };

        let saga_params = sagas::vpc_subnet_update::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            authz_vpc,
            authz_subnet,
            custom_router,
            update: params.clone().into(),
        };

        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::vpc_subnet_update::SagaVpcSubnetUpdate>(
                saga_params,
            )
            .await?;

        let out = saga_outputs
            .lookup_node_output::<VpcSubnet>("output")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from vpc update saga")?;

        self.vpc_needed_notify_sleds();

        Ok(out)
    }

    pub(crate) async fn vpc_delete_subnet(
        &self,
        opctx: &OpContext,
        vpc_subnet_lookup: &lookup::VpcSubnet<'_>,
    ) -> DeleteResult {
        let (.., authz_vpc, authz_subnet, db_subnet) =
            vpc_subnet_lookup.fetch_for(authz::Action::Delete).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can delete VPC subnets
        self.check_networking_restrictions(opctx).await?;

        let saga_params = sagas::vpc_subnet_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            authz_vpc,
            authz_subnet,
            db_subnet,
        };

        self.sagas
            .saga_execute::<sagas::vpc_subnet_delete::SagaVpcSubnetDelete>(
                saga_params,
            )
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(())
    }

    pub(crate) async fn subnet_list_instance_network_interfaces(
        &self,
        opctx: &OpContext,
        subnet_lookup: &lookup::VpcSubnet<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InstanceNetworkInterface> {
        let (.., authz_subnet) =
            subnet_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .subnet_list_instance_network_interfaces(
                opctx,
                &authz_subnet,
                pagparams,
            )
            .await
    }
}
