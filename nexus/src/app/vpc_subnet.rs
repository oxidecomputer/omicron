// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC Subnets and their network interfaces

use crate::app::vpc::Vpc;
use crate::external_api::params;
use nexus_config::Tunables;
use nexus_config::MIN_VPC_IPV4_SUBNET_PREFIX;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::queries::vpc_subnet::SubnetError;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::Ipv6NetExt;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on VPC subnets
#[derive(Clone)]
pub struct VpcSubnet {
    log: Logger,
    datastore: Arc<db::DataStore>,
    tunables: Tunables,
    vpc: Vpc,
}

impl VpcSubnet {
    pub fn new(
        log: Logger,
        datastore: Arc<db::DataStore>,
        tunables: Tunables,
        vpc: Vpc,
    ) -> VpcSubnet {
        VpcSubnet { log, datastore, tunables, vpc }
    }

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
                let subnet = LookupPath::new(opctx, &self.datastore)
                    .vpc_subnet_id(id);
                Ok(subnet)
            }
            params::SubnetSelector {
                subnet: NameOrId::Name(name),
                vpc: Some(vpc),
                project,
            } => {
                let subnet = self
                    .vpc
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
    // TODO: When a subnet is created it should add a route entry into the VPC's
    // system router
    pub(crate) async fn vpc_create_subnet(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        params: &params::VpcSubnetCreate,
    ) -> CreateResult<db::model::VpcSubnet> {
        let (.., authz_vpc, db_vpc) = vpc_lookup.fetch().await?;

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
                concat!(
                    "VPC Subnet IPv4 address ranges must have prefix ",
                    "length between {} and {}, inclusive"
                ),
                MIN_VPC_IPV4_SUBNET_PREFIX,
                self.tunables.max_vpc_ipv4_subnet_prefix,
            )));
        }

        // Allocate an ID and insert the record.
        //
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
        let subnet_id = Uuid::new_v4();
        match params.ipv6_block {
            None => {
                const NUM_RETRIES: usize = 2;
                let mut retry = 0;
                let result = loop {
                    let ipv6_block = db_vpc
                        .ipv6_prefix
                        .random_subnet(
                            oxnet::Ipv6Net::VPC_SUBNET_IPV6_PREFIX_LENGTH,
                        )
                        .map(|block| block.0)
                        .ok_or_else(|| {
                            external::Error::internal_error(
                                "Failed to create random IPv6 subnet",
                            )
                        })?;
                    let subnet = db::model::VpcSubnet::new(
                        subnet_id,
                        authz_vpc.id(),
                        params.identity.clone(),
                        params.ipv4_block,
                        ipv6_block,
                    );
                    let result = self
                        .datastore
                        .vpc_create_subnet(opctx, &authz_vpc, subnet)
                        .await;
                    match result {
                        // Allow NUM_RETRIES retries, after the first attempt.
                        //
                        // Note that we only catch IPv6 overlaps. The client
                        // always specifies the IPv4 range, so we fail the
                        // request if that overlaps with an existing range.
                        Err(SubnetError::OverlappingIpRange(ip))
                            if retry <= NUM_RETRIES && ip.is_ipv6() =>
                        {
                            debug!(
                                self.log,
                                "autogenerated random IPv6 range overlap";
                                "subnet_id" => ?subnet_id,
                                "ipv6_block" => %ipv6_block
                            );
                            retry += 1;
                            continue;
                        }
                        other => break other,
                    }
                };
                match result {
                    Err(SubnetError::OverlappingIpRange(ip))
                        if ip.is_ipv6() =>
                    {
                        // TODO-monitoring TODO-debugging
                        //
                        // We should maintain a counter for this occurrence, and
                        // export that via `oximeter`, so that we can see these
                        // failures through the timeseries database. The main
                        // goal here is for us to notice that this is happening
                        // before it becomes a major issue for customers.
                        let vpc_id = authz_vpc.id();
                        error!(
                            self.log,
                            "failed to generate unique random IPv6 address \
                            range in {} retries",
                            NUM_RETRIES;
                            "vpc_id" => ?vpc_id,
                            "subnet_id" => ?subnet_id,
                        );
                        Err(external::Error::internal_error(
                            "Unable to allocate unique IPv6 address range \
                            for VPC Subnet",
                        ))
                    }
                    Err(SubnetError::OverlappingIpRange(_)) => {
                        // Overlapping IPv4 ranges, which is always a client error.
                        Err(result.unwrap_err().into_external())
                    }
                    Err(SubnetError::External(e)) => Err(e),
                    Ok((.., subnet)) => Ok(subnet),
                }
            }
            Some(ipv6_block) => {
                if !ipv6_block.is_vpc_subnet(&db_vpc.ipv6_prefix) {
                    return Err(external::Error::invalid_request(&format!(
                        concat!(
                            "VPC Subnet IPv6 address range '{}' is not valid for ",
                            "VPC with IPv6 prefix '{}'",
                        ),
                        ipv6_block, db_vpc.ipv6_prefix.0,
                    )));
                }
                let subnet = db::model::VpcSubnet::new(
                    subnet_id,
                    db_vpc.id(),
                    params.identity.clone(),
                    params.ipv4_block,
                    ipv6_block,
                );
                self.datastore
                    .vpc_create_subnet(opctx, &authz_vpc, subnet)
                    .await
                    .map(|(.., subnet)| subnet)
                    .map_err(SubnetError::into_external)
            }
        }
    }

    pub(crate) async fn vpc_subnet_list(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::VpcSubnet> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.datastore.vpc_subnet_list(opctx, &authz_vpc, pagparams).await
    }

    pub(crate) async fn vpc_update_subnet(
        &self,
        opctx: &OpContext,
        vpc_subnet_lookup: &lookup::VpcSubnet<'_>,
        params: &params::VpcSubnetUpdate,
    ) -> UpdateResult<db::model::VpcSubnet> {
        let (.., authz_subnet) =
            vpc_subnet_lookup.lookup_for(authz::Action::Modify).await?;
        self.datastore
            .vpc_update_subnet(&opctx, &authz_subnet, params.clone().into())
            .await
    }

    // TODO: When a subnet is deleted it should remove its entry from the VPC's
    // system router.
    pub(crate) async fn vpc_delete_subnet(
        &self,
        opctx: &OpContext,
        vpc_subnet_lookup: &lookup::VpcSubnet<'_>,
    ) -> DeleteResult {
        let (.., authz_subnet, db_subnet) =
            vpc_subnet_lookup.fetch_for(authz::Action::Delete).await?;
        self.datastore.vpc_delete_subnet(opctx, &db_subnet, &authz_subnet).await
    }

    pub(crate) async fn subnet_list_instance_network_interfaces(
        &self,
        opctx: &OpContext,
        subnet_lookup: &lookup::VpcSubnet<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InstanceNetworkInterface> {
        let (.., authz_subnet) =
            subnet_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.datastore
            .subnet_list_instance_network_interfaces(
                opctx,
                &authz_subnet,
                pagparams,
            )
            .await
    }
}
