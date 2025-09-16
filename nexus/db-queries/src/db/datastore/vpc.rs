// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Vpc`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_attach::AttachError;
use crate::db::collection_attach::DatastoreAttachTarget;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::Resource;
use crate::db::model::ApplySledFilterExt;
use crate::db::model::DbTypedUuid;
use crate::db::model::IncompleteVpc;
use crate::db::model::InstanceNetworkInterface;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::RouterRoute;
use crate::db::model::RouterRouteKind;
use crate::db::model::RouterRouteUpdate;
use crate::db::model::Sled;
use crate::db::model::Vni;
use crate::db::model::Vpc;
use crate::db::model::VpcFirewallRule;
use crate::db::model::VpcRouter;
use crate::db::model::VpcRouterKind;
use crate::db::model::VpcRouterUpdate;
use crate::db::model::VpcSubnet;
use crate::db::model::VpcSubnetUpdate;
use crate::db::model::VpcUpdate;
use crate::db::model::to_db_typed_uuid;
use crate::db::model::{Ipv4Net, Ipv6Net};
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::vpc::InsertVpcQuery;
use crate::db::queries::vpc::VniSearchIter;
use crate::db::queries::vpc_subnet::InsertVpcSubnetError;
use crate::db::queries::vpc_subnet::InsertVpcSubnetQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use ipnetwork::IpNetwork;
use nexus_auth::authz::ApiResource;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_fixed_data::vpc::SERVICES_INTERNET_GATEWAY_DEFAULT_ROUTE_V4;
use nexus_db_fixed_data::vpc::SERVICES_INTERNET_GATEWAY_DEFAULT_ROUTE_V6;
use nexus_db_fixed_data::vpc::SERVICES_INTERNET_GATEWAY_ID;
use nexus_db_fixed_data::vpc::SERVICES_VPC_ID;
use nexus_db_fixed_data::vpc_firewall_rule::NEXUS_ICMP_FW_RULE_NAME;
use nexus_db_lookup::DbConnection;
use nexus_db_model::DbBpZoneDisposition;
use nexus_db_model::ExternalIp;
use nexus_db_model::InternetGateway;
use nexus_db_model::InternetGatewayIpAddress;
use nexus_db_model::InternetGatewayIpPool;
use nexus_db_model::IpPoolRange;
use nexus_db_model::NetworkInterfaceKind;
use nexus_types::deployment::SledFilter;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteKind as ExternalRouteKind;
use omicron_common::api::external::ServiceIcmpConfig;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::Vni as ExternalVni;
use omicron_common::api::external::VpcFirewallRuleStatus;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::internal::shared::InternetGatewayRouterTarget;
use omicron_common::api::internal::shared::ResolvedVpcRoute;
use omicron_common::api::internal::shared::RouterTarget;
use omicron_uuid_kinds::SledUuid;
use oxnet::IpNet;
use ref_cast::RefCast;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use uuid::Uuid;

impl DataStore {
    /// Load built-in VPCs into the database.
    pub async fn load_builtin_vpcs(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_fixed_data::project::SERVICES_PROJECT_ID;
        use nexus_db_fixed_data::vpc::SERVICES_VPC;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in VPCs");

        // Create built-in VPC for Oxide Services

        let (_, authz_project) = nexus_db_lookup::LookupPath::new(opctx, self)
            .project_id(*SERVICES_PROJECT_ID)
            .lookup_for(authz::Action::CreateChild)
            .await
            .internal_context("lookup built-in services project")?;
        let vpc_query = InsertVpcQuery::new_system(
            SERVICES_VPC.clone(),
            Some(Vni(ExternalVni::SERVICES_VNI)),
        );
        let authz_vpc = match self
            .project_create_vpc_raw(opctx, &authz_project, vpc_query)
            .await
        {
            Ok(None) => {
                let msg = "VNI exhaustion detected when creating built-in VPCs";
                error!(opctx.log, "{}", msg);
                Err(Error::internal_error(msg))
            }
            Ok(Some((authz_vpc, _))) => Ok(authz_vpc),
            Err(Error::ObjectAlreadyExists { .. }) => Ok(authz::Vpc::new(
                authz_project.clone(),
                *SERVICES_VPC_ID,
                LookupType::ByName(SERVICES_VPC.identity.name.to_string()),
            )),
            Err(e) => Err(e),
        }?;

        let igw = db::model::InternetGateway::new(
            *SERVICES_INTERNET_GATEWAY_ID,
            authz_vpc.id(),
            nexus_types::external_api::params::InternetGatewayCreate {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description: String::from("Default VPC gateway"),
                },
            },
        );

        match self.vpc_create_internet_gateway(&opctx, &authz_vpc, igw).await {
            Ok(_) => Ok(()),
            Err(e) => match e {
                Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(e),
            },
        }?;

        // Also add the system router and internet gateway route

        let system_router = nexus_db_lookup::LookupPath::new(opctx, self)
            .vpc_router_id(SERVICES_VPC.system_router_id)
            .lookup_for(authz::Action::CreateChild)
            .await;
        let authz_router = if let Ok((_, _, _, authz_router)) = system_router {
            authz_router
        } else {
            let router = VpcRouter::new(
                SERVICES_VPC.system_router_id,
                *SERVICES_VPC_ID,
                VpcRouterKind::System,
                nexus_types::external_api::params::VpcRouterCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "system".parse().unwrap(),
                        description: "Built-in VPC Router for Oxide Services"
                            .to_string(),
                    },
                },
            );
            self.vpc_create_router(opctx, &authz_vpc, router.clone())
                .await
                .map(|(authz_router, _)| authz_router)?
        };

        let default_v4 = RouterRoute::new(
            *SERVICES_INTERNET_GATEWAY_DEFAULT_ROUTE_V4,
            SERVICES_VPC.system_router_id,
            ExternalRouteKind::Default,
            nexus_types::external_api::params::RouterRouteCreate {
                identity: IdentityMetadataCreateParams {
                    name: "default-v4".parse().unwrap(),
                    description: String::from("Default IPv4 route"),
                },
                target: RouteTarget::InternetGateway(
                    "default".parse().unwrap(),
                ),
                destination: RouteDestination::IpNet(
                    "0.0.0.0/0".parse().unwrap(),
                ),
            },
        );

        let default_v6 = RouterRoute::new(
            *SERVICES_INTERNET_GATEWAY_DEFAULT_ROUTE_V6,
            SERVICES_VPC.system_router_id,
            ExternalRouteKind::Default,
            nexus_types::external_api::params::RouterRouteCreate {
                identity: IdentityMetadataCreateParams {
                    name: "default-v6".parse().unwrap(),
                    description: String::from("Default IPv6 route"),
                },
                target: RouteTarget::InternetGateway(
                    "default".parse().unwrap(),
                ),
                destination: RouteDestination::IpNet("::/0".parse().unwrap()),
            },
        );

        for route in [default_v4, default_v6] {
            self.router_create_route(opctx, &authz_router, route)
                .await
                .map(|_| ())
                .or_else(|e| match e {
                    Error::ObjectAlreadyExists { .. } => Ok(()),
                    _ => Err(e),
                })?;
        }

        self.load_builtin_vpc_fw_rules(opctx).await?;
        self.load_builtin_vpc_subnets(opctx, &authz_router).await?;

        info!(opctx.log, "created built-in services vpc");

        Ok(())
    }

    /// Load firewall rules for built-in VPCs.
    async fn load_builtin_vpc_fw_rules(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_fixed_data::vpc_firewall_rule::DNS_VPC_FW_RULE;
        use nexus_db_fixed_data::vpc_firewall_rule::NEXUS_ICMP_FW_RULE;
        use nexus_db_fixed_data::vpc_firewall_rule::NEXUS_VPC_FW_RULE;

        debug!(opctx.log, "attempting to create built-in VPC firewall rules");

        // Create firewall rules for Oxide Services

        let (_, _, authz_vpc) = nexus_db_lookup::LookupPath::new(opctx, self)
            .vpc_id(*SERVICES_VPC_ID)
            .lookup_for(authz::Action::CreateChild)
            .await
            .internal_context("lookup built-in services vpc")?;

        let mut fw_rules = self
            .vpc_list_firewall_rules(opctx, &authz_vpc)
            .await?
            .into_iter()
            .map(|rule| (rule.name().clone(), rule))
            .collect::<BTreeMap<_, _>>();

        // these have to be done this way because the contructor returns a result
        if !fw_rules.contains_key(&DNS_VPC_FW_RULE.name) {
            let rule = VpcFirewallRule::new(
                Uuid::new_v4(),
                *SERVICES_VPC_ID,
                &DNS_VPC_FW_RULE,
            )?;
            fw_rules.insert(DNS_VPC_FW_RULE.name.clone(), rule);
        }

        if !fw_rules.contains_key(&NEXUS_VPC_FW_RULE.name) {
            let rule = VpcFirewallRule::new(
                Uuid::new_v4(),
                *SERVICES_VPC_ID,
                &NEXUS_VPC_FW_RULE,
            )?;
            fw_rules.insert(NEXUS_VPC_FW_RULE.name.clone(), rule);
        }

        if !fw_rules.contains_key(&NEXUS_ICMP_FW_RULE.name) {
            let rule = VpcFirewallRule::new(
                Uuid::new_v4(),
                *SERVICES_VPC_ID,
                &NEXUS_ICMP_FW_RULE,
            )?;
            fw_rules.insert(NEXUS_ICMP_FW_RULE.name.clone(), rule);
        }

        let rules = fw_rules
            .into_values()
            .map(|mut rule| {
                rule.identity.id = Uuid::new_v4();
                rule
            })
            .collect();
        self.vpc_update_firewall_rules(opctx, &authz_vpc, rules).await?;

        info!(opctx.log, "created built-in VPC firewall rules");

        Ok(())
    }

    /// Load built-in VPC Subnets into the database.
    async fn load_builtin_vpc_subnets(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
    ) -> Result<(), Error> {
        use nexus_db_fixed_data::vpc_subnet::DNS_VPC_SUBNET;
        use nexus_db_fixed_data::vpc_subnet::DNS_VPC_SUBNET_ROUTE_ID;
        use nexus_db_fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
        use nexus_db_fixed_data::vpc_subnet::NEXUS_VPC_SUBNET_ROUTE_ID;
        use nexus_db_fixed_data::vpc_subnet::NTP_VPC_SUBNET;
        use nexus_db_fixed_data::vpc_subnet::NTP_VPC_SUBNET_ROUTE_ID;

        debug!(opctx.log, "attempting to create built-in VPC Subnets");

        // Create built-in VPC Subnets for Oxide Services

        let (_, _, authz_vpc) = nexus_db_lookup::LookupPath::new(opctx, self)
            .vpc_id(*SERVICES_VPC_ID)
            .lookup_for(authz::Action::CreateChild)
            .await
            .internal_context("lookup built-in services vpc")?;
        for (vpc_subnet, route_id) in [
            (&*DNS_VPC_SUBNET, *DNS_VPC_SUBNET_ROUTE_ID),
            (&*NEXUS_VPC_SUBNET, *NEXUS_VPC_SUBNET_ROUTE_ID),
            (&*NTP_VPC_SUBNET, *NTP_VPC_SUBNET_ROUTE_ID),
        ] {
            if let Ok(_) = nexus_db_lookup::LookupPath::new(opctx, self)
                .vpc_subnet_id(vpc_subnet.id())
                .fetch()
                .await
            {
                continue;
            }
            let subnet = self
                .vpc_create_subnet(opctx, &authz_vpc, vpc_subnet.clone())
                .await
                .map(Some)
                .map_err(InsertVpcSubnetError::into_external)
                .or_else(|e| match e {
                    Error::ObjectAlreadyExists { .. } => Ok(None),
                    _ => Err(e),
                })?;
            if let Some((.., subnet)) = subnet {
                self.vpc_create_subnet_route(
                    opctx,
                    &authz_router,
                    &subnet,
                    route_id,
                )
                .await
                .map(|_| ())
                .or_else(|e| match e {
                    Error::ObjectAlreadyExists { .. } => Ok(()),
                    _ => Err(e),
                })?;
            }
        }

        self.vpc_increment_rpw_version(opctx, authz_vpc.id()).await?;

        info!(opctx.log, "created built-in services vpc subnets");

        Ok(())
    }

    pub async fn vpc_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Vpc> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use nexus_db_schema::schema::vpc::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::vpc, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::vpc,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::project_id.eq(authz_project.id()))
        .select(Vpc::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        mut vpc: IncompleteVpc,
    ) -> Result<(authz::Vpc, Vpc), Error> {
        // Generate an iterator that allows us to search the entire space of
        // VNIs for this VPC, in manageable chunks to limit memory usage.
        let vnis = VniSearchIter::new(vpc.vni.0);
        for (i, vni) in vnis.enumerate() {
            vpc.vni = Vni(vni);
            let id = usdt::UniqueId::new();
            // TODO: silence this cast in usdt:
            // https://github.com/oxidecomputer/usdt/issues/270
            #[allow(clippy::cast_lossless)]
            {
                crate::probes::vni__search__range__start!(|| {
                    (&id, u32::from(vni), VniSearchIter::STEP_SIZE)
                });
            }
            match self
                .project_create_vpc_raw(
                    opctx,
                    authz_project,
                    InsertVpcQuery::new(vpc.clone()),
                )
                .await
            {
                Ok(Some((authz_vpc, vpc))) => {
                    // TODO: silence this cast in usdt:
                    // https://github.com/oxidecomputer/usdt/issues/270
                    #[allow(clippy::cast_lossless)]
                    {
                        crate::probes::vni__search__range__found!(|| {
                            (&id, u32::from(vpc.vni.0))
                        });
                    }
                    return Ok((authz_vpc, vpc));
                }
                Err(e) => return Err(e),
                Ok(None) => {
                    crate::probes::vni__search__range__empty!(|| (&id));
                    debug!(
                        opctx.log,
                        "No VNIs available within current search range, retrying";
                        "attempt" => i,
                        "vpc_name" => %vpc.identity.name,
                        "start_vni" => ?vni,
                    );
                }
            }
        }

        // We've failed to find a VNI after searching the entire range, so we'll
        // return a 503 at this point.
        error!(
            opctx.log,
            "failed to find a VNI after searching entire range";
        );
        Err(Error::insufficient_capacity(
            "No free virtual network was found",
            "Failed to find a free VNI for this VPC",
        ))
    }

    // Internal implementation for creating a VPC.
    //
    // This returns an optional VPC. If it is None, then we failed to insert a
    // VPC specifically because there are no available VNIs. All other errors
    // are returned in the `Result::Err` variant.
    async fn project_create_vpc_raw(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        vpc_query: InsertVpcQuery,
    ) -> Result<Option<(authz::Vpc, Vpc)>, Error> {
        use nexus_db_schema::schema::vpc::dsl;

        assert_eq!(authz_project.id(), vpc_query.vpc.project_id);

        // Create a VPC authz resource for authorization check
        let authz_vpc = authz::Vpc::new(
            authz_project.clone(),
            vpc_query.vpc.identity.id,
            omicron_common::api::external::LookupType::ById(
                vpc_query.vpc.identity.id,
            ),
        );

        // Check if the actor can create this VPC (including networking restrictions)
        opctx.authorize(authz::Action::CreateChild, &authz_vpc).await?;

        let name = vpc_query.vpc.identity.name.clone();
        let project_id = vpc_query.vpc.project_id;

        let conn = self.pool_connection_authorized(opctx).await?;
        let result: Result<Vpc, _> = Project::insert_resource(
            project_id,
            diesel::insert_into(dsl::vpc).values(vpc_query),
        )
        .insert_and_get_result_async(&conn)
        .await;
        match result {
            Ok(vpc) => Ok(Some((
                authz::Vpc::new(
                    authz_project.clone(),
                    vpc.id(),
                    LookupType::ByName(vpc.name().to_string()),
                ),
                vpc,
            ))),
            Err(AsyncInsertError::CollectionNotFound) => {
                Err(Error::ObjectNotFound {
                    type_name: ResourceType::Project,
                    lookup_type: LookupType::ById(project_id),
                })
            }
            Err(AsyncInsertError::DatabaseError(
                DieselError::DatabaseError(
                    DatabaseErrorKind::NotNullViolation,
                    info,
                ),
            )) if info
                .message()
                .starts_with("null value in column \"vni\"") =>
            {
                // We failed the non-null check on the VNI column, which means
                // we could not find a valid VNI in our search range. Return
                // None instead to signal the error.
                Ok(None)
            }
            Err(AsyncInsertError::DatabaseError(e)) => {
                Err(public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::Vpc, name.as_str()),
                ))
            }
        }
    }

    pub async fn project_update_vpc(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        updates: VpcUpdate,
    ) -> UpdateResult<Vpc> {
        opctx.authorize(authz::Action::Modify, authz_vpc).await?;

        use nexus_db_schema::schema::vpc::dsl;
        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_vpc.id()))
            .set(updates)
            .returning(Vpc::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })
    }

    pub async fn project_delete_vpc(
        &self,
        opctx: &OpContext,
        db_vpc: &Vpc,
        authz_vpc: &authz::Vpc,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_vpc).await?;

        use nexus_db_schema::schema::vpc::dsl;
        use nexus_db_schema::schema::vpc_subnet;

        // Note that we don't ensure the firewall rules are empty here, because
        // we allow deleting VPCs with firewall rules present. Inserting new
        // rules is serialized with respect to the deletion by the row lock
        // associated with the VPC row, since we use the collection insert CTE
        // pattern to add firewall rules.

        // We _do_ need to check for the existence of subnets. VPC Subnets
        // cannot be deleted while there are network interfaces in them
        // (associations between an instance and a VPC Subnet). Because VPC
        // Subnets are themselves containers for resources that we don't want to
        // auto-delete (now, anyway), we've got to check there aren't any. We
        // _might_ be able to make this a check for NICs, rather than subnets,
        // but we can't have NICs be a child of both tables at this point, and
        // we need to prevent VPC Subnets from being deleted while they have
        // NICs in them as well.
        if vpc_subnet::dsl::vpc_subnet
            .filter(vpc_subnet::dsl::vpc_id.eq(authz_vpc.id()))
            .filter(vpc_subnet::dsl::time_deleted.is_null())
            .select(vpc_subnet::dsl::id)
            .limit(1)
            .first_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .is_some()
        {
            return Err(Error::invalid_request(
                "VPC cannot be deleted while VPC Subnets exist",
            ));
        }

        // Delete the VPC, conditional on the subnet_gen not having changed.
        let now = Utc::now();
        let updated_rows = diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_vpc.id()))
            .filter(dsl::subnet_gen.eq(db_vpc.subnet_gen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })?;
        if updated_rows == 0 {
            Err(Error::invalid_request(
                "deletion failed due to concurrent modification",
            ))
        } else {
            Ok(())
        }
    }

    pub async fn vpc_list_firewall_rules(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
    ) -> ListResultVec<VpcFirewallRule> {
        // Firewall rules are modeled in the API as a single resource under the
        // Vpc (rather than individual child resources with their own CRUD
        // endpoints).  You cannot look them up individually, create them,
        // remove them, or update them.  You can only modify the whole set.  So
        // for authz, we treat them as part of the Vpc itself.
        opctx.authorize(authz::Action::Read, authz_vpc).await?;
        use nexus_db_schema::schema::vpc_firewall_rule::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        dsl::vpc_firewall_rule
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .order(dsl::name.asc())
            .select(VpcFirewallRule::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn vpc_delete_all_firewall_rules(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_vpc).await?;
        use nexus_db_schema::schema::vpc_firewall_rule::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let now = Utc::now();
        // TODO-performance: Paginate this update to avoid long queries
        diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })?;
        Ok(())
    }

    /// Replace all firewall rules with the given rules
    pub async fn vpc_update_firewall_rules(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        mut rules: Vec<VpcFirewallRule>,
    ) -> UpdateResult<Vec<VpcFirewallRule>> {
        opctx.authorize(authz::Action::Modify, authz_vpc).await?;
        for r in &rules {
            assert_eq!(r.vpc_id, authz_vpc.id());
        }

        // Sort the rules in the same order that we would return them when
        // listing them.  This is because we're going to use RETURNING to return
        // the inserted rows from the database and we want them to come back in
        // the same order that we would normally list them.
        rules.sort_by_key(|r| r.name().to_string());

        use nexus_db_schema::schema::vpc_firewall_rule::dsl;

        let now = Utc::now();
        let delete_old_query = diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .set(dsl::time_deleted.eq(now));

        let rules_is_empty = rules.is_empty();
        #[derive(Debug)]
        enum FirewallUpdateError {
            CollectionNotFound,
        }

        let err = OptionalError::new();

        // TODO-scalability: Ideally this would be a CTE so we don't need to
        // hold a transaction open across multiple roundtrips from the database,
        // but for now we're using a transaction due to the severely decreased
        // legibility of CTEs via diesel right now.
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("vpc_update_firewall_rules")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let delete_old_query = delete_old_query.clone();
                let rules = rules.clone();
                async move {
                    delete_old_query.execute_async(&conn).await?;

                    // The generation count update on the vpc table row will take a
                    // write lock on the row, ensuring that the vpc was not deleted
                    // concurently.
                    if rules_is_empty {
                        return Ok(vec![]);
                    }
                    Vpc::insert_resource(
                        authz_vpc.id(),
                        diesel::insert_into(dsl::vpc_firewall_rule)
                            .values(rules),
                    )
                    .insert_and_get_results_async(&conn)
                    .await
                    .map_err(|e| match e {
                        AsyncInsertError::CollectionNotFound => {
                            err.bail(FirewallUpdateError::CollectionNotFound)
                        }
                        AsyncInsertError::DatabaseError(e) => e,
                    })
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        FirewallUpdateError::CollectionNotFound => {
                            Error::not_found_by_id(
                                ResourceType::Vpc,
                                &authz_vpc.id(),
                            )
                        }
                    }
                } else {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByResource(authz_vpc),
                    )
                }
            })
    }

    pub async fn nexus_inbound_icmp_view(
        &self,
        opctx: &OpContext,
    ) -> Result<ServiceIcmpConfig, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::vpc_firewall_rule::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let rule = dsl::vpc_firewall_rule
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*SERVICES_VPC_ID))
            .filter(dsl::name.eq(NEXUS_ICMP_FW_RULE_NAME))
            .limit(1)
            .select(VpcFirewallRule::as_select())
            .get_result_async::<VpcFirewallRule>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if let Some(rule) = rule {
            Ok(ServiceIcmpConfig {
                enabled: rule.status.0 == VpcFirewallRuleStatus::Enabled,
            })
        } else {
            Err(Error::internal_error(&format!(
                "services VPC is missing the builtin firewall rule \
                {NEXUS_ICMP_FW_RULE_NAME}"
            )))
        }
    }

    pub async fn nexus_inbound_icmp_update(
        &self,
        opctx: &OpContext,
        config: ServiceIcmpConfig,
    ) -> Result<ServiceIcmpConfig, Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::vpc_firewall_rule::dsl;

        let ServiceIcmpConfig { enabled } = config;

        let conn = self.pool_connection_authorized(opctx).await?;

        let status = nexus_db_model::VpcFirewallRuleStatus(if enabled {
            VpcFirewallRuleStatus::Enabled
        } else {
            VpcFirewallRuleStatus::Disabled
        });

        let now = Utc::now();
        let rule = diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*SERVICES_VPC_ID))
            .filter(dsl::name.eq(NEXUS_ICMP_FW_RULE_NAME))
            .set((dsl::time_modified.eq(now), dsl::status.eq(status)))
            .returning(VpcFirewallRule::as_returning())
            .get_result_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if let Some(rule) = rule {
            Ok(ServiceIcmpConfig {
                enabled: rule.status.0 == VpcFirewallRuleStatus::Enabled,
            })
        } else {
            Err(Error::internal_error(&format!(
                "services VPC is missing the builtin firewall rule \
                {NEXUS_ICMP_FW_RULE_NAME}"
            )))
        }
    }

    /// Return the list of `Sled`s hosting instances or control plane services
    /// with network interfaces on the provided VPC.
    pub async fn vpc_resolve_to_sleds(
        &self,
        vpc_id: Uuid,
        sleds_filter: &[SledUuid],
    ) -> Result<Vec<Sled>, Error> {
        // Resolve each VNIC in the VPC to the Sled it's on, so we know which
        // Sleds to notify when firewall rules change.
        use nexus_db_schema::schema::{
            bp_omicron_zone, bp_target, instance, instance_network_interface,
            service_network_interface, sled, vmm,
        };
        // Diesel requires us to use aliases in order to refer to the
        // `bp_target` table twice in the same query.
        let (bp_target1, bp_target2) = diesel::alias!(
            nexus_db_schema::schema::bp_target as bp_target1,
            nexus_db_schema::schema::bp_target as bp_target2
        );

        let instance_query = instance_network_interface::table
            .inner_join(instance::table)
            .inner_join(
                vmm::table
                    .on(vmm::id.nullable().eq(instance::active_propolis_id)),
            )
            .inner_join(sled::table.on(sled::id.eq(vmm::sled_id)))
            .filter(instance_network_interface::vpc_id.eq(vpc_id))
            .filter(instance_network_interface::time_deleted.is_null())
            .filter(instance::time_deleted.is_null())
            .filter(vmm::time_deleted.is_null())
            .select(Sled::as_select());

        let service_query = service_network_interface::table
            .inner_join(bp_omicron_zone::table.on(
                bp_omicron_zone::id.eq(service_network_interface::service_id),
            ))
            .inner_join(
                bp_target1.on(bp_omicron_zone::blueprint_id
                    .eq(bp_target1.field(bp_target::blueprint_id))),
            )
            .inner_join(sled::table.on(sled::id.eq(bp_omicron_zone::sled_id)))
            .filter(
                // This filters us down to the one current target blueprint (if
                // it exists); i.e., the target with the maximal version. We
                // could also check that the current target is `enabled`, but
                // that could very easily be incorrect: if the current target
                // or any of its blueprint ancestors were _ever_ enabled, it's
                // possible the current target blueprint describes running
                // services that were added after RSS and therefore wouldn't be
                // seen in `rss_service_query`.
                bp_target1.field(bp_target::version).eq_any(
                    bp_target2
                        .select(bp_target2.field(bp_target::version))
                        .order_by(bp_target2.field(bp_target::version).desc())
                        .limit(1),
                ),
            )
            // Filter out services that are expunged and shouldn't be resolved
            // here.
            //
            // TODO: We should reference a rendezvous table instead of filtering
            // for in-service zones.
            .filter(
                bp_omicron_zone::disposition.eq(DbBpZoneDisposition::InService),
            )
            .filter(service_network_interface::vpc_id.eq(vpc_id))
            .filter(service_network_interface::time_deleted.is_null())
            .select(Sled::as_select());

        let mut sleds = sled::table
            .select(Sled::as_select())
            .filter(sled::time_deleted.is_null())
            .sled_filter(SledFilter::VpcFirewall)
            .into_boxed();
        if !sleds_filter.is_empty() {
            sleds = sleds.filter(
                sled::id.eq_any(
                    sleds_filter
                        .iter()
                        .map(|id| to_db_typed_uuid(*id))
                        .collect::<Vec<DbTypedUuid<_>>>(),
                ),
            );
        }

        let conn = self.pool_connection_unauthorized().await?;
        sleds
            .intersect(instance_query.union(service_query))
            .get_results_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn vpc_subnet_list(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<VpcSubnet> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use nexus_db_schema::schema::vpc_subnet::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::vpc_subnet, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::vpc_subnet,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::vpc_id.eq(authz_vpc.id()))
        .select(VpcSubnet::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert a VPC Subnet, checking for unique IP address ranges.
    pub async fn vpc_create_subnet(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        subnet: VpcSubnet,
    ) -> Result<(authz::VpcSubnet, VpcSubnet), InsertVpcSubnetError> {
        opctx
            .authorize(authz::Action::CreateChild, authz_vpc)
            .await
            .map_err(InsertVpcSubnetError::External)?;
        assert_eq!(authz_vpc.id(), subnet.vpc_id);

        let db_subnet = self.vpc_create_subnet_raw(subnet).await?;

        Ok((
            authz::VpcSubnet::new(
                authz_vpc.clone(),
                db_subnet.id(),
                LookupType::ById(db_subnet.id()),
            ),
            db_subnet,
        ))
    }

    pub(crate) async fn vpc_create_subnet_raw(
        &self,
        subnet: VpcSubnet,
    ) -> Result<VpcSubnet, InsertVpcSubnetError> {
        let conn = self
            .pool_connection_unauthorized()
            .await
            .map_err(InsertVpcSubnetError::External)?;
        let query = InsertVpcSubnetQuery::new(subnet.clone());
        query
            .get_result_async(&*conn)
            .await
            .map_err(|e| InsertVpcSubnetError::from_diesel(e, &subnet))
    }

    pub async fn vpc_delete_subnet(
        &self,
        opctx: &OpContext,
        db_subnet: &VpcSubnet,
        authz_subnet: &authz::VpcSubnet,
    ) -> DeleteResult {
        let updated_rows =
            self.vpc_delete_subnet_raw(opctx, db_subnet, authz_subnet).await?;
        if updated_rows == 0 {
            return Err(Error::invalid_request(
                "deletion failed due to concurrent modification",
            ));
        }
        Ok(())
    }

    pub async fn vpc_delete_subnet_raw(
        &self,
        opctx: &OpContext,
        db_subnet: &VpcSubnet,
        authz_subnet: &authz::VpcSubnet,
    ) -> Result<usize, Error> {
        opctx.authorize(authz::Action::Delete, authz_subnet).await?;

        use nexus_db_schema::schema::network_interface;
        use nexus_db_schema::schema::vpc_subnet::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        // Verify there are no child network interfaces in this VPC Subnet
        if network_interface::dsl::network_interface
            .filter(network_interface::dsl::subnet_id.eq(authz_subnet.id()))
            .filter(network_interface::dsl::time_deleted.is_null())
            .select(network_interface::dsl::id)
            .limit(1)
            .first_async::<Uuid>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .is_some()
        {
            return Err(Error::invalid_request(
                "VPC Subnet cannot be deleted while network interfaces in the \
                subnet exist",
            ));
        }

        // Delete the subnet, conditional on the rcgen not having changed.
        let now = Utc::now();
        diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_subnet.id()))
            .filter(dsl::rcgen.eq(db_subnet.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                )
            })
    }

    /// Insert the system route for a VPC Subnet.
    pub async fn vpc_create_subnet_route(
        &self,
        opctx: &OpContext,
        authz_system_router: &authz::VpcRouter,
        subnet: &VpcSubnet,
        route_id: Uuid,
    ) -> CreateResult<(authz::RouterRoute, RouterRoute)> {
        let route = db::model::RouterRoute::new_subnet(
            route_id,
            authz_system_router.id(),
            subnet.name().clone().into(),
            subnet.id(),
        );

        let route =
            self.router_create_route(opctx, authz_system_router, route).await?;

        Ok((
            authz::RouterRoute::new(
                authz_system_router.clone(),
                route_id,
                LookupType::ById(subnet.id()),
            ),
            route,
        ))
    }

    /// Delete the system route for a VPC Subnet.
    pub async fn vpc_delete_subnet_route(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
    ) -> DeleteResult {
        use nexus_db_schema::schema::router_route::dsl as rr_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::update(rr_dsl::router_route)
            .filter(rr_dsl::time_deleted.is_null())
            .filter(
                rr_dsl::kind.eq(RouterRouteKind(ExternalRouteKind::VpcSubnet)),
            )
            .filter(rr_dsl::vpc_subnet_id.eq(Some(authz_subnet.id())))
            .set(rr_dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                )
            })?;

        Ok(())
    }

    pub async fn vpc_update_subnet(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        custom_router: Option<&authz::VpcRouter>,
        mut updates: VpcSubnetUpdate,
    ) -> UpdateResult<VpcSubnet> {
        opctx.authorize(authz::Action::Modify, authz_subnet).await?;
        if let Some(authz_router) = custom_router {
            opctx.authorize(authz::Action::Read, authz_router).await?;

            // Leave untouched in update query, then attach new.
            updates.custom_router_id = None;
        } else {
            // Explicit removal in update query.
            updates.custom_router_id = Some(None);
        }

        let time_modified = updates.time_modified;
        let err = OptionalError::new();

        #[derive(Debug)]
        enum SubnetError<'a> {
            SubnetModify(DieselError),
            SystemRoute(DieselError),
            SubnetAttachNoRouter(&'a authz::VpcRouter),
            SubnetAttachNoSubnet,
            SubnetAttachRouterIsSystem,
        }

        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("vpc_subnet_update")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let updates = updates.clone();
                async move {
                    use nexus_db_schema::schema::vpc_subnet::dsl;
                    let name = updates.name.clone();

                    // Update subnet metadata, and unset subnet->custom router attachment
                    // if required.
                    let no_attach_res = match diesel::update(dsl::vpc_subnet)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_subnet.id()))
                        .set(updates)
                        .returning(VpcSubnet::as_returning())
                        .get_result_async(&conn)
                        .await
                    {
                        Ok(o) => o,
                        Err(e) => {
                            return Err(err.bail_retryable_or_else(
                                e,
                                SubnetError::SubnetModify,
                            ));
                        }
                    };

                    // Fix the presentation of the matching system route.
                    if let Some(new_name) = name {
                        use nexus_db_schema::schema::router_route::dsl as rr_dsl;
                        if let Err(e) = diesel::update(rr_dsl::router_route)
                            .filter(rr_dsl::time_deleted.is_null())
                            .filter(rr_dsl::kind.eq(RouterRouteKind(
                                ExternalRouteKind::VpcSubnet,
                            )))
                            .filter(
                                rr_dsl::vpc_subnet_id
                                    .eq(Some(authz_subnet.id())),
                            )
                            .set(RouterRouteUpdate::vpc_subnet_rename(
                                new_name,
                                time_modified,
                            ))
                            .execute_async(&conn)
                            .await
                        {
                            return Err(err.bail_retryable_or_else(
                                e,
                                SubnetError::SystemRoute,
                            ));
                        }
                    }

                    // Apply subnet->custom router attachment/detachment.
                    if let Some(authz_router) = custom_router {
                        use nexus_db_schema::schema::vpc_router::dsl as router_dsl;
                        use nexus_db_schema::schema::vpc_subnet::dsl as subnet_dsl;

                        let query = VpcRouter::attach_resource(
                            authz_router.id(),
                            authz_subnet.id(),
                            router_dsl::vpc_router.into_boxed().filter(
                                router_dsl::kind.eq(VpcRouterKind::Custom),
                            ),
                            subnet_dsl::vpc_subnet.into_boxed(),
                            u32::MAX,
                            diesel::update(subnet_dsl::vpc_subnet).set((
                                subnet_dsl::time_modified.eq(Utc::now()),
                                subnet_dsl::custom_router_id
                                    .eq(authz_router.id()),
                            )),
                        );

                        query
                            .attach_and_get_result_async(&conn)
                            .await
                            .map(|(_, resource)| resource)
                            .map_err(|e| match e {
                                AttachError::CollectionNotFound => {
                                    err.bail(SubnetError::SubnetAttachNoRouter(
                                        authz_router,
                                    ))
                                }
                                AttachError::ResourceNotFound => {
                                    err.bail(SubnetError::SubnetAttachNoSubnet)
                                }
                                // The only other failure reason can be an attempt to use a system router.
                                AttachError::NoUpdate { .. } => err.bail(
                                    SubnetError::SubnetAttachRouterIsSystem,
                                ),
                                AttachError::DatabaseError(e) => e,
                            })
                    } else {
                        Ok(no_attach_res)
                    }
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(SubnetError::SubnetModify(e)) => public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                ),
                Some(SubnetError::SystemRoute(e)) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context(
                            "failed to modify nexus-managed system route",
                        )
                }
                Some(SubnetError::SubnetAttachNoSubnet) => {
                    Error::ObjectNotFound {
                        type_name: ResourceType::VpcSubnet,
                        lookup_type: authz_subnet.lookup_type().clone(),
                    }
                }
                Some(SubnetError::SubnetAttachNoRouter(r)) => {
                    Error::ObjectNotFound {
                        type_name: ResourceType::VpcRouter,
                        lookup_type: r.lookup_type().clone(),
                    }
                }
                Some(SubnetError::SubnetAttachRouterIsSystem) => {
                    Error::invalid_request(
                        "cannot attach a system router to a VPC subnet",
                    )
                }
                _ => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    pub async fn vpc_subnet_set_custom_router(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        authz_router: &authz::VpcRouter,
    ) -> Result<VpcSubnet, Error> {
        self.vpc_update_subnet(
            opctx,
            authz_subnet,
            Some(authz_router),
            VpcSubnetUpdate {
                name: None,
                description: None,
                time_modified: Utc::now(),
                // Filled by `vpc_update_subnet`.
                custom_router_id: None,
            },
        )
        .await
    }

    pub async fn vpc_subnet_unset_custom_router(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
    ) -> Result<VpcSubnet, Error> {
        self.vpc_update_subnet(
            opctx,
            authz_subnet,
            None,
            VpcSubnetUpdate {
                name: None,
                description: None,
                time_modified: Utc::now(),
                // Filled by `vpc_update_subnet`.
                custom_router_id: None,
            },
        )
        .await
    }

    pub async fn subnet_list_instance_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InstanceNetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_subnet).await?;

        use nexus_db_schema::schema::instance_network_interface::dsl;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::instance_network_interface, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::instance_network_interface,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::subnet_id.eq(authz_subnet.id()))
        .select(InstanceNetworkInterface::as_select())
        .load_async::<InstanceNetworkInterface>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn vpc_router_list(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<VpcRouter> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use nexus_db_schema::schema::vpc_router::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::vpc_router, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::vpc_router,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::vpc_id.eq(authz_vpc.id()))
        .select(VpcRouter::as_select())
        .load_async::<db::model::VpcRouter>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn internet_gateway_list(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InternetGateway> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use nexus_db_schema::schema::internet_gateway::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::internet_gateway, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::internet_gateway,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::vpc_id.eq(authz_vpc.id()))
        .select(InternetGateway::as_select())
        .load_async::<db::model::InternetGateway>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn internet_gateway_has_ip_pools(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
    ) -> LookupResult<bool> {
        opctx.authorize(authz::Action::ListChildren, authz_igw).await?;

        use nexus_db_schema::schema::internet_gateway_ip_pool::dsl;
        let result = dsl::internet_gateway_ip_pool
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::internet_gateway_id.eq(authz_igw.id()))
            .select(InternetGatewayIpPool::as_select())
            .limit(1)
            .load_async::<db::model::InternetGatewayIpPool>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(!result.is_empty())
    }

    pub async fn internet_gateway_has_ip_addresses(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
    ) -> LookupResult<bool> {
        opctx.authorize(authz::Action::ListChildren, authz_igw).await?;

        use nexus_db_schema::schema::internet_gateway_ip_address::dsl;
        let result = dsl::internet_gateway_ip_address
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::internet_gateway_id.eq(authz_igw.id()))
            .select(InternetGatewayIpAddress::as_select())
            .limit(1)
            .load_async::<db::model::InternetGatewayIpAddress>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(!result.is_empty())
    }

    pub async fn internet_gateway_list_ip_pools(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InternetGatewayIpPool> {
        opctx.authorize(authz::Action::ListChildren, authz_igw).await?;

        use nexus_db_schema::schema::internet_gateway_ip_pool::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::internet_gateway_ip_pool, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::internet_gateway_ip_pool,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::internet_gateway_id.eq(authz_igw.id()))
        .select(InternetGatewayIpPool::as_select())
        .load_async::<db::model::InternetGatewayIpPool>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn internet_gateway_list_ip_addresses(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InternetGatewayIpAddress> {
        opctx.authorize(authz::Action::ListChildren, authz_igw).await?;

        use nexus_db_schema::schema::internet_gateway_ip_address::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::internet_gateway_ip_address, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::internet_gateway_ip_address,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::internet_gateway_id.eq(authz_igw.id()))
        .select(InternetGatewayIpAddress::as_select())
        .load_async::<db::model::InternetGatewayIpAddress>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn vpc_create_router(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        router: VpcRouter,
    ) -> CreateResult<(authz::VpcRouter, VpcRouter)> {
        opctx.authorize(authz::Action::CreateChild, authz_vpc).await?;

        use nexus_db_schema::schema::vpc_router::dsl;
        let name = router.name().clone();
        let router = diesel::insert_into(dsl::vpc_router)
            .values(router)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(VpcRouter::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::VpcRouter,
                        name.as_str(),
                    ),
                )
            })?;
        Ok((
            authz::VpcRouter::new(
                authz_vpc.clone(),
                router.id(),
                LookupType::ById(router.id()),
            ),
            router,
        ))
    }

    pub async fn vpc_create_internet_gateway(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        igw: InternetGateway,
    ) -> CreateResult<(authz::InternetGateway, InternetGateway)> {
        opctx.authorize(authz::Action::CreateChild, authz_vpc).await?;

        use nexus_db_schema::schema::internet_gateway::dsl;
        let name = igw.name().clone();
        let igw = diesel::insert_into(dsl::internet_gateway)
            .values(igw)
            .returning(InternetGateway::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::InternetGateway,
                        name.as_str(),
                    ),
                )
            })?;

        // This is a named resource in router rules, so router resolution
        // will change on add/delete.
        self.vpc_increment_rpw_version(opctx, authz_vpc.id()).await?;

        Ok((
            authz::InternetGateway::new(
                authz_vpc.clone(),
                igw.id(),
                LookupType::ById(igw.id()),
            ),
            igw,
        ))
    }

    pub async fn vpc_delete_router(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_router).await?;

        use nexus_db_schema::schema::vpc_router::dsl;
        let now = Utc::now();
        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_router.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_router),
                )
            })?;

        // All child routes are deleted.
        use nexus_db_schema::schema::router_route::dsl as rr;
        let now = Utc::now();
        diesel::update(rr::router_route)
            .filter(rr::time_deleted.is_null())
            .filter(rr::vpc_router_id.eq(authz_router.id()))
            .set(rr::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Unlink all subnets from this router.
        // This will temporarily leave some hanging subnet attachments.
        // `vpc_get_active_custom_routers` will join and then filter,
        // so such rows will be treated as though they have no custom router
        // by the RPW.
        use nexus_db_schema::schema::vpc_subnet::dsl as vpc;
        diesel::update(vpc::vpc_subnet)
            .filter(vpc::time_deleted.is_null())
            .filter(vpc::custom_router_id.eq(authz_router.id()))
            .set(vpc::custom_router_id.eq(Option::<Uuid>::None))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn vpc_delete_internet_gateway(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
        vpc_id: Uuid,
        cascade: bool,
    ) -> DeleteResult {
        let res = if cascade {
            self.vpc_delete_internet_gateway_cascade(opctx, authz_igw).await
        } else {
            self.vpc_delete_internet_gateway_no_cascade(opctx, authz_igw).await
        };

        if res.is_ok() {
            // This is a named resource in router rules, so router resolution
            // will change on add/delete.
            self.vpc_increment_rpw_version(opctx, vpc_id).await?;
        }

        res
    }

    pub async fn vpc_delete_internet_gateway_no_cascade(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_igw).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        #[derive(Debug)]
        enum DeleteError {
            IpPoolsExist,
            IpAddressesExist,
        }

        self.transaction_retry_wrapper("vpc_delete_internet_gateway_no_cascade")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    // Delete ip pool associations
                    use nexus_db_schema::schema::internet_gateway_ip_pool::dsl as pool;
                    let count = pool::internet_gateway_ip_pool
                        .filter(pool::time_deleted.is_null())
                        .filter(pool::internet_gateway_id.eq(authz_igw.id()))
                        .count()
                        .first_async::<i64>(&conn)
                        .await?;
                    if count > 0 {
                        return Err(err.bail(DeleteError::IpPoolsExist));
                    }

                    // Delete ip address associations
                    use nexus_db_schema::schema::internet_gateway_ip_address::dsl as addr;
                    let count = addr::internet_gateway_ip_address
                        .filter(addr::time_deleted.is_null())
                        .filter(addr::internet_gateway_id.eq(authz_igw.id()))
                        .count()
                        .first_async::<i64>(&conn)
                        .await?;
                    if count > 0 {
                        return Err(err.bail(DeleteError::IpAddressesExist));
                    }

                    use nexus_db_schema::schema::internet_gateway::dsl;
                    let now = Utc::now();
                    diesel::update(dsl::internet_gateway)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_igw.id()))
                        .set(dsl::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        DeleteError::IpPoolsExist => Error::invalid_request("Ip pools referencing this gateway exist. To perform a cascading delete set the cascade option"),
                        DeleteError::IpAddressesExist => Error::invalid_request("Ip addresses referencing this gateway exist. To perform a cascading delete set the cascade option"),
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn vpc_delete_internet_gateway_cascade(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_igw).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("vpc_delete_internet_gateway_cascade")
            .transaction(&conn, |conn| {
                async move {
                    use nexus_db_schema::schema::internet_gateway::dsl as igw;
                    let igw_info = igw::internet_gateway
                        .filter(igw::time_deleted.is_null())
                        .filter(igw::id.eq(authz_igw.id()))
                        .select(InternetGateway::as_select())
                        .first_async(&conn)
                        .await?;

                    use nexus_db_schema::schema::internet_gateway::dsl;
                    let now = Utc::now();
                    diesel::update(dsl::internet_gateway)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_igw.id()))
                        .set(dsl::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await?;

                    // Delete ip pool associations
                    use nexus_db_schema::schema::internet_gateway_ip_pool::dsl as pool;
                    let now = Utc::now();
                    diesel::update(pool::internet_gateway_ip_pool)
                        .filter(pool::time_deleted.is_null())
                        .filter(pool::internet_gateway_id.eq(authz_igw.id()))
                        .set(pool::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await?;

                    // Delete ip address associations
                    use nexus_db_schema::schema::internet_gateway_ip_address::dsl as addr;
                    let now = Utc::now();
                    diesel::update(addr::internet_gateway_ip_address)
                        .filter(addr::time_deleted.is_null())
                        .filter(addr::internet_gateway_id.eq(authz_igw.id()))
                        .set(addr::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await?;

                    // Delete routes targeting this igw
                    use nexus_db_schema::schema::vpc_router::dsl as vr;
                    let vpc_routers = vr::vpc_router
                        .filter(vr::time_deleted.is_null())
                        .filter(vr::vpc_id.eq(igw_info.vpc_id))
                        .select(VpcRouter::as_select())
                        .load_async(&conn)
                        .await?
                        .into_iter()
                        .map(|x| x.id())
                        .collect::<Vec<_>>();

                    use nexus_db_schema::schema::router_route::dsl as rr;
                    let now = Utc::now();
                    diesel::update(rr::router_route)
                        .filter(rr::time_deleted.is_null())
                        .filter(rr::vpc_router_id.eq_any(vpc_routers))
                        .filter(
                            rr::target
                                .eq(format!("inetgw:{}", igw_info.name())),
                        )
                        .set(rr::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        updates: VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        opctx.authorize(authz::Action::Modify, authz_router).await?;

        use nexus_db_schema::schema::vpc_router::dsl;
        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_router.id()))
            .set(updates)
            .returning(VpcRouter::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_router),
                )
            })
    }

    pub async fn vpc_router_route_list(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<RouterRoute> {
        opctx.authorize(authz::Action::ListChildren, authz_router).await?;

        use nexus_db_schema::schema::router_route::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::router_route, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::router_route,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::vpc_router_id.eq(authz_router.id()))
        .select(RouterRoute::as_select())
        .load_async::<db::model::RouterRoute>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn router_create_route(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        route: RouterRoute,
    ) -> CreateResult<RouterRoute> {
        assert_eq!(authz_router.id(), route.vpc_router_id);
        opctx.authorize(authz::Action::CreateChild, authz_router).await?;

        Self::router_create_route_on_connection(
            route,
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
    }

    pub async fn router_create_route_on_connection(
        route: RouterRoute,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> CreateResult<RouterRoute> {
        use nexus_db_schema::schema::router_route::dsl;
        let router_id = route.vpc_router_id;
        let name = route.name().clone();

        VpcRouter::insert_resource(
            router_id,
            diesel::insert_into(dsl::router_route).values(route),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::VpcRouter,
                lookup_type: LookupType::ById(router_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::RouterRoute,
                    name.as_str(),
                ),
            ),
        })
    }

    pub async fn internet_gateway_attach_ip_pool(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
        igwip: InternetGatewayIpPool,
    ) -> CreateResult<InternetGatewayIpPool> {
        use nexus_db_schema::schema::internet_gateway_ip_pool::dsl;
        opctx.authorize(authz::Action::CreateChild, authz_igw).await?;

        let igw_id = igwip.internet_gateway_id;
        let name = igwip.name().clone();

        InternetGateway::insert_resource(
            igw_id,
            diesel::insert_into(dsl::internet_gateway_ip_pool).values(igwip),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::InternetGateway,
                lookup_type: LookupType::ById(igw_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::InternetGatewayIpPool,
                    name.as_str(),
                ),
            ),
        })
    }

    pub async fn internet_gateway_attach_ip_address(
        &self,
        opctx: &OpContext,
        authz_igw: &authz::InternetGateway,
        igwip: InternetGatewayIpAddress,
    ) -> CreateResult<InternetGatewayIpAddress> {
        use nexus_db_schema::schema::internet_gateway_ip_address::dsl;
        opctx.authorize(authz::Action::CreateChild, authz_igw).await?;

        let igw_id = igwip.internet_gateway_id;
        let name = igwip.name().clone();

        InternetGateway::insert_resource(
            igw_id,
            diesel::insert_into(dsl::internet_gateway_ip_address).values(igwip),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::InternetGateway,
                lookup_type: LookupType::ById(igw_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::InternetGatewayIpAddress,
                    name.as_str(),
                ),
            ),
        })
    }

    pub async fn router_delete_route(
        &self,
        opctx: &OpContext,
        authz_route: &authz::RouterRoute,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_route).await?;

        use nexus_db_schema::schema::router_route::dsl;
        let now = Utc::now();
        diesel::update(dsl::router_route)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_route.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_route),
                )
            })?;
        Ok(())
    }

    pub async fn internet_gateway_detach_ip_pool(
        &self,
        opctx: &OpContext,
        igw_name: String,
        authz_igw_pool: &authz::InternetGatewayIpPool,
        ip_pool_id: Uuid,
        vpc_id: Uuid,
        cascade: bool,
    ) -> DeleteResult {
        if cascade {
            self.internet_gateway_detach_ip_pool_cascade(opctx, authz_igw_pool)
                .await
        } else {
            self.internet_gateway_detach_ip_pool_no_cascade(
                opctx,
                igw_name,
                authz_igw_pool,
                ip_pool_id,
                vpc_id,
            )
            .await
        }
    }

    pub async fn internet_gateway_detach_ip_pool_no_cascade(
        &self,
        opctx: &OpContext,
        igw_name: String,
        authz_igw_pool: &authz::InternetGatewayIpPool,
        ip_pool_id: Uuid,
        vpc_id: Uuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_igw_pool).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        #[derive(Debug)]
        enum DeleteError {
            DependentInstances,
        }

        self.transaction_retry_wrapper("internet_gateway_detach_ip_pool_no_cascade")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let igw_name = igw_name.clone();
                async move {
                    // determine if there are routes that target this igw
                    let this_target = format!("inetgw:{}", igw_name);
                    use  nexus_db_schema::schema::router_route::dsl as rr;
                    let count = rr::router_route
                        .filter(rr::time_deleted.is_null())
                        .filter(rr::target.eq(this_target))
                        .count()
                        .first_async::<i64>(&conn)
                        .await?;

                    info!(self.log, "detach ip pool: applies to {} routes", count);

                    if count > 0 {
                        // determine if there are instances that have IP
                        // addresses in the IP pool being removed.

                        use nexus_db_schema::schema::ip_pool_range::dsl as ipr;
                        let pr = ipr::ip_pool_range
                            .filter(ipr::time_deleted.is_null())
                            .filter(ipr::ip_pool_id.eq(ip_pool_id))
                            .select(IpPoolRange::as_select())
                            .first_async::<IpPoolRange>(&conn)
                            .await?;
                        info!(self.log, "POOL {pr:#?}");

                        use nexus_db_schema::schema::instance_network_interface::dsl as ini;
                        let vpc_interfaces = ini::instance_network_interface
                            .filter(ini::time_deleted.is_null())
                            .filter(ini::vpc_id.eq(vpc_id))
                            .select(InstanceNetworkInterface::as_select())
                            .load_async::<InstanceNetworkInterface>(&conn)
                            .await?;

                        info!(self.log, "detach ip pool: applies to {} interfaces", vpc_interfaces.len());

                        for ifx in &vpc_interfaces {
                            info!(self.log, "IFX {ifx:#?}");

                            use nexus_db_schema::schema::external_ip::dsl as xip;
                            let ext_ips = xip::external_ip
                                .filter(xip::time_deleted.is_null())
                                .filter(xip::parent_id.eq(ifx.instance_id))
                                .select(ExternalIp::as_select())
                                .load_async::<ExternalIp>(&conn)
                                .await?;

                            info!(self.log, "EXT IP {ext_ips:#?}");

                            for ext_ip in &ext_ips {
                                if ext_ip.ip.ip() >= pr.first_address.ip() &&
                                    ext_ip.ip.ip() <= pr.last_address.ip() {
                                    return Err(err.bail(DeleteError::DependentInstances));
                                }
                            }
                        }
                    }

                    use nexus_db_schema::schema::internet_gateway_ip_pool::dsl;
                    let now = Utc::now();
                    diesel::update(dsl::internet_gateway_ip_pool)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_igw_pool.id()))
                        .set(dsl::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        DeleteError::DependentInstances => Error::invalid_request("VPC routes dependent on this IP pool. To perform a cascading delete set the cascade option"),
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn internet_gateway_detach_ip_pool_cascade(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::InternetGatewayIpPool,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_pool).await?;
        use nexus_db_schema::schema::internet_gateway_ip_pool::dsl;
        let now = Utc::now();
        diesel::update(dsl::internet_gateway_ip_pool)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_pool.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn internet_gateway_detach_ip_address(
        &self,
        opctx: &OpContext,
        igw_name: String,
        authz_addr: &authz::InternetGatewayIpAddress,
        addr: IpAddr,
        vpc_id: Uuid,
        cascade: bool,
    ) -> DeleteResult {
        if cascade {
            self.internet_gateway_detach_ip_address_cascade(opctx, authz_addr)
                .await
        } else {
            self.internet_gateway_detach_ip_address_no_cascade(
                opctx, igw_name, authz_addr, addr, vpc_id,
            )
            .await
        }
    }

    pub async fn internet_gateway_detach_ip_address_cascade(
        &self,
        opctx: &OpContext,
        authz_addr: &authz::InternetGatewayIpAddress,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_addr).await?;

        use nexus_db_schema::schema::internet_gateway_ip_address::dsl;
        let now = Utc::now();
        diesel::update(dsl::internet_gateway_ip_address)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_addr.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_addr),
                )
            })?;
        Ok(())
    }

    pub async fn internet_gateway_detach_ip_address_no_cascade(
        &self,
        opctx: &OpContext,
        igw_name: String,
        authz_addr: &authz::InternetGatewayIpAddress,
        addr: IpAddr,
        vpc_id: Uuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_addr).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        #[derive(Debug)]
        enum DeleteError {
            DependentInstances,
        }
        self.transaction_retry_wrapper("internet_gateway_detach_ip_address_no_cascade")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let igw_name = igw_name.clone();
                async move {
                    // determine if there are routes that target this igw
                    let this_target = format!("inetgw:{}", igw_name);
                    use  nexus_db_schema::schema::router_route::dsl as rr;
                    let count = rr::router_route
                        .filter(rr::time_deleted.is_null())
                        .filter(rr::target.eq(this_target))
                        .count()
                        .first_async::<i64>(&conn)
                        .await?;

                    if count > 0 {
                        use nexus_db_schema::schema::instance_network_interface::dsl as ini;
                        let vpc_interfaces = ini::instance_network_interface
                            .filter(ini::time_deleted.is_null())
                            .filter(ini::vpc_id.eq(vpc_id))
                            .select(InstanceNetworkInterface::as_select())
                            .load_async::<InstanceNetworkInterface>(&conn)
                            .await?;

                        for ifx in &vpc_interfaces {

                            use nexus_db_schema::schema::external_ip::dsl as xip;
                            let ext_ips = xip::external_ip
                                .filter(xip::time_deleted.is_null())
                                .filter(xip::parent_id.eq(ifx.instance_id))
                                .select(ExternalIp::as_select())
                                .load_async::<ExternalIp>(&conn)
                                .await?;

                            for ext_ip in &ext_ips {
                                if ext_ip.ip.ip() == addr {
                                    return Err(err.bail(DeleteError::DependentInstances));
                                }
                            }
                        }
                    }

                    use nexus_db_schema::schema::internet_gateway_ip_address::dsl;
                    let now = Utc::now();
                    diesel::update(dsl::internet_gateway_ip_address)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_addr.id()))
                        .set(dsl::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        DeleteError::DependentInstances => Error::invalid_request("VPC routes dependent on this IP pool. To perform a cascading delete set the cascade option"),
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn router_update_route(
        &self,
        opctx: &OpContext,
        authz_route: &authz::RouterRoute,
        route_update: RouterRouteUpdate,
    ) -> UpdateResult<RouterRoute> {
        opctx.authorize(authz::Action::Modify, authz_route).await?;

        use nexus_db_schema::schema::router_route::dsl;
        diesel::update(dsl::router_route)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_route.id()))
            .set(route_update)
            .returning(RouterRoute::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_route),
                )
            })
    }

    /// Identify all subnets in use by each VpcSubnet
    pub async fn resolve_vpc_subnets_to_ip_networks<
        T: IntoIterator<Item = Name>,
    >(
        &self,
        vpc: &Vpc,
        subnet_names: T,
    ) -> Result<BTreeMap<Name, Vec<IpNetwork>>, Error> {
        #[derive(diesel::Queryable)]
        struct SubnetIps {
            name: Name,
            ipv4_block: Ipv4Net,
            ipv6_block: Ipv6Net,
        }

        use nexus_db_schema::schema::vpc_subnet;
        let subnets = vpc_subnet::table
            .filter(vpc_subnet::vpc_id.eq(vpc.id()))
            .filter(vpc_subnet::name.eq_any(subnet_names))
            .filter(vpc_subnet::time_deleted.is_null())
            .select((
                vpc_subnet::name,
                vpc_subnet::ipv4_block,
                vpc_subnet::ipv6_block,
            ))
            .get_results_async::<SubnetIps>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let mut result = BTreeMap::new();
        for subnet in subnets {
            let entry = result.entry(subnet.name).or_insert_with(Vec::new);
            entry.push(IpNetwork::V4(subnet.ipv4_block.0.into()));
            entry.push(IpNetwork::V6(subnet.ipv6_block.0.into()));
        }
        Ok(result)
    }

    /// Look up a VPC by VNI.
    pub async fn resolve_vni_to_vpc(
        &self,
        opctx: &OpContext,
        vni: Vni,
    ) -> LookupResult<Vpc> {
        use nexus_db_schema::schema::vpc::dsl;
        dsl::vpc
            .filter(dsl::vni.eq(vni))
            .filter(dsl::time_deleted.is_null())
            .select(Vpc::as_select())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vpc,
                        LookupType::ByCompositeId("VNI".to_string()),
                    ),
                )
            })
    }

    /// Look up a VNI by VPC.
    pub async fn resolve_vpc_to_vni(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> LookupResult<Vni> {
        use nexus_db_schema::schema::vpc::dsl;
        dsl::vpc
            .filter(dsl::id.eq(vpc_id))
            .filter(dsl::time_deleted.is_null())
            .select(dsl::vni)
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vpc,
                        LookupType::ByCompositeId("VNI".to_string()),
                    ),
                )
            })
    }

    /// Fetch a VPC's system router by the VPC's ID.
    pub async fn vpc_get_system_router(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> LookupResult<VpcRouter> {
        use nexus_db_schema::schema::vpc::dsl as vpc_dsl;
        use nexus_db_schema::schema::vpc_router::dsl as router_dsl;

        vpc_dsl::vpc
            .inner_join(
                router_dsl::vpc_router
                    .on(router_dsl::id.eq(vpc_dsl::system_router_id)),
            )
            .filter(vpc_dsl::time_deleted.is_null())
            .filter(vpc_dsl::id.eq(vpc_id))
            .filter(router_dsl::time_deleted.is_null())
            .filter(router_dsl::vpc_id.eq(vpc_id))
            .select(VpcRouter::as_select())
            .limit(1)
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vpc,
                        LookupType::ById(vpc_id),
                    ),
                )
            })
    }

    /// Fetch all active custom routers (and their associated subnets)
    /// in a VPC.
    pub async fn vpc_get_active_custom_routers_with_associated_subnets(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> ListResultVec<(VpcSubnet, VpcRouter)> {
        use nexus_db_schema::schema::vpc_router::dsl as router_dsl;
        use nexus_db_schema::schema::vpc_subnet::dsl as subnet_dsl;

        subnet_dsl::vpc_subnet
            .inner_join(
                router_dsl::vpc_router.on(router_dsl::id
                    .nullable()
                    .eq(subnet_dsl::custom_router_id)),
            )
            .filter(subnet_dsl::time_deleted.is_null())
            .filter(subnet_dsl::vpc_id.eq(vpc_id))
            .filter(router_dsl::time_deleted.is_null())
            .filter(router_dsl::vpc_id.eq(vpc_id))
            .select((VpcSubnet::as_select(), VpcRouter::as_select()))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vpc,
                        LookupType::ById(vpc_id),
                    ),
                )
            })
    }

    /// Fetch all custom routers in a VPC.
    pub async fn vpc_get_custom_routers(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> ListResultVec<VpcRouter> {
        use nexus_db_schema::schema::vpc_router::dsl as router_dsl;

        router_dsl::vpc_router
            .filter(router_dsl::time_deleted.is_null())
            .filter(router_dsl::vpc_id.eq(vpc_id))
            .select(VpcRouter::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vpc,
                        LookupType::ById(vpc_id),
                    ),
                )
            })
    }

    // XXX: maybe wants to live in external IP?
    /// Returns a (Ip, NicId) -> InetGwId map which identifies, for a given sled:
    /// * a) which external IPs belong to any of its services/instances/probe NICs.
    /// * b) whether each IP is linked to an Internet Gateway via its parent pool.
    pub async fn vpc_resolve_sled_external_ips_to_gateways(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<HashMap<Uuid, HashMap<IpAddr, HashSet<Uuid>>>, Error> {
        // TODO: give GW-bound addresses preferential treatment.
        use nexus_db_schema::schema::external_ip as eip;
        use nexus_db_schema::schema::external_ip::dsl as eip_dsl;
        use nexus_db_schema::schema::internet_gateway as igw;
        use nexus_db_schema::schema::internet_gateway::dsl as igw_dsl;
        use nexus_db_schema::schema::internet_gateway_ip_address as igw_ip;
        use nexus_db_schema::schema::internet_gateway_ip_address::dsl as igw_ip_dsl;
        use nexus_db_schema::schema::internet_gateway_ip_pool as igw_pool;
        use nexus_db_schema::schema::internet_gateway_ip_pool::dsl as igw_pool_dsl;
        use nexus_db_schema::schema::network_interface as ni;
        use nexus_db_schema::schema::vmm;

        // We don't know at first glance which VPC ID each IP addr has.
        // VPC info is necessary to map back to the intended gateway.
        // Goal is to join sled-specific
        //  (IP, IP pool ID, VPC ID) X (IGW, IP pool ID)
        let conn = self.pool_connection_authorized(opctx).await?;

        let mut out = HashMap::new();

        let instance_mappings = eip_dsl::external_ip
            .inner_join(
                vmm::table.on(vmm::instance_id
                    .nullable()
                    .eq(eip::parent_id)
                    .and(vmm::sled_id.eq(to_db_typed_uuid(sled_id)))),
            )
            .inner_join(
                ni::table.on(ni::parent_id.nullable().eq(eip::parent_id)),
            )
            .inner_join(
                igw_pool::table
                    .on(eip_dsl::ip_pool_id.eq(igw_pool_dsl::ip_pool_id)),
            )
            .inner_join(
                igw::table.on(igw_dsl::id
                    .eq(igw_pool_dsl::internet_gateway_id)
                    .and(igw_dsl::vpc_id.eq(ni::vpc_id))),
            )
            .filter(eip::time_deleted.is_null())
            .filter(ni::time_deleted.is_null())
            .filter(ni::is_primary.eq(true))
            .filter(vmm::time_deleted.is_null())
            .filter(igw_dsl::time_deleted.is_null())
            .filter(igw_pool_dsl::time_deleted.is_null())
            .select((eip_dsl::ip, ni::id, igw_dsl::id))
            .load_async::<(IpNetwork, Uuid, Uuid)>(&*conn)
            .await;

        match instance_mappings {
            Ok(map) => {
                for (ip, nic_id, inet_gw_id) in map {
                    let per_nic: &mut HashMap<_, _> =
                        out.entry(nic_id).or_default();
                    let igw_list: &mut HashSet<_> =
                        per_nic.entry(ip.ip()).or_default();

                    igw_list.insert(inet_gw_id);
                }
            }
            Err(e) => {
                return Err(Error::non_resourcetype_not_found(&format!(
                    "unable to find IGW mappings for sled {sled_id}: {e}"
                )));
            }
        }

        // Map all individual IPs bound to IGWs to NICs in their VPC.
        let indiv_ip_mappings = ni::table
            .inner_join(igw::table.on(igw_dsl::vpc_id.eq(ni::vpc_id)))
            .inner_join(
                igw_ip::table
                    .on(igw_ip_dsl::internet_gateway_id.eq(igw_dsl::id)),
            )
            .filter(ni::time_deleted.is_null())
            .filter(ni::kind.eq(NetworkInterfaceKind::Instance))
            .filter(igw_dsl::time_deleted.is_null())
            .filter(igw_ip_dsl::time_deleted.is_null())
            .select((igw_ip_dsl::address, ni::id, igw_dsl::id))
            .load_async::<(IpNetwork, Uuid, Uuid)>(&*conn)
            .await;

        match indiv_ip_mappings {
            Ok(map) => {
                for (ip, nic_id, inet_gw_id) in map {
                    let per_nic: &mut HashMap<_, _> =
                        out.entry(nic_id).or_default();
                    let igw_list: &mut HashSet<_> =
                        per_nic.entry(ip.ip()).or_default();

                    igw_list.insert(inet_gw_id);
                }
            }
            Err(e) => {
                return Err(Error::non_resourcetype_not_found(&format!(
                    "unable to find IGW mappings for sled {sled_id}: {e}"
                )));
            }
        }

        // TODO: service & probe EIP mappings.
        //       note that the current sled-agent design
        //       does not yet allow us to re-ensure the set of
        //       external IPs for non-instance entities.
        //       if we insert those here, we need to be sure that
        //       the mappings are ignored by sled-agent for new
        //       services/probes/etc.

        Ok(out)
    }

    /// Resolve all targets in a router into concrete details.
    pub async fn vpc_resolve_router_rules(
        &self,
        opctx: &OpContext,
        vpc_router_id: Uuid,
    ) -> Result<HashSet<ResolvedVpcRoute>, Error> {
        // Get all rules in target router.
        opctx.check_complex_operations_allowed()?;

        let (.., authz_project, authz_vpc, authz_router) =
            nexus_db_lookup::LookupPath::new(opctx, self)
                .vpc_router_id(vpc_router_id)
                .lookup_for(authz::Action::Read)
                .await
                .internal_context("lookup router by id for rules")?;
        let vpc_id = authz_vpc.id();

        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let mut all_rules = vec![];
        while let Some(p) = paginator.next() {
            let batch = self
                .vpc_router_route_list(
                    opctx,
                    &authz_router,
                    &PaginatedBy::Id(p.current_pagparams()),
                )
                .await?;
            paginator = p
                .found_batch(&batch, &|s: &nexus_db_model::RouterRoute| s.id());
            all_rules.extend(batch);
        }

        // This is not in a transaction, because...
        // We're not necessarily too concerned about getting partially
        // updated state when resolving these names. See the header discussion
        // in `nexus/src/app/background/vpc_routes.rs`: any state updates
        // are followed by a version bump/notify, so we will be eventually
        // consistent with route resolution.
        let mut subnet_names = HashSet::new();
        let mut subnet_ids = HashSet::new();
        let mut vpc_names = HashSet::new();
        let mut inetgw_names = HashSet::new();
        let mut instance_names = HashSet::new();
        for rule in &all_rules {
            match &rule.target.0 {
                RouteTarget::Vpc(n) => {
                    vpc_names.insert(n.clone());
                }
                RouteTarget::Subnet(n) => {
                    subnet_names.insert(n.clone());
                }
                RouteTarget::Instance(n) => {
                    instance_names.insert(n.clone());
                }
                RouteTarget::InternetGateway(n) => {
                    inetgw_names.insert(n.clone());
                }
                _ => {}
            }

            match &rule.destination.0 {
                RouteDestination::Vpc(n) => {
                    vpc_names.insert(n.clone());
                }
                RouteDestination::Subnet(n) => {
                    subnet_names.insert(n.clone());
                }
                _ => {}
            }

            if let (ExternalRouteKind::VpcSubnet, Some(id)) =
                (rule.kind.0, rule.vpc_subnet_id)
            {
                subnet_ids.insert(id);
            }
        }

        // Generally, we want all name lookups here to be fallible as we
        // don't have strong links between destinations, targets, and their
        // underlying resources. Such an error will be converted to a no-op
        // for any child rules.
        // However, we cannot allow 503s/CRDB communication failures to be
        // be part of any router version -- we'll have omitted rules and
        // sled-agent won't accept an update unless the version has bumped up.
        // We'll eventually retry this router when the RPW is scheduled again.
        fn allow_not_found<O>(e: Error) -> Option<Result<O, Error>> {
            match e {
                Error::ObjectNotFound { .. } | Error::NotFound { .. } => None,
                e => Some(Err(e)),
            }
        }

        // VpcSubnet routes are control-plane managed, and have a foreign-key relationship
        // to subnets they mirror. Prefer this over name resolution when possible, knowing
        // that some user routes still require name resolution.
        // TODO: This would be nice to solve in fewer queries. Should we just pull the
        //       complete set if we're a system router? We'll need it anyway.
        let mut subnets_by_id = stream::iter(subnet_ids)
            .filter_map(|id| async move {
                nexus_db_lookup::LookupPath::new(opctx, self)
                    .vpc_subnet_id(id)
                    .fetch()
                    .await
                    .map_or_else(allow_not_found, |(.., subnet)| {
                        Some(Ok((id, subnet)))
                    })
            })
            .try_collect::<HashMap<_, _>>()
            .await?;

        let mut subnets_by_name = subnets_by_id
            .values()
            .cloned()
            .map(|v| (v.name().clone(), v))
            .collect::<HashMap<_, _>>();

        for name in &subnet_names {
            if !subnets_by_name.contains_key(name) {
                continue;
            }

            let Some(res) = nexus_db_lookup::LookupPath::new(opctx, self)
                .vpc_id(vpc_id)
                .vpc_subnet_name(Name::ref_cast(name))
                .fetch()
                .await
                .map_or_else(allow_not_found, |v| Some(Ok(v)))
            else {
                continue;
            };

            let (.., subnet) = res?;

            subnets_by_id.insert(subnet.id(), subnet.clone());
            subnets_by_name.insert(subnet.name().clone(), subnet);
        }

        // TODO: unused until VPC peering.
        let _vpcs = stream::iter(vpc_names)
            .filter_map(|name| async {
                nexus_db_lookup::LookupPath::new(opctx, self)
                    .project_id(authz_project.id())
                    .vpc_name(Name::ref_cast(&name))
                    .fetch()
                    .await
                    .map_or_else(allow_not_found, |(.., vpc)| {
                        Some(Ok((name, vpc)))
                    })
            })
            .try_collect::<HashMap<_, _>>()
            .await?;

        let inetgws = stream::iter(inetgw_names)
            .filter_map(|name| async {
                nexus_db_lookup::LookupPath::new(opctx, self)
                    .vpc_id(vpc_id)
                    .internet_gateway_name(Name::ref_cast(&name))
                    .fetch()
                    .await
                    .map_or_else(allow_not_found, |(.., igw)| {
                        Some(Ok((name, igw)))
                    })
            })
            .try_collect::<HashMap<_, _>>()
            .await?;

        let instances = stream::iter(instance_names)
            .filter_map(|name| async {
                nexus_db_lookup::LookupPath::new(opctx, self)
                    .project_id(authz_project.id())
                    .instance_name(Name::ref_cast(&name))
                    .fetch()
                    .await
                    .map_or_else(allow_not_found, |(.., auth, inst)| {
                        Some(Ok((name, auth, inst)))
                    })
            })
            .try_filter_map(|(name, authz_instance, instance)| async move {
                // XXX: currently an instance can have one primary NIC,
                //      and it is not dual-stack (v4 + v6). We need
                //      to clarify what should be resolved in the v6 case.
                self.instance_get_primary_network_interface(
                    opctx,
                    &authz_instance,
                )
                .await
                .map_or_else(allow_not_found, |primary_nic| {
                    Some(Ok((name, (instance, primary_nic))))
                })
                .transpose()
            })
            .try_collect::<HashMap<_, _>>()
            .await?;

        // See the discussion in `resolve_firewall_rules_for_sled_agent` on
        // how we should resolve name misses in route resolution.
        // This method adopts the same strategy: a lookup failure corresponds
        // to a NO-OP rule.
        let mut out = HashSet::new();
        for rule in all_rules {
            let parent_subnet =
                if let (ExternalRouteKind::VpcSubnet, Some(id)) =
                    (rule.kind.0, rule.vpc_subnet_id.as_ref())
                {
                    subnets_by_id.get(id)
                } else {
                    None
                };

            // Some dests/targets (e.g., subnet) resolve to *several* specifiers
            // to handle both v4 and v6. The user-facing API will prevent severe
            // mistakes on naked IPs/CIDRs (mixed v4/6), but we need to be smarter
            // around named entities here.
            let (v4_dest, v6_dest) = match (rule.destination.0, parent_subnet) {
                // This is a VpcSubnet route. Force a `Subnet` destination.
                (_, Some(sub)) => (
                    Some(sub.ipv4_block.0.into()),
                    Some(sub.ipv6_block.0.into()),
                ),
                // Else, use the named resources. These classes of rule are
                // user-mutable.
                (RouteDestination::Ip(ip @ IpAddr::V4(_)), _) => {
                    (Some(IpNet::host_net(ip)), None)
                }
                (RouteDestination::Ip(ip @ IpAddr::V6(_)), _) => {
                    (None, Some(IpNet::host_net(ip)))
                }
                (RouteDestination::IpNet(ip @ IpNet::V4(_)), _) => {
                    (Some(ip), None)
                }
                (RouteDestination::IpNet(ip @ IpNet::V6(_)), _) => {
                    (None, Some(ip))
                }
                (RouteDestination::Subnet(n), _) => subnets_by_name
                    .get(&n)
                    .map(|s| {
                        (
                            Some(s.ipv4_block.0.into()),
                            Some(s.ipv6_block.0.into()),
                        )
                    })
                    .unwrap_or_default(),

                // TODO: VPC peering.
                (RouteDestination::Vpc(_), _) => (None, None),
            };

            let (v4_target, v6_target) = match (&rule.target.0, parent_subnet) {
                // This is a VpcSubnet route. Force a `Subnet` target.
                (_, Some(sub)) => (
                    Some(RouterTarget::VpcSubnet(sub.ipv4_block.0.into())),
                    Some(RouterTarget::VpcSubnet(sub.ipv6_block.0.into())),
                ),
                // Else, use the named resources. These classes of rule are
                // user-mutable.
                (RouteTarget::Ip(ip @ IpAddr::V4(_)), _) => {
                    (Some(RouterTarget::Ip(*ip)), None)
                }
                (RouteTarget::Ip(ip @ IpAddr::V6(_)), _) => {
                    (None, Some(RouterTarget::Ip(*ip)))
                }
                (RouteTarget::Subnet(n), _) => subnets_by_name
                    .get(&n)
                    .map(|s| {
                        (
                            Some(RouterTarget::VpcSubnet(
                                s.ipv4_block.0.into(),
                            )),
                            Some(RouterTarget::VpcSubnet(
                                s.ipv6_block.0.into(),
                            )),
                        )
                    })
                    .unwrap_or_default(),
                (RouteTarget::Instance(n), _) => instances
                    .get(&n)
                    .map(|i| match i.1.ip {
                        // TODO: update for dual-stack v4/6.
                        ip @ IpNetwork::V4(_) => {
                            (Some(RouterTarget::Ip(ip.ip())), None)
                        }
                        ip @ IpNetwork::V6(_) => {
                            (None, Some(RouterTarget::Ip(ip.ip())))
                        }
                    })
                    .unwrap_or_default(),
                (RouteTarget::Drop, _) => {
                    (Some(RouterTarget::Drop), Some(RouterTarget::Drop))
                }

                // Internet gateways tag matching packets with their ID, for
                // NAT IP selection.
                (RouteTarget::InternetGateway(n), _) => inetgws
                    .get(&n)
                    .map(|igw| {
                        let gateway_target = if is_services_vpc_gateway(igw) {
                            InternetGatewayRouterTarget::System
                        } else {
                            InternetGatewayRouterTarget::Instance(igw.id())
                        };
                        let target =
                            RouterTarget::InternetGateway(gateway_target);
                        (Some(target), Some(target))
                    })
                    .unwrap_or_default(),

                // TODO: VPC Peering.
                (RouteTarget::Vpc(_), _) => (None, None),
            };

            // XXX: Is there another way we should be handling destination
            //      collisions within a router? 'first/last wins' is fairly
            //      arbitrary when lookups are sorted on UUID, but it's
            //      unpredictable.
            //      It would be really useful to raise collisions and
            //      misses to users, somehow.
            if let (Some(dest), Some(target)) = (v4_dest, v4_target) {
                out.insert(ResolvedVpcRoute { dest, target });
            }

            if let (Some(dest), Some(target)) = (v6_dest, v6_target) {
                out.insert(ResolvedVpcRoute { dest, target });
            }
        }

        Ok(out)
    }

    /// Trigger an RPW version bump on a single VPC router in response
    /// to CRUD operations on individual routes.
    pub async fn vpc_router_increment_rpw_version(
        &self,
        opctx: &OpContext,
        router_id: Uuid,
    ) -> UpdateResult<()> {
        // NOTE: this operation and `vpc_increment_rpw_version` do not
        // have auth checks, as these can occur in connection with unrelated
        // resources -- the current user may have access to those, but be unable
        // to modify the entire set of VPC routers in a project.

        use nexus_db_schema::schema::vpc_router::dsl;
        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(router_id))
            .set(dsl::resolved_version.eq(dsl::resolved_version + 1))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Trigger an RPW version bump on *all* routers within a VPC in
    /// response to changes to named entities (e.g., subnets, instances).
    pub async fn vpc_increment_rpw_version(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::vpc_router::dsl;
        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(vpc_id))
            .set(dsl::resolved_version.eq(dsl::resolved_version + 1))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}

// Return true if this gateway belongs to the internal services VPC.
fn is_services_vpc_gateway(igw: &InternetGateway) -> bool {
    igw.vpc_id == *SERVICES_VPC_ID
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test_utils::IneligibleSleds;
    use crate::db::model::Project;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::SledUpdateBuilder;
    use crate::db::queries::vpc::MAX_VNI_SEARCH_RANGE_SIZE;
    use nexus_db_fixed_data::silo::DEFAULT_SILO;
    use nexus_db_fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
    use nexus_db_model::IncompleteNetworkInterface;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::planner::PlannerRng;
    use nexus_reconfigurator_planning::system::SledBuilder;
    use nexus_reconfigurator_planning::system::SystemDescription;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::external_api::params;
    use nexus_types::identity::Asset;
    use omicron_common::api::external;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
    use oxnet::IpNet;
    use oxnet::Ipv4Net;
    use slog::info;

    // Test that we detect the right error condition and return None when we
    // fail to insert a VPC due to VNI exhaustion.
    //
    // This is a bit awkward, but we'll test this by inserting a bunch of VPCs,
    // and checking that we get the expected error response back from the
    // `project_create_vpc_raw` call.
    #[tokio::test]
    async fn test_project_create_vpc_raw_returns_none_on_vni_exhaustion() {
        let logctx = dev::test_setup_log(
            "test_project_create_vpc_raw_returns_none_on_vni_exhaustion",
        );
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project.
        let project_params = params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: "project".parse().unwrap(),
                description: String::from("test project"),
            },
        };
        let project = Project::new(Uuid::new_v4(), project_params);
        let (authz_project, _) = datastore
            .project_create(&opctx, project)
            .await
            .expect("failed to create project");

        let starting_vni = 2048;
        let description = String::from("test vpc");
        for vni in 0..=MAX_VNI_SEARCH_RANGE_SIZE {
            // Create an incomplete VPC and make sure it has the next available
            // VNI.
            let name: external::Name = format!("vpc{vni}").parse().unwrap();
            let mut incomplete_vpc = IncompleteVpc::new(
                Uuid::new_v4(),
                authz_project.id(),
                Uuid::new_v4(),
                params::VpcCreate {
                    identity: IdentityMetadataCreateParams {
                        name: name.clone(),
                        description: description.clone(),
                    },
                    ipv6_prefix: None,
                    dns_name: name.clone(),
                },
            )
            .expect("failed to create incomplete VPC");
            let this_vni =
                Vni(external::Vni::try_from(starting_vni + vni).unwrap());
            incomplete_vpc.vni = this_vni;
            info!(
                log,
                "creating initial VPC";
                "index" => vni,
                "vni" => ?this_vni,
            );
            let query = InsertVpcQuery::new(incomplete_vpc);
            let (_, db_vpc) = datastore
                .project_create_vpc_raw(&opctx, &authz_project, query)
                .await
                .expect("failed to create initial set of VPCs")
                .expect("expected an actual VPC");
            info!(
                log,
                "created VPC";
                "vpc" => ?db_vpc,
            );
        }

        // At this point, we've filled all the VNIs starting from 2048. Let's
        // try to allocate one more, also starting from that position. This
        // should fail, because we've explicitly filled the entire range we'll
        // search above.
        let name: external::Name = "dead-vpc".parse().unwrap();
        let mut incomplete_vpc = IncompleteVpc::new(
            Uuid::new_v4(),
            authz_project.id(),
            Uuid::new_v4(),
            params::VpcCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.clone(),
                    description: description.clone(),
                },
                ipv6_prefix: None,
                dns_name: name.clone(),
            },
        )
        .expect("failed to create incomplete VPC");
        let this_vni = Vni(external::Vni::try_from(starting_vni).unwrap());
        incomplete_vpc.vni = this_vni;
        info!(
            log,
            "creating VPC when all VNIs are allocated";
            "vni" => ?this_vni,
        );
        let query = InsertVpcQuery::new(incomplete_vpc);
        let Ok(None) = datastore
            .project_create_vpc_raw(&opctx, &authz_project, query)
            .await
        else {
            panic!(
                "Expected Ok(None) when creating a VPC without any available VNIs"
            );
        };
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Test that we appropriately retry when there are no available VNIs.
    //
    // This is a bit awkward, but we'll test this by inserting a bunch of VPCs,
    // and then check that we correctly retry
    #[tokio::test]
    async fn test_project_create_vpc_retries() {
        let logctx = dev::test_setup_log("test_project_create_vpc_retries");
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project.
        let project_params = params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: "project".parse().unwrap(),
                description: String::from("test project"),
            },
        };
        let project = Project::new(Uuid::new_v4(), project_params);
        let (authz_project, _) = datastore
            .project_create(&opctx, project)
            .await
            .expect("failed to create project");

        let starting_vni = 2048;
        let description = String::from("test vpc");
        for vni in 0..=MAX_VNI_SEARCH_RANGE_SIZE {
            // Create an incomplete VPC and make sure it has the next available
            // VNI.
            let name: external::Name = format!("vpc{vni}").parse().unwrap();
            let mut incomplete_vpc = IncompleteVpc::new(
                Uuid::new_v4(),
                authz_project.id(),
                Uuid::new_v4(),
                params::VpcCreate {
                    identity: IdentityMetadataCreateParams {
                        name: name.clone(),
                        description: description.clone(),
                    },
                    ipv6_prefix: None,
                    dns_name: name.clone(),
                },
            )
            .expect("failed to create incomplete VPC");
            let this_vni =
                Vni(external::Vni::try_from(starting_vni + vni).unwrap());
            incomplete_vpc.vni = this_vni;
            info!(
                log,
                "creating initial VPC";
                "index" => vni,
                "vni" => ?this_vni,
            );
            let query = InsertVpcQuery::new(incomplete_vpc);
            let (_, db_vpc) = datastore
                .project_create_vpc_raw(&opctx, &authz_project, query)
                .await
                .expect("failed to create initial set of VPCs")
                .expect("expected an actual VPC");
            info!(
                log,
                "created VPC";
                "vpc" => ?db_vpc,
            );
        }

        // Similar to the above test, we've fill all available VPCs starting at
        // `starting_vni`. Let's attempt to allocate one beginning there, which
        // _should_ fail and be internally retried. Note that we're using
        // `project_create_vpc()` here instead of the raw version, to check that
        // retry logic.
        let name: external::Name = "dead-at-first-vpc".parse().unwrap();
        let mut incomplete_vpc = IncompleteVpc::new(
            Uuid::new_v4(),
            authz_project.id(),
            Uuid::new_v4(),
            params::VpcCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.clone(),
                    description: description.clone(),
                },
                ipv6_prefix: None,
                dns_name: name.clone(),
            },
        )
        .expect("failed to create incomplete VPC");
        let this_vni = Vni(external::Vni::try_from(starting_vni).unwrap());
        incomplete_vpc.vni = this_vni;
        info!(
            log,
            "creating VPC when all VNIs are allocated";
            "vni" => ?this_vni,
        );
        match datastore
            .project_create_vpc(&opctx, &authz_project, incomplete_vpc.clone())
            .await
        {
            Ok((_, vpc)) => {
                assert_eq!(vpc.id(), incomplete_vpc.identity.id);
                let expected_vni = starting_vni + MAX_VNI_SEARCH_RANGE_SIZE + 1;
                assert_eq!(u32::from(vpc.vni.0), expected_vni);
                info!(log, "successfully created VPC after retries"; "vpc" => ?vpc);
            }
            Err(e) => panic!("Unexpected error when inserting VPC: {e}"),
        };
        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn assert_service_sled_ids(
        datastore: &DataStore,
        expected_sled_ids: &[SledUuid],
    ) {
        let mut service_sled_ids = datastore
            .vpc_resolve_to_sleds(*SERVICES_VPC_ID, &[])
            .await
            .expect("failed to resolve to sleds")
            .into_iter()
            .map(|sled| sled.id())
            .collect::<Vec<_>>();
        service_sled_ids.sort();
        assert_eq!(expected_sled_ids, service_sled_ids);
    }

    async fn bp_insert_and_make_target(
        opctx: &OpContext,
        datastore: &DataStore,
        bp: &Blueprint,
    ) {
        datastore
            .blueprint_insert(opctx, bp)
            .await
            .expect("inserted blueprint");
        datastore
            .blueprint_target_set_current(
                opctx,
                BlueprintTarget {
                    target_id: bp.id,
                    enabled: true,
                    time_made_target: Utc::now(),
                },
            )
            .await
            .expect("made blueprint the target");
    }

    #[tokio::test]
    async fn test_vpc_resolve_to_sleds_uses_current_target_blueprint() {
        // Test setup.
        let logctx = dev::test_setup_log(
            "test_vpc_resolve_to_sleds_uses_current_target_blueprint",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Set up our fake system with 5 sleds.
        let rack_id = Uuid::new_v4();
        let mut system = SystemDescription::new();
        let mut sled_ids = Vec::new();
        for _ in 0..5 {
            let sled_id = SledUuid::new_v4();
            sled_ids.push(sled_id);
            system.sled(SledBuilder::new().id(sled_id)).expect("adding sled");
            let sled_update = SledUpdateBuilder::new()
                .sled_id(sled_id)
                .rack_id(rack_id)
                .build();
            datastore.sled_upsert(sled_update).await.expect("upserting sled");
        }
        sled_ids.sort_unstable();
        let planning_input = system
            .to_planning_input_builder()
            .expect("creating planning builder")
            .build();

        // Helper to convert a zone's nic into an insertable nic.
        let db_nic_from_zone = |zone_config: &BlueprintZoneConfig| {
            let (_, nic) = zone_config
                .zone_type
                .external_networking()
                .expect("external networking for zone type");
            IncompleteNetworkInterface::new_service(
                nic.id,
                zone_config.id.into_untyped_uuid(),
                NEXUS_VPC_SUBNET.clone(),
                IdentityMetadataCreateParams {
                    name: nic.name.clone(),
                    description: nic.name.to_string(),
                },
                nic.ip,
                nic.mac,
                nic.slot,
            )
            .expect("creating service nic")
        };

        // Create an initial, empty blueprint, and make it the target.
        let bp0 = BlueprintBuilder::build_empty_with_sleds(
            sled_ids.iter().copied(),
            "test",
        );
        bp_insert_and_make_target(&opctx, &datastore, &bp0).await;

        // Our blueprint doesn't describe any services, so we shouldn't find any
        // sled IDs running services.
        assert_service_sled_ids(&datastore, &[]).await;

        // Build an initial empty collection
        let collection =
            system.to_collection_builder().expect("collection builder").build();

        // Create a blueprint that has a Nexus on our third sled.
        let bp1 = {
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &bp0,
                &planning_input,
                &collection,
                "test",
                PlannerRng::from_entropy(),
            )
            .expect("created blueprint builder");
            for &sled_id in &sled_ids {
                builder
                    .sled_add_disks(
                        sled_id,
                        &planning_input
                            .sled_lookup(SledFilter::InService, sled_id)
                            .expect("found sled")
                            .resources,
                    )
                    .expect("ensured disks");
            }
            builder
                .sled_add_zone_nexus_with_config(
                    sled_ids[2],
                    false,
                    Vec::new(),
                    BlueprintZoneImageSource::InstallDataset,
                )
                .expect("added nexus to third sled");
            builder.build()
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp1).await;

        // bp1 is the target, but we haven't yet inserted a vNIC record, so
        // we still won't see any services on sleds.
        assert_service_sled_ids(&datastore, &[]).await;

        // Insert the relevant service NIC record (normally performed by the
        // reconfigurator's executor).
        let bp1_nic = datastore
            .service_create_network_interface_raw(
                &opctx,
                db_nic_from_zone(
                    bp1.sleds[&sled_ids[2]].zones.first().unwrap(),
                ),
            )
            .await
            .expect("failed to insert service VNIC");
        // We should now see our third sled running a service.
        assert_service_sled_ids(&datastore, &[sled_ids[2]]).await;

        // Create another blueprint, remove the one nexus we added, and make it
        // the target.
        let bp2 = {
            let mut bp2 = bp1.clone();
            bp2.id = BlueprintUuid::new_v4();
            bp2.parent_blueprint_id = Some(bp1.id);
            let sled2 =
                bp2.sleds.get_mut(&sled_ids[2]).expect("config for third sled");
            sled2.zones.clear();
            sled2.sled_agent_generation = sled2.sled_agent_generation.next();
            bp2
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp2).await;

        // We haven't removed the service NIC record, but we should no longer
        // see the third sled here. We should be back to no sleds with services.
        assert_service_sled_ids(&datastore, &[]).await;

        // Delete the service NIC record so we can reuse this IP later.
        datastore
            .service_delete_network_interface(
                &opctx,
                bp1.sleds[&sled_ids[2]]
                    .zones
                    .first()
                    .unwrap()
                    .id
                    .into_untyped_uuid(),
                bp1_nic.id(),
            )
            .await
            .expect("deleted bp1 nic");

        // Create a blueprint with Nexus on all our sleds.
        let bp3 = {
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &bp2,
                &planning_input,
                &collection,
                "test",
                PlannerRng::from_entropy(),
            )
            .expect("created blueprint builder");
            for &sled_id in &sled_ids {
                builder
                    .sled_add_zone_nexus_with_config(
                        sled_id,
                        false,
                        Vec::new(),
                        BlueprintZoneImageSource::InstallDataset,
                    )
                    .expect("added nexus to third sled");
            }
            builder.build()
        };

        // Insert the service NIC records for all the Nexuses.
        for &sled_id in &sled_ids {
            datastore
                .service_create_network_interface_raw(
                    &opctx,
                    db_nic_from_zone(
                        bp3.sleds[&sled_id].zones.first().unwrap(),
                    ),
                )
                .await
                .expect("failed to insert service VNIC");
        }

        // We haven't made bp3 the target yet, so our resolution is still based
        // on bp2; more service vNICs shouldn't matter.
        assert_service_sled_ids(&datastore, &[]).await;

        // Make bp3 the target; we should immediately resolve that there are
        // services on the sleds we set up in bp3.
        bp_insert_and_make_target(&opctx, &datastore, &bp3).await;
        assert_service_sled_ids(&datastore, &sled_ids).await;

        // ---

        // Mark some sleds as ineligible. Only the non-provisionable and
        // in-service sleds should be returned.
        let ineligible = IneligibleSleds {
            expunged: sled_ids[0],
            decommissioned: sled_ids[1],
            illegal_decommissioned: sled_ids[2],
            non_provisionable: sled_ids[3],
        };
        ineligible
            .setup(&opctx, &datastore)
            .await
            .expect("failed to set up ineligible sleds");
        assert_service_sled_ids(&datastore, &sled_ids[3..=4]).await;

        // ---

        // Bring the sleds marked above back to life.
        ineligible
            .undo(&opctx, &datastore)
            .await
            .expect("failed to undo ineligible sleds");
        assert_service_sled_ids(&datastore, &sled_ids).await;

        // Make a new blueprint marking one of the zones as expunged. Ensure
        // that the sled  the expunged zone is not returned by
        // vpc_resolve_to_sleds. (But other services are still running.)
        let bp4 = {
            let mut bp4 = bp3.clone();
            bp4.id = BlueprintUuid::new_v4();
            bp4.parent_blueprint_id = Some(bp3.id);

            // Sled index 3's zone is expunged (should be excluded).
            let sled3 =
                bp4.sleds.get_mut(&sled_ids[3]).expect("config for sled");
            sled3.zones.iter_mut().next().unwrap().disposition =
                BlueprintZoneDisposition::Expunged {
                    as_of_generation: Generation::new(),
                    ready_for_cleanup: false,
                };
            sled3.sled_agent_generation = sled3.sled_agent_generation.next();

            bp4
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp4).await;
        assert_service_sled_ids(
            &datastore,
            &[sled_ids[0], sled_ids[1], sled_ids[2], sled_ids[4]],
        )
        .await;

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn create_initial_vpc(
        log: &slog::Logger,
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> (authz::Project, authz::Vpc, Vpc, authz::VpcRouter, VpcRouter) {
        // Create a project and VPC.
        let project_params = params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: "project".parse().unwrap(),
                description: String::from("test project"),
            },
        };
        let project = Project::new(DEFAULT_SILO.id(), project_params);
        let (authz_project, _) = datastore
            .project_create(&opctx, project)
            .await
            .expect("failed to create project");

        let vpc_name: external::Name = "my-vpc".parse().unwrap();
        let description = String::from("test vpc");
        let mut incomplete_vpc = IncompleteVpc::new(
            Uuid::new_v4(),
            authz_project.id(),
            Uuid::new_v4(),
            params::VpcCreate {
                identity: IdentityMetadataCreateParams {
                    name: vpc_name.clone(),
                    description: description.clone(),
                },
                ipv6_prefix: None,
                dns_name: vpc_name.clone(),
            },
        )
        .expect("failed to create incomplete VPC");
        let this_vni = Vni(external::Vni::try_from(2048).unwrap());
        incomplete_vpc.vni = this_vni;
        info!(
            log,
            "creating initial VPC";
            "vni" => ?this_vni,
        );
        let query = InsertVpcQuery::new(incomplete_vpc);
        let (authz_vpc, db_vpc) = datastore
            .project_create_vpc_raw(&opctx, &authz_project, query)
            .await
            .expect("failed to create initial set of VPCs")
            .expect("expected an actual VPC");
        info!(
            log,
            "created VPC";
            "vpc" => ?db_vpc,
        );

        // Now create the system router for this VPC. Subnet CRUD
        // operations need this defined to succeed.
        let router = VpcRouter::new(
            db_vpc.system_router_id,
            db_vpc.id(),
            VpcRouterKind::System,
            nexus_types::external_api::params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: "system".parse().unwrap(),
                    description: description.clone(),
                },
            },
        );

        let (authz_router, db_router) = datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await
            .unwrap();

        (authz_project, authz_vpc, db_vpc, authz_router, db_router)
    }

    /// Create a new subnet, while also doing the additional steps
    /// covered by the subnet create saga.
    #[allow(clippy::too_many_arguments)]
    async fn new_subnet_ez(
        opctx: &OpContext,
        datastore: &DataStore,
        db_vpc: &Vpc,
        authz_vpc: &authz::Vpc,
        authz_router: &authz::VpcRouter,
        name: &str,
        ip: [u8; 4],
        prefix_len: u8,
    ) -> (authz::VpcSubnet, VpcSubnet) {
        let ipv6_block = db_vpc
            .ipv6_prefix
            .random_subnet(
                omicron_common::address::VPC_SUBNET_IPV6_PREFIX_LENGTH,
            )
            .map(|block| block.0)
            .unwrap();

        let out = datastore
            .vpc_create_subnet(
                &opctx,
                authz_vpc,
                db::model::VpcSubnet::new(
                    Uuid::new_v4(),
                    db_vpc.id(),
                    IdentityMetadataCreateParams {
                        name: name.parse().unwrap(),
                        description: "A subnet...".into(),
                    },
                    Ipv4Net::new(core::net::Ipv4Addr::from(ip), prefix_len)
                        .unwrap(),
                    ipv6_block,
                ),
            )
            .await
            .unwrap();

        datastore
            .vpc_create_subnet_route(
                &opctx,
                authz_router,
                &out.1,
                Uuid::new_v4(),
            )
            .await
            .unwrap();

        datastore
            .vpc_increment_rpw_version(&opctx, authz_vpc.id())
            .await
            .unwrap();

        out
    }

    /// Remove an existing subnet, while also doing the additional steps
    /// covered by the subnet delete saga.
    async fn del_subnet_ez(
        opctx: &OpContext,
        datastore: &DataStore,
        authz_subnet: &authz::VpcSubnet,
        db_subnet: &db::model::VpcSubnet,
    ) {
        datastore
            .vpc_delete_subnet(opctx, db_subnet, authz_subnet)
            .await
            .unwrap();
        datastore.vpc_delete_subnet_route(opctx, authz_subnet).await.unwrap();
        datastore
            .vpc_increment_rpw_version(&opctx, db_subnet.vpc_id)
            .await
            .unwrap();
    }

    // Test to verify that subnet CRUD operations are correctly
    // reflected in the nexus-managed system router attached to a VPC,
    // and that these resolve to the v4/6 subnets of each.
    #[tokio::test]
    async fn test_vpc_system_router_sync_to_subnets() {
        let logctx =
            dev::test_setup_log("test_vpc_system_router_sync_to_subnets");
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (_, authz_vpc, db_vpc, authz_router, db_router) =
            create_initial_vpc(log, &opctx, &datastore).await;

        // InternetGateway route creation is handled by the saga proper,
        // so we'll only have subnet routes here. Initially, we start with none:
        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[],
        )
        .await;

        // Add a new subnet and we should get a new route.
        let (authz_sub0, sub0) = new_subnet_ez(
            &opctx,
            &datastore,
            &db_vpc,
            &authz_vpc,
            &authz_router,
            "s0",
            [172, 30, 0, 0],
            22,
        )
        .await;

        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[&sub0],
        )
        .await;

        // Add another, and get another route.
        let (authz_sub1, sub1) = new_subnet_ez(
            &opctx,
            &datastore,
            &db_vpc,
            &authz_vpc,
            &authz_router,
            "s1",
            [172, 31, 0, 0],
            22,
        )
        .await;

        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[&sub0, &sub1],
        )
        .await;

        // Rename one subnet, and our invariants should hold.
        let sub0 = datastore
            .vpc_update_subnet(
                &opctx,
                &authz_sub0,
                None,
                VpcSubnetUpdate {
                    name: Some(
                        "a-new-name".parse::<external::Name>().unwrap().into(),
                    ),
                    description: None,
                    time_modified: Utc::now(),
                    custom_router_id: None,
                },
            )
            .await
            .unwrap();

        datastore
            .vpc_increment_rpw_version(&opctx, authz_vpc.id())
            .await
            .unwrap();

        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[&sub0, &sub1],
        )
        .await;

        // Delete one, and routes should stay in sync.
        del_subnet_ez(&opctx, &datastore, &authz_sub0, &sub0).await;

        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[&sub1],
        )
        .await;

        // If we use a reserved name, we should be able to update the table.
        let sub1 = datastore
            .vpc_update_subnet(
                &opctx,
                &authz_sub1,
                None,
                VpcSubnetUpdate {
                    name: Some(
                        "default-v4".parse::<external::Name>().unwrap().into(),
                    ),
                    description: None,
                    time_modified: Utc::now(),
                    custom_router_id: None,
                },
            )
            .await
            .unwrap();

        datastore
            .vpc_increment_rpw_version(&opctx, authz_vpc.id())
            .await
            .unwrap();

        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[&sub1],
        )
        .await;

        // Ditto for adding such a route.
        let (_, sub0) = new_subnet_ez(
            &opctx,
            &datastore,
            &db_vpc,
            &authz_vpc,
            &authz_router,
            "default-v6",
            [172, 30, 0, 0],
            22,
        )
        .await;

        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[&sub0, &sub1],
        )
        .await;

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn verify_all_subnet_routes_in_router(
        opctx: &OpContext,
        datastore: &DataStore,
        router_id: Uuid,
        subnets: &[&VpcSubnet],
    ) -> Vec<RouterRoute> {
        let conn = datastore.pool_connection_authorized(opctx).await.unwrap();

        use nexus_db_schema::schema::router_route::dsl;
        let routes = dsl::router_route
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_router_id.eq(router_id))
            .filter(dsl::kind.eq(RouterRouteKind(ExternalRouteKind::VpcSubnet)))
            .select(RouterRoute::as_select())
            .load_async(&*conn)
            .await
            .unwrap();

        // We should have exactly as many subnet routes as subnets.
        assert_eq!(routes.len(), subnets.len());

        let mut names: HashMap<_, _> =
            subnets.iter().map(|s| (s.name().clone(), 0usize)).collect();

        // Each should have a target+dest bound to a subnet by name.
        for route in &routes {
            let found_name = match &route.target.0 {
                RouteTarget::Subnet(name) => name,
                e => panic!("found target {e:?} instead of Subnet({{name}})"),
            };

            match &route.destination.0 {
                RouteDestination::Subnet(name) => assert_eq!(name, found_name),
                e => panic!("found dest {e:?} instead of Subnet({{name}})"),
            }

            *names.get_mut(found_name).unwrap() += 1;
        }

        // Each name should be used exactly once.
        for (name, count) in names {
            assert_eq!(count, 1, "subnet {name} should appear exactly once")
        }

        // Resolve the routes: we should have two for each entry:
        let resolved = datastore
            .vpc_resolve_router_rules(&opctx, router_id)
            .await
            .unwrap();
        assert_eq!(resolved.len(), 2 * subnets.len());

        // And each subnet generates a v4->v4 and v6->v6.
        for subnet in subnets {
            assert!(resolved.iter().any(|x| {
                let k = &x.dest;
                let v = &x.target;
                *k == subnet.ipv4_block.0.into()
                    && match v {
                        RouterTarget::VpcSubnet(ip) => {
                            *ip == subnet.ipv4_block.0.into()
                        }
                        _ => false,
                    }
            }));
            assert!(resolved.iter().any(|x| {
                let k = &x.dest;
                let v = &x.target;
                *k == subnet.ipv6_block.0.into()
                    && match v {
                        RouterTarget::VpcSubnet(ip) => {
                            *ip == subnet.ipv6_block.0.into()
                        }
                        _ => false,
                    }
            }));
        }

        routes
    }

    // Test to verify that VPC routers resolve to the primary addr
    // of an instance NIC.
    #[tokio::test]
    async fn test_vpc_router_rule_instance_resolve() {
        let logctx =
            dev::test_setup_log("test_vpc_router_rule_instance_resolve");
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, authz_vpc, db_vpc, authz_router, _) =
            create_initial_vpc(log, &opctx, &datastore).await;

        // Create a subnet for an instance to live in.
        let (authz_sub0, sub0) = new_subnet_ez(
            &opctx,
            &datastore,
            &db_vpc,
            &authz_vpc,
            &authz_router,
            "s0",
            [172, 30, 0, 0],
            22,
        )
        .await;

        // Add a rule pointing to the instance before it is created.
        // We're commiting some minor data integrity sins by putting
        // these into a system router, but that's irrelevant to resolution.
        let inst_name = "insty".parse::<external::Name>().unwrap();
        let _ = datastore
            .router_create_route(
                &opctx,
                &authz_router,
                RouterRoute::new(
                    Uuid::new_v4(),
                    authz_router.id(),
                    external::RouterRouteKind::Custom,
                    params::RouterRouteCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "to-vpn".parse().unwrap(),
                            description: "A rule...".into(),
                        },
                        target: external::RouteTarget::Instance(
                            inst_name.clone(),
                        ),
                        destination: external::RouteDestination::IpNet(
                            "192.168.0.0/16".parse().unwrap(),
                        ),
                    },
                ),
            )
            .await
            .unwrap();

        // Resolve the rules: we will have two entries generated by the
        // VPC subnet (v4, v6). The absence of an instance does not cause
        // us to bail, it is simply a name resolution failure.
        let routes = datastore
            .vpc_resolve_router_rules(&opctx, authz_router.id())
            .await
            .unwrap();

        assert_eq!(routes.len(), 2);

        // Create an instance, this will have no effect for now as
        // the instance lacks a NIC.
        let db_inst = datastore
            .project_create_instance(
                &opctx,
                &authz_project,
                db::model::Instance::new(
                    InstanceUuid::new_v4(),
                    authz_project.id(),
                    &params::InstanceCreate {
                        identity: IdentityMetadataCreateParams {
                            name: inst_name.clone(),
                            description: "An instance...".into(),
                        },
                        ncpus: external::InstanceCpuCount(1),
                        memory: 10.into(),
                        hostname: "insty".parse().unwrap(),
                        user_data: vec![],
                        network_interfaces:
                            params::InstanceNetworkInterfaceAttachment::None,
                        external_ips: vec![],
                        disks: vec![],
                        boot_disk: None,
                        ssh_public_keys: None,
                        start: false,
                        auto_restart_policy: Default::default(),
                        anti_affinity_groups: Vec::new(),
                    },
                ),
            )
            .await
            .unwrap();
        let (.., authz_instance) =
            nexus_db_lookup::LookupPath::new(&opctx, datastore)
                .instance_id(db_inst.id())
                .lookup_for(authz::Action::CreateChild)
                .await
                .unwrap();

        let routes = datastore
            .vpc_resolve_router_rules(&opctx, authz_router.id())
            .await
            .unwrap();

        assert_eq!(routes.len(), 2);

        // Create a primary NIC on the instance; the route can now resolve
        // to the instance's IP.
        let nic = datastore
            .instance_create_network_interface(
                &opctx,
                &authz_sub0,
                &authz_instance,
                IncompleteNetworkInterface::new_instance(
                    Uuid::new_v4(),
                    InstanceUuid::from_untyped_uuid(db_inst.id()),
                    sub0,
                    IdentityMetadataCreateParams {
                        name: "nic".parse().unwrap(),
                        description: "A NIC...".into(),
                    },
                    None,
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let routes = datastore
            .vpc_resolve_router_rules(&opctx, authz_router.id())
            .await
            .unwrap();

        // Verify we now have a route pointing at this instance.
        assert_eq!(routes.len(), 3);
        assert!(routes.iter().any(|x| (x.dest
            == "192.168.0.0/16".parse::<IpNet>().unwrap())
            && match x.target {
                RouterTarget::Ip(ip) => ip == nic.ip.ip(),
                _ => false,
            }));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
