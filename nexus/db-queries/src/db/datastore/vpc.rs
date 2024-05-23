// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Vpc`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::fixed_data::vpc::SERVICES_VPC_ID;
use crate::db::identity::Resource;
use crate::db::model::ApplyBlueprintZoneFilterExt;
use crate::db::model::ApplySledFilterExt;
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
use crate::db::model::{Ipv4Net, Ipv6Net};
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::queries::vpc::InsertVpcQuery;
use crate::db::queries::vpc::VniSearchIter;
use crate::db::queries::vpc_subnet::FilterConflictingVpcSubnetRangesQuery;
use crate::db::queries::vpc_subnet::SubnetError;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use ipnetwork::IpNetwork;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::SledFilter;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteKind as ExternalRouteKind;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::Vni as ExternalVni;
use omicron_common::api::internal::shared::RouterTarget;
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
        use crate::db::fixed_data::project::SERVICES_PROJECT_ID;
        use crate::db::fixed_data::vpc::SERVICES_VPC;
        use crate::db::fixed_data::vpc::SERVICES_VPC_DEFAULT_V4_ROUTE_ID;
        use crate::db::fixed_data::vpc::SERVICES_VPC_DEFAULT_V6_ROUTE_ID;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in VPCs");

        // Create built-in VPC for Oxide Services

        let (_, authz_project) = db::lookup::LookupPath::new(opctx, self)
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

        // Also add the system router and internet gateway route

        let system_router = db::lookup::LookupPath::new(opctx, self)
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

        // Unwrap safety: these are known valid CIDR blocks.
        let default_ips = [
            (
                "default-v4",
                "0.0.0.0/0".parse().unwrap(),
                *SERVICES_VPC_DEFAULT_V4_ROUTE_ID,
            ),
            (
                "default-v6",
                "::/0".parse().unwrap(),
                *SERVICES_VPC_DEFAULT_V6_ROUTE_ID,
            ),
        ];

        for (name, default, uuid) in default_ips {
            let route = RouterRoute::new(
                uuid,
                SERVICES_VPC.system_router_id,
                ExternalRouteKind::Default,
                nexus_types::external_api::params::RouterRouteCreate {
                    identity: IdentityMetadataCreateParams {
                        name: name.parse().unwrap(),
                        description:
                            "Default internet gateway route for Oxide Services"
                                .to_string(),
                    },
                    target: RouteTarget::InternetGateway(
                        "outbound".parse().unwrap(),
                    ),
                    destination: RouteDestination::IpNet(default),
                },
            );
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
        use db::fixed_data::vpc_firewall_rule::DNS_VPC_FW_RULE;
        use db::fixed_data::vpc_firewall_rule::NEXUS_VPC_FW_RULE;

        debug!(opctx.log, "attempting to create built-in VPC firewall rules");

        // Create firewall rules for Oxide Services

        let (_, _, authz_vpc) = db::lookup::LookupPath::new(opctx, self)
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

        fw_rules.entry(DNS_VPC_FW_RULE.name.clone()).or_insert_with(|| {
            VpcFirewallRule::new(
                Uuid::new_v4(),
                *SERVICES_VPC_ID,
                &DNS_VPC_FW_RULE,
            )
        });
        fw_rules.entry(NEXUS_VPC_FW_RULE.name.clone()).or_insert_with(|| {
            VpcFirewallRule::new(
                Uuid::new_v4(),
                *SERVICES_VPC_ID,
                &NEXUS_VPC_FW_RULE,
            )
        });

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
        use crate::db::fixed_data::vpc::SERVICES_VPC;
        use crate::db::fixed_data::vpc_subnet::DNS_VPC_SUBNET;
        use crate::db::fixed_data::vpc_subnet::DNS_VPC_SUBNET_ROUTE_ID;
        use crate::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
        use crate::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET_ROUTE_ID;
        use crate::db::fixed_data::vpc_subnet::NTP_VPC_SUBNET;
        use crate::db::fixed_data::vpc_subnet::NTP_VPC_SUBNET_ROUTE_ID;

        debug!(opctx.log, "attempting to create built-in VPC Subnets");

        // Create built-in VPC Subnets for Oxide Services

        let (_, _, authz_vpc) = db::lookup::LookupPath::new(opctx, self)
            .vpc_id(*SERVICES_VPC_ID)
            .lookup_for(authz::Action::CreateChild)
            .await
            .internal_context("lookup built-in services vpc")?;
        for (vpc_subnet, route_id) in [
            (&*DNS_VPC_SUBNET, *DNS_VPC_SUBNET_ROUTE_ID),
            (&*NEXUS_VPC_SUBNET, *NEXUS_VPC_SUBNET_ROUTE_ID),
            (&*NTP_VPC_SUBNET, *NTP_VPC_SUBNET_ROUTE_ID),
        ] {
            if let Ok(_) = db::lookup::LookupPath::new(opctx, self)
                .vpc_subnet_id(vpc_subnet.id())
                .fetch()
                .await
            {
                continue;
            }
            self.vpc_create_subnet(opctx, &authz_vpc, vpc_subnet.clone())
                .await
                .map(|_| ())
                .map_err(SubnetError::into_external)
                .or_else(|e| match e {
                    Error::ObjectAlreadyExists { .. } => Ok(()),
                    _ => Err(e),
                })?;

            let route = RouterRoute::for_subnet(
                route_id,
                SERVICES_VPC.system_router_id,
                vpc_subnet.name().clone().into(),
            )
            .expect("builtin service names are short enough for route naming");
            self.router_create_route(opctx, &authz_router, route)
                .await
                .map(|_| ())
                .or_else(|e| match e {
                    Error::ObjectAlreadyExists { .. } => Ok(()),
                    _ => Err(e),
                })?;
        }

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

        use db::schema::vpc::dsl;
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
            crate::probes::vni__search__range__start!(|| {
                (&id, u32::from(vni), VniSearchIter::STEP_SIZE)
            });
            match self
                .project_create_vpc_raw(
                    opctx,
                    authz_project,
                    InsertVpcQuery::new(vpc.clone()),
                )
                .await
            {
                Ok(Some((authz_vpc, vpc))) => {
                    crate::probes::vni__search__range__found!(|| {
                        (&id, u32::from(vpc.vni.0))
                    });
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
        use db::schema::vpc::dsl;

        assert_eq!(authz_project.id(), vpc_query.vpc.project_id);
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

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

        use db::schema::vpc::dsl;
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

        use db::schema::vpc::dsl;
        use db::schema::vpc_subnet;

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
        use db::schema::vpc_firewall_rule::dsl;

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
        use db::schema::vpc_firewall_rule::dsl;

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

        use db::schema::vpc_firewall_rule::dsl;

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

    /// Return the list of `Sled`s hosting instances or control plane services
    /// with network interfaces on the provided VPC.
    pub async fn vpc_resolve_to_sleds(
        &self,
        vpc_id: Uuid,
        sleds_filter: &[Uuid],
    ) -> Result<Vec<Sled>, Error> {
        // Resolve each VNIC in the VPC to the Sled it's on, so we know which
        // Sleds to notify when firewall rules change.
        use db::schema::{
            bp_omicron_zone, bp_target, instance, instance_network_interface,
            service_network_interface, sled, vmm,
        };
        // Diesel requires us to use aliases in order to refer to the
        // `bp_target` table twice in the same query.
        let (bp_target1, bp_target2) = diesel::alias!(
            db::schema::bp_target as bp_target1,
            db::schema::bp_target as bp_target2
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
            .blueprint_zone_filter(
                BlueprintZoneFilter::ShouldDeployVpcFirewallRules,
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
            sleds = sleds.filter(sled::id.eq_any(sleds_filter.to_vec()));
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

        use db::schema::vpc_subnet::dsl;
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
    ) -> Result<(authz::VpcSubnet, VpcSubnet), SubnetError> {
        opctx
            .authorize(authz::Action::CreateChild, authz_vpc)
            .await
            .map_err(SubnetError::External)?;
        assert_eq!(authz_vpc.id(), subnet.vpc_id);

        let db_subnet = self.vpc_create_subnet_raw(subnet).await?;
        self.vpc_system_router_ensure_subnet_routes(opctx, authz_vpc.id())
            .await
            .map_err(SubnetError::External)?;
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
    ) -> Result<VpcSubnet, SubnetError> {
        use db::schema::vpc_subnet::dsl;
        let values = FilterConflictingVpcSubnetRangesQuery::new(subnet.clone());
        let conn = self
            .pool_connection_unauthorized()
            .await
            .map_err(SubnetError::External)?;

        diesel::insert_into(dsl::vpc_subnet)
            .values(values)
            .returning(VpcSubnet::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| SubnetError::from_diesel(e, &subnet))
    }

    pub async fn vpc_delete_subnet(
        &self,
        opctx: &OpContext,
        db_subnet: &VpcSubnet,
        authz_subnet: &authz::VpcSubnet,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_subnet).await?;

        use db::schema::network_interface;
        use db::schema::vpc_subnet::dsl;

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
        let updated_rows = diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_subnet.id()))
            .filter(dsl::rcgen.eq(db_subnet.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                )
            })?;
        if updated_rows == 0 {
            return Err(Error::invalid_request(
                "deletion failed due to concurrent modification",
            ));
        } else {
            self.vpc_system_router_ensure_subnet_routes(
                opctx,
                db_subnet.vpc_id,
            )
            .await?;

            Ok(())
        }
    }

    pub async fn vpc_update_subnet(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        updates: VpcSubnetUpdate,
    ) -> UpdateResult<VpcSubnet> {
        opctx.authorize(authz::Action::Modify, authz_subnet).await?;

        use db::schema::vpc_subnet::dsl;
        let out = diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_subnet.id()))
            .set(updates)
            .returning(VpcSubnet::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                )
            })?;

        self.vpc_system_router_ensure_subnet_routes(opctx, out.vpc_id).await?;

        Ok(out)
    }

    pub async fn subnet_list_instance_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InstanceNetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_subnet).await?;

        use db::schema::instance_network_interface::dsl;

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

        use db::schema::vpc_router::dsl;
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

    pub async fn vpc_create_router(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        router: VpcRouter,
    ) -> CreateResult<(authz::VpcRouter, VpcRouter)> {
        opctx.authorize(authz::Action::CreateChild, authz_vpc).await?;

        use db::schema::vpc_router::dsl;
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

    pub async fn vpc_delete_router(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_router).await?;

        use db::schema::vpc_router::dsl;
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
        use db::schema::router_route::dsl as rr;
        let now = Utc::now();
        diesel::update(rr::router_route)
            .filter(rr::time_deleted.is_null())
            .filter(rr::vpc_router_id.eq(authz_router.id()))
            .set(rr::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Unlink all subnets from this router.
        // XXX: We might this want to error out before the delete fires.
        use db::schema::vpc_subnet::dsl as vpc;
        diesel::update(vpc::vpc_subnet)
            .filter(vpc::time_deleted.is_null())
            .filter(vpc::custom_router_id.eq(authz_router.id()))
            .set(vpc::custom_router_id.eq(Option::<Uuid>::None))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        updates: VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        opctx.authorize(authz::Action::Modify, authz_router).await?;

        use db::schema::vpc_router::dsl;
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

        use db::schema::router_route::dsl;
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
        conn: &async_bb8_diesel::Connection<crate::db::DbConnection>,
    ) -> CreateResult<RouterRoute> {
        use db::schema::router_route::dsl;
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

    pub async fn router_delete_route(
        &self,
        opctx: &OpContext,
        authz_route: &authz::RouterRoute,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_route).await?;

        use db::schema::router_route::dsl;
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

    pub async fn router_update_route(
        &self,
        opctx: &OpContext,
        authz_route: &authz::RouterRoute,
        route_update: RouterRouteUpdate,
    ) -> UpdateResult<RouterRoute> {
        opctx.authorize(authz::Action::Modify, authz_route).await?;

        use db::schema::router_route::dsl;
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

        use db::schema::vpc_subnet;
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
            entry.push(IpNetwork::V4(subnet.ipv4_block.0 .0));
            entry.push(IpNetwork::V6(subnet.ipv6_block.0 .0));
        }
        Ok(result)
    }

    /// Look up a VPC by VNI.
    pub async fn resolve_vni_to_vpc(
        &self,
        opctx: &OpContext,
        vni: Vni,
    ) -> LookupResult<Vpc> {
        use db::schema::vpc::dsl;
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
        use db::schema::vpc::dsl;
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

    /// Ensure the system router for a VPC has the correct set of subnet
    /// routing rules, after any changes to a subnet.
    pub async fn vpc_system_router_ensure_subnet_routes(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> Result<(), Error> {
        // These rules are immutable from a user's perspective, and
        // aren't something which they can meaningfully interact with,
        // so uuid stability on e.g. VPC rename is not a primary concern.
        // We make sure only to alter VPC subnet rules here: users may
        // modify other system routes like internet gateways (which are
        // `RouteKind::Default`).
        let conn = self.pool_connection_authorized(opctx).await?;
        let log = opctx.log.clone();
        self.transaction_retry_wrapper("vpc_subnet_route_reconcile")
            .transaction(&conn, |conn| {
                let log = log.clone();
                async move {

                use db::schema::router_route::dsl;
                use db::schema::vpc_subnet::dsl as subnet;
                use db::schema::vpc::dsl as vpc;

                let system_router_id = vpc::vpc
                    .filter(vpc::id.eq(vpc_id))
                    .filter(vpc::time_deleted.is_null())
                    .select(vpc::system_router_id)
                    .limit(1)
                    .get_result_async(&conn)
                    .await?;

                let valid_subnets: Vec<VpcSubnet> = subnet::vpc_subnet
                    .filter(subnet::vpc_id.eq(vpc_id))
                    .filter(subnet::time_deleted.is_null())
                    .select(VpcSubnet::as_select())
                    .load_async(&conn)
                    .await?;

                let current_rules: Vec<RouterRoute> = dsl::router_route
                    .filter(dsl::kind.eq(RouterRouteKind(ExternalRouteKind::VpcSubnet)))
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::vpc_router_id.eq(system_router_id))
                    .select(RouterRoute::as_select())
                    .load_async(&conn)
                    .await?;

                // Build the add/delete sets.
                let expected_names: HashSet<Name> = valid_subnets.iter()
                    .map(|v| v.identity.name.clone())
                    .collect();

                let mut found_names = HashSet::new();
                let mut invalid = Vec::new();
                for rule in current_rules {
                    let id = rule.id();
                    match (rule.kind.0, rule.target.0) {
                        (ExternalRouteKind::VpcSubnet, RouteTarget::Subnet(n))
                            if expected_names.contains(Name::ref_cast(&n)) =>
                                {let _ = found_names.insert(n.into());},
                        _ => invalid.push(id),
                    }
                }

                // Add/Remove routes. Retry if number is incorrect due to
                // concurrent modification.
                let now = Utc::now();
                let to_update = invalid.len();
                let updated_rows = diesel::update(dsl::router_route)
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::id.eq_any(invalid))
                    .set(dsl::time_deleted.eq(now))
                    .execute_async(&conn)
                    .await?;

                if updated_rows != to_update {
                    return Err(DieselError::RollbackTransaction);
                }

                // Duplicate rules are caught here using the UNIQUE constraint
                // on names in a router. Only nexus can alter the system router,
                // so there is no risk of collision with user-specified names.
                for subnet in expected_names.difference(&found_names) {
                    let route_id = Uuid::new_v4();
                    // XXX this is fallible as it is based on subnet name.
                    //     need to control this somewhere sane.
                    let Ok(route) = db::model::RouterRoute::for_subnet(
                        route_id,
                        system_router_id,
                        subnet.clone(),
                    ) else {
                        error!(
                            log,
                            "Reconciling VPC routes: name {} in vpc {} is too long",
                            subnet,
                            vpc_id,
                        );
                        continue;
                    };

                    match Self::router_create_route_on_connection(route, &conn).await {
                        Err(Error::Conflict { .. }) => return Err(DieselError::RollbackTransaction),
                        Err(_) => return Err(DieselError::NotFound),
                        _ => {},
                    }
                }

                // Verify that route set is exactly as intended, and rollback otherwise.
                let current_rules: Vec<RouterRoute> = dsl::router_route
                    .filter(dsl::kind.eq(RouterRouteKind(ExternalRouteKind::VpcSubnet)))
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::vpc_router_id.eq(system_router_id))
                    .select(RouterRoute::as_select())
                    .load_async(&conn)
                    .await?;

                if current_rules.len() != expected_names.len() {
                    return Err(DieselError::RollbackTransaction)
                }

                for rule in current_rules {
                    match (rule.kind.0, rule.target.0) {
                        (ExternalRouteKind::VpcSubnet, RouteTarget::Subnet(n))
                            if expected_names.contains(Name::ref_cast(&n)) => {},
                        _ => return Err(DieselError::RollbackTransaction),
                    }
                }

                Ok(())
            }}).await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        self.vpc_increment_rpw_version(opctx, vpc_id).await
    }

    /// Look up a VPC by VNI.
    pub async fn vpc_get_system_router(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> LookupResult<VpcRouter> {
        use db::schema::vpc::dsl as vpc_dsl;
        use db::schema::vpc_router::dsl as router_dsl;

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

    /// Fetch all active custom routers (and their parent subnets)
    /// in a VPC.
    pub async fn vpc_get_active_custom_routers(
        &self,
        opctx: &OpContext,
        vpc_id: Uuid,
    ) -> ListResultVec<(VpcSubnet, VpcRouter)> {
        use db::schema::vpc_router::dsl as router_dsl;
        use db::schema::vpc_subnet::dsl as subnet_dsl;

        subnet_dsl::vpc_subnet
            .inner_join(
                router_dsl::vpc_router.on(router_dsl::id
                    .nullable()
                    .eq(subnet_dsl::custom_router_id)),
            )
            .filter(subnet_dsl::time_deleted.is_null())
            .filter(subnet_dsl::vpc_id.is_null())
            .filter(router_dsl::time_deleted.is_null())
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

    /// Resolve all targets in a router into concrete details.
    pub async fn vpc_resolve_router_rules(
        &self,
        opctx: &OpContext,
        vpc_router_id: Uuid,
    ) -> Result<HashMap<IpNet, RouterTarget>, Error> {
        // Get all rules in target router.
        opctx.check_complex_operations_allowed()?;

        let (.., authz_project, authz_vpc, authz_router) =
            db::lookup::LookupPath::new(opctx, self)
                .vpc_router_id(vpc_router_id)
                .lookup_for(authz::Action::Read)
                .await
                .internal_context("lookup router by id for rules")?;
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
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
        }

        // TODO: This would be nice to solve in fewer queries.
        let mut subnets = HashMap::new();
        for name in subnet_names.drain() {
            if let Ok((.., subnet)) = db::lookup::LookupPath::new(opctx, self)
                .vpc_id(authz_vpc.id())
                .vpc_subnet_name(Name::ref_cast(&name))
                .fetch()
                .await
            {
                subnets.insert(name, subnet);
            }
        }
        let mut vpcs = HashMap::new();
        for name in vpc_names.drain() {
            if let Ok((.., vpc)) = db::lookup::LookupPath::new(opctx, self)
                .project_id(authz_project.id())
                .vpc_name(Name::ref_cast(&name))
                .fetch()
                .await
            {
                vpcs.insert(name, vpc);
            }
        }
        let mut instances = HashMap::new();
        for name in instance_names.drain() {
            if let Ok((.., authz_instance, instance)) =
                db::lookup::LookupPath::new(opctx, self)
                    .project_id(authz_project.id())
                    .instance_name(Name::ref_cast(&name))
                    .fetch()
                    .await
            {
                // XXX: currently an instance can have one primary NIC,
                //      and it is not dual-stack (v4 + v6). We need
                //      to clarify what should be resolved in the v6 case.
                if let Ok(primary_nic) = self
                    .instance_get_primary_network_interface(
                        opctx,
                        &authz_instance,
                    )
                    .await
                {
                    instances.insert(name, (instance, primary_nic));
                }
            }
        }
        // TODO: validate names of Internet Gateways.

        // See the discussion in `resolve_firewall_rules_for_sled_agent` on
        // how we should resolve name misses in route resolution.
        // This method adopts the same strategy: a lookup failure corresponds
        // to a NO-OP rule.
        let mut out = HashMap::new();
        for rule in all_rules {
            // Some dests/targets (e.g., subnet) resolve to *several* specifiers
            // to handle both v4 and v6. The user-facing API will prevent severe
            // mistakes on naked IPs/CIDRs (mixed v4/6), but we need to be smarter
            // around named entities here.
            let (v4_dest, v6_dest) = match rule.destination.0 {
                RouteDestination::Ip(ip @ IpAddr::V4(_)) => {
                    (Some(IpNet::single(ip)), None)
                }
                RouteDestination::Ip(ip @ IpAddr::V6(_)) => {
                    (None, Some(IpNet::single(ip)))
                }
                RouteDestination::IpNet(ip @ IpNet::V4(_)) => (Some(ip), None),
                RouteDestination::IpNet(ip @ IpNet::V6(_)) => (None, Some(ip)),
                RouteDestination::Subnet(n) => subnets
                    .get(&n)
                    .map(|s| {
                        (
                            Some(s.ipv4_block.0.into()),
                            Some(s.ipv6_block.0.into()),
                        )
                    })
                    .unwrap_or_default(),

                // TODO: VPC peering.
                RouteDestination::Vpc(_) => (None, None),
            };

            let (v4_target, v6_target) = match rule.target.0 {
                RouteTarget::Ip(ip @ IpAddr::V4(_)) => {
                    (Some(RouterTarget::Ip(ip)), None)
                }
                RouteTarget::Ip(ip @ IpAddr::V6(_)) => {
                    (None, Some(RouterTarget::Ip(ip)))
                }
                RouteTarget::Subnet(n) => subnets
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
                RouteTarget::Instance(n) => instances
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
                RouteTarget::Drop => {
                    (Some(RouterTarget::Drop), Some(RouterTarget::Drop))
                }

                // TODO: Internet Gateways.
                //       The semantic here is 'name match => allow',
                //       as the other aspect they will control is SNAT
                //       IP allocation. Today, presence of this rule
                //       allows upstream regardless of name.
                RouteTarget::InternetGateway(_n) => (
                    Some(RouterTarget::InternetGateway),
                    Some(RouterTarget::InternetGateway),
                ),

                // TODO: VPC Peering.
                RouteTarget::Vpc(_) => (None, None),
            };

            // XXX: Is there another way we should be handling destination
            //      collisions within a router? 'first/last wins' is fairly
            //      arbitrary when lookups are sorted on UUID, but it's
            //      unpredictable.
            //      It would be really useful to raise collisions and
            //      misses to users, somehow.
            if let (Some(dest), Some(target)) = (v4_dest, v4_target) {
                out.insert(dest, target);
            }

            if let (Some(dest), Some(target)) = (v6_dest, v6_target) {
                out.insert(dest, target);
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

        use db::schema::vpc_router::dsl;
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
        use db::schema::vpc_router::dsl;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test::sled_baseboard_for_test;
    use crate::db::datastore::test::sled_system_hardware_for_test;
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::datastore::test_utils::IneligibleSleds;
    use crate::db::fixed_data::silo::DEFAULT_SILO;
    use crate::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
    use crate::db::model::Project;
    use crate::db::queries::vpc::MAX_VNI_SEARCH_RANGE_SIZE;
    use ipnetwork::Ipv4Network;
    use nexus_db_model::IncompleteNetworkInterface;
    use nexus_db_model::SledUpdate;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::system::SledBuilder;
    use nexus_reconfigurator_planning::system::SystemDescription;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::external_api::params;
    use nexus_types::identity::Asset;
    use omicron_common::api::external;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::SledUuid;
    use slog::info;

    // Test that we detect the right error condition and return None when we
    // fail to insert a VPC due to VNI exhaustion.
    //
    // This is a bit awkward, but we'll test this by inserting a bunch of VPCs,
    // and checking that we get the expected error response back from the
    // `project_create_vpc_raw` call.
    #[tokio::test]
    async fn test_project_create_vpc_raw_returns_none_on_vni_exhaustion() {
        usdt::register_probes().unwrap();
        let logctx = dev::test_setup_log(
            "test_project_create_vpc_raw_returns_none_on_vni_exhaustion",
        );
        let log = &logctx.log;
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

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
            panic!("Expected Ok(None) when creating a VPC without any available VNIs");
        };
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Test that we appropriately retry when there are no available VNIs.
    //
    // This is a bit awkward, but we'll test this by inserting a bunch of VPCs,
    // and then check that we correctly retry
    #[tokio::test]
    async fn test_project_create_vpc_retries() {
        usdt::register_probes().unwrap();
        let logctx = dev::test_setup_log("test_project_create_vpc_retries");
        let log = &logctx.log;
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

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
        db.cleanup().await.unwrap();
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
            .map(|sled| SledUuid::from_untyped_uuid(sled.id()))
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
        usdt::register_probes().unwrap();
        let logctx = dev::test_setup_log(
            "test_vpc_resolve_to_sleds_uses_current_target_blueprint",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Set up our fake system with 5 sleds.
        let rack_id = Uuid::new_v4();
        let mut system = SystemDescription::new();
        let mut sled_ids = Vec::new();
        for _ in 0..5 {
            let sled_id = SledUuid::new_v4();
            sled_ids.push(sled_id);
            system.sled(SledBuilder::new().id(sled_id)).expect("adding sled");
            datastore
                .sled_upsert(SledUpdate::new(
                    sled_id.into_untyped_uuid(),
                    "[::1]:0".parse().unwrap(),
                    sled_baseboard_for_test(),
                    sled_system_hardware_for_test(),
                    rack_id,
                    Generation::new().into(),
                ))
                .await
                .expect("upserting sled");
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

        // Create a blueprint that has a Nexus on our third sled.
        let bp1 = {
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &bp0,
                &planning_input,
                "test",
            )
            .expect("created blueprint builder");
            builder
                .sled_ensure_zone_multiple_nexus_with_config(
                    sled_ids[2],
                    1,
                    false,
                    Vec::new(),
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
                db_nic_from_zone(&bp1.blueprint_zones[&sled_ids[2]].zones[0]),
            )
            .await
            .expect("failed to insert service VNIC");
        // We should now see our third sled running a service.
        assert_service_sled_ids(&datastore, &[sled_ids[2]]).await;

        // Create another blueprint, remove the one nexus we added, and make it
        // the target.
        let bp2 = {
            let mut bp2 = bp1.clone();
            bp2.id = Uuid::new_v4();
            bp2.parent_blueprint_id = Some(bp1.id);
            let sled2_zones = bp2
                .blueprint_zones
                .get_mut(&sled_ids[2])
                .expect("zones for third sled");
            sled2_zones.zones.clear();
            sled2_zones.generation = sled2_zones.generation.next();
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
                bp1.blueprint_zones[&sled_ids[2]].zones[0]
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
                "test",
            )
            .expect("created blueprint builder");
            for &sled_id in &sled_ids {
                builder
                    .sled_ensure_zone_multiple_nexus_with_config(
                        sled_id,
                        1,
                        false,
                        Vec::new(),
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
                    db_nic_from_zone(&bp3.blueprint_zones[&sled_id].zones[0]),
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

        // Make a new blueprint marking one of the zones as quiesced and one as
        // expunged. Ensure that the sled with *quiesced* zone is returned by
        // vpc_resolve_to_sleds, but the sled with the *expunged* zone is not.
        // (But other services are still running.)
        let bp4 = {
            let mut bp4 = bp3.clone();
            bp4.id = Uuid::new_v4();
            bp4.parent_blueprint_id = Some(bp3.id);

            // Sled index 2's Nexus is quiesced (should be included).
            let sled2 = bp4
                .blueprint_zones
                .get_mut(&sled_ids[2])
                .expect("zones for sled");
            sled2.zones[0].disposition = BlueprintZoneDisposition::Quiesced;
            sled2.generation = sled2.generation.next();

            // Sled index 3's zone is expunged (should be excluded).
            let sled3 = bp4
                .blueprint_zones
                .get_mut(&sled_ids[3])
                .expect("zones for sled");
            sled3.zones[0].disposition = BlueprintZoneDisposition::Expunged;
            sled3.generation = sled3.generation.next();

            bp4
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp4).await;
        assert_service_sled_ids(
            &datastore,
            &[sled_ids[0], sled_ids[1], sled_ids[2], sled_ids[4]],
        )
        .await;

        db.cleanup().await.unwrap();
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

    async fn new_subnet_ez(
        opctx: &OpContext,
        datastore: &DataStore,
        db_vpc: &Vpc,
        authz_vpc: &authz::Vpc,
        name: &str,
        ip: [u8; 4],
        prefix_len: u8,
    ) -> (authz::VpcSubnet, VpcSubnet) {
        let ipv6_block = db_vpc
            .ipv6_prefix
            .random_subnet(external::Ipv6Net::VPC_SUBNET_IPV6_PREFIX_LENGTH)
            .map(|block| block.0)
            .unwrap();

        datastore
            .vpc_create_subnet(
                &opctx,
                &authz_vpc,
                db::model::VpcSubnet::new(
                    Uuid::new_v4(),
                    db_vpc.id(),
                    IdentityMetadataCreateParams {
                        name: name.parse().unwrap(),
                        description: "A subnet...".into(),
                    },
                    external::Ipv4Net(
                        Ipv4Network::new(
                            core::net::Ipv4Addr::from(ip),
                            prefix_len,
                        )
                        .unwrap(),
                    ),
                    ipv6_block,
                ),
            )
            .await
            .unwrap()
    }

    // Test to verify that subnet CRUD operations are correctly
    // reflected in the nexus-managed system router attached to a VPC,
    // and that these resolve to the v4/6 subnets of each.
    #[tokio::test]
    async fn test_vpc_system_router_sync_to_subnets() {
        usdt::register_probes().unwrap();
        let logctx =
            dev::test_setup_log("test_vpc_system_router_sync_to_subnets");
        let log = &logctx.log;
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let (_, authz_vpc, db_vpc, _, db_router) =
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
        let (_, sub1) = new_subnet_ez(
            &opctx,
            &datastore,
            &db_vpc,
            &authz_vpc,
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
                VpcSubnetUpdate {
                    name: Some(
                        "a-new-name".parse::<external::Name>().unwrap().into(),
                    ),
                    description: None,
                    time_modified: Utc::now(),
                },
            )
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
        datastore.vpc_delete_subnet(&opctx, &sub0, &authz_sub0).await.unwrap();

        verify_all_subnet_routes_in_router(
            &opctx,
            &datastore,
            db_router.id(),
            &[&sub1],
        )
        .await;

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn verify_all_subnet_routes_in_router(
        opctx: &OpContext,
        datastore: &DataStore,
        router_id: Uuid,
        subnets: &[&VpcSubnet],
    ) -> Vec<RouterRoute> {
        let conn = datastore.pool_connection_authorized(opctx).await.unwrap();

        use db::schema::router_route::dsl;
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
            assert!(resolved.iter().any(|(k, v)| {
                *k == subnet.ipv4_block.0.into()
                    && match v {
                        RouterTarget::VpcSubnet(ip) => {
                            *ip == subnet.ipv4_block.0.into()
                        }
                        _ => false,
                    }
            }));
            assert!(resolved.iter().any(|(k, v)| {
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
        usdt::register_probes().unwrap();
        let logctx =
            dev::test_setup_log("test_vpc_router_rule_instance_resolve");
        let log = &logctx.log;
        let db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let (authz_project, authz_vpc, db_vpc, authz_router, _) =
            create_initial_vpc(log, &opctx, &datastore).await;

        // Create a subnet for an instance to live in.
        let (authz_sub0, sub0) = new_subnet_ez(
            &opctx,
            &datastore,
            &db_vpc,
            &authz_vpc,
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
        // VPC subnet (v4, v6).
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
                    Uuid::new_v4(),
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
                        ssh_public_keys: None,
                        start: false,
                    },
                ),
            )
            .await
            .unwrap();
        let (.., authz_instance) =
            db::lookup::LookupPath::new(&opctx, &datastore)
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
                    db_inst.id(),
                    sub0,
                    IdentityMetadataCreateParams {
                        name: "nic".parse().unwrap(),
                        description: "A NIC...".into(),
                    },
                    None,
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
        assert!(routes.iter().any(|(k, v)| (*k
            == "192.168.0.0/16".parse::<IpNet>().unwrap())
            && match v {
                RouterTarget::Ip(ip) => *ip == nic.ip.ip(),
                _ => false,
            }));
    }
}
