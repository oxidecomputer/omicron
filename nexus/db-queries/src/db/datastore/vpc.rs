// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Vpc`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::fixed_data::vpc::SERVICES_VPC_ID;
use crate::db::identity::Resource;
use crate::db::model::IncompleteVpc;
use crate::db::model::InstanceNetworkInterface;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::RouterRoute;
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
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use omicron_common::api::external::http_pagination::PaginatedBy;
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
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::Vni as ExternalVni;
use ref_cast::RefCast;
use std::collections::BTreeMap;
use uuid::Uuid;

impl DataStore {
    /// Load built-in VPCs into the database.
    pub async fn load_builtin_vpcs(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use crate::db::fixed_data::project::SERVICES_PROJECT_ID;
        use crate::db::fixed_data::vpc::SERVICES_VPC;
        use crate::db::fixed_data::vpc::SERVICES_VPC_DEFAULT_ROUTE_ID;

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

        let route = RouterRoute::new(
            *SERVICES_VPC_DEFAULT_ROUTE_ID,
            SERVICES_VPC.system_router_id,
            RouterRouteKind::Default,
            nexus_types::external_api::params::RouterRouteCreate {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description:
                        "Default internet gateway route for Oxide Services"
                            .to_string(),
                },
                target: RouteTarget::InternetGateway(
                    "outbound".parse().unwrap(),
                ),
                destination: RouteDestination::Vpc(
                    SERVICES_VPC.identity.name.clone().into(),
                ),
            },
        );
        self.router_create_route(opctx, &authz_router, route)
            .await
            .map(|_| ())
            .or_else(|e| match e {
                Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(e),
            })?;

        self.load_builtin_vpc_fw_rules(opctx).await?;
        self.load_builtin_vpc_subnets(opctx).await?;

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
    ) -> Result<(), Error> {
        use crate::db::fixed_data::vpc_subnet::DNS_VPC_SUBNET;
        use crate::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
        use crate::db::fixed_data::vpc_subnet::NTP_VPC_SUBNET;

        debug!(opctx.log, "attempting to create built-in VPC Subnets");

        // Create built-in VPC Subnets for Oxide Services

        let (_, _, authz_vpc) = db::lookup::LookupPath::new(opctx, self)
            .vpc_id(*SERVICES_VPC_ID)
            .lookup_for(authz::Action::CreateChild)
            .await
            .internal_context("lookup built-in services vpc")?;
        for vpc_subnet in
            [&*DNS_VPC_SUBNET, &*NEXUS_VPC_SUBNET, &*NTP_VPC_SUBNET]
        {
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
            service, service_network_interface, sled, vmm,
        };
        // Diesel requires us to use aliases in order to refer to the
        // `bp_target` table twice in the same query.
        let (bp_target1, bp_target2) = diesel::alias!(
            db::schema::bp_target as bp_target1,
            db::schema::bp_target as bp_target2
        );

        let instance_query = instance_network_interface::table
            .inner_join(
                instance::table
                    .on(instance::id
                        .eq(instance_network_interface::instance_id)),
            )
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

        // When Nexus accepts the rack initialization handoff from RSS, it
        // populates the `service` table. We eventually want to retire it
        // (https://github.com/oxidecomputer/omicron/issues/4947), and the
        // Reconfigurator does not add new entries to it. We still need to query
        // it for systems that are not yet under Reconfigurator control...
        let rss_service_query = service_network_interface::table
            .inner_join(
                service::table
                    .on(service::id.eq(service_network_interface::service_id)),
            )
            .inner_join(sled::table.on(sled::id.eq(service::sled_id)))
            .filter(service_network_interface::vpc_id.eq(vpc_id))
            .filter(service_network_interface::time_deleted.is_null())
            .select(Sled::as_select());

        // ... and we also need to query for the current target blueprint to
        // support systems that _are_ under Reconfigurator control.

        {
            // Ideally this would do something like:
            //
            // .filter(bp_omicron_zone::disposition.eq_any(
            //     BlueprintZoneDisposition::all_matching(
            //         BlueprintZoneFilter::VpcFirewall,
            //     ),
            // )
            //
            // But that doesn't quite work today because we currently don't
            // store the disposition enum next to each zone. Instead, this code
            // makes its decision to select which sleds to return by just
            // ignoring the zones_in_service table today.
            //
            // The purpose of this otherwise pointless block is to ensure that
            // it is correct to ensure that the expressed logic by
            // `BlueprintZoneFilter::VpcFirewall` matches the actual
            // implementation. It will hopefully soon be replaced with storing
            // the disposition in the bp_omicron_zone table and using the
            // filter directly.

            let mut matching = BlueprintZoneDisposition::all_matching(
                BlueprintZoneFilter::VpcFirewall,
            )
            .collect::<Vec<_>>();
            matching.sort();
            let mut all = BlueprintZoneDisposition::all_matching(
                BlueprintZoneFilter::All,
            )
            .collect::<Vec<_>>();
            all.sort();
            debug_assert_eq!(
                matching, all,
                "vpc firewall dispositions should match all dispositions"
            );
        }

        let reconfig_service_query = service_network_interface::table
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
            .filter(service_network_interface::vpc_id.eq(vpc_id))
            .filter(service_network_interface::time_deleted.is_null())
            .select(Sled::as_select());

        let mut sleds = sled::table
            .select(Sled::as_select())
            .filter(sled::time_deleted.is_null())
            .into_boxed();
        if !sleds_filter.is_empty() {
            sleds = sleds.filter(sled::id.eq_any(sleds_filter.to_vec()));
        }

        let conn = self.pool_connection_unauthorized().await?;
        sleds
            .intersect(
                instance_query
                    .union(rss_service_query)
                    .union(reconfig_service_query),
            )
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
        diesel::update(dsl::vpc_subnet)
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
            })
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

        use db::schema::router_route::dsl;
        let router_id = route.vpc_router_id;
        let name = route.name().clone();

        VpcRouter::insert_resource(
            router_id,
            diesel::insert_into(dsl::router_route).values(route),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(opctx).await?,
        )
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test::sled_baseboard_for_test;
    use crate::db::datastore::test::sled_system_hardware_for_test;
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
    use crate::db::model::Project;
    use crate::db::queries::vpc::MAX_VNI_SEARCH_RANGE_SIZE;
    use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
    use nexus_db_model::SledUpdate;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZonesConfig;
    use nexus_types::deployment::OmicronZoneConfig;
    use nexus_types::deployment::OmicronZoneType;
    use nexus_types::external_api::params;
    use nexus_types::identity::Asset;
    use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
    use omicron_common::api::external;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::IpNet;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_common::api::internal::shared::NetworkInterface;
    use omicron_common::api::internal::shared::NetworkInterfaceKind;
    use omicron_test_utils::dev;
    use slog::info;
    use std::collections::BTreeMap;
    use std::net::IpAddr;

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

    #[derive(Debug)]
    struct Harness {
        rack_id: Uuid,
        sled_ids: Vec<Uuid>,
        nexuses: Vec<HarnessNexus>,
    }

    #[derive(Debug)]
    struct HarnessNexus {
        id: Uuid,
        ip: IpAddr,
        mac: MacAddr,
        nic_id: Uuid,
    }

    impl Harness {
        fn new(num_sleds: usize) -> Self {
            let mut sled_ids =
                (0..num_sleds).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
            sled_ids.sort();

            let mut nexus_ips = NEXUS_OPTE_IPV4_SUBNET
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                .map(IpAddr::from);
            let mut nexus_macs = MacAddr::iter_system();
            let nexuses = (0..num_sleds)
                .map(|_| HarnessNexus {
                    id: Uuid::new_v4(),
                    ip: nexus_ips.next().unwrap(),
                    mac: nexus_macs.next().unwrap(),
                    nic_id: Uuid::new_v4(),
                })
                .collect::<Vec<_>>();
            Self { rack_id: Uuid::new_v4(), sled_ids, nexuses }
        }

        fn db_sleds(&self) -> impl Iterator<Item = SledUpdate> + '_ {
            self.sled_ids.iter().copied().map(|sled_id| {
                SledUpdate::new(
                    sled_id,
                    "[::1]:0".parse().unwrap(),
                    sled_baseboard_for_test(),
                    sled_system_hardware_for_test(),
                    self.rack_id,
                    Generation::new().into(),
                )
            })
        }

        fn db_services(
            &self,
        ) -> impl Iterator<
            Item = (db::model::Service, db::model::IncompleteNetworkInterface),
        > + '_ {
            self.sled_ids.iter().zip(&self.nexuses).map(|(sled_id, nexus)| {
                let service = db::model::Service::new(
                    nexus.id,
                    *sled_id,
                    Some(nexus.id),
                    "[::1]:0".parse().unwrap(),
                    db::model::ServiceKind::Nexus,
                );
                let name = format!("test-nexus-{}", nexus.id);
                let nic = db::model::IncompleteNetworkInterface::new_service(
                    nexus.nic_id,
                    nexus.id,
                    NEXUS_VPC_SUBNET.clone(),
                    IdentityMetadataCreateParams {
                        name: name.parse().unwrap(),
                        description: name,
                    },
                    nexus.ip,
                    nexus.mac,
                    0,
                )
                .expect("failed to create incomplete Nexus NIC");
                (service, nic)
            })
        }

        fn blueprint_zone_configs(
            &self,
        ) -> impl Iterator<Item = (Uuid, BlueprintZoneConfig)> + '_ {
            self.db_services().map(|(service, nic)| {
                let config = OmicronZoneConfig {
                    id: service.id(),
                    underlay_address: "::1".parse().unwrap(),
                    zone_type: OmicronZoneType::Nexus {
                        internal_address: "[::1]:0".to_string(),
                        external_ip: "::1".parse().unwrap(),
                        nic: NetworkInterface {
                            id: nic.identity.id,
                            kind: NetworkInterfaceKind::Service {
                                id: service.id(),
                            },
                            name: format!("test-nic-{}", nic.identity.id)
                                .parse()
                                .unwrap(),
                            ip: nic.ip.unwrap(),
                            mac: nic.mac.unwrap(),
                            subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
                            vni: Vni::SERVICES_VNI,
                            primary: true,
                            slot: nic.slot.unwrap(),
                        },
                        external_tls: false,
                        external_dns_servers: Vec::new(),
                    },
                };
                let zone_config = BlueprintZoneConfig {
                    config,
                    disposition: BlueprintZoneDisposition::InService,
                };
                (service.sled_id, zone_config)
            })
        }
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

        // Helper function to fetch and sort the IDs of sleds we've resolved the
        // SERVICES_VPC_ID to.
        let fetch_service_sled_ids = || async {
            let mut service_sled_ids = datastore
                .vpc_resolve_to_sleds(*SERVICES_VPC_ID, &[])
                .await
                .expect("failed to resolve to sleds")
                .into_iter()
                .map(|sled| sled.id())
                .collect::<Vec<_>>();
            service_sled_ids.sort();
            service_sled_ids
        };

        // Create four sleds.
        let harness = Harness::new(4);
        for sled in harness.db_sleds() {
            datastore.sled_upsert(sled).await.expect("failed to upsert sled");
        }

        // Insert two Nexus records into `service`, emulating RSS.
        for (service, nic) in harness.db_services().take(2) {
            datastore
                .service_upsert(&opctx, service)
                .await
                .expect("failed to insert RSS-like service");
            datastore
                .service_create_network_interface_raw(&opctx, nic)
                .await
                .expect("failed to insert Nexus NIC");
        }

        // Ensure we find the two sleds we expect after adding Nexus records.
        assert_eq!(&harness.sled_ids[..2], fetch_service_sled_ids().await);

        // Create a blueprint that has a Nexus on our third sled. (This
        // blueprint is completely invalid in many ways, but all we care about
        // here is inserting relevant records in `bp_omicron_zone`.)
        let bp1_zones = {
            let (sled_id, zone_config) = harness
                .blueprint_zone_configs()
                .nth(2)
                .expect("fewer than 3 services in test harness");
            let mut zones = BTreeMap::new();
            zones.insert(
                sled_id,
                BlueprintZonesConfig {
                    generation: Generation::new(),
                    zones: vec![zone_config],
                },
            );
            zones
        };
        let bp1_id = Uuid::new_v4();
        let bp1 = Blueprint {
            id: bp1_id,
            blueprint_zones: bp1_zones,
            parent_blueprint_id: None,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            time_created: Utc::now(),
            creator: "test".to_string(),
            comment: "test".to_string(),
        };
        datastore
            .blueprint_insert(&opctx, &bp1)
            .await
            .expect("failed to insert blueprint");

        // We haven't set a blueprint target yet, so we should still only see
        // the two RSS-inserted service-running sleds.
        assert_eq!(&harness.sled_ids[..2], fetch_service_sled_ids().await);

        // Make bp1 the current target.
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: bp1_id,
                    enabled: true,
                    time_made_target: Utc::now(),
                },
            )
            .await
            .expect("failed to set blueprint target");

        // bp1 is the target, but we haven't yet inserted a vNIC record, so
        // we'll still only see the original 2 sleds.
        assert_eq!(&harness.sled_ids[..2], fetch_service_sled_ids().await);

        // Insert the relevant service NIC record (normally performed by the
        // reconfigurator's executor).
        datastore
            .service_create_network_interface_raw(
                &opctx,
                harness.db_services().nth(2).unwrap().1,
            )
            .await
            .expect("failed to insert service VNIC");

        // We should now see _three_ sleds running services.
        assert_eq!(&harness.sled_ids[..3], fetch_service_sled_ids().await);

        // Create another blueprint with no services and make it the target.
        let bp2_id = Uuid::new_v4();
        let bp2 = Blueprint {
            id: bp2_id,
            blueprint_zones: BTreeMap::new(),
            parent_blueprint_id: Some(bp1_id),
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            time_created: Utc::now(),
            creator: "test".to_string(),
            comment: "test".to_string(),
        };
        datastore
            .blueprint_insert(&opctx, &bp2)
            .await
            .expect("failed to insert blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: bp2_id,
                    enabled: true,
                    time_made_target: Utc::now(),
                },
            )
            .await
            .expect("failed to set blueprint target");

        // We haven't removed the service NIC record, but we should no longer
        // see the third sled here, because we should be back to just the
        // original two services in the `service` table.
        assert_eq!(&harness.sled_ids[..2], fetch_service_sled_ids().await);

        // Insert a service NIC record for our fourth sled's Nexus. This
        // shouldn't change our VPC resolution.
        datastore
            .service_create_network_interface_raw(
                &opctx,
                harness.db_services().nth(3).unwrap().1,
            )
            .await
            .expect("failed to insert service VNIC");
        assert_eq!(&harness.sled_ids[..2], fetch_service_sled_ids().await);

        // Create a blueprint that has a Nexus on our fourth sled. This
        // shouldn't change our VPC resolution.
        let bp3_zones = {
            let (sled_id, zone_config) = harness
                .blueprint_zone_configs()
                .nth(3)
                .expect("fewer than 3 services in test harness");
            let mut zones = BTreeMap::new();
            zones.insert(
                sled_id,
                BlueprintZonesConfig {
                    generation: Generation::new(),
                    zones: vec![zone_config],
                },
            );
            zones
        };
        let bp3_id = Uuid::new_v4();
        let bp3 = Blueprint {
            id: bp3_id,
            blueprint_zones: bp3_zones,
            parent_blueprint_id: Some(bp2_id),
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            time_created: Utc::now(),
            creator: "test".to_string(),
            comment: "test".to_string(),
        };
        datastore
            .blueprint_insert(&opctx, &bp3)
            .await
            .expect("failed to insert blueprint");
        assert_eq!(&harness.sled_ids[..2], fetch_service_sled_ids().await);

        // Make this blueprint the target. We've already created the service
        // VNIC, so we should immediately see our fourth sled in VPC resolution.
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: bp3_id,
                    enabled: true,
                    time_made_target: Utc::now(),
                },
            )
            .await
            .expect("failed to set blueprint target");
        assert_eq!(
            &[harness.sled_ids[0], harness.sled_ids[1], harness.sled_ids[3]]
                as &[Uuid],
            fetch_service_sled_ids().await
        );

        // Finally, create a blueprint that includes our third and fourth sleds,
        // make it the target, and ensure we resolve to all four sleds.
        let bp4_zones = {
            let mut zones = BTreeMap::new();
            for (sled_id, zone_config) in
                harness.blueprint_zone_configs().skip(2)
            {
                zones.insert(
                    sled_id,
                    BlueprintZonesConfig {
                        generation: Generation::new(),
                        zones: vec![zone_config],
                    },
                );
            }
            zones
        };
        let bp4_id = Uuid::new_v4();
        let bp4 = Blueprint {
            id: bp4_id,
            blueprint_zones: bp4_zones,
            parent_blueprint_id: Some(bp3_id),
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            time_created: Utc::now(),
            creator: "test".to_string(),
            comment: "test".to_string(),
        };
        datastore
            .blueprint_insert(&opctx, &bp4)
            .await
            .expect("failed to insert blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: bp4_id,
                    enabled: true,
                    time_made_target: Utc::now(),
                },
            )
            .await
            .expect("failed to set blueprint target");
        assert_eq!(harness.sled_ids, fetch_service_sled_ids().await);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
