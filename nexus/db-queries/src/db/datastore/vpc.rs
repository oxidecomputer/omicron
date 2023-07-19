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
use crate::db::error::diesel_pool_result_optional;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
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
use crate::db::queries::vpc_subnet::FilterConflictingVpcSubnetRangesQuery;
use crate::db::queries::vpc_subnet::SubnetError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use ipnetwork::IpNetwork;
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
        let authz_vpc = self
            .project_create_vpc_raw(opctx, &authz_project, vpc_query)
            .await
            .map(|(authz_vpc, _)| authz_vpc)
            .or_else(|e| match e {
                Error::ObjectAlreadyExists { .. } => Ok(authz::Vpc::new(
                    authz_project.clone(),
                    *SERVICES_VPC_ID,
                    LookupType::ByName(SERVICES_VPC.identity.name.to_string()),
                )),
                _ => Err(e),
            })?;

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
        .load_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        vpc: IncompleteVpc,
    ) -> Result<(authz::Vpc, Vpc), Error> {
        self.project_create_vpc_raw(
            opctx,
            authz_project,
            InsertVpcQuery::new(vpc),
        )
        .await
    }

    async fn project_create_vpc_raw(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        vpc_query: InsertVpcQuery,
    ) -> Result<(authz::Vpc, Vpc), Error> {
        use db::schema::vpc::dsl;

        assert_eq!(authz_project.id(), vpc_query.vpc.project_id);
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let name = vpc_query.vpc.identity.name.clone();
        let project_id = vpc_query.vpc.project_id;

        let vpc: Vpc = Project::insert_resource(
            project_id,
            diesel::insert_into(dsl::vpc).values(vpc_query),
        )
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Project,
                lookup_type: LookupType::ById(project_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Vpc, name.as_str()),
                )
            }
        })?;
        Ok((
            authz::Vpc::new(
                authz_project.clone(),
                vpc.id(),
                LookupType::ByName(vpc.name().to_string()),
            ),
            vpc,
        ))
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
        if diesel_pool_result_optional(
            vpc_subnet::dsl::vpc_subnet
                .filter(vpc_subnet::dsl::vpc_id.eq(authz_vpc.id()))
                .filter(vpc_subnet::dsl::time_deleted.is_null())
                .select(vpc_subnet::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?
        .is_some()
        {
            return Err(Error::InvalidRequest {
                message: String::from(
                    "VPC cannot be deleted while VPC Subnets exist",
                ),
            });
        }

        // Delete the VPC, conditional on the subnet_gen not having changed.
        let now = Utc::now();
        let updated_rows = diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_vpc.id()))
            .filter(dsl::subnet_gen.eq(db_vpc.subnet_gen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })?;
        if updated_rows == 0 {
            Err(Error::InvalidRequest {
                message: String::from(
                    "deletion failed to to concurrent modification",
                ),
            })
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

        dsl::vpc_firewall_rule
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .order(dsl::name.asc())
            .select(VpcFirewallRule::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn vpc_delete_all_firewall_rules(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_vpc).await?;
        use db::schema::vpc_firewall_rule::dsl;

        let now = Utc::now();
        // TODO-performance: Paginate this update to avoid long queries
        diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
        let insert_new_query = Vpc::insert_resource(
            authz_vpc.id(),
            diesel::insert_into(dsl::vpc_firewall_rule).values(rules),
        );

        #[derive(Debug)]
        enum FirewallUpdateError {
            CollectionNotFound,
        }
        type TxnError = TransactionError<FirewallUpdateError>;

        // TODO-scalability: Ideally this would be a CTE so we don't need to
        // hold a transaction open across multiple roundtrips from the database,
        // but for now we're using a transaction due to the severely decreased
        // legibility of CTEs via diesel right now.
        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                delete_old_query.execute_async(&conn).await?;

                // The generation count update on the vpc table row will take a
                // write lock on the row, ensuring that the vpc was not deleted
                // concurently.
                if rules_is_empty {
                    return Ok(vec![]);
                }
                insert_new_query
                    .insert_and_get_results_async(&conn)
                    .await
                    .map_err(|e| match e {
                        AsyncInsertError::CollectionNotFound => {
                            TxnError::CustomError(
                                FirewallUpdateError::CollectionNotFound,
                            )
                        }
                        AsyncInsertError::DatabaseError(e) => e.into(),
                    })
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(
                    FirewallUpdateError::CollectionNotFound,
                ) => Error::not_found_by_id(ResourceType::Vpc, &authz_vpc.id()),
                TxnError::Pool(e) => public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                ),
            })
    }

    /// Return the list of `Sled`s hosting instances with network interfaces
    /// on the provided VPC.
    pub async fn vpc_resolve_to_sleds(
        &self,
        vpc_id: Uuid,
        sleds_filter: &[Uuid],
    ) -> Result<Vec<Sled>, Error> {
        // Resolve each VNIC in the VPC to the Sled it's on, so we know which
        // Sleds to notify when firewall rules change.
        use db::schema::{
            instance, instance_network_interface, service,
            service_network_interface, sled,
        };

        let instance_query = instance_network_interface::table
            .inner_join(
                instance::table
                    .on(instance::id
                        .eq(instance_network_interface::instance_id)),
            )
            .inner_join(sled::table.on(sled::id.eq(instance::active_sled_id)))
            .filter(instance_network_interface::vpc_id.eq(vpc_id))
            .filter(instance_network_interface::time_deleted.is_null())
            .filter(instance::time_deleted.is_null())
            .select(Sled::as_select());

        let service_query = service_network_interface::table
            .inner_join(
                service::table
                    .on(service::id.eq(service_network_interface::service_id)),
            )
            .inner_join(sled::table.on(sled::id.eq(service::sled_id)))
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

        sleds
            .intersect(instance_query.union(service_query))
            .get_results_async(self.pool())
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn vpc_subnet_list(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<VpcSubnet> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use db::schema::vpc_subnet::dsl;
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
        .load_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
        diesel::insert_into(dsl::vpc_subnet)
            .values(values)
            .returning(VpcSubnet::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| SubnetError::from_pool(e, &subnet))
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

        // Verify there are no child network interfaces in this VPC Subnet
        if diesel_pool_result_optional(
            network_interface::dsl::network_interface
                .filter(network_interface::dsl::subnet_id.eq(authz_subnet.id()))
                .filter(network_interface::dsl::time_deleted.is_null())
                .select(network_interface::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?
        .is_some()
        {
            return Err(Error::InvalidRequest {
                message: String::from(
                    "VPC Subnet cannot be deleted while \
                    network interfaces in the subnet exist",
                ),
            });
        }

        // Delete the subnet, conditional on the rcgen not having changed.
        let now = Utc::now();
        let updated_rows = diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_subnet.id()))
            .filter(dsl::rcgen.eq(db_subnet.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                )
            })?;
        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: String::from(
                    "deletion failed to to concurrent modification",
                ),
            });
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            self.pool_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
        .load_async::<db::model::VpcRouter>(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            self.pool_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::VpcRouter,
                lookup_type: LookupType::ById(router_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::RouterRoute,
                        name.as_str(),
                    ),
                )
            }
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
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            .get_results_async::<SubnetIps>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vpc,
                        LookupType::ByCompositeId("VNI".to_string()),
                    ),
                )
            })
    }
}
