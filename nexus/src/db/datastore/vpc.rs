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
use crate::db::collection_insert::SyncInsertError;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Resource;
use crate::db::model::IncompleteVpc;
use crate::db::model::Name;
use crate::db::model::NetworkInterface;
use crate::db::model::RouterRoute;
use crate::db::model::RouterRouteUpdate;
use crate::db::model::Vpc;
use crate::db::model::VpcFirewallRule;
use crate::db::model::VpcRouter;
use crate::db::model::VpcRouterUpdate;
use crate::db::model::VpcSubnet;
use crate::db::model::VpcSubnetUpdate;
use crate::db::model::VpcUpdate;
use crate::db::pagination::paginated;
use crate::db::queries::vpc::InsertVpcQuery;
use crate::db::queries::vpc_subnet::FilterConflictingVpcSubnetRangesQuery;
use crate::db::queries::vpc_subnet::SubnetError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;

impl DataStore {
    pub async fn project_list_vpcs(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Vpc> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::vpc::dsl;
        paginated(dsl::vpc, dsl::name, &pagparams)
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
        use db::schema::vpc::dsl;

        assert_eq!(authz_project.id(), vpc.project_id);
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        // TODO-correctness Shouldn't this use "insert_resource"?
        //
        // Note that to do so requires adding an `rcgen` column to the project
        // table.
        let name = vpc.identity.name.clone();
        let query = InsertVpcQuery::new(vpc);
        let vpc = diesel::insert_into(dsl::vpc)
            .values(query)
            .returning(Vpc::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Vpc, name.as_str()),
                )
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
        authz_vpc: &authz::Vpc,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_vpc).await?;

        use db::schema::vpc::dsl;

        // Note that we don't ensure the firewall rules are empty here, because
        // we allow deleting VPCs with firewall rules present. Inserting new
        // rules is serialized with respect to the deletion by the row lock
        // associated with the VPC row, since we use the collection insert CTE
        // pattern to add firewall rules.

        let now = Utc::now();
        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_vpc.id()))
            .set(dsl::time_deleted.eq(now))
            .returning(Vpc::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })?;
        Ok(())
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
            .transaction(move |conn| {
                delete_old_query.execute(conn)?;

                // The generation count update on the vpc table row will take a
                // write lock on the row, ensuring that the vpc was not deleted
                // concurently.
                insert_new_query.insert_and_get_results(conn).map_err(|e| {
                    match e {
                        SyncInsertError::CollectionNotFound => {
                            TxnError::CustomError(
                                FirewallUpdateError::CollectionNotFound,
                            )
                        }
                        SyncInsertError::DatabaseError(e) => e.into(),
                    }
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

    pub async fn vpc_list_subnets(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<VpcSubnet> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use db::schema::vpc_subnet::dsl;
        paginated(dsl::vpc_subnet, dsl::name, &pagparams)
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
    ) -> Result<VpcSubnet, SubnetError> {
        opctx
            .authorize(authz::Action::CreateChild, authz_vpc)
            .await
            .map_err(SubnetError::External)?;
        assert_eq!(authz_vpc.id(), subnet.vpc_id);

        self.vpc_create_subnet_raw(subnet).await
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
        authz_subnet: &authz::VpcSubnet,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_subnet).await?;

        use db::schema::vpc_subnet::dsl;
        let now = Utc::now();
        diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_subnet.id()))
            .set(dsl::time_deleted.eq(now))
            .returning(VpcSubnet::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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

    pub async fn subnet_list_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<NetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_subnet).await?;

        use db::schema::network_interface::dsl;
        paginated(dsl::network_interface, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::subnet_id.eq(authz_subnet.id()))
            .select(NetworkInterface::as_select())
            .load_async::<db::model::NetworkInterface>(
                self.pool_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn vpc_list_routers(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<VpcRouter> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use db::schema::vpc_router::dsl;
        paginated(dsl::vpc_router, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .select(VpcRouter::as_select())
            .load_async::<db::model::VpcRouter>(
                self.pool_authorized(opctx).await?,
            )
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
            .returning(VpcRouter::as_returning())
            .get_result_async(self.pool())
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

    pub async fn router_list_routes(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<RouterRoute> {
        opctx.authorize(authz::Action::ListChildren, authz_router).await?;

        use db::schema::router_route::dsl;
        paginated(dsl::router_route, dsl::name, pagparams)
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
}
