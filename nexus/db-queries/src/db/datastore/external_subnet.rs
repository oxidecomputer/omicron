// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Subnet Pools and External Subnets.

use crate::db::DataStore;
use crate::db::pagination::paginated;
use crate::db::queries::external_subnet::decode_delete_external_subnet_error;
use crate::db::queries::external_subnet::decode_insert_external_subnet_error;
use crate::db::queries::external_subnet::decode_unlink_subnet_pool_from_silo_result;
use crate::db::queries::external_subnet::delete_external_subnet_query;
use crate::db::queries::external_subnet::insert_external_subnet_query;
use crate::db::queries::external_subnet::insert_subnet_pool_member_query;
use crate::db::queries::external_subnet::link_subnet_pool_to_silo_query;
use crate::db::queries::external_subnet::unlink_subnet_pool_from_silo_query;
use async_bb8_diesel::AsyncRunQueryDsl as _;
use chrono::Utc;
use diesel::ExpressionMethods as _;
use diesel::JoinOnDsl as _;
use diesel::QueryDsl as _;
use diesel::SelectableHelper as _;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use nexus_auth::authz;
use nexus_auth::authz::SUBNET_POOL_LIST;
use nexus_auth::context::OpContext;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::ExternalSubnet;
use nexus_db_model::ExternalSubnetIdentity;
use nexus_db_model::ExternalSubnetUpdate;
use nexus_db_model::IpNet;
use nexus_db_model::IpVersion;
use nexus_db_model::Name;
use nexus_db_model::SubnetPool;
use nexus_db_model::SubnetPoolMember;
use nexus_db_model::SubnetPoolSiloLink;
use nexus_db_model::SubnetPoolUpdate;
use nexus_db_model::to_db_typed_uuid;
use nexus_types::external_api::params;
use nexus_types::external_api::params::ExternalSubnetCreate;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::ExternalSubnetUuid;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::SubnetPoolUuid;
use ref_cast::RefCast as _;
use uuid::Uuid;

impl DataStore {
    /// Lookup a Subnet Pool by name or ID.
    pub fn lookup_subnet_pool<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
    ) -> lookup::SubnetPool<'a> {
        match pool {
            NameOrId::Id(id) => {
                let id = SubnetPoolUuid::from_untyped_uuid(*id);
                LookupPath::new(opctx, self).subnet_pool_id(id)
            }
            NameOrId::Name(name) => LookupPath::new(opctx, self)
                .subnet_pool_name(name.clone().into()),
        }
    }

    /// List Subnet Pools, paginated by name or ID.
    pub async fn list_subnet_pools(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SubnetPool> {
        opctx.authorize(authz::Action::ListChildren, &SUBNET_POOL_LIST).await?;
        use nexus_db_schema::schema::subnet_pool::dsl;
        match pagparams {
            PaginatedBy::Id(by_id) => {
                paginated(dsl::subnet_pool, dsl::id, by_id)
            }
            PaginatedBy::Name(by_name) => paginated(
                dsl::subnet_pool,
                dsl::name,
                &by_name.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .select(SubnetPool::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Create a Subnet Pool.
    pub async fn create_subnet_pool(
        &self,
        opctx: &OpContext,
        params: params::SubnetPoolCreate,
    ) -> CreateResult<SubnetPool> {
        opctx
            .authorize(authz::Action::CreateChild, &authz::SUBNET_POOL_LIST)
            .await?;
        use nexus_db_schema::schema::subnet_pool::dsl;
        let pool = SubnetPool::new(params.identity, params.ip_version.into());
        diesel::insert_into(dsl::subnet_pool)
            .values(pool)
            .on_conflict(dsl::id)
            .do_update()
            .set(dsl::time_modified.eq(dsl::time_modified))
            .returning(SubnetPool::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete a Subnet Pool.
    ///
    /// Deletion fails if there are any child pool members.
    pub async fn delete_subnet_pool(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        db_pool: &SubnetPool,
    ) -> DeleteResult {
        use nexus_db_schema::schema::subnet_pool::dsl as pool_dsl;
        use nexus_db_schema::schema::subnet_pool_member::dsl as member_dsl;

        opctx.authorize(authz::Action::Delete, authz_pool).await?;

        // Look for any child external subnets.
        let pool_id = to_db_typed_uuid(authz_pool.id());
        let conn = self.pool_connection_authorized(opctx).await?;
        let has_children = diesel::dsl::select(diesel::dsl::exists(
            member_dsl::subnet_pool_member
                .filter(member_dsl::subnet_pool_id.eq(pool_id))
                .filter(member_dsl::time_deleted.is_null()),
        ))
        .get_result_async::<bool>(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if has_children {
            return Err(Error::invalid_request(
                "Cannot delete external subnet pool while it contains \
                members. Please delete all external subnet pool members first.",
            ));
        }

        // Conditionally delete the pool, checking the generation is the same.
        let n_deleted = diesel::update(
            pool_dsl::subnet_pool
                .filter(pool_dsl::id.eq(pool_id))
                .filter(pool_dsl::time_deleted.is_null())
                .filter(pool_dsl::rcgen.eq(db_pool.rcgen)),
        )
        .set(pool_dsl::time_deleted.eq(Some(Utc::now())))
        .execute_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if n_deleted != 1 {
            return Err(Error::invalid_request(
                "deletion failed due to concurrent modification",
            ));
        }
        Ok(())
    }

    /// Update metadata for a Subnet Pool.
    pub async fn update_subnet_pool(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        updates: SubnetPoolUpdate,
    ) -> UpdateResult<SubnetPool> {
        use nexus_db_schema::schema::subnet_pool::dsl;
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::update(
            dsl::subnet_pool
                .find(to_db_typed_uuid(authz_pool.id()))
                .filter(dsl::time_deleted.is_null()),
        )
        .set(updates)
        .returning(SubnetPool::as_returning())
        .get_result_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Link a Subnet Pool to a Silo.
    pub async fn link_subnet_pool_to_silo(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        authz_silo: &authz::Silo,
        is_default: bool,
    ) -> CreateResult<SubnetPoolSiloLink> {
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        link_subnet_pool_to_silo_query(
            authz_pool.id(),
            authz_silo.id(),
            is_default,
        )
        .get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| match &e {
            DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                info,
            ) if info.constraint_name() == Some("single_default_per_silo") => {
                Error::invalid_request(
                    "Can only have a single default Subnet Pool for a \
                    Silo for each IP version.",
                )
            }
            DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                info,
            ) if info.constraint_name()
                == Some("subnet_pool_silo_link_pkey") =>
            {
                Error::conflict("Subnet Pool is already linked to Silo")
            }
            DieselError::DatabaseError(
                DatabaseErrorKind::NotNullViolation,
                info,
            ) if info.message().contains("\"silo_id\"") => {
                Error::not_found_by_id(ResourceType::Silo, &authz_silo.id())
            }
            DieselError::DatabaseError(
                DatabaseErrorKind::NotNullViolation,
                info,
            ) if info.message().contains("\"subnet_pool_id\"") => {
                Error::not_found_by_id(
                    ResourceType::SubnetPool,
                    authz_pool.id().as_untyped_uuid(),
                )
            }
            _ => public_error_from_diesel(e, ErrorHandler::Server),
        })
    }

    /// Unlink a Subnet Pool from a Silo.
    pub async fn unlink_subnet_pool_from_silo(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        db_pool: &SubnetPool,
        authz_silo: &authz::Silo,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        use nexus_db_schema::schema::external_subnet::dsl;
        use nexus_db_schema::schema::project::dsl as project_dsl;

        // Look for child external subnets in the silo, fail if any exist.
        let conn = self.pool_connection_authorized(opctx).await?;
        let has_children = diesel::dsl::select(diesel::dsl::exists(
            project_dsl::project
                .inner_join(
                    dsl::external_subnet
                        .on(dsl::project_id.eq(project_dsl::id)),
                )
                .filter(project_dsl::silo_id.eq(authz_silo.id()))
                .filter(project_dsl::time_deleted.is_null())
                .filter(
                    dsl::subnet_pool_id.eq(to_db_typed_uuid(authz_pool.id())),
                )
                .filter(dsl::time_deleted.is_null()),
        ))
        .get_result_async::<bool>(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if has_children {
            return Err(Error::invalid_request(
                "Cannot unlink Subnet Pool from Silo while there are \
                External Subnets allocated from the pool. Delete the \
                subnets first, and try again.",
            ));
        }
        let result = unlink_subnet_pool_from_silo_query(
            authz_pool.id(),
            authz_silo.id(),
            db_pool.rcgen,
        )
        .execute_async(&*self.pool_connection_authorized(opctx).await?)
        .await;
        decode_unlink_subnet_pool_from_silo_result(
            result, authz_pool, authz_silo,
        )
    }

    /// Update the link between a Subnet Pool and Silo.
    pub async fn update_subnet_pool_silo_link(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        authz_silo: &authz::Silo,
        is_default: bool,
    ) -> UpdateResult<SubnetPoolSiloLink> {
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        use nexus_db_schema::schema::subnet_pool_silo_link::dsl;
        diesel::update(
            dsl::subnet_pool_silo_link
                .filter(
                    dsl::subnet_pool_id.eq(to_db_typed_uuid(authz_pool.id())),
                )
                .filter(dsl::silo_id.eq(authz_silo.id())),
        )
        .set(dsl::is_default.eq(is_default))
        .returning(SubnetPoolSiloLink::as_returning())
        .get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            DieselError::NotFound => LookupType::ByCompositeId(format!(
                "subnet_pool_id: {}, silo_id: {}",
                authz_pool.id(),
                authz_silo.id(),
            ))
            .into_not_found(ResourceType::SubnetPoolSiloLink),
            DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                ref info,
            ) if info.constraint_name() == Some("single_default_per_silo") => {
                Error::invalid_request(
                    "Can only have a single default Subnet Pool for a \
                    Silo for each IP version.",
                )
            }
            e => public_error_from_diesel(e, ErrorHandler::Server),
        })
    }

    /// List silos linked to a subnet pool.
    pub async fn list_silos_linked_to_subnet_pool(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SubnetPoolSiloLink> {
        opctx.authorize(authz::Action::ListChildren, authz_pool).await?;
        use nexus_db_schema::schema::subnet_pool_silo_link::dsl;
        paginated(dsl::subnet_pool_silo_link, dsl::silo_id, &pagparams)
            .filter(dsl::subnet_pool_id.eq(to_db_typed_uuid(authz_pool.id())))
            .select(SubnetPoolSiloLink::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Add a new Subnet Pool Member.
    ///
    /// IP subnets must be unique across all Subnet Pool Members, in all pools.
    /// Any request to create a member with an overlapping IP subnet will fail.
    pub async fn add_subnet_pool_member(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        db_pool: &SubnetPool,
        params: &params::SubnetPoolMemberAdd,
    ) -> CreateResult<SubnetPoolMember> {
        opctx.authorize(authz::Action::CreateChild, authz_pool).await?;

        // First check we're adding members of the same IP version.
        let pool_version = db_pool.ip_version;
        let member_version = match params.subnet {
            oxnet::IpNet::V4(_) => IpVersion::V4,
            oxnet::IpNet::V6(_) => IpVersion::V6,
        };
        if pool_version != member_version {
            return Err(Error::invalid_request(&format!(
                "Cannot add IP{} members to IP{} Subnet Pool",
                member_version, pool_version,
            )));
        }

        let member = SubnetPoolMember::new(params, authz_pool.id())?;
        insert_subnet_pool_member_query(&member)
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| match e {
                DieselError::NotFound => Error::invalid_request(&format!(
                    "The IP subnet {} overlaps with an existing \
                    Subnet Pool member or IP Pool range",
                    params.subnet,
                )),
                e => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// List the members of a Subnet Pool, paginated by IP subnet.
    pub async fn list_subnet_pool_members(
        &self,
        opctx: &OpContext,
        pool: &authz::SubnetPool,
        pagparams: &DataPageParams<'_, nexus_db_model::IpNet>,
    ) -> ListResultVec<SubnetPoolMember> {
        use nexus_db_schema::schema::subnet_pool_member::dsl;
        opctx.authorize(authz::Action::ListChildren, pool).await?;
        paginated(dsl::subnet_pool_member, dsl::subnet, pagparams)
            .filter(dsl::subnet_pool_id.eq(to_db_typed_uuid(pool.id())))
            .filter(dsl::time_deleted.is_null())
            .select(SubnetPoolMember::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete Subnet Pool Members by their subnet.
    ///
    /// This will fail if there are any oustanding External Subnets.
    pub async fn delete_subnet_pool_member(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::SubnetPool,
        subnet: IpNet,
    ) -> DeleteResult {
        use nexus_db_schema::schema::external_subnet::dsl as subnet_dsl;
        use nexus_db_schema::schema::subnet_pool_member::dsl as member_dsl;

        opctx.authorize(authz::Action::Modify, authz_pool).await?;

        // Fetch the pool member itself, if it exists.
        let conn = self.pool_connection_authorized(opctx).await?;
        let member = member_dsl::subnet_pool_member
            .filter(
                member_dsl::subnet_pool_id
                    .eq(to_db_typed_uuid(authz_pool.id())),
            )
            .filter(member_dsl::subnet.eq(subnet))
            .filter(member_dsl::time_deleted.is_null())
            .select(SubnetPoolMember::as_select())
            .get_result_async::<SubnetPoolMember>(&*conn)
            .await
            .map_err(|e| match e {
                DieselError::NotFound => Error::invalid_request(format!(
                    "A provided External Subnet Pool member with \
                        subnet {subnet} does not exist",
                )),
                _ => public_error_from_diesel(e, ErrorHandler::Server),
            })?;

        // Look for any child external subnets allocated out of the pool member.
        let has_children = diesel::dsl::select(diesel::dsl::exists(
            subnet_dsl::external_subnet
                .filter(subnet_dsl::subnet_pool_member_id.eq(member.id))
                .filter(subnet_dsl::time_deleted.is_null()),
        ))
        .get_result_async::<bool>(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if has_children {
            return Err(Error::invalid_request(
                "Cannot delete external subnet pool member while it contains \
                external subnets. Please delete all external subnets first.",
            ));
        }

        // Conditionally delete the member, checking the generation is the same.
        let n_deleted = diesel::update(
            member_dsl::subnet_pool_member
                .filter(member_dsl::id.eq(member.id))
                .filter(member_dsl::time_deleted.is_null())
                .filter(member_dsl::rcgen.eq(member.rcgen())),
        )
        .set(member_dsl::time_deleted.eq(Utc::now()))
        .execute_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if n_deleted != 1 {
            return Err(Error::invalid_request(
                "deletion failed due to concurrent modification",
            ));
        }
        Ok(())
    }

    /// Create an External Subnet.
    pub async fn create_external_subnet(
        &self,
        opctx: &OpContext,
        silo_id: &Uuid,
        authz_project: &authz::Project,
        params: params::ExternalSubnetCreate,
    ) -> CreateResult<ExternalSubnet> {
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;
        let ExternalSubnetCreate { identity, allocator } = params;
        let identity =
            ExternalSubnetIdentity::new(ExternalSubnetUuid::new_v4(), identity);
        insert_external_subnet_query(
            silo_id,
            &authz_project.id(),
            identity,
            &allocator,
        )
        .get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| {
            decode_insert_external_subnet_error(
                e,
                silo_id,
                authz_project,
                &allocator,
            )
        })
    }

    /// Update the metadata for an External Subnet.
    pub async fn update_external_subnet(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::ExternalSubnet,
        updates: ExternalSubnetUpdate,
    ) -> UpdateResult<ExternalSubnet> {
        use nexus_db_schema::schema::external_subnet::dsl;
        opctx.authorize(authz::Action::Modify, authz_subnet).await?;
        diesel::update(
            dsl::external_subnet
                .find(to_db_typed_uuid(authz_subnet.id()))
                .filter(dsl::time_deleted.is_null()),
        )
        .set(updates)
        .returning(ExternalSubnet::as_returning())
        .get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete an External Subnet.
    ///
    /// External Subnets must be detached from an instance before they can be
    /// deleted.
    pub async fn delete_external_subnet(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::ExternalSubnet,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_subnet).await?;
        let id = authz_subnet.id().into_untyped_uuid();
        delete_external_subnet_query(&id)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(decode_delete_external_subnet_error)
            .and_then(|count| {
                if count == 0 {
                    Err(Error::not_found_by_id(
                        ResourceType::ExternalSubnet,
                        &id,
                    ))
                } else {
                    Ok(())
                }
            })
    }

    /// List external subnets.
    pub async fn list_external_subnets(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ExternalSubnet> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;
        use nexus_db_schema::schema::external_subnet::dsl;
        match pagparams {
            PaginatedBy::Id(by_id) => {
                paginated(dsl::external_subnet, dsl::id, by_id)
            }
            PaginatedBy::Name(by_name) => paginated(
                dsl::external_subnet,
                dsl::name,
                &by_name.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::project_id.eq(authz_project.id()))
        .filter(dsl::time_deleted.is_null())
        .select(ExternalSubnet::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::queries::external_subnet::MULTIPLE_LINKED_DEFAULT_POOLS_ERR_MSG;
    use crate::db::queries::external_subnet::NO_LINKED_DEFAULT_POOL_ERR_MSG;
    use crate::db::queries::external_subnet::NO_LINKED_POOL_CONTAINS_SUBNET_ERR_MSG;
    use crate::db::queries::external_subnet::SUBNET_OVERLAPS_EXISTING_ERR_MSG;
    use async_bb8_diesel::AsyncRunQueryDsl as _;
    use chrono::Utc;
    use diesel::ExpressionMethods as _;
    use diesel::QueryDsl as _;
    use dropshot::PaginationOrder;
    use dropshot::test_util::LogContext;
    use nexus_auth::authz;
    use nexus_db_model::IpAttachState;
    use nexus_db_model::IpPool;
    use nexus_db_model::IpPoolReservationType;
    use nexus_db_model::Name;
    use nexus_db_model::Project;
    use nexus_db_model::SubnetPool;
    use nexus_db_model::SubnetPoolMember;
    use nexus_db_model::SubnetPoolUpdate;
    use nexus_db_model::to_db_typed_uuid;
    use nexus_types::external_api::params::ExternalSubnetAllocator;
    use nexus_types::external_api::params::ExternalSubnetCreate;
    use nexus_types::external_api::params::PoolSelector;
    use nexus_types::external_api::params::ProjectCreate;
    use nexus_types::external_api::params::SubnetPoolCreate;
    use nexus_types::external_api::params::SubnetPoolMemberAdd;
    use nexus_types::identity::Resource;
    use nexus_types::silo::DEFAULT_SILO_ID;
    use omicron_common::address::IpRange;
    use omicron_common::address::IpVersion;
    use omicron_common::address::Ipv6Range;
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::LookupType;
    use omicron_common::api::external::Name as ExternalName;
    use omicron_common::api::external::NameOrId;
    use omicron_common::api::external::ResourceType;
    use omicron_common::api::external::http_pagination::PaginatedBy;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ExternalSubnetUuid;
    use omicron_uuid_kinds::GenericUuid;
    use oxnet::IpNet;
    use oxnet::Ipv6Net;
    use rand::Rng;
    use rand::distr::Distribution;
    use rand::distr::StandardUniform;
    use rand::distr::Uniform;
    use rand::rng;
    use rand::rngs::ThreadRng;
    use rand::seq::IteratorRandom as _;
    use std::collections::BTreeMap;
    use std::net::Ipv6Addr;
    use strum::EnumCount;
    use strum::FromRepr;

    #[tokio::test]
    async fn basic_subnet_pool_crud() {
        let logctx = dev::test_setup_log("basic_subnet_pool_crud");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with no pools.
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: 100.try_into().unwrap(),
        });
        assert!(
            datastore
                .list_subnet_pools(opctx, &pagparams)
                .await
                .expect("able to list external subnet pools")
                .is_empty(),
            "Should be no external subnet pools yet"
        );

        // Create a pool, basic sanity checks.
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        assert_eq!(pool.identity.name.as_str(), "my-pool");
        assert_eq!(pool.identity.time_created, pool.identity.time_modified);
        assert!(pool.identity.time_deleted.is_none());
        assert_eq!(pool.ip_version, IpVersion::V4.into());

        let by_id = NameOrId::Id(pool.identity.id.into_untyped_uuid());
        let (authz_pool, db_pool) = datastore
            .lookup_subnet_pool(opctx, &by_id)
            .fetch()
            .await
            .expect("able to look up subnet pool by id");
        assert_eq!(pool, db_pool);

        let pools = datastore
            .list_subnet_pools(opctx, &pagparams)
            .await
            .expect("able to list external subnet pools");
        assert_eq!(pools.len(), 1);
        assert_eq!(&pool, &pools[0]);

        // Update
        let updates = SubnetPoolUpdate {
            name: Some(Name("new-name".parse().unwrap())),
            description: None,
            time_modified: Utc::now(),
        };
        let _ = datastore
            .update_subnet_pool(opctx, &authz_pool, updates)
            .await
            .expect("able to update subnet pool");
        let (_, new_db_pool) = datastore
            .lookup_subnet_pool(opctx, &by_id)
            .fetch()
            .await
            .expect("able to look up subnet pool by id");
        assert_eq!(new_db_pool.identity.id, pool.identity.id);
        assert_eq!(
            new_db_pool.identity.time_created,
            pool.identity.time_created
        );
        assert_eq!(new_db_pool.identity.name.as_str(), "new-name");

        // Delete
        datastore
            .delete_subnet_pool(opctx, &authz_pool, &new_db_pool)
            .await
            .expect("should be able to delete pool");
        assert!(
            datastore
                .list_subnet_pools(opctx, &pagparams)
                .await
                .expect("able to list external subnet pools")
                .is_empty(),
            "Should be no external subnet pools after deleting all"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_delete_subnet_pool_with_outstanding_members() {
        let logctx = dev::test_setup_log(
            "cannot_delete_subnet_pool_with_outstanding_members",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, add a member
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let pool_id = NameOrId::Id(pool.identity.id.into_untyped_uuid());

        // Re-fetch to get the authz object
        let (authz_pool, db_pool) = datastore
            .lookup_subnet_pool(opctx, &pool_id)
            .fetch()
            .await
            .expect("able to lookup subnet pool we just made");

        let subnet = IpNet::V4("10.0.0.0/16".parse().unwrap());
        let params = SubnetPoolMemberAdd {
            subnet,
            min_prefix_length: Some(16),
            max_prefix_length: Some(24),
        };
        let member = datastore
            .add_subnet_pool_member(opctx, &authz_pool, &db_pool, &params)
            .await
            .expect("able to create subnet pool member");

        let err = datastore
            .delete_subnet_pool(opctx, &authz_pool, &db_pool)
            .await
            .expect_err("failure to delete pool with outstanding members");
        let Error::InvalidRequest { message } = &err else {
            panic!("expected a NotFound error, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "Cannot delete external subnet pool while it contains \
            members. Please delete all external subnet pool members first."
        );

        // Delete it, and now confirm we can delete the pool.
        datastore
            .delete_subnet_pool_member(opctx, &authz_pool, member.subnet)
            .await
            .expect("able to delete pool member");

        // A little silly, but we need to look this up again for the delete
        // call.
        let (authz_pool, db_pool) = datastore
            .lookup_subnet_pool(opctx, &pool_id)
            .fetch()
            .await
            .expect("able to lookup subnet pool we just made");
        datastore
            .delete_subnet_pool(opctx, &authz_pool, &db_pool)
            .await
            .expect("able to delete subnet pool after deleting all members");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_insert_subnet_pool_member_with_overlapping_ip_subnet() {
        let logctx = dev::test_setup_log(
            "cannot_insert_subnet_pool_member_with_overlapping_ip_subnet",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, add a member
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let pool_id = NameOrId::Id(pool.identity.id.into_untyped_uuid());

        // Re-fetch to get the authz object
        let (authz_pool, db_pool) = datastore
            .lookup_subnet_pool(opctx, &pool_id)
            .fetch()
            .await
            .expect("able to lookup subnet pool we just made");

        let subnet = IpNet::V4("10.1.0.0/16".parse().unwrap());
        let params = SubnetPoolMemberAdd {
            subnet,
            min_prefix_length: Some(16),
            max_prefix_length: Some(24),
        };
        let _member = datastore
            .add_subnet_pool_member(opctx, &authz_pool, &db_pool, &params)
            .await
            .expect("able to create subnet pool member");

        // Try to add member with overlapping subnet.
        let subnet = IpNet::V4("10.1.1.0/24".parse().unwrap());
        let params = SubnetPoolMemberAdd {
            subnet,
            min_prefix_length: Some(24),
            max_prefix_length: Some(26),
        };
        let err = datastore
            .add_subnet_pool_member(opctx, &authz_pool, &db_pool, &params)
            .await
            .expect_err("failure to create overlapping subnet member");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected invalid request, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "The IP subnet 10.1.1.0/24 overlaps with an existing \
            Subnet Pool member or IP Pool range"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn can_link_subnet_pool_to_silo() {
        usdt::register_probes().unwrap();

        let logctx = dev::test_setup_log("can_link_subnet_pool_to_silo");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, link it to the default silo
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            pool.identity.id.into(),
            LookupType::ById(pool.identity.id.into_untyped_uuid()),
        );

        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );

        let _link = datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect("able to link pool to silo");

        // Should fail to link a second time.
        let err = datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect_err("able to link pool to silo");
        let Error::Conflict { message } = &err else {
            panic!("Expected invalid request, found: {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "Subnet Pool is already linked to Silo"
        );

        // We should not be able to link another default of the same IP version.
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-new-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let new_pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let new_authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            new_pool.identity.id.into(),
            LookupType::ById(new_pool.identity.id.into_untyped_uuid()),
        );
        let err = datastore
            .link_subnet_pool_to_silo(opctx, &new_authz_pool, &authz_silo, true)
            .await
            .expect_err("able to link pool to silo");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected invalid request, found: {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "Can only have a single default Subnet Pool for a Silo \
            for each IP version."
        );

        // Now unlink the first, and we should be able to link the second as a
        // default. Note we have to fetch the pool again, since we have bumped
        // the rcgen by doing the above link.
        let (authz_pool, pool) = datastore
            .lookup_subnet_pool(
                opctx,
                &NameOrId::Id(pool.id().into_untyped_uuid()),
            )
            .fetch()
            .await
            .unwrap();
        let _ = datastore
            .unlink_subnet_pool_from_silo(
                opctx,
                &authz_pool,
                &pool,
                &authz_silo,
            )
            .await
            .expect("able to unlink pool from silo");
        let _ = datastore
            .link_subnet_pool_to_silo(opctx, &new_authz_pool, &authz_silo, true)
            .await
            .expect("able to link pool to silo after unlinking other");

        // Now we could re-link the first, as long as it's not the default.
        let _ = datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, false)
            .await
            .expect("able to link second non-defult pool to silo");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_link_subnet_pool_to_deleted_silo() {
        let logctx =
            dev::test_setup_log("cannot_link_subnet_pool_to_deleted_silo");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, link it to the default silo
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            pool.identity.id.into(),
            LookupType::ById(pool.identity.id.into_untyped_uuid()),
        );

        // Delete the silo directly, so we don't have to mess with the DNS
        // infrastructure.
        {
            use nexus_db_schema::schema::silo::dsl;
            diesel::update(dsl::silo.filter(dsl::id.eq(DEFAULT_SILO_ID)))
                .set(dsl::time_deleted.eq(Utc::now()))
                .execute_async(
                    &*datastore
                        .pool_connection_authorized(opctx)
                        .await
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );
        let err = datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect_err("should fail to link pool to deleted silo");
        let Error::ObjectNotFound { type_name: ResourceType::Silo, .. } = &err
        else {
            panic!("Expected ObjectNotFound, found {err:#?}");
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_link_deleted_subnet_pool_to_silo() {
        let logctx =
            dev::test_setup_log("cannot_link_deleted_subnet_pool_to_silo");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, link it to the default silo
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            pool.identity.id.into(),
            LookupType::ById(pool.identity.id.into_untyped_uuid()),
        );

        // Delete the subnet pool directly
        {
            use nexus_db_schema::schema::subnet_pool::dsl;
            diesel::update(
                dsl::subnet_pool
                    .filter(dsl::id.eq(pool.id().into_untyped_uuid())),
            )
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(
                &*datastore.pool_connection_authorized(opctx).await.unwrap(),
            )
            .await
            .unwrap();
        }

        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );
        let err = datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect_err("should fail to link deleted pool to silo");
        let Error::ObjectNotFound {
            type_name: ResourceType::SubnetPool, ..
        } = &err
        else {
            panic!("Expected NotFound, found {err:#?}");
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    struct Context {
        logctx: LogContext,
        db: TestDatabase,
        db_pool: SubnetPool,
        authz_pool: authz::SubnetPool,
        members: Vec<SubnetPoolMember>,
        authz_silo: authz::Silo,
        authz_project: authz::Project,
    }

    async fn setup_external_subnet_test(test_name: &str) -> Context {
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, link it to the default silo
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V6,
        };
        let db_pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            db_pool.identity.id.into(),
            LookupType::ById(db_pool.identity.id.into_untyped_uuid()),
        );
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );
        datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect("able to link pool to default silo");

        // Create a few pool members in the pool.
        let subnets = &[
            "2001:db8:1::/48".parse().unwrap(),
            "2001:db8:2::/48".parse().unwrap(),
            "2001:db8:3::/48".parse().unwrap(),
        ];
        let mut members = Vec::with_capacity(subnets.len());
        for subnet in subnets.iter().copied() {
            let member = datastore
                .add_subnet_pool_member(
                    opctx,
                    &authz_pool,
                    &db_pool,
                    &SubnetPoolMemberAdd {
                        subnet,
                        min_prefix_length: Some(48),
                        max_prefix_length: Some(64),
                    },
                )
                .await
                .expect("able to create subnet pool member");
            members.push(member);
        }

        // Create a dummy project. This doesn't have all the details we normally
        // need, like default VPC or VPC Subnets, but it has enough to test the
        // below methods.
        let (authz_project, _db_project) = db
            .datastore()
            .project_create(
                db.opctx(),
                Project::new(
                    DEFAULT_SILO_ID,
                    ProjectCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "my-project".parse().unwrap(),
                            description: String::new(),
                        },
                    },
                ),
            )
            .await
            .expect("able to create a project");

        // Refetch the pool. We've bumped the generation a few times with the
        // above operations.
        let (authz_pool, db_pool) = datastore
            .lookup_subnet_pool(
                db.opctx(),
                &authz_pool.id().into_untyped_uuid().into(),
            )
            .fetch()
            .await
            .unwrap();

        Context {
            logctx,
            db,
            db_pool,
            authz_pool,
            members,
            authz_silo,
            authz_project,
        }
    }

    #[tokio::test]
    async fn cannot_unlink_subnet_pool_with_external_subnets_in_silo() {
        let context = setup_external_subnet_test(
            "cannot_unlink_subnet_pool_with_external_subnets_in_silo",
        )
        .await;
        let ip_subnet = "2001:db8:1::/64".parse().unwrap();
        let _ = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: ip_subnet,
                    },
                },
            )
            .await
            .expect("able to insert subnet explicitly from IP subnet");

        // Now, we should fail to unlink the pool from the silo.
        let err = context
            .db
            .datastore()
            .unlink_subnet_pool_from_silo(
                context.db.opctx(),
                &context.authz_pool,
                &context.db_pool,
                &context.authz_silo,
            )
            .await
            .expect_err("should fail to unlink");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found: {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "Cannot unlink Subnet Pool from Silo while there are \
            External Subnets allocated from the pool. Delete the \
            subnets first, and try again.",
        );

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn can_insert_external_subnet_from_explicit_ip_subnet() {
        let context = setup_external_subnet_test(
            "can_insert_external_subnet_from_explicit_ip_subnet",
        )
        .await;
        let ip_subnet = "2001:db8:1::/48".parse().unwrap();
        let mut subnets = Vec::with_capacity(3);
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: ip_subnet,
                    },
                },
            )
            .await
            .expect("able to insert subnet explicitly from IP subnet");
        assert_eq!(subnet.subnet_pool_id, context.db_pool.id().into());
        assert_eq!(subnet.subnet_pool_member_id, context.members[0].id);
        assert_eq!(subnet.subnet, ip_subnet.into());
        subnets.push(subnet);

        // We can take another couple of chunks of the next subnet pool member
        // too.
        for (i, ip_subnet) in [
            "2001:db8:2:0::/64".parse().unwrap(),
            "2001:db8:2:1::/64".parse().unwrap(),
        ]
        .into_iter()
        .enumerate()
        {
            let subnet = context
                .db
                .datastore()
                .create_external_subnet(
                    context.db.opctx(),
                    &DEFAULT_SILO_ID,
                    &context.authz_project,
                    ExternalSubnetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: format!("sub{i}").parse().unwrap(),
                            description: String::new(),
                        },
                        allocator: ExternalSubnetAllocator::Explicit {
                            subnet: ip_subnet,
                        },
                    },
                )
                .await
                .expect("able to insert subnet explicitly from IP subnet");
            assert_eq!(subnet.subnet_pool_id, context.db_pool.id().into());
            assert_eq!(subnet.subnet_pool_member_id, context.members[1].id);
            assert_eq!(subnet.subnet, ip_subnet.into());
            subnets.push(subnet);
        }

        // We can delete them all now.
        for subnet in subnets.into_iter() {
            let authz_subnet = authz::ExternalSubnet::new(
                context.authz_project.clone(),
                subnet.id(),
                LookupType::ById(subnet.id().into_untyped_uuid()),
            );
            context
                .db
                .datastore()
                .delete_external_subnet(context.db.opctx(), &authz_subnet)
                .await
                .expect("able to delete external subnet");
        }

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_request_explicit_ip_subnet_outside_of_linked_pools() {
        let context = setup_external_subnet_test(
            "cannot_request_explicit_ip_subnet_outside_of_linked_pools",
        )
        .await;
        let ip_subnet = "2001:db8:a::/48".parse().unwrap();
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: ip_subnet,
                    },
                },
            )
            .await
            .expect_err(
                "unable to insert subnet explicitly from IP subnet \
                outside of the linked pools",
            );
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "The requested IP subnet is not contained in any \
            Subnet Pool available in the current Silo."
        );

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_delete_external_subnet_attached_to_instance() {
        let context = setup_external_subnet_test(
            "cannot_delete_external_subnet_attached_to_instance",
        )
        .await;
        let ip_subnet = "2001:db8:1::/64".parse().unwrap();
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: ip_subnet,
                    },
                },
            )
            .await
            .expect("able to insert subnet explicitly from IP subnet");
        assert_eq!(subnet.subnet_pool_id, context.db_pool.id().into());
        assert_eq!(subnet.subnet_pool_member_id, context.members[0].id);
        assert_eq!(subnet.subnet, ip_subnet.into());

        // "attach" it.
        let instance_id = uuid::Uuid::new_v4();
        {
            use nexus_db_schema::schema::external_subnet::dsl;
            diesel::update(
                dsl::external_subnet
                    .filter(dsl::id.eq(to_db_typed_uuid(subnet.id()))),
            )
            .set((
                dsl::attach_state.eq(IpAttachState::Attached),
                dsl::instance_id.eq(Some(instance_id)),
            ))
            .execute_async(
                &*context
                    .db
                    .datastore()
                    .pool_connection_authorized(context.db.opctx())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        }

        // We should fail to delete it now.
        let authz_subnet = authz::ExternalSubnet::new(
            context.authz_project.clone(),
            subnet.id(),
            LookupType::ById(subnet.id().into_untyped_uuid()),
        );
        let err = context
            .db
            .datastore()
            .delete_external_subnet(context.db.opctx(), &authz_subnet)
            .await
            .expect_err("unable to delete external subnet");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "Cannot delete external subnet while it is attached \
            to an instance. Detach it and try again."
        );

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn can_insert_external_subnet_from_explicit_pool_selection() {
        let context = setup_external_subnet_test(
            "can_insert_external_subnet_from_explicit_pool_selection",
        )
        .await;
        let prefix_len = 64;
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Explicit {
                            pool: NameOrId::Id(
                                context.db_pool.id().into_untyped_uuid(),
                            ),
                        },
                        prefix_len,
                    },
                },
            )
            .await
            .expect("able to insert subnet explicitly from subnet pool");
        assert_eq!(subnet.subnet_pool_id, context.db_pool.id().into());
        assert_eq!(subnet.subnet_pool_member_id, context.members[0].id);

        // Should take the first /64 from any pool member.
        let expected_subnet = IpNet::new(
            oxnet::IpNet::from(context.members[0].subnet).addr(),
            prefix_len,
        )
        .unwrap();
        assert_eq!(oxnet::IpNet::from(subnet.subnet), expected_subnet);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn can_insert_external_subnet_from_ip_version() {
        let context = setup_external_subnet_test(
            "can_insert_external_subnet_from_ip_version",
        )
        .await;
        let prefix_len = 64;
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto {
                            ip_version: Some(IpVersion::V6),
                        },
                        prefix_len,
                    },
                },
            )
            .await
            .expect("able to insert subnet from IP version");
        assert_eq!(subnet.subnet_pool_id, context.db_pool.id().into());
        assert_eq!(subnet.subnet_pool_member_id, context.members[0].id);

        // Should take the first /64 from any pool member.
        let expected_subnet = IpNet::new(
            oxnet::IpNet::from(context.members[0].subnet).addr(),
            prefix_len,
        )
        .unwrap();
        assert_eq!(oxnet::IpNet::from(subnet.subnet), expected_subnet);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn can_insert_external_subnet_using_default_pool() {
        let context = setup_external_subnet_test(
            "can_insert_external_subnet_using_default_pool",
        )
        .await;
        let prefix_len = 64;
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len,
                    },
                },
            )
            .await
            .expect("able to insert subnet using default pool");
        assert_eq!(subnet.subnet_pool_id, context.db_pool.id().into());
        assert_eq!(subnet.subnet_pool_member_id, context.members[0].id);

        // Should take the first /64 from any pool member.
        let expected_subnet = IpNet::new(
            oxnet::IpNet::from(context.members[0].subnet).addr(),
            prefix_len,
        )
        .unwrap();
        assert_eq!(oxnet::IpNet::from(subnet.subnet), expected_subnet);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_insert_overlapping_external_subnets() {
        let context = setup_external_subnet_test(
            "cannot_insert_overlapping_external_subnets",
        )
        .await;
        let ip_subnet = "2001:db8:1::/64".parse().unwrap();
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: ip_subnet,
                    },
                },
            )
            .await
            .expect("able to insert subnet explicitly from IP subnet");
        assert_eq!(subnet.subnet_pool_id, context.db_pool.id().into());
        assert_eq!(subnet.subnet_pool_member_id, context.members[0].id);
        assert_eq!(subnet.subnet, ip_subnet.into());

        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-other-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: ip_subnet,
                    },
                },
            )
            .await
            .expect_err("not able to insert overlapping subnet");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            SUBNET_OVERLAPS_EXISTING_ERR_MSG,
        );

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fail_to_allocate_external_subnet_with_bad_prefix() {
        let context = setup_external_subnet_test(
            "fail_to_allocate_external_subnet_with_bad_prefix",
        )
        .await;
        let ip_subnet = "2001:db8::/32".parse().unwrap();
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: ip_subnet,
                    },
                },
            )
            .await
            .expect_err("fail to insert subnet with bad prefix");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            NO_LINKED_POOL_CONTAINS_SUBNET_ERR_MSG,
        );

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fail_on_subnet_pool_exhaustion() {
        let context =
            setup_external_subnet_test("fail_on_subnet_pool_exhaustion").await;
        // Take the whole member range for each subnet.
        let mut subnets = Vec::with_capacity(context.members.len());
        for (i, member) in context.members.iter().enumerate() {
            let subnet = context
                .db
                .datastore()
                .create_external_subnet(
                    context.db.opctx(),
                    &DEFAULT_SILO_ID,
                    &context.authz_project,
                    ExternalSubnetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: format!("sub{i}").parse().unwrap(),
                            description: String::new(),
                        },
                        allocator: ExternalSubnetAllocator::Auto {
                            pool_selector: PoolSelector::Auto {
                                ip_version: None,
                            },
                            prefix_len: 48,
                        },
                    },
                )
                .await
                .expect("able to insert subnet from default pool");
            assert_eq!(subnet.subnet, member.subnet);
            subnets.push(subnet);
        }

        // Trying to take another should fail.
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "subn".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len: 48,
                    },
                },
            )
            .await
            .expect_err("unable to insert subnet when all members are used");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "All subnets are used in the default Subnet Pool for \
            the current Silo",
        );

        // Also fail, but since we're requesting a pool by version, the error
        // message should reflect that.
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "subn".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto {
                            ip_version: Some(IpVersion::V6),
                        },
                        prefix_len: 48,
                    },
                },
            )
            .await
            .expect_err("unable to insert subnet when all members are used");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "All subnets are used in the default IPv6 Subnet Pool \
            for the current Silo",
        );

        // Also fail, but since we're requesting a pool, the error message
        // should reflect that.
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "subn".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Explicit {
                            pool: NameOrId::Id(
                                context.authz_pool.id().into_untyped_uuid(),
                            ),
                        },
                        prefix_len: 48,
                    },
                },
            )
            .await
            .expect_err("unable to insert subnet when all members are used");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            format!(
                "All subnets are used in the Subnet Pool with ID '{}'",
                context.authz_pool.id().into_untyped_uuid(),
            ),
        );

        // Also fail, but since we're requesting a pool _by name_, the error
        // message should reflect that.
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "subn".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Explicit {
                            pool: NameOrId::Name(
                                context.db_pool.name().clone(),
                            ),
                        },
                        prefix_len: 56,
                    },
                },
            )
            .await
            .expect_err("unable to insert subnet when all members are used");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            format!(
                "All subnets are used in the Subnet Pool with name '{}'",
                context.db_pool.name(),
            ),
        );

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn creating_subnet_with_multiple_default_pools_fails() {
        let context = setup_external_subnet_test(
            "creating_subnet_with_multiple_default_pools_fails",
        )
        .await;

        // Add an IPv4 Subnet Pool and link it. We now have more than one
        // default.
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "other-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let db_pool = context
            .db
            .datastore()
            .create_subnet_pool(context.db.opctx(), params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            db_pool.id(),
            LookupType::ById(db_pool.id().into_untyped_uuid()),
        );
        context
            .db
            .datastore()
            .link_subnet_pool_to_silo(
                context.db.opctx(),
                &authz_pool,
                &context.authz_silo,
                true,
            )
            .await
            .expect("able to link pool to default silo");

        // Now when we try to allocate by taking "the" default for our silo, we
        // should fail predictably.
        let prefix_len = 64;
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len,
                    },
                },
            )
            .await
            .expect_err("should fail with multiple default pools");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            MULTIPLE_LINKED_DEFAULT_POOLS_ERR_MSG,
        );

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn creating_subnet_with_no_default_pools_fails() {
        let context = setup_external_subnet_test(
            "creating_subnet_with_no_default_pools_fails",
        )
        .await;

        // Unlink the existing default pool.
        context
            .db
            .datastore()
            .unlink_subnet_pool_from_silo(
                context.db.opctx(),
                &context.authz_pool,
                &context.db_pool,
                &context.authz_silo,
            )
            .await
            .expect("able to unlink pool");

        // Now when we try to allocate by taking "the" default for our silo, we
        // should fail predictably.
        let prefix_len = 64;
        let err = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "my-subnet".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len,
                    },
                },
            )
            .await
            .expect_err("should fail with multiple default pools");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(message.external_message(), NO_LINKED_DEFAULT_POOL_ERR_MSG,);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn external_subnet_allocation_takes_smallest_gap_at_member_start() {
        let context = setup_external_subnet_test(
            "external_subnet_allocation_takes_smallest_gap_at_member_start",
        )
        .await;

        // Allocate 2 /56s.
        let mut subnets = Vec::with_capacity(2);
        let prefix_len = 56;
        for i in 0..subnets.capacity() {
            let subnet = context
                .db
                .datastore()
                .create_external_subnet(
                    context.db.opctx(),
                    &DEFAULT_SILO_ID,
                    &context.authz_project,
                    ExternalSubnetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: format!("sub{i}").parse().unwrap(),
                            description: String::new(),
                        },
                        allocator: ExternalSubnetAllocator::Auto {
                            pool_selector: PoolSelector::Auto {
                                ip_version: None,
                            },
                            prefix_len,
                        },
                    },
                )
                .await
                .expect("able to insert subnet using default pool");
            subnets.push(subnet);
        }

        // Free the first one.
        context
            .db
            .datastore()
            .delete_external_subnet(
                context.db.opctx(),
                &authz::ExternalSubnet::new(
                    context.authz_project.clone(),
                    subnets[0].id(),
                    LookupType::ById(subnets[0].id().into_untyped_uuid()),
                ),
            )
            .await
            .expect("able to delete external subnet we just made");

        // And allocate it again.
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "sub".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len,
                    },
                },
            )
            .await
            .expect("able to insert subnet using default pool");
        assert_eq!(subnet.subnet, subnets[0].subnet);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn external_subnet_allocation_takes_smallest_gap_in_member_middle() {
        let context = setup_external_subnet_test(
            "external_subnet_allocation_takes_smallest_gap_in_member_middle",
        )
        .await;

        // Allocate 3 /56s.
        let mut subnets = Vec::with_capacity(3);
        let prefix_len = 56;
        for i in 0..subnets.capacity() {
            let subnet = context
                .db
                .datastore()
                .create_external_subnet(
                    context.db.opctx(),
                    &DEFAULT_SILO_ID,
                    &context.authz_project,
                    ExternalSubnetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: format!("sub{i}").parse().unwrap(),
                            description: String::new(),
                        },
                        allocator: ExternalSubnetAllocator::Auto {
                            pool_selector: PoolSelector::Auto {
                                ip_version: None,
                            },
                            prefix_len,
                        },
                    },
                )
                .await
                .expect("able to insert subnet using default pool");
            subnets.push(subnet);
        }

        // Free the middle one.
        context
            .db
            .datastore()
            .delete_external_subnet(
                context.db.opctx(),
                &authz::ExternalSubnet::new(
                    context.authz_project.clone(),
                    subnets[1].id(),
                    LookupType::ById(subnets[1].id().into_untyped_uuid()),
                ),
            )
            .await
            .expect("able to delete external subnet we just made");

        // And allocate it again.
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "sub".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len,
                    },
                },
            )
            .await
            .expect("able to insert subnet using default pool");
        assert_eq!(subnet.subnet, subnets[1].subnet);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn external_subnet_allocation_skips_too_small_gaps_in_member_middle()
    {
        let context = setup_external_subnet_test(
            "external_subnet_allocation_skips_too_small_gaps_in_member_middle",
        )
        .await;

        // Allocate 3 /56s.
        let mut minis = Vec::with_capacity(3);
        let prefix_len = 56;
        for i in 0..3 {
            let subnet = context
                .db
                .datastore()
                .create_external_subnet(
                    context.db.opctx(),
                    &DEFAULT_SILO_ID,
                    &context.authz_project,
                    ExternalSubnetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: format!("sub{i}").parse().unwrap(),
                            description: String::new(),
                        },
                        allocator: ExternalSubnetAllocator::Auto {
                            pool_selector: PoolSelector::Auto {
                                ip_version: None,
                            },
                            prefix_len,
                        },
                    },
                )
                .await
                .expect("able to insert subnet using default pool");
            println!("{}", subnet.subnet);
            minis.push(subnet);
        }

        // Free the middle one.
        context
            .db
            .datastore()
            .delete_external_subnet(
                context.db.opctx(),
                &authz::ExternalSubnet::new(
                    context.authz_project.clone(),
                    minis[1].id(),
                    LookupType::ById(minis[1].id().into_untyped_uuid()),
                ),
            )
            .await
            .expect("able to delete external subnet we just made");

        // Now if we try to take a /48, which won't fit in the gap we just made,
        // it should come from the next pool member. That's the next place we
        // have a prefix-aligned /48.
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "sub".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len: 48,
                    },
                },
            )
            .await
            .expect("able to insert subnet using default pool");
        assert_eq!(subnet.subnet, context.members[1].subnet);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn external_subnet_allocation_skips_too_small_gaps_at_member_start() {
        let context = setup_external_subnet_test(
            "external_subnet_allocation_skips_too_small_gaps_at_member_start",
        )
        .await;

        // Allocate 2 /56s.
        let mut minis = Vec::with_capacity(2);
        let prefix_len = 56;
        for i in 0..minis.capacity() {
            let subnet = context
                .db
                .datastore()
                .create_external_subnet(
                    context.db.opctx(),
                    &DEFAULT_SILO_ID,
                    &context.authz_project,
                    ExternalSubnetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: format!("sub{i}").parse().unwrap(),
                            description: String::new(),
                        },
                        allocator: ExternalSubnetAllocator::Auto {
                            pool_selector: PoolSelector::Auto {
                                ip_version: None,
                            },
                            prefix_len,
                        },
                    },
                )
                .await
                .expect("able to insert subnet using default pool");
            println!("{}", subnet.subnet);
            minis.push(subnet);
        }

        // Free the first one.
        context
            .db
            .datastore()
            .delete_external_subnet(
                context.db.opctx(),
                &authz::ExternalSubnet::new(
                    context.authz_project.clone(),
                    minis[0].id(),
                    LookupType::ById(minis[0].id().into_untyped_uuid()),
                ),
            )
            .await
            .expect("able to delete external subnet we just made");

        // Now if we try to take a /48, which won't fit in the gap we just made,
        // it should come from the next pool member. That's the next place we
        // have a prefix-aligned /48.
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "sub".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len: 48,
                    },
                },
            )
            .await
            .expect("able to insert subnet using default pool");
        assert_eq!(subnet.subnet, context.members[1].subnet);

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn external_subnet_allocation_skips_too_small_prefix_aligned_gap() {
        let context = setup_external_subnet_test(
            "external_subnet_allocation_skips_too_small_prefix_aligned_gap",
        )
        .await;

        // Allocate 4 /57s.
        let mut minis = Vec::with_capacity(4);
        let prefix_len = 57;
        for i in 0..minis.capacity() {
            let subnet = context
                .db
                .datastore()
                .create_external_subnet(
                    context.db.opctx(),
                    &DEFAULT_SILO_ID,
                    &context.authz_project,
                    ExternalSubnetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: format!("sub{i}").parse().unwrap(),
                            description: String::new(),
                        },
                        allocator: ExternalSubnetAllocator::Auto {
                            pool_selector: PoolSelector::Auto {
                                ip_version: None,
                            },
                            prefix_len,
                        },
                    },
                )
                .await
                .expect("able to insert subnet using default pool");
            println!("{}", subnet.subnet);
            minis.push(subnet);
        }

        // Free the third one.
        context
            .db
            .datastore()
            .delete_external_subnet(
                context.db.opctx(),
                &authz::ExternalSubnet::new(
                    context.authz_project.clone(),
                    minis[0].id(),
                    LookupType::ById(minis[0].id().into_untyped_uuid()),
                ),
            )
            .await
            .expect("able to delete external subnet we just made");

        // Now, if we try to take a /56, we should take the one after the last
        // mini. The issue here is that the gap we just freed is aligned to a
        // /56 boundary, but it's not big enough or our /56, since it's just a
        // /57.
        let subnet = context
            .db
            .datastore()
            .create_external_subnet(
                context.db.opctx(),
                &DEFAULT_SILO_ID,
                &context.authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "sub".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        pool_selector: PoolSelector::Auto { ip_version: None },
                        prefix_len: 56,
                    },
                },
            )
            .await
            .expect("able to insert subnet using default pool");

        let IpNet::V6(last_mini) = IpNet::from(minis.last().unwrap().subnet)
        else {
            unreachable!("We're using IPv6 here");
        };
        let next_start = Ipv6Addr::from(u128::from(last_mini.last_addr()) + 1);
        let expected_subnet = Ipv6Net::new(next_start, 56).unwrap();
        assert_eq!(subnet.subnet, expected_subnet.into());

        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[derive(Debug, EnumCount, FromRepr)]
    enum AllocationKind {
        ExplicitInvalidSubnet,
        ExplicitValidSubnet,
        ExplicitPool,
        AutoVersion,
        AutoDefaultPool,
    }

    impl Distribution<AllocationKind> for StandardUniform {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> AllocationKind {
            AllocationKind::from_repr(
                rng.random_range(0..AllocationKind::COUNT),
            )
            .unwrap()
        }
    }

    fn random_network_in(
        rng: &mut ThreadRng,
        net: Ipv6Net,
        prefix: u8,
    ) -> Ipv6Net {
        assert!(prefix >= net.width());
        assert!(prefix <= 128);
        let base = u128::from(net.first_addr());
        let rand_bits = prefix - net.width();
        let rand_addr = rng.random::<u128>() & ((1u128 << rand_bits) - 1);
        let rand_part = rand_addr << (128 - prefix);
        let addr = Ipv6Addr::from(base | rand_part);
        Ipv6Net::new(addr, prefix).unwrap()
    }

    fn random_member_subnet(
        member: &SubnetPoolMember,
        rng: &mut ThreadRng,
    ) -> IpNet {
        let min: u8 = member.min_prefix_length().into();
        let max: u8 = member.max_prefix_length().into();
        let prefix = rng.random_range(min..=max);

        // Create a completely random address, mask it to the chosen prefix
        // length, and set the first bits to that of the member.
        let nexus_db_model::IpNet::V6(net) = member.subnet else {
            unreachable!("This test only uses IPv6 addresses");
        };
        let net: Ipv6Net = net.into();
        let subnet = random_network_in(rng, net, prefix);
        assert!(subnet.is_network_address());
        subnet.into()
    }

    fn random_network(rng: &mut ThreadRng, prefix: u8) -> IpNet {
        assert!(prefix <= 128);
        let base: u128 = rng.random();
        let netmask = if prefix == 0 { 0 } else { (!0u128) << (128 - prefix) };
        Ipv6Net::new(Ipv6Addr::from(base & netmask), prefix).unwrap().into()
    }

    fn random_allocator(
        context: &Context,
    ) -> Option<(AllocationKind, ExternalName, ExternalSubnetAllocator)> {
        let mut rng = rng();
        let prefix_len: u8 = rng.random_range(0..=64);
        let kind: AllocationKind = rng.random();
        let allocator = match kind {
            AllocationKind::ExplicitInvalidSubnet => {
                // Pick a completely random subnet. We're pretty likely to
                // be outside all the members, but retry until that's true. Make
                // sure we don't loop forever though.
                let mut i = 0;
                let subnet = loop {
                    let net = random_network(&mut rng, prefix_len);
                    if context.members.iter().all(|member| {
                        let subnet = IpNet::from(member.subnet);
                        !subnet.overlaps(&net)
                    }) {
                        break net;
                    }
                    i += 1;
                    if i > 100 {
                        println!("failed to find non-overlapping subnet");
                        return None;
                    }
                };
                ExternalSubnetAllocator::Explicit { subnet }
            }
            AllocationKind::ExplicitValidSubnet => {
                // Pick each member uniformly, generate a random subnet
                // within it.
                let member = context.members.iter().choose(&mut rng).unwrap();
                let subnet = random_member_subnet(member, &mut rng);
                ExternalSubnetAllocator::Explicit { subnet }
            }
            AllocationKind::ExplicitPool => ExternalSubnetAllocator::Auto {
                pool_selector: PoolSelector::Explicit {
                    pool: NameOrId::Id(
                        context.db_pool.id().into_untyped_uuid(),
                    ),
                },
                prefix_len,
            },
            AllocationKind::AutoVersion => ExternalSubnetAllocator::Auto {
                pool_selector: PoolSelector::Auto {
                    ip_version: Some(IpVersion::V6),
                },
                prefix_len,
            },
            AllocationKind::AutoDefaultPool => ExternalSubnetAllocator::Auto {
                pool_selector: PoolSelector::Auto { ip_version: None },
                prefix_len,
            },
        };
        Some((kind, random_name(&mut rng), allocator))
    }

    fn random_name(rng: &mut ThreadRng) -> ExternalName {
        let dist = Uniform::new('a', 'z').unwrap();
        rng.sample_iter(dist).take(16).collect::<String>().parse().unwrap()
    }

    async fn free_random_subnet(
        context: &Context,
        allocated_subnets: &mut BTreeMap<Ipv6Net, ExternalSubnetUuid>,
    ) {
        // Free a random subnet.
        let Some((addrs, key)) = allocated_subnets
            .iter()
            .choose(&mut rand::rng())
            .map(|(a, k)| (*a, *k))
        else {
            return;
        };
        println!("action=free addrs={addrs} key={key}");
        context
            .db
            .datastore()
            .delete_external_subnet(
                context.db.opctx(),
                &authz::ExternalSubnet::new(
                    context.authz_project.clone(),
                    key,
                    LookupType::by_id(key),
                ),
            )
            .await
            .expect("able to delete existing external subnet");
        let _ = allocated_subnets.remove(&addrs).unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn stress_test_external_subnet_allocation() {
        let context = setup_external_subnet_test(
            "stress_test_external_subnet_allocation",
        )
        .await;
        const FREE_PROB: f64 = 0.25;
        const N_ACTIONS: usize = 1_000;
        let mut allocated_subnets =
            BTreeMap::<Ipv6Net, ExternalSubnetUuid>::new();
        for _ in 0..N_ACTIONS {
            if rand::random_bool(FREE_PROB) {
                free_random_subnet(&context, &mut allocated_subnets).await;
            } else {
                // Perform a random allocation
                let Some((kind, subnet_name, allocator)) =
                    random_allocator(&context)
                else {
                    free_random_subnet(&context, &mut allocated_subnets).await;
                    continue;
                };
                let result = context
                    .db
                    .datastore()
                    .create_external_subnet(
                        context.db.opctx(),
                        &DEFAULT_SILO_ID,
                        &context.authz_project,
                        ExternalSubnetCreate {
                            identity: IdentityMetadataCreateParams {
                                name: subnet_name,
                                description: String::new(),
                            },
                            allocator: allocator.clone(),
                        },
                    )
                    .await;
                println!(
                    "action={kind:?} selector={allocator:?} result={result:?}"
                );
                match kind {
                    AllocationKind::ExplicitInvalidSubnet => {
                        let Err(Error::InvalidRequest { message }) = &result
                        else {
                            panic!(
                                "Expected an error trying to insert an invalid \
                                subnet explicitly, selector={allocator:#?}, \
                                result={result:#?}",
                            );
                        };
                        assert_eq!(
                            message.external_message(),
                            NO_LINKED_POOL_CONTAINS_SUBNET_ERR_MSG
                        );
                    }
                    AllocationKind::ExplicitValidSubnet => {
                        // If the subnet we asked for overlaps with any of the ones
                        // we've already inserted, this should fail. Otherwise it
                        // should either succeed or fail. Exhaustion is not a
                        // separate error message here, because we're not
                        // automatically allocated a subnet.
                        let ExternalSubnetAllocator::Explicit {
                            subnet: requested_subnet,
                        } = &allocator
                        else {
                            unreachable!();
                        };
                        let IpNet::V6(requested_v6_net) = requested_subnet
                        else {
                            unreachable!();
                        };
                        let overlapping =
                            allocated_subnets.iter().find(|(subnet, _key)| {
                                subnet.overlaps(requested_v6_net)
                            });
                        if let Some((subnet, _key)) = overlapping {
                            let Err(Error::InvalidRequest { message }) =
                                &result
                            else {
                                panic!(
                                    "Expected InvalidRequest when inserting an \
                                    explicit subnet that overlaps with one we've
                                    already allocated, selector={allocator:#?}, \
                                    allocated_subnet={subnet}, result={result:#?}"
                                );
                            };
                            assert_eq!(
                                message.external_message(),
                                SUBNET_OVERLAPS_EXISTING_ERR_MSG,
                            );
                        } else {
                            let Ok(external_subnet) = &result else {
                                panic!(
                                    "Expected to succeed inserting an explicit \
                                    subnet that doesn't overlap with any we've \
                                    already inserted, selector={allocator:#?}, \
                                    result={result:#?}",
                                );
                            };
                            let IpNet::V6(subnet) =
                                IpNet::from(external_subnet.subnet)
                            else {
                                unreachable!();
                            };
                            allocated_subnets
                                .insert(subnet, external_subnet.id());
                        }
                    }
                    AllocationKind::ExplicitPool
                    | AllocationKind::AutoVersion
                    | AllocationKind::AutoDefaultPool => {
                        // We either succeeded, or get an exhaustion error. We
                        // should definitely not get errors about overlap, since
                        // we're picking the subnet.
                        match result {
                            Ok(subnet) => {
                                let IpNet::V6(net) = subnet.subnet.into()
                                else {
                                    unreachable!();
                                };
                                if let Some((overlapping, _)) =
                                    allocated_subnets.iter().find(
                                        |(subnet, _key)| subnet.overlaps(&net),
                                    )
                                {
                                    panic!(
                                        "Automatically created subnet overlaps an \
                                        existing, existing={}, new={}",
                                        overlapping, net,
                                    );
                                }
                                // Insert the new one.
                                allocated_subnets.insert(net, subnet.id());
                            }
                            Err(Error::InvalidRequest { message }) => {
                                let expected_message = match kind {
                                    AllocationKind::ExplicitInvalidSubnet
                                    | AllocationKind::ExplicitValidSubnet => {
                                        unreachable!()
                                    }
                                    AllocationKind::ExplicitPool => {
                                        format!(
                                            "All subnets are used in the \
                                            Subnet Pool with ID '{}'",
                                            context.authz_pool.id(),
                                        )
                                    }
                                    AllocationKind::AutoVersion => {
                                        String::from(
                                            "All subnets are used in the \
                                            default IPv6 Subnet Pool for \
                                            the current Silo",
                                        )
                                    }
                                    AllocationKind::AutoDefaultPool => {
                                        String::from(
                                            "All subnets are used in the \
                                            default Subnet Pool for \
                                            the current Silo",
                                        )
                                    }
                                };
                                assert_eq!(
                                    message.external_message(),
                                    expected_message,
                                    "Expected an error message about \
                                    pool exhaustion"
                                );
                            }
                            Err(other) => {
                                panic!(
                                    "Expected to succeed or fail with an \
                                    InvalidRequest and an error message \
                                    about pool exhaustion,found: {other:#?}"
                                );
                            }
                        }
                    }
                }
            }
        }
        context.db.terminate().await;
        context.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn handle_overflow_at_subnet_boundaries() {
        let logctx =
            dev::test_setup_log("handle_overflow_at_subnet_boundaries");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, link it to the default silo
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let db_pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            db_pool.identity.id.into(),
            LookupType::ById(db_pool.identity.id.into_untyped_uuid()),
        );
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );
        datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect("able to link pool to default silo");

        // Create one member in the pool, explicitly chosen to be at the end of
        // the IP address space.
        let _member = datastore
            .add_subnet_pool_member(
                opctx,
                &authz_pool,
                &db_pool,
                &SubnetPoolMemberAdd {
                    subnet: "255.255.255.0/24".parse().unwrap(),
                    min_prefix_length: Some(24),
                    max_prefix_length: Some(30),
                },
            )
            .await
            .expect("able to create subnet pool member");

        // Create a dummy project. This doesn't have all the details we normally
        // need, like default VPC or VPC Subnets, but it has enough to test the
        // below methods.
        let (authz_project, _db_project) = db
            .datastore()
            .project_create(
                db.opctx(),
                Project::new(
                    DEFAULT_SILO_ID,
                    ProjectCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "my-project".parse().unwrap(),
                            description: String::new(),
                        },
                    },
                ),
            )
            .await
            .expect("able to create a project");

        // Now, the real test.
        //
        // Create one external subnet that takes the upper half of the member.
        // This allocates up to 255.255.255.255 for the last address in the
        // subnet.
        let _subnet = datastore
            .create_external_subnet(
                opctx,
                &DEFAULT_SILO_ID,
                &authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "first".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Explicit {
                        subnet: "255.255.255.128/26".parse().unwrap(),
                    },
                },
            )
            .await
            .expect("able to allocate first external subnet");

        // Now what happens when we do any other allocation?
        let subnet = datastore
            .create_external_subnet(
                opctx,
                &DEFAULT_SILO_ID,
                &authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "next".parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        prefix_len: 26,
                        pool_selector: PoolSelector::Auto { ip_version: None },
                    },
                },
            )
            .await
            .expect("able to allocate external subnet");
        assert_eq!(
            oxnet::IpNet::from(subnet.subnet),
            "255.255.255.0/26".parse::<oxnet::IpNet>().unwrap()
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_insert_subnet_pool_member_which_overlaps_ip_pool_range() {
        let logctx = dev::test_setup_log(
            "cannot_insert_subnet_pool_member_which_overlaps_ip_pool_range",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // First create an IP Pool and range.
        let identity = IdentityMetadataCreateParams {
            name: "test-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V6.into(),
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create IP pool");
        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            pool.id(),
            LookupType::ById(pool.id()),
        );
        let range = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
            )
            .unwrap(),
        );
        let _range = datastore
            .ip_pool_add_range(&opctx, &authz_pool, &pool, &range)
            .await
            .expect("should insert IP Pool range");

        // Next create a Subnet Pool and try to add an overlapping range.
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V6,
        };
        let db_pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            db_pool.identity.id.into(),
            LookupType::ById(db_pool.identity.id.into_untyped_uuid()),
        );

        let err = datastore
            .add_subnet_pool_member(
                opctx,
                &authz_pool,
                &db_pool,
                &SubnetPoolMemberAdd {
                    subnet: "fd00::/48".parse().unwrap(),
                    min_prefix_length: None,
                    max_prefix_length: None,
                },
            )
            .await
            .expect_err("should not be able to create subnet pool member");
        let Error::InvalidRequest { message } = &err else {
            panic!("Expected InvalidRequest, found {err:#?}");
        };
        assert_eq!(
            message.external_message(),
            "The IP subnet fd00::/48 overlaps with an existing Subnet Pool \
            member or IP Pool range",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn updating_nonexistent_pool_silo_link_fails() {
        let logctx =
            dev::test_setup_log("updating_nonexistent_pool_silo_link_fails");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, don't link it.
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let db_pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            db_pool.identity.id.into(),
            LookupType::ById(db_pool.identity.id.into_untyped_uuid()),
        );

        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );

        // Try to update it.
        let err = datastore
            .update_subnet_pool_silo_link(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect_err("should fail to update link that doesn't exist");
        let Error::ObjectNotFound { type_name, lookup_type } = &err else {
            panic!("Expected ObjectNotFound, found: {err:#?}");
        };
        assert!(matches!(type_name, ResourceType::SubnetPoolSiloLink));
        let LookupType::ByCompositeId(id) = &lookup_type else {
            panic!("Expected composite id lookup, found {lookup_type:#?}");
        };
        assert!(id.contains(&authz_silo.id().to_string()));
        assert!(id.contains(&authz_pool.id().to_string()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn deleting_nonexistent_pool_silo_link_fails() {
        let logctx =
            dev::test_setup_log("deleting_nonexistent_pool_silo_link_fails");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, don't link it.
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let db_pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            db_pool.identity.id.into(),
            LookupType::ById(db_pool.identity.id.into_untyped_uuid()),
        );

        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );

        // Try to unlink it.
        let err = datastore
            .unlink_subnet_pool_from_silo(
                opctx,
                &authz_pool,
                &db_pool,
                &authz_silo,
            )
            .await
            .expect_err("should fail to delete link that doesn't exist");
        let Error::ObjectNotFound { type_name, lookup_type } = &err else {
            panic!("Expected ObjectNotFound, found: {err:#?}");
        };
        assert!(matches!(type_name, ResourceType::SubnetPoolSiloLink));
        let LookupType::ByCompositeId(id) = &lookup_type else {
            panic!("Expected composite id lookup, found {lookup_type:#?}");
        };
        assert!(id.contains(&authz_silo.id().to_string()));
        assert!(id.contains(&authz_pool.id().to_string()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn linking_subnet_pool_and_silo_multiple_times_fails() {
        let logctx = dev::test_setup_log(
            "linking_subnet_pool_and_silo_multiple_times_fails",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a pool, link it once.
        let params = SubnetPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-pool".parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
        };
        let db_pool = datastore
            .create_subnet_pool(opctx, params)
            .await
            .expect("able to create external subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            db_pool.identity.id.into(),
            LookupType::ById(db_pool.identity.id.into_untyped_uuid()),
        );

        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );
        let _link = datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect("Should succeed linking the first time");

        // Linking it again should fail.
        let err = datastore
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect_err("Should fail linking the second time");
        let Error::Conflict { .. } = &err else {
            panic!("Expected Conflict, found {err:#?}");
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
