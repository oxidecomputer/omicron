// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Affinity Groups

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::column_walker::AllColumnsOf;
use crate::db::datastore::InstanceStateComputer;
use crate::db::datastore::OpContext;
use crate::db::error::ErrorHandler;
use crate::db::error::public_error_from_diesel;
use crate::db::identity::Resource;
use crate::db::model::AffinityGroup;
use crate::db::model::AffinityGroupInstanceMembership;
use crate::db::model::AffinityGroupUpdate;
use crate::db::model::AntiAffinityGroup;
use crate::db::model::AntiAffinityGroupInstanceMembership;
use crate::db::model::AntiAffinityGroupUpdate;
use crate::db::model::InstanceState;
use crate::db::model::InstanceStateEnum;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::VmmState;
use crate::db::model::VmmStateEnum;
use crate::db::pagination::RawPaginator;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::helper_types::AsSelect;
use diesel::pg::Pg;
use diesel::prelude::*;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::AffinityGroupUuid;
use omicron_uuid_kinds::AntiAffinityGroupUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use uuid::Uuid;

impl DataStore {
    pub async fn affinity_group_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AffinityGroup> {
        use db::schema::affinity_group::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        let mut paginator = RawPaginator::new();
        paginator
            .source()
            .sql("SELECT ")
            .sql(AllColumnsOf::<dsl::affinity_group>::as_str())
            .sql(" FROM affinity_group WHERE project_id = ")
            .param()
            .bind::<diesel::sql_types::Uuid, _>(authz_project.id())
            .sql(" AND time_deleted IS NULL");
        paginator
            .paginate_by_id_or_name(pagparams)
            .query::<AsSelect<AffinityGroup, Pg>>()
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn anti_affinity_group_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AntiAffinityGroup> {
        use db::schema::anti_affinity_group::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        let mut paginator = RawPaginator::new();
        paginator
            .source()
            .sql("SELECT ")
            .sql(AllColumnsOf::<dsl::anti_affinity_group>::as_str())
            .sql(" FROM anti_affinity_group WHERE project_id = ")
            .param()
            .bind::<diesel::sql_types::Uuid, _>(authz_project.id())
            .sql(" AND time_deleted IS NULL");
        paginator
            .paginate_by_id_or_name(pagparams)
            .query::<AsSelect<AntiAffinityGroup, Pg>>()
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn affinity_group_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        group: AffinityGroup,
    ) -> CreateResult<AffinityGroup> {
        use db::schema::affinity_group::dsl;

        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let name = group.name().as_str().to_string();

        let affinity_group: AffinityGroup = Project::insert_resource(
            authz_project.id(),
            diesel::insert_into(dsl::affinity_group).values(group),
        )
        .insert_and_get_result_async(&conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_project.not_found(),
            AsyncInsertError::DatabaseError(diesel_error) => {
                public_error_from_diesel(
                    diesel_error,
                    ErrorHandler::Conflict(ResourceType::AffinityGroup, &name),
                )
            }
        })?;
        Ok(affinity_group)
    }

    pub async fn anti_affinity_group_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        group: AntiAffinityGroup,
    ) -> CreateResult<AntiAffinityGroup> {
        use db::schema::anti_affinity_group::dsl;

        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let name = group.name().as_str().to_string();

        let anti_affinity_group: AntiAffinityGroup = Project::insert_resource(
            authz_project.id(),
            diesel::insert_into(dsl::anti_affinity_group).values(group),
        )
        .insert_and_get_result_async(&conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_project.not_found(),
            AsyncInsertError::DatabaseError(diesel_error) => {
                public_error_from_diesel(
                    diesel_error,
                    ErrorHandler::Conflict(
                        ResourceType::AntiAffinityGroup,
                        &name,
                    ),
                )
            }
        })?;
        Ok(anti_affinity_group)
    }

    pub async fn affinity_group_update(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        updates: AffinityGroupUpdate,
    ) -> UpdateResult<AffinityGroup> {
        opctx.authorize(authz::Action::Modify, authz_affinity_group).await?;

        use db::schema::affinity_group::dsl;
        diesel::update(dsl::affinity_group)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_affinity_group.id()))
            .set(updates)
            .returning(AffinityGroup::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_affinity_group),
                )
            })
    }

    pub async fn affinity_group_delete(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_affinity_group).await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("affinity_group_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    use db::schema::affinity_group::dsl as group_dsl;
                    let now = Utc::now();

                    // Delete the Affinity Group
                    diesel::update(group_dsl::affinity_group)
                        .filter(group_dsl::time_deleted.is_null())
                        .filter(group_dsl::id.eq(authz_affinity_group.id()))
                        .set(group_dsl::time_deleted.eq(now))
                        .returning(AffinityGroup::as_returning())
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_affinity_group,
                                    ),
                                )
                            })
                        })?;

                    // Ensure all memberships in the affinity group are deleted
                    use db::schema::affinity_group_instance_membership::dsl as member_dsl;
                    diesel::delete(member_dsl::affinity_group_instance_membership)
                        .filter(member_dsl::group_id.eq(authz_affinity_group.id()))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(e, ErrorHandler::Server)
                            })
                        })?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn anti_affinity_group_update(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        updates: AntiAffinityGroupUpdate,
    ) -> UpdateResult<AntiAffinityGroup> {
        opctx
            .authorize(authz::Action::Modify, authz_anti_affinity_group)
            .await?;

        use db::schema::anti_affinity_group::dsl;
        diesel::update(dsl::anti_affinity_group)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_anti_affinity_group.id()))
            .set(updates)
            .returning(AntiAffinityGroup::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_anti_affinity_group),
                )
            })
    }

    pub async fn anti_affinity_group_delete(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
    ) -> DeleteResult {
        opctx
            .authorize(authz::Action::Delete, authz_anti_affinity_group)
            .await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("anti_affinity_group_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    use db::schema::anti_affinity_group::dsl as group_dsl;
                    let now = Utc::now();

                    // Delete the Anti Affinity Group
                    diesel::update(group_dsl::anti_affinity_group)
                        .filter(group_dsl::time_deleted.is_null())
                        .filter(group_dsl::id.eq(authz_anti_affinity_group.id()))
                        .set(group_dsl::time_deleted.eq(now))
                        .returning(AntiAffinityGroup::as_returning())
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_anti_affinity_group,
                                    ),
                                )
                            })
                        })?;

                    // Ensure all memberships in the anti affinity group are deleted
                    use db::schema::anti_affinity_group_instance_membership::dsl as member_dsl;
                    diesel::delete(member_dsl::anti_affinity_group_instance_membership)
                        .filter(member_dsl::group_id.eq(authz_anti_affinity_group.id()))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(e, ErrorHandler::Server)
                            })
                        })?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn affinity_group_member_list(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<external::AffinityGroupMember> {
        opctx.authorize(authz::Action::Read, authz_affinity_group).await?;

        let mut paginator = RawPaginator::new();
        paginator
            .source()
            .sql(
                "
                SELECT
                    instance.id as id,
                    instance.name as name,
                    instance.state,
                    instance.migration_id,
                    vmm.state
                FROM affinity_group_instance_membership
                INNER JOIN instance
                ON instance.id = affinity_group_instance_membership.instance_id
                LEFT JOIN vmm
                ON instance.active_propolis_id = vmm.id
                WHERE
                    instance.time_deleted IS NULL AND
                    vmm.time_deleted IS NULL AND
                    group_id = ",
            )
            .param()
            .bind::<diesel::sql_types::Uuid, _>(authz_affinity_group.id());
        paginator.paginate_by_id_or_name(pagparams)
            .query::<(
                diesel::sql_types::Uuid,
                diesel::sql_types::Text,
                InstanceStateEnum,
                diesel::sql_types::Nullable<diesel::sql_types::Uuid>,
                diesel::sql_types::Nullable<VmmStateEnum>,
            )>()
            .load_async::<(Uuid, Name, InstanceState, Option<Uuid>, Option<VmmState>)>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .map(|(id, name, instance_state, migration_id, vmm_state)| {
                Ok(external::AffinityGroupMember::Instance {
                    id: InstanceUuid::from_untyped_uuid(id),
                    name: name.into(),
                    run_state: InstanceStateComputer::compute_state_from(
                        &instance_state,
                        migration_id.as_ref(),
                        vmm_state.as_ref(),
                    ),
                })
            })
            .collect()
    }

    pub async fn anti_affinity_group_member_list(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<external::AntiAffinityGroupMember> {
        opctx.authorize(authz::Action::Read, authz_anti_affinity_group).await?;

        let mut paginator = RawPaginator::new();
        paginator.source()
            .sql("
                SELECT
                    instance.id as id,
                    instance.name as name,
                    instance.state as instance_state,
                    instance.migration_id as migration_id,
                    vmm.state as vmm_state
                FROM anti_affinity_group_instance_membership
                INNER JOIN instance
                ON instance.id = anti_affinity_group_instance_membership.instance_id
                LEFT JOIN vmm
                ON instance.active_propolis_id = vmm.id
                WHERE
                    instance.time_deleted IS NULL AND
                    vmm.time_deleted IS NULL AND
                    group_id = ",
            )
            .param()
            .bind::<diesel::sql_types::Uuid, _>(authz_anti_affinity_group.id());
        paginator.paginate_by_id_or_name(pagparams)
            .query::<(
                diesel::sql_types::Uuid,
                diesel::sql_types::Text,
                diesel::sql_types::Nullable<InstanceStateEnum>,
                diesel::sql_types::Nullable<diesel::sql_types::Uuid>,
                diesel::sql_types::Nullable<VmmStateEnum>,
            )>()
            .load_async::<(
                Uuid,
                Name,
                Option<InstanceState>,
                Option<Uuid>,
                Option<VmmState>,
            )>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .map(|(id, name, instance_state, migration_id, vmm_state)| {
                let Some(instance_state) = instance_state else {
                    return Err(external::Error::internal_error(
                        "Anti-Affinity instance member missing state in database"
                    ));
                };
                Ok(external::AntiAffinityGroupMember::Instance {
                    id: InstanceUuid::from_untyped_uuid(id),
                    name: name.into(),
                    run_state: InstanceStateComputer::compute_state_from(
                        &instance_state,
                        migration_id.as_ref(),
                        vmm_state.as_ref(),
                    ),
                })
            })
            .collect()
    }

    pub async fn affinity_group_member_instance_view(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        instance_id: InstanceUuid,
    ) -> Result<external::AffinityGroupMember, Error> {
        opctx.authorize(authz::Action::Read, authz_affinity_group).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use db::schema::affinity_group_instance_membership::dsl;
        use db::schema::instance::dsl as instance_dsl;
        use db::schema::vmm::dsl as vmm_dsl;
        dsl::affinity_group_instance_membership
            .filter(dsl::group_id.eq(authz_affinity_group.id()))
            .filter(dsl::instance_id.eq(instance_id.into_untyped_uuid()))
            .inner_join(
                instance_dsl::instance
                    .on(instance_dsl::id.eq(dsl::instance_id)),
            )
            .filter(instance_dsl::time_deleted.is_null())
            .left_join(vmm_dsl::vmm.on(
                instance_dsl::active_propolis_id.eq(vmm_dsl::id.nullable()),
            ))
            .filter(vmm_dsl::time_deleted.is_null())
            .select((
                AffinityGroupInstanceMembership::as_select(),
                instance_dsl::name,
                instance_dsl::state,
                instance_dsl::migration_id,
                vmm_dsl::state.nullable(),
            ))
            .get_result_async::<(
                AffinityGroupInstanceMembership,
                Name,
                InstanceState,
                Option<Uuid>,
                Option<VmmState>,
            )>(&*conn)
            .await
            .map(|(member, name, instance_state, migration_id, vmm_state)| {
                let run_state = InstanceStateComputer::compute_state_from(
                    &instance_state,
                    migration_id.as_ref(),
                    vmm_state.as_ref(),
                );
                member.to_external(name.into(), run_state)
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::AffinityGroupMember,
                        LookupType::by_id(instance_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    pub async fn anti_affinity_group_member_instance_view(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        instance_id: InstanceUuid,
    ) -> Result<external::AntiAffinityGroupMember, Error> {
        opctx.authorize(authz::Action::Read, authz_anti_affinity_group).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use db::schema::anti_affinity_group_instance_membership::dsl;
        use db::schema::instance::dsl as instance_dsl;
        use db::schema::vmm::dsl as vmm_dsl;
        dsl::anti_affinity_group_instance_membership
            .filter(dsl::group_id.eq(authz_anti_affinity_group.id()))
            .filter(dsl::instance_id.eq(instance_id.into_untyped_uuid()))
            .inner_join(
                instance_dsl::instance
                    .on(instance_dsl::id.eq(dsl::instance_id)),
            )
            .filter(instance_dsl::time_deleted.is_null())
            .left_join(vmm_dsl::vmm.on(
                instance_dsl::active_propolis_id.eq(vmm_dsl::id.nullable()),
            ))
            .filter(vmm_dsl::time_deleted.is_null())
            .select((
                AntiAffinityGroupInstanceMembership::as_select(),
                instance_dsl::name,
                instance_dsl::state,
                instance_dsl::migration_id,
                vmm_dsl::state.nullable(),
            ))
            .get_result_async::<(
                AntiAffinityGroupInstanceMembership,
                Name,
                InstanceState,
                Option<Uuid>,
                Option<VmmState>,
            )>(&*conn)
            .await
            .map(|(member, name, instance_state, migration_id, vmm_state)| {
                let run_state = InstanceStateComputer::compute_state_from(
                    &instance_state,
                    migration_id.as_ref(),
                    vmm_state.as_ref(),
                );
                member.to_external(name.into(), run_state)
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::AntiAffinityGroupMember,
                        LookupType::by_id(instance_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    pub async fn affinity_group_member_instance_add(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_affinity_group).await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("affinity_group_member_instance_add")
            .transaction(&conn, |conn| {
                let err = err.clone();
                use db::schema::affinity_group::dsl as group_dsl;
                use db::schema::affinity_group_instance_membership::dsl as membership_dsl;
                use db::schema::instance::dsl as instance_dsl;
                use db::schema::sled_resource_vmm::dsl as resource_dsl;

                async move {
                    // Check that the group exists
                    group_dsl::affinity_group
                        .filter(group_dsl::time_deleted.is_null())
                        .filter(group_dsl::id.eq(authz_affinity_group.id()))
                        .select(group_dsl::id)
                        .first_async::<uuid::Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_affinity_group,
                                    ),
                                )
                            })
                        })?;

                    // Check that the instance exists, and has no sled
                    // reservation.
                    //
                    // NOTE: I'd prefer to use the "LookupPath" infrastructure
                    // to look up the path, but that API does not give the
                    // option to use the transaction's database connection.
                    //
                    // Looking up the instance on a different database
                    // connection than the transaction risks several concurrency
                    // issues, so we do the lookup manually.

                    let _check_instance_exists = instance_dsl::instance
                        .filter(instance_dsl::time_deleted.is_null())
                        .filter(instance_dsl::id.eq(instance_id.into_untyped_uuid()))
                        .select(instance_dsl::id)
                        .get_result_async::<uuid::Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByLookup(
                                        ResourceType::Instance,
                                        LookupType::ById(instance_id.into_untyped_uuid())
                                    ),
                                )
                            })
                        })?;
                    let has_reservation: bool = diesel::select(
                            diesel::dsl::exists(
                                resource_dsl::sled_resource_vmm
                                    .filter(resource_dsl::instance_id.eq(instance_id.into_untyped_uuid()))
                            )
                        ).get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                            })
                        })?;

                    // NOTE: It may be possible to add non-stopped instances to
                    // affinity groups, depending on where they have already
                    // been placed. However, only operating on "stopped"
                    // instances is much easier to work with, as it does not
                    // require any understanding of the group policy.
                    if has_reservation {
                        return Err(err.bail(Error::invalid_request(
                            "Instance cannot be added to affinity group with reservation".to_string()
                        )));
                    }

                    diesel::insert_into(membership_dsl::affinity_group_instance_membership)
                        .values(AffinityGroupInstanceMembership::new(
                            AffinityGroupUuid::from_untyped_uuid(authz_affinity_group.id()),
                            instance_id,
                        ))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Conflict(
                                        ResourceType::AffinityGroupMember,
                                        &instance_id.to_string(),
                                    ),
                                )
                            })
                        })?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn anti_affinity_group_member_instance_add(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        opctx
            .authorize(authz::Action::Modify, authz_anti_affinity_group)
            .await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("anti_affinity_group_member_instance_add")
            .transaction(&conn, |conn| {
                let err = err.clone();
                use db::schema::anti_affinity_group::dsl as group_dsl;
                use db::schema::anti_affinity_group_instance_membership::dsl as membership_dsl;
                use db::schema::instance::dsl as instance_dsl;
                use db::schema::sled_resource_vmm::dsl as resource_dsl;

                async move {
                    // Check that the group exists
                    group_dsl::anti_affinity_group
                        .filter(group_dsl::time_deleted.is_null())
                        .filter(group_dsl::id.eq(authz_anti_affinity_group.id()))
                        .select(group_dsl::id)
                        .first_async::<uuid::Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_anti_affinity_group,
                                    ),
                                )
                            })
                        })?;

                    // Check that the instance exists, and has no sled
                    // reservation.
                    let _check_instance_exists = instance_dsl::instance
                        .filter(instance_dsl::time_deleted.is_null())
                        .filter(instance_dsl::id.eq(instance_id.into_untyped_uuid()))
                        .select(instance_dsl::id)
                        .get_result_async::<uuid::Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByLookup(
                                        ResourceType::Instance,
                                        LookupType::ById(instance_id.into_untyped_uuid())
                                    ),
                                )
                            })
                        })?;
                    let has_reservation: bool = diesel::select(
                            diesel::dsl::exists(
                                resource_dsl::sled_resource_vmm
                                    .filter(resource_dsl::instance_id.eq(instance_id.into_untyped_uuid()))
                            )
                        ).get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                            })
                        })?;

                    // NOTE: It may be possible to add non-stopped instances to
                    // anti-affinity groups, depending on where they have already
                    // been placed. However, only operating on "stopped"
                    // instances is much easier to work with, as it does not
                    // require any understanding of the group policy.
                    if has_reservation {
                        return Err(err.bail(Error::invalid_request(
                            "Instance cannot be added to anti-affinity group with reservation".to_string()
                        )));
                    }

                    diesel::insert_into(membership_dsl::anti_affinity_group_instance_membership)
                        .values(AntiAffinityGroupInstanceMembership::new(
                            AntiAffinityGroupUuid::from_untyped_uuid(authz_anti_affinity_group.id()),
                            instance_id,
                        ))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Conflict(
                                        ResourceType::AntiAffinityGroupMember,
                                        &instance_id.to_string(),
                                    ),
                                )
                            })
                        })?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn instance_affinity_group_memberships_delete(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        use db::schema::affinity_group_instance_membership::dsl;

        diesel::delete(dsl::affinity_group_instance_membership)
            .filter(dsl::instance_id.eq(instance_id.into_untyped_uuid()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    pub async fn instance_anti_affinity_group_memberships_delete(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        use db::schema::anti_affinity_group_instance_membership::dsl;

        diesel::delete(dsl::anti_affinity_group_instance_membership)
            .filter(dsl::instance_id.eq(instance_id.into_untyped_uuid()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    pub async fn affinity_group_member_instance_delete(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_affinity_group).await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("affinity_group_member_instance_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                use db::schema::affinity_group::dsl as group_dsl;
                use db::schema::affinity_group_instance_membership::dsl as membership_dsl;

                async move {
                    // Check that the group exists
                    group_dsl::affinity_group
                        .filter(group_dsl::time_deleted.is_null())
                        .filter(group_dsl::id.eq(authz_affinity_group.id()))
                        .select(group_dsl::id)
                        .first_async::<uuid::Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_affinity_group,
                                    ),
                                )
                            })
                        })?;

                    let rows = diesel::delete(membership_dsl::affinity_group_instance_membership)
                        .filter(membership_dsl::group_id.eq(authz_affinity_group.id()))
                        .filter(membership_dsl::instance_id.eq(instance_id.into_untyped_uuid()))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(e, ErrorHandler::Server)
                            })
                        })?;
                    if rows == 0 {
                        return Err(err.bail(LookupType::ById(instance_id.into_untyped_uuid()).into_not_found(
                            ResourceType::AffinityGroupMember,
                        )));
                    }
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    /// Deletes an anti-affinity member, when that member is an instance
    pub async fn anti_affinity_group_member_instance_delete(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        opctx
            .authorize(authz::Action::Modify, authz_anti_affinity_group)
            .await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("anti_affinity_group_member_instance_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                use db::schema::anti_affinity_group::dsl as group_dsl;
                use db::schema::anti_affinity_group_instance_membership::dsl as membership_dsl;

                async move {
                    // Check that the group exists
                    group_dsl::anti_affinity_group
                        .filter(group_dsl::time_deleted.is_null())
                        .filter(group_dsl::id.eq(authz_anti_affinity_group.id()))
                        .select(group_dsl::id)
                        .first_async::<uuid::Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_anti_affinity_group,
                                    ),
                                )
                            })
                        })?;

                    let rows = diesel::delete(membership_dsl::anti_affinity_group_instance_membership)
                        .filter(membership_dsl::group_id.eq(authz_anti_affinity_group.id()))
                        .filter(membership_dsl::instance_id.eq(instance_id.into_untyped_uuid()))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(e, ErrorHandler::Server)
                            })
                        })?;
                    if rows == 0 {
                        return Err(err.bail(LookupType::ById(instance_id.into_untyped_uuid()).into_not_found(
                            ResourceType::AntiAffinityGroupMember,
                        )));
                    }
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::lookup::LookupPath;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::create_project;
    use crate::db::pub_test_utils::helpers::create_stopped_instance_record;
    use nexus_db_model::Resources;
    use nexus_db_model::SledResourceVmm;
    use nexus_types::external_api::params;
    use omicron_common::api::external::{
        self, ByteCount, DataPageParams, IdentityMetadataCreateParams,
        SimpleIdentityOrName,
    };
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
    use omicron_uuid_kinds::PropolisUuid;
    use omicron_uuid_kinds::SledUuid;
    use std::num::NonZeroU32;

    // Helper function for creating an affinity group with
    // arbitrary configuration.
    async fn create_affinity_group(
        opctx: &OpContext,
        datastore: &DataStore,
        authz_project: &authz::Project,
        name: &str,
    ) -> CreateResult<AffinityGroup> {
        let group = AffinityGroup::new(
            authz_project.id(),
            params::AffinityGroupCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: "".to_string(),
                },
                policy: external::AffinityPolicy::Fail,
                failure_domain: external::FailureDomain::Sled,
            },
        );
        datastore.affinity_group_create(&opctx, &authz_project, group).await
    }

    // Helper function for creating an anti-affinity group with
    // arbitrary configuration.
    async fn create_anti_affinity_group(
        opctx: &OpContext,
        datastore: &DataStore,
        authz_project: &authz::Project,
        name: &str,
    ) -> CreateResult<AntiAffinityGroup> {
        let group = AntiAffinityGroup::new(
            authz_project.id(),
            params::AntiAffinityGroupCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: "".to_string(),
                },
                policy: external::AffinityPolicy::Fail,
                failure_domain: external::FailureDomain::Sled,
            },
        );
        datastore
            .anti_affinity_group_create(&opctx, &authz_project, group)
            .await
    }

    // Helper for explicitly modifying sled resource usage
    //
    // The interaction we typically use to create and modify instance state
    // is more complex in production, using a real allocation algorithm.
    //
    // Here, we just set the value of state explicitly. Be warned, there
    // are no guardrails!
    async fn allocate_instance_reservation(
        datastore: &DataStore,
        instance: InstanceUuid,
    ) {
        use db::schema::sled_resource_vmm::dsl;
        diesel::insert_into(dsl::sled_resource_vmm)
            .values(SledResourceVmm::new(
                PropolisUuid::new_v4(),
                instance,
                SledUuid::new_v4(),
                Resources::new(
                    1,
                    ByteCount::from_kibibytes_u32(1).into(),
                    ByteCount::from_kibibytes_u32(1).into(),
                ),
            ))
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
    }

    async fn delete_instance_reservation(
        datastore: &DataStore,
        instance: InstanceUuid,
    ) {
        use db::schema::sled_resource_vmm::dsl;
        diesel::delete(dsl::sled_resource_vmm)
            .filter(dsl::instance_id.eq(instance.into_untyped_uuid()))
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn affinity_groups_are_project_scoped() {
        // Setup
        let logctx = dev::test_setup_log("affinity_groups_are_project_scoped");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, "my-project").await;

        let (authz_other_project, _) =
            create_project(&opctx, &datastore, "my-other-project").await;

        let pagparams_id = DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let pagbyid = PaginatedBy::Id(pagparams_id);

        // To start: No groups exist
        let groups = datastore
            .affinity_group_list(&opctx, &authz_project, &pagbyid)
            .await
            .unwrap();
        assert!(groups.is_empty());

        // Create a group
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        // Now when we list groups, we'll see the one we created.
        let groups = datastore
            .affinity_group_list(&opctx, &authz_project, &pagbyid)
            .await
            .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], group);

        // This group won't appear in the other project
        let groups = datastore
            .affinity_group_list(&opctx, &authz_other_project, &pagbyid)
            .await
            .unwrap();
        assert!(groups.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_groups_are_project_scoped() {
        // Setup
        let logctx =
            dev::test_setup_log("anti_affinity_groups_are_project_scoped");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, "my-project").await;

        let (authz_other_project, _) =
            create_project(&opctx, &datastore, "my-other-project").await;

        let pagparams_id = DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let pagbyid = PaginatedBy::Id(pagparams_id);

        // To start: No groups exist
        let groups = datastore
            .anti_affinity_group_list(&opctx, &authz_project, &pagbyid)
            .await
            .unwrap();
        assert!(groups.is_empty());

        // Create a group
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        // Now when we list groups, we'll see the one we created.
        let groups = datastore
            .anti_affinity_group_list(&opctx, &authz_project, &pagbyid)
            .await
            .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], group);

        // This group won't appear in the other project
        let groups = datastore
            .anti_affinity_group_list(&opctx, &authz_other_project, &pagbyid)
            .await
            .unwrap();
        assert!(groups.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_groups_prevent_project_deletion() {
        // Setup
        let logctx =
            dev::test_setup_log("affinity_groups_prevent_project_deletion");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, mut project) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        // If we try to delete the project, we'll fail.
        let err = datastore
            .project_delete(&opctx, &authz_project, &project)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidRequest { .. }));
        assert!(
            err.to_string()
                .contains("project to be deleted contains an affinity group"),
            "{err:?}"
        );

        // Delete the group, then try to delete the project again.
        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Delete)
            .await
            .unwrap();
        datastore.affinity_group_delete(&opctx, &authz_group).await.unwrap();

        // When the group was created, it bumped the rcgen in the project. If we
        // have an old view of the project, we expect a "concurrent
        // modification" error.
        let err = datastore
            .project_delete(&opctx, &authz_project, &project)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("concurrent modification"), "{err:?}");

        // If we update rcgen, however, and the group has been deleted,
        // we can successfully delete the project.
        project.rcgen = project.rcgen.next().into();
        datastore
            .project_delete(&opctx, &authz_project, &project)
            .await
            .unwrap();

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_groups_prevent_project_deletion() {
        // Setup
        let logctx = dev::test_setup_log(
            "anti_affinity_groups_prevent_project_deletion",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, mut project) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        // If we try to delete the project, we'll fail.
        let err = datastore
            .project_delete(&opctx, &authz_project, &project)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidRequest { .. }));
        assert!(
            err.to_string().contains(
                "project to be deleted contains an anti affinity group"
            ),
            "{err:?}"
        );

        // Delete the group, then try to delete the project again.
        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Delete)
            .await
            .unwrap();
        datastore
            .anti_affinity_group_delete(&opctx, &authz_group)
            .await
            .unwrap();

        // When the group was created, it bumped the rcgen in the project. If we
        // have an old view of the project, we expect a "concurrent
        // modification" error.
        let err = datastore
            .project_delete(&opctx, &authz_project, &project)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("concurrent modification"), "{err:?}");

        // If we update rcgen, however, and the group has been deleted,
        // we can successfully delete the project.
        project.rcgen = project.rcgen.next().into();
        datastore
            .project_delete(&opctx, &authz_project, &project)
            .await
            .unwrap();

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_names_are_unique_in_project() {
        // Setup
        let logctx =
            dev::test_setup_log("affinity_group_names_are_unique_in_project");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create two projects
        let (authz_project1, _) =
            create_project(&opctx, &datastore, "my-project-1").await;
        let (authz_project2, _) =
            create_project(&opctx, &datastore, "my-project-2").await;

        // We can create a group wiht the same name in different projects
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project1,
            "my-group",
        )
        .await
        .unwrap();
        create_affinity_group(&opctx, &datastore, &authz_project2, "my-group")
            .await
            .unwrap();

        // If we try to create a new group with the same name in the same
        // project, we'll see an error.
        let err = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project1,
            "my-group",
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, Error::ObjectAlreadyExists {
            type_name,
            object_name,
        } if *type_name == ResourceType::AffinityGroup &&
         *object_name == "my-group"),
            "Unexpected error: {err:?}"
        );

        // If we delete the group from the project, we can re-use the name.
        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Delete)
            .await
            .unwrap();
        datastore.affinity_group_delete(&opctx, &authz_group).await.unwrap();

        create_affinity_group(&opctx, &datastore, &authz_project1, "my-group")
            .await
            .expect("Should have been able to re-use name after deletion");

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_group_names_are_unique_in_project() {
        // Setup
        let logctx = dev::test_setup_log(
            "anti_affinity_group_names_are_unique_in_project",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create two projects
        let (authz_project1, _) =
            create_project(&opctx, &datastore, "my-project-1").await;
        let (authz_project2, _) =
            create_project(&opctx, &datastore, "my-project-2").await;

        // We can create a group wiht the same name in different projects
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project1,
            "my-group",
        )
        .await
        .unwrap();
        create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project2,
            "my-group",
        )
        .await
        .unwrap();

        // If we try to create a new group with the same name in the same
        // project, we'll see an error.
        let err = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project1,
            "my-group",
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, Error::ObjectAlreadyExists {
            type_name,
            object_name,
        } if *type_name == ResourceType::AntiAffinityGroup &&
         *object_name == "my-group"),
            "Unexpected error: {err:?}"
        );

        // If we delete the group from the project, we can re-use the name.
        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Delete)
            .await
            .unwrap();
        datastore
            .anti_affinity_group_delete(&opctx, &authz_group)
            .await
            .unwrap();

        create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project1,
            "my-group",
        )
        .await
        .expect("Should have been able to re-use name after deletion");

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_membership_add_list_remove() {
        // Setup
        let logctx =
            dev::test_setup_log("affinity_group_membership_add_list_remove");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagparams_id = DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let pagbyid = PaginatedBy::Id(pagparams_id);
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance without a VMM.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;

        // Add the instance as a member to the group
        datastore
            .affinity_group_member_instance_add(&opctx, &authz_group, instance)
            .await
            .unwrap();

        // We should now be able to list the new member
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap()
            .into_iter()
            .map(|m| InstanceUuid::from_untyped_uuid(m.id()))
            .collect::<Vec<_>>();
        assert_eq!(members.len(), 1);
        assert_eq!(instance, members[0]);

        // We can delete the member and observe an empty member list
        datastore
            .affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_group_membership_add_list_remove() {
        // Setup
        let logctx = dev::test_setup_log(
            "anti_affinity_group_membership_add_list_remove",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance without a VMM.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;

        // Add the instance as a member to the group
        datastore
            .anti_affinity_group_member_instance_add(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();

        // We should now be able to list the new member
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(members.len(), 1);
        assert!(matches!(
            members[0],
            external::AntiAffinityGroupMember::Instance {
                id,
                ..
            } if id == instance,
        ));

        // We can delete the member and observe an empty member list
        datastore
            .anti_affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_membership_list_extended() {
        // Setup
        let logctx =
            dev::test_setup_log("affinity_group_membership_list_extended");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Add some instances, so we have data to list over.

        const INSTANCE_COUNT: usize = 6;

        let mut members = Vec::new();
        for i in 0..INSTANCE_COUNT {
            let name = format!("instance-{i}");
            let instance = create_stopped_instance_record(
                &opctx,
                &datastore,
                &authz_project,
                &name,
            )
            .await;

            // Add the instance as a member to the group
            let member = external::AffinityGroupMember::Instance {
                id: instance,
                name: name.try_into().unwrap(),
                run_state: external::InstanceState::Stopped,
            };
            datastore
                .affinity_group_member_instance_add(
                    &opctx,
                    &authz_group,
                    instance,
                )
                .await
                .unwrap();
            members.push(member);
        }

        // Order by UUID
        members.sort_unstable_by_key(|m1| m1.id());

        // We can list all members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let observed_members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(observed_members, members);

        // We can paginate over the results
        let marker = members[2].id();
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: Some(&marker),
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });

        let observed_members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(observed_members, members[3..]);

        // We can list limited results
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: Some(&marker),
            limit: NonZeroU32::new(2).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let observed_members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(observed_members, members[3..5]);

        // We can list in descending order too
        members.reverse();
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        });
        let observed_members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(observed_members, members);

        // Order by name
        members.sort_unstable_by_key(|m1| m1.name().clone());

        // We can list all members
        let pagparams = PaginatedBy::Name(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let observed_members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(observed_members, members);
        let marker = members[2].name();
        let pagparams = PaginatedBy::Name(DataPageParams {
            marker: Some(marker),
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });

        let observed_members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(observed_members, members[3..]);

        // We can list in descending order too
        members.reverse();
        let pagparams = PaginatedBy::Name(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        });
        let observed_members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(observed_members, members);

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Anti-affinity group member listing has a slightly more complicated
    // implementation, because it queries multiple tables and JOINs them
    // together.
    //
    // This test exists to validate that manual implementation.
    #[tokio::test]
    async fn anti_affinity_group_membership_list_extended() {
        // Setup
        let logctx =
            dev::test_setup_log("anti_affinity_group_membership_list_extended");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_aa_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert!(members.is_empty());

        // Add some instances, so we have data to list over.

        const INSTANCE_COUNT: usize = 6;

        let mut members = Vec::new();

        for i in 0..INSTANCE_COUNT {
            let name = format!("instance-{i}");
            let instance = create_stopped_instance_record(
                &opctx,
                &datastore,
                &authz_project,
                &name,
            )
            .await;

            // Add the instance as a member to the group
            let member = external::AntiAffinityGroupMember::Instance {
                id: instance,
                name: name.try_into().unwrap(),
                run_state: external::InstanceState::Stopped,
            };
            datastore
                .anti_affinity_group_member_instance_add(
                    &opctx,
                    &authz_aa_group,
                    instance,
                )
                .await
                .unwrap();
            members.push(member);
        }

        // Order by UUID
        members.sort_unstable_by_key(|m1| m1.id());

        // We can list all members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let observed_members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(observed_members, members);

        // We can paginate over the results
        let marker = members[2].id();
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: Some(&marker),
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });

        let observed_members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(observed_members, members[3..]);

        // We can list limited results
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: Some(&marker),
            limit: NonZeroU32::new(2).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let observed_members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(observed_members, members[3..5]);

        // We can list in descending order too
        members.reverse();
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        });
        let observed_members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(observed_members, members);

        // Order by name
        members.sort_unstable_by_key(|m1| m1.name().clone());

        // We can list all members
        let pagparams = PaginatedBy::Name(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let observed_members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(observed_members, members);
        let marker = members[2].name();
        let pagparams = PaginatedBy::Name(DataPageParams {
            marker: Some(marker),
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });

        let observed_members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(observed_members, members[3..]);

        // We can list in descending order too
        members.reverse();
        let pagparams = PaginatedBy::Name(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        });
        let observed_members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(observed_members, members);

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_membership_add_remove_instance_with_vmm() {
        // Setup
        let logctx = dev::test_setup_log(
            "affinity_group_membership_add_remove_instance_with_vmm",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagbyid = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance with a VMM.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;

        allocate_instance_reservation(&datastore, instance).await;

        // Cannot add the instance to the group while it's running.
        let err = datastore
            .affinity_group_member_instance_add(&opctx, &authz_group, instance)
            .await
            .expect_err(
                "Shouldn't be able to add running instances to affinity groups",
            );
        assert!(matches!(err, Error::InvalidRequest { .. }));
        assert!(
            err.to_string().contains(
                "Instance cannot be added to affinity group with reservation"
            ),
            "{err:?}"
        );

        // If we have no reservation for the instance, we can add it to the group.
        delete_instance_reservation(&datastore, instance).await;
        datastore
            .affinity_group_member_instance_add(&opctx, &authz_group, instance)
            .await
            .unwrap();

        // Now we can reserve a sled for the instance once more.
        allocate_instance_reservation(&datastore, instance).await;

        // We should now be able to list the new member
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap()
            .into_iter()
            .map(|m| InstanceUuid::from_untyped_uuid(m.id()))
            .collect::<Vec<_>>();
        assert_eq!(members.len(), 1);
        assert_eq!(instance, members[0]);

        // We can delete the member and observe an empty member list -- even
        // though it's running!
        datastore
            .affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_group_membership_add_remove_instance_with_vmm() {
        // Setup
        let logctx = dev::test_setup_log(
            "anti_affinity_group_membership_add_remove_instance_with_vmm",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance with a VMM.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;
        allocate_instance_reservation(&datastore, instance).await;

        // Cannot add the instance to the group while it's running.
        let err = datastore
            .anti_affinity_group_member_instance_add(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .expect_err(
                "Shouldn't be able to add running instances to anti-affinity groups",
            );
        assert!(matches!(err, Error::InvalidRequest { .. }));
        assert!(
            err.to_string().contains(
                "Instance cannot be added to anti-affinity group with reservation"
            ),
            "{err:?}"
        );

        // If we have no reservation for the instance, we can add it to the group.
        delete_instance_reservation(&datastore, instance).await;
        datastore
            .anti_affinity_group_member_instance_add(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();

        // Now we can reserve a sled for the instance once more.
        allocate_instance_reservation(&datastore, instance).await;

        // We should now be able to list the new member
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(members.len(), 1);
        assert!(matches!(
            members[0],
            external::AntiAffinityGroupMember::Instance {
                id,
                ..
            } if id == instance,
        ));
        // We can delete the member and observe an empty member list -- even
        // though it's running!
        datastore
            .anti_affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_delete_group_deletes_members() {
        // Setup
        let logctx =
            dev::test_setup_log("affinity_group_delete_group_deletes_members");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagbyid = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance without a VMM, add it to the group.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;
        datastore
            .affinity_group_member_instance_add(&opctx, &authz_group, instance)
            .await
            .unwrap();

        // Delete the group
        datastore.affinity_group_delete(&opctx, &authz_group).await.unwrap();

        // Confirm that no instance members exist
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_group_delete_group_deletes_members() {
        // Setup
        let logctx = dev::test_setup_log(
            "anti_affinity_group_delete_group_deletes_members",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_aa_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance without a VMM, add it to the group.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;
        datastore
            .anti_affinity_group_member_instance_add(
                &opctx,
                &authz_aa_group,
                instance,
            )
            .await
            .unwrap();

        // Delete the group
        datastore
            .anti_affinity_group_delete(&opctx, &authz_aa_group)
            .await
            .unwrap();

        // Confirm that no group members exist
        let members = datastore
            .anti_affinity_group_member_list(
                &opctx,
                &authz_aa_group,
                &pagparams,
            )
            .await
            .unwrap();
        assert!(
            members.is_empty(),
            "No members should exist, but these do: {members:?}"
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_delete_instance_deletes_membership() {
        // Setup
        let logctx = dev::test_setup_log(
            "affinity_group_delete_instance_deletes_membership",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagbyid = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance without a VMM, add it to the group.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;
        datastore
            .affinity_group_member_instance_add(&opctx, &authz_group, instance)
            .await
            .unwrap();

        // Delete the instance
        let (.., authz_instance) = LookupPath::new(opctx, datastore)
            .instance_id(instance.into_untyped_uuid())
            .lookup_for(authz::Action::Delete)
            .await
            .unwrap();
        datastore
            .project_delete_instance(&opctx, &authz_instance)
            .await
            .unwrap();

        // Confirm that no instance members exist
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_group_delete_instance_deletes_membership() {
        // Setup
        let logctx = dev::test_setup_log(
            "anti_affinity_group_delete_instance_deletes_membership",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();

        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // A new group should have no members
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Create an instance without a VMM, add it to the group.
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;
        datastore
            .anti_affinity_group_member_instance_add(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();

        // Delete the instance
        let (.., authz_instance) = LookupPath::new(opctx, datastore)
            .instance_id(instance.into_untyped_uuid())
            .lookup_for(authz::Action::Delete)
            .await
            .unwrap();
        datastore
            .project_delete_instance(&opctx, &authz_instance)
            .await
            .unwrap();

        // Confirm that no instance members exist
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_membership_for_deleted_objects() {
        // Setup
        let logctx = dev::test_setup_log(
            "affinity_group_membership_for_deleted_objects",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;

        struct TestArgs {
            // Does the group exist?
            group: bool,
            // Does the instance exist?
            instance: bool,
        }

        let args = [
            TestArgs { group: false, instance: false },
            TestArgs { group: true, instance: false },
            TestArgs { group: false, instance: true },
        ];

        for arg in args {
            // Create an affinity group, and maybe delete it.
            let group = create_affinity_group(
                &opctx,
                &datastore,
                &authz_project,
                "my-group",
            )
            .await
            .unwrap();

            let (.., authz_group) = LookupPath::new(opctx, datastore)
                .affinity_group_id(group.id())
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();

            if !arg.group {
                datastore
                    .affinity_group_delete(&opctx, &authz_group)
                    .await
                    .unwrap();
            }

            // Create an instance, and maybe delete it.
            let instance = create_stopped_instance_record(
                &opctx,
                &datastore,
                &authz_project,
                "my-instance",
            )
            .await;
            let (.., authz_instance) = LookupPath::new(opctx, datastore)
                .instance_id(instance.into_untyped_uuid())
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();
            if !arg.instance {
                datastore
                    .project_delete_instance(&opctx, &authz_instance)
                    .await
                    .unwrap();
            }

            // Try to add the instance to the group.
            //
            // Expect to see specific errors, depending on whether or not the
            // group/instance exist.
            let err = datastore
                .affinity_group_member_instance_add(
                    &opctx,
                    &authz_group,
                    instance,
                )
                .await
                .expect_err("Should have failed");

            match (arg.group, arg.instance) {
                (false, _) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::AffinityGroup),
                        "{err:?}"
                    );
                }
                (true, false) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::Instance),
                        "{err:?}"
                    );
                }
                (true, true) => {
                    panic!("If both exist, we won't throw an error")
                }
            }

            // Do the same thing, but for group membership removal.
            let err = datastore
                .affinity_group_member_instance_delete(
                    &opctx,
                    &authz_group,
                    instance,
                )
                .await
                .expect_err("Should have failed");
            match (arg.group, arg.instance) {
                (false, _) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::AffinityGroup),
                        "{err:?}"
                    );
                }
                (true, false) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::AffinityGroupMember),
                        "{err:?}"
                    );
                }
                (true, true) => {
                    panic!("If both exist, we won't throw an error")
                }
            }

            // Cleanup, if we actually created anything.
            if arg.instance {
                datastore
                    .project_delete_instance(&opctx, &authz_instance)
                    .await
                    .unwrap();
            }
            if arg.group {
                datastore
                    .affinity_group_delete(&opctx, &authz_group)
                    .await
                    .unwrap();
            }
        }

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_group_membership_for_deleted_objects() {
        // Setup
        let logctx = dev::test_setup_log(
            "anti_affinity_group_membership_for_deleted_objects",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;

        struct TestArgs {
            // Does the group exist?
            group: bool,
            // Does the instance exist?
            instance: bool,
        }

        let args = [
            TestArgs { group: false, instance: false },
            TestArgs { group: true, instance: false },
            TestArgs { group: false, instance: true },
        ];

        for arg in args {
            // Create an anti-affinity group, and maybe delete it.
            let group = create_anti_affinity_group(
                &opctx,
                &datastore,
                &authz_project,
                "my-group",
            )
            .await
            .unwrap();

            let (.., authz_group) = LookupPath::new(opctx, datastore)
                .anti_affinity_group_id(group.id())
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();

            if !arg.group {
                datastore
                    .anti_affinity_group_delete(&opctx, &authz_group)
                    .await
                    .unwrap();
            }

            // Create an instance, and maybe deletes it
            let instance = create_stopped_instance_record(
                &opctx,
                &datastore,
                &authz_project,
                "my-instance",
            )
            .await;

            let (.., authz_instance) = LookupPath::new(opctx, datastore)
                .instance_id(instance.into_untyped_uuid())
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();

            if !arg.instance {
                datastore
                    .project_delete_instance(&opctx, &authz_instance)
                    .await
                    .unwrap();
            }

            // Try to add the instnace to the group.
            //
            // Expect to see specific errors, depending on whether or not the
            // group/instance exist.
            let err = datastore
                .anti_affinity_group_member_instance_add(
                    &opctx,
                    &authz_group,
                    instance,
                )
                .await
                .expect_err("Should have failed");

            match (arg.group, arg.instance) {
                (false, _) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::AntiAffinityGroup),
                        "{err:?}"
                    );
                }
                (true, false) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::Instance),
                        "{err:?}"
                    );
                }
                (true, true) => {
                    panic!("If both exist, we won't throw an error")
                }
            }

            // Do the same thing, but for group membership removal.
            let err = datastore
                .anti_affinity_group_member_instance_delete(
                    &opctx,
                    &authz_group,
                    instance,
                )
                .await
                .expect_err("Should have failed");

            match (arg.group, arg.instance) {
                (false, _) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::AntiAffinityGroup),
                        "{err:?}"
                    );
                }
                (true, false) => {
                    assert!(
                        matches!(err, Error::ObjectNotFound {
                        type_name, ..
                    } if type_name == ResourceType::AntiAffinityGroupMember),
                        "{err:?}"
                    );
                }
                (true, true) => {
                    panic!("If both exist, we won't throw an error")
                }
            }

            // Cleanup, if we actually created anything.
            if arg.instance {
                datastore
                    .project_delete_instance(&opctx, &authz_instance)
                    .await
                    .unwrap();
            }
            if arg.group {
                datastore
                    .anti_affinity_group_delete(&opctx, &authz_group)
                    .await
                    .unwrap();
            }
        }

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_group_membership_idempotency() {
        // Setup
        let logctx =
            dev::test_setup_log("affinity_group_membership_idempotency");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();
        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // Create an instance
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;

        // Add the instance to the group
        datastore
            .affinity_group_member_instance_add(&opctx, &authz_group, instance)
            .await
            .unwrap();

        // Add the instance to the group again
        let err = datastore
            .affinity_group_member_instance_add(&opctx, &authz_group, instance)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::ObjectAlreadyExists {
                    type_name: ResourceType::AffinityGroupMember,
                    ..
                }
            ),
            "Error: {err:?}"
        );

        // We should still only observe a single member in the group.
        //
        // Two calls to "affinity_group_member_add" should be the same
        // as a single call.
        let pagbyid = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert_eq!(members.len(), 1);

        // We should be able to delete the membership idempotently.
        datastore
            .affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();
        let err = datastore
            .affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::ObjectNotFound {
                    type_name: ResourceType::AffinityGroupMember,
                    ..
                }
            ),
            "Error: {err:?}"
        );

        let members = datastore
            .affinity_group_member_list(&opctx, &authz_group, &pagbyid)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_group_membership_idempotency() {
        // Setup
        let logctx =
            dev::test_setup_log("anti_affinity_group_membership_idempotency");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a project and a group
        let (authz_project, ..) =
            create_project(&opctx, &datastore, "my-project").await;
        let group = create_anti_affinity_group(
            &opctx,
            &datastore,
            &authz_project,
            "my-group",
        )
        .await
        .unwrap();
        let (.., authz_group) = LookupPath::new(opctx, datastore)
            .anti_affinity_group_id(group.id())
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        // Create an instance
        let instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "my-instance",
        )
        .await;

        // Add the instance to the group
        datastore
            .anti_affinity_group_member_instance_add(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();

        // Add the instance to the group again
        let err = datastore
            .anti_affinity_group_member_instance_add(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::ObjectAlreadyExists {
                    type_name: ResourceType::AntiAffinityGroupMember,
                    ..
                }
            ),
            "Error: {err:?}"
        );

        // We should still only observe a single member in the group.
        //
        // Two calls to "anti_affinity_group_member_instance_add" should be the same
        // as a single call.
        let pagparams = PaginatedBy::Id(DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        });
        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert_eq!(members.len(), 1);

        // We should be able to delete the membership idempotently.
        datastore
            .anti_affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap();
        let err = datastore
            .anti_affinity_group_member_instance_delete(
                &opctx,
                &authz_group,
                instance,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::ObjectNotFound {
                    type_name: ResourceType::AntiAffinityGroupMember,
                    ..
                }
            ),
            "Error: {err:?}"
        );

        let members = datastore
            .anti_affinity_group_member_list(&opctx, &authz_group, &pagparams)
            .await
            .unwrap();
        assert!(members.is_empty());

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
