// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to roles.

use super::DataStore;
use crate::authz;
use crate::authz::AuthorizedResource;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::RunnableQuery;
use crate::db::datastore::RunnableQueryNoReturn;
use crate::db::model::DatabaseString;
use crate::db::model::IdentityType;
use crate::db::model::RoleAssignment;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_fixed_data::role_assignment::BUILTIN_ROLE_ASSIGNMENTS;
use nexus_db_lookup::DbConnection;
use nexus_types::external_api::shared;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::bail_unless;

impl DataStore {
    /// Load role assignments for built-in users and built-in roles into the
    /// database
    pub async fn load_builtin_role_asgns(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::role_assignment::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in role assignments");
        let conn = self.pool_connection_authorized(opctx).await?;
        let count = diesel::insert_into(dsl::role_assignment)
            .values(&*BUILTIN_ROLE_ASSIGNMENTS)
            .on_conflict((
                dsl::identity_type,
                dsl::identity_id,
                dsl::resource_type,
                dsl::resource_id,
                dsl::role_name,
            ))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        info!(opctx.log, "created {} built-in role assignments", count);
        Ok(())
    }

    /// Fetches all of the externally-visible role assignments for the specified
    /// resource
    ///
    /// Role assignments for internal identities (e.g., built-in users) are not
    /// included in this list.
    ///
    /// This function is generic over all resources that can accept roles (e.g.,
    /// Fleet, Silo, etc.).
    // TODO-scalability In an ideal world, this would be paginated.  The impact
    // is mitigated because we cap the number of role assignments per resource
    // pretty tightly.
    pub async fn role_assignment_fetch_visible<
        T: authz::ApiResourceWithRoles + AuthorizedResource + Clone,
    >(
        &self,
        opctx: &OpContext,
        authz_resource: &T,
    ) -> ListResultVec<db::model::RoleAssignment> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.role_assignment_fetch_visible_conn(opctx, authz_resource, &conn)
            .await
    }

    pub async fn role_assignment_fetch_visible_conn<
        T: authz::ApiResourceWithRoles + AuthorizedResource + Clone,
    >(
        &self,
        opctx: &OpContext,
        authz_resource: &T,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> ListResultVec<db::model::RoleAssignment> {
        opctx.authorize(authz::Action::ReadPolicy, authz_resource).await?;
        let resource_type = authz_resource.resource_type();
        let resource_id = authz_resource.resource_id();
        use nexus_db_schema::schema::role_assignment::dsl;
        dsl::role_assignment
            .filter(dsl::resource_type.eq(resource_type.to_string()))
            .filter(dsl::resource_id.eq(resource_id))
            .filter(dsl::identity_type.ne(IdentityType::UserBuiltin))
            .order(dsl::role_name.asc())
            .then_order_by(dsl::identity_id.asc())
            .select(RoleAssignment::as_select())
            .load_async::<RoleAssignment>(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Removes all existing externally-visble role assignments on
    /// `authz_resource` and adds those specified by `new_assignments`
    ///
    /// Role assignments for internal identities (e.g., built-in users) are not
    /// affected.
    ///
    /// The expectation is that the caller will have just fetched the role
    /// assignments, modified them, and is giving us the complete new list.
    ///
    /// This function is generic over all resources that can accept roles (e.g.,
    /// Fleet, Silo, etc.).
    // TODO-correctness As with the rest of the API, we're lacking an ability
    // for an ETag precondition check here.
    // TODO-scalability In an ideal world, this would update in batches.  That's
    // tricky without first-classing the Policy in the database.  The impact is
    // mitigated because we cap the number of role assignments per resource
    // pretty tightly.
    pub async fn role_assignment_replace_visible<T>(
        &self,
        opctx: &OpContext,
        authz_resource: &T,
        new_assignments: &[shared::RoleAssignment<T::AllowedRoles>],
    ) -> ListResultVec<db::model::RoleAssignment>
    where
        T: authz::ApiResourceWithRolesType + AuthorizedResource + Clone,
    {
        opctx.authorize(authz::Action::ModifyPolicy, authz_resource).await?;

        let authz_resource = authz_resource.clone();
        let new_assignments = new_assignments.to_vec().clone();

        let queries = DataStore::role_assignment_replace_visible_queries(
            opctx,
            &authz_resource,
            &new_assignments,
        )
        .await?;

        let (delete_old_query, insert_new_query) = queries;

        // TODO-scalability: Ideally this would be a batched transaction so we
        // don't need to hold a transaction open across multiple roundtrips from
        // the database, but for now we're using a transaction due to the
        // severely decreased legibility of CTEs via diesel right now.
        // We might instead want to first-class the idea of Policies in the
        // database so that we can build up a whole new Policy in batches and
        // then flip the resource over to using it.

        // This method should probably be retryable, but this is slightly
        // complicated by the cloning semantics of the queries, which
        // must be Clone to be retried.
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_non_retry_wrapper("role_assignment_replace_visible")
            .transaction(&conn, |conn| async move {
                delete_old_query.execute_async(&conn).await?;
                Ok(insert_new_query.get_results_async(&conn).await?)
            })
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn role_assignment_replace_visible_queries<T>(
        opctx: &OpContext,
        authz_resource: &T,
        new_assignments: &[shared::RoleAssignment<T::AllowedRoles>],
    ) -> Result<
        (
            impl RunnableQueryNoReturn + use<T>,
            impl RunnableQuery<db::model::RoleAssignment> + use<T>,
        ),
        Error,
    >
    where
        T: authz::ApiResourceWithRolesType + AuthorizedResource + Clone,
    {
        opctx.authorize(authz::Action::ModifyPolicy, authz_resource).await?;

        bail_unless!(
            new_assignments.len() <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE
        );

        let resource_type = authz_resource.resource_type();
        let resource_id = authz_resource.resource_id();

        // Sort the records in the same order that we would return them when
        // listing them.  This is because we're going to use RETURNING to return
        // the inserted rows from the database and we want them to come back in
        // the same order that we would normally list them.
        let mut new_assignments = new_assignments
            .iter()
            .map(|r| {
                db::model::RoleAssignment::new(
                    db::model::IdentityType::from(r.identity_type),
                    r.identity_id,
                    resource_type,
                    resource_id,
                    &r.role_name.to_database_string(),
                )
            })
            .collect::<Vec<_>>();
        new_assignments.sort_by(|r1, r2| {
            (&r1.role_name, r1.identity_id)
                .cmp(&(&r2.role_name, r2.identity_id))
        });

        use nexus_db_schema::schema::role_assignment::dsl;

        let delete_old_query = diesel::delete(dsl::role_assignment)
            .filter(dsl::resource_id.eq(resource_id))
            .filter(dsl::resource_type.eq(resource_type.to_string()))
            .filter(dsl::identity_type.ne(IdentityType::UserBuiltin));

        let insert_new_query = diesel::insert_into(dsl::role_assignment)
            .values(new_assignments)
            .returning(RoleAssignment::as_returning());

        Ok((delete_old_query, insert_new_query))
    }
}
