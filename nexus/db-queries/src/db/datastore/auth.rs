// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implements the [Storage] interface for [nexus_auth] integration.

use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_auth::context::OpContext;
use nexus_auth::storage::Storage;
use nexus_db_model::IdentityType;
use nexus_db_model::RoleAssignment;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

#[async_trait::async_trait]
impl Storage for super::DataStore {
    /// Return the built-in roles that the given built-in user has for the given
    /// resource
    async fn role_asgn_list_for(
        &self,
        opctx: &OpContext,
        identity_type: IdentityType,
        identity_id: Uuid,
        resource_type: ResourceType,
        resource_id: Uuid,
    ) -> Result<Vec<RoleAssignment>, Error> {
        use db::schema::role_assignment::dsl as role_dsl;
        use db::schema::silo_group_membership::dsl as group_dsl;

        // There is no resource-specific authorization check because all
        // authenticated users need to be able to list their own roles --
        // otherwise we can't do any authorization checks.
        // TODO-security rethink this -- how do we know the user is looking up
        // their own roles?  Maybe this should use an internal authz context.

        // TODO-scalability TODO-security This needs to be paginated.  It's not
        // exposed via an external API right now but someone could still put us
        // into some hurt by assigning loads of roles to someone and having that
        // person attempt to access anything.

        let direct_roles_query = role_dsl::role_assignment
            .filter(role_dsl::identity_type.eq(identity_type.clone()))
            .filter(role_dsl::identity_id.eq(identity_id))
            .filter(role_dsl::resource_type.eq(resource_type.to_string()))
            .filter(role_dsl::resource_id.eq(resource_id))
            .select(RoleAssignment::as_select());

        let roles_from_groups_query = role_dsl::role_assignment
            .filter(role_dsl::identity_type.eq(IdentityType::SiloGroup))
            .filter(
                role_dsl::identity_id.eq_any(
                    group_dsl::silo_group_membership
                        .filter(group_dsl::silo_user_id.eq(identity_id))
                        .select(group_dsl::silo_group_id),
                ),
            )
            .filter(role_dsl::resource_type.eq(resource_type.to_string()))
            .filter(role_dsl::resource_id.eq(resource_id))
            .select(RoleAssignment::as_select());

        let conn = self.pool_connection_authorized(opctx).await?;
        if identity_type == IdentityType::SiloUser {
            direct_roles_query
                .union(roles_from_groups_query)
                .load_async::<RoleAssignment>(&*conn)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        } else {
            direct_roles_query
                .load_async::<RoleAssignment>(&*conn)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        }
    }
}
