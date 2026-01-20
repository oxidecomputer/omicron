// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! scim2-rs uses the pattern of implementing a SCIM "provider" over something
//! that implements a "provider store" trait that durably stores the SCIM
//! related information. Nexus uses cockroachdb as the provider store.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::silo_group::SiloGroup;
use crate::db::datastore::silo_group::SiloGroupScim;
use crate::db::datastore::silo_user::SiloUser;
use crate::db::datastore::silo_user::SiloUserScim;
use crate::db::model;
use crate::db::model::to_db_typed_uuid;
use anyhow::anyhow;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use iddqd::IdOrdMap;
use nexus_auth::authz::ApiResource;
use nexus_auth::authz::ApiResourceWithRoles;
use nexus_auth::authz::SiloGroupList;
use nexus_auth::authz::SiloUserList;
use nexus_db_errors::OptionalError;
use nexus_db_lookup::DbConnection;
use nexus_db_model::DatabaseString;
use nexus_db_model::IdentityType;
use nexus_types::external_api::shared::SiloRole;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloGroupUuid;
use omicron_uuid_kinds::SiloUserUuid;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use scim2_rs::CreateGroupRequest;
use scim2_rs::CreateUserRequest;
use scim2_rs::FilterOp;
use scim2_rs::Group;
use scim2_rs::GroupMember;
use scim2_rs::ProviderStore;
use scim2_rs::ProviderStoreDeleteResult;
use scim2_rs::ProviderStoreError;
use scim2_rs::ResourceType;
use scim2_rs::StoredMeta;
use scim2_rs::StoredParts;
use scim2_rs::User;
use scim2_rs::UserGroup;
use scim2_rs::UserGroupType;

pub struct CrdbScimProviderStore<'a> {
    authz_silo: authz::Silo,
    datastore: Arc<DataStore>,
    opctx: &'a OpContext,
}

// Define the lower case function here: some SCIM attributes like user name are
// case insensitive!
define_sql_function!(
    fn lower(
        a: diesel::sql_types::Nullable<diesel::sql_types::Text>,
    ) -> diesel::sql_types::Text
);

fn external_error_to_provider_error(
    error: omicron_common::api::external::Error,
) -> ProviderStoreError {
    match error {
        omicron_common::api::external::Error::Unauthenticated { .. } => {
            ProviderStoreError::Scim(scim2_rs::Error::unauthorized())
        }
        omicron_common::api::external::Error::Forbidden => {
            ProviderStoreError::Scim(scim2_rs::Error::forbidden())
        }
        err => ProviderStoreError::StoreError(err.into()),
    }
}

impl<'a> CrdbScimProviderStore<'a> {
    pub fn new(
        authz_silo: authz::Silo,
        datastore: Arc<DataStore>,
        opctx: &'a OpContext,
    ) -> Self {
        CrdbScimProviderStore { authz_silo, datastore, opctx }
    }

    /// Remove a users sessions and tokens
    async fn remove_user_sessions_and_tokens_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        user_id: SiloUserUuid,
    ) -> Result<(), diesel::result::Error> {
        // Delete console sessions.
        {
            use nexus_db_schema::schema::console_session::dsl;
            diesel::delete(dsl::console_session)
                .filter(dsl::silo_user_id.eq(to_db_typed_uuid(user_id)))
                .execute_async(conn)
                .await?;
        }

        // Delete device authentication tokens.
        {
            use nexus_db_schema::schema::device_access_token::dsl;
            diesel::delete(dsl::device_access_token)
                .filter(dsl::silo_user_id.eq(to_db_typed_uuid(user_id)))
                .execute_async(conn)
                .await?;
        }

        Ok(())
    }

    /// Nuke sessions, tokens, etc, for deactivated users
    async fn on_user_active_to_false_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        user_id: SiloUserUuid,
    ) -> Result<(), diesel::result::Error> {
        self.remove_user_sessions_and_tokens_in_txn(conn, user_id).await
    }

    async fn get_user_groups_for_user_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        user_id: SiloUserUuid,
    ) -> Result<Option<Vec<UserGroup>>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group::dsl as group_dsl;
        use nexus_db_schema::schema::silo_group_membership::dsl;

        struct Columns {
            group_id: SiloGroupUuid,
            display_name: String,
        }

        let tuples: Vec<Columns> = dsl::silo_group_membership
            .inner_join(
                group_dsl::silo_group.on(dsl::silo_group_id.eq(group_dsl::id)),
            )
            .filter(group_dsl::silo_id.eq(self.authz_silo.id()))
            .filter(
                group_dsl::user_provision_type
                    .eq(model::UserProvisionType::Scim),
            )
            .filter(dsl::silo_user_id.eq(to_db_typed_uuid(user_id)))
            .filter(group_dsl::time_deleted.is_null())
            .select((group_dsl::id, group_dsl::display_name))
            .load_async(conn)
            .await?
            .into_iter()
            .map(|(group_id, display_name): (Uuid, Option<String>)| Columns {
                group_id: SiloGroupUuid::from_untyped_uuid(group_id),
                display_name: display_name.expect(
                    "the constraint `display_name_consistency` prevents a \
                    group with provision type 'scim' from having a null \
                    display_name",
                ),
            })
            .collect();

        if tuples.is_empty() {
            Ok(None)
        } else {
            let groups = tuples
                .into_iter()
                .map(|column| UserGroup {
                    // Note neither the scim2-rs crate or Nexus supports nested
                    // groups
                    member_type: Some(UserGroupType::Direct),
                    value: Some(column.group_id.to_string()),
                    display: Some(column.display_name),
                })
                .collect();

            Ok(Some(groups))
        }
    }

    async fn get_group_members_for_group_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        group_id: SiloGroupUuid,
    ) -> Result<Option<IdOrdMap<GroupMember>>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group_membership::dsl;

        let users: Vec<Uuid> = dsl::silo_group_membership
            .filter(dsl::silo_group_id.eq(to_db_typed_uuid(group_id)))
            .select(dsl::silo_user_id)
            .load_async(conn)
            .await?;

        if users.is_empty() {
            Ok(None)
        } else {
            let members = users
                .into_iter()
                .map(|user_id| GroupMember {
                    // Note neither the scim2-rs crate or Nexus support nested
                    // groups
                    resource_type: Some(ResourceType::User.to_string()),
                    value: Some(user_id.to_string()),
                })
                .collect();

            Ok(Some(members))
        }
    }

    async fn get_user_by_id_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        _err: OptionalError<ProviderStoreError>,
        user_id: SiloUserUuid,
    ) -> Result<Option<StoredParts<User>>, diesel::result::Error> {
        let maybe_user = {
            use nexus_db_schema::schema::silo_user::dsl;

            dsl::silo_user
                .filter(dsl::silo_id.eq(self.authz_silo.id()))
                .filter(
                    dsl::user_provision_type.eq(model::UserProvisionType::Scim),
                )
                .filter(dsl::id.eq(to_db_typed_uuid(user_id)))
                .filter(dsl::time_deleted.is_null())
                .select(model::SiloUser::as_returning())
                .first_async(conn)
                .await
                .optional()?
        };

        let Some(user) = maybe_user else {
            return Ok(None);
        };

        let groups: Option<Vec<UserGroup>> =
            self.get_user_groups_for_user_in_txn(conn, user_id).await?;

        let SiloUser::Scim(user) = user.into() else {
            // With the user provision type filter, this should never be another
            // type.
            unreachable!();
        };

        Ok(Some(convert_to_scim_user(user, groups)))
    }

    async fn create_user_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        user_request: CreateUserRequest,
    ) -> Result<StoredParts<User>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_user::dsl;

        // userName is meant to be unique: If the user request is adding a
        // userName that already exists, reject it

        let maybe_other_user = dsl::silo_user
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(lower(dsl::user_name).eq(lower(user_request.name.clone())))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloUser::as_returning())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_other_user.is_some() {
            return Err(err.bail(
                scim2_rs::Error::conflict(format!(
                    "username {}",
                    user_request.name
                ))
                .into(),
            ));
        }

        let new_user = SiloUserScim::new(
            self.authz_silo.id(),
            SiloUserUuid::new_v4(),
            user_request.name,
            user_request.active,
            user_request.external_id,
        );

        let model: model::SiloUser = new_user.clone().into();

        diesel::insert_into(dsl::silo_user)
            .values(model)
            .execute_async(conn)
            .await?;

        Ok(convert_to_scim_user(new_user, None))
    }

    async fn list_users_with_groups(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        filter: Option<FilterOp>,
    ) -> Result<Vec<StoredParts<User>>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group::dsl as group_dsl;
        use nexus_db_schema::schema::silo_group_membership::dsl as membership_dsl;
        use nexus_db_schema::schema::silo_user::dsl as user_dsl;
        use std::collections::HashMap;

        let mut query = user_dsl::silo_user
            .left_join(
                membership_dsl::silo_group_membership
                    .on(user_dsl::id.eq(membership_dsl::silo_user_id)),
            )
            .left_join(
                group_dsl::silo_group.on(membership_dsl::silo_group_id
                    .eq(group_dsl::id)
                    .and(group_dsl::silo_id.eq(self.authz_silo.id()))
                    .and(
                        group_dsl::user_provision_type
                            .eq(model::UserProvisionType::Scim),
                    )
                    .and(group_dsl::time_deleted.is_null())),
            )
            .filter(user_dsl::silo_id.eq(self.authz_silo.id()))
            .filter(
                user_dsl::user_provision_type
                    .eq(model::UserProvisionType::Scim),
            )
            .filter(user_dsl::time_deleted.is_null())
            .into_boxed();

        match filter {
            Some(FilterOp::UserNameEq(username)) => {
                // userName is defined as `"caseExact" : false` in RFC 7643,
                // section 8.7.1
                query = query.filter(
                    lower(user_dsl::user_name).eq(lower(username.clone())),
                );
            }

            None => {
                // ok
            }

            Some(_) => {
                return Err(err.bail(
                    scim2_rs::Error::invalid_filter(
                        "invalid or unsupported filter".to_string(),
                    )
                    .into(),
                ));
            }
        }

        // Select user fields and optional group fields
        // Note: We need to explicitly select the group columns to work with the
        // boxed query and LEFT JOIN
        type UserRow = (model::SiloUser, Option<(Uuid, Option<String>)>);
        type UsersMap = HashMap<
            Uuid,
            (Option<model::SiloUser>, Vec<(SiloGroupUuid, String)>),
        >;

        let rows: Vec<UserRow> = query
            .select((
                model::SiloUser::as_select(),
                (group_dsl::id, group_dsl::display_name).nullable(),
            ))
            .load_async(conn)
            .await?;

        // Group the results by user_id
        let mut users_map: UsersMap = HashMap::new();

        for (user, maybe_group_info) in rows {
            let user_id = user.identity.id.into_untyped_uuid();

            let entry =
                users_map.entry(user_id).or_insert_with(|| (None, Vec::new()));

            // Store the user on first occurrence
            if entry.0.is_none() {
                entry.0 = Some(user);
            }

            // If this row has a group, add it to the user's groups
            if let Some((group_id, maybe_display_name)) = maybe_group_info {
                let display_name = maybe_display_name.expect(
                    "the constraint `display_name_consistency` prevents a \
                    group with provision type 'scim' from having a null \
                    display_name",
                );

                entry.1.push((
                    SiloGroupUuid::from_untyped_uuid(group_id),
                    display_name,
                ));
            }
        }

        // Convert to the expected return type
        let mut returned_users = Vec::with_capacity(users_map.len());

        for (_user_id, (maybe_user, groups)) in users_map {
            let user = maybe_user.expect("user should always be present");

            let groups = if groups.is_empty() {
                None
            } else {
                Some(
                    groups
                        .into_iter()
                        .map(|(group_id, display_name)| UserGroup {
                            // Note neither the scim2-rs crate or Nexus supports
                            // nested groups
                            member_type: Some(UserGroupType::Direct),
                            value: Some(group_id.to_string()),
                            display: Some(display_name),
                        })
                        .collect(),
                )
            };

            let SiloUser::Scim(user) = user.into() else {
                // With the user provision type filter, this should never be
                // another type.
                unreachable!();
            };

            returned_users.push(convert_to_scim_user(user, groups));
        }

        Ok(returned_users)
    }

    async fn replace_user_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        user_id: SiloUserUuid,
        user_request: CreateUserRequest,
    ) -> Result<StoredParts<User>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_user::dsl;

        let maybe_user = dsl::silo_user
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(user_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloUser::as_returning())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_user.is_none() {
            return Err(err
                .bail(scim2_rs::Error::not_found(user_id.to_string()).into()));
        }

        let CreateUserRequest { name, active, external_id, groups: _ } =
            user_request;

        // userName is meant to be unique: If the user request is changing the
        // userName to one that already exists, reject it

        let maybe_other_user = dsl::silo_user
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(lower(dsl::user_name).eq(lower(name.clone())))
            .filter(dsl::id.ne(to_db_typed_uuid(user_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloUser::as_returning())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_other_user.is_some() {
            return Err(err.bail(
                scim2_rs::Error::conflict(format!("username {}", name)).into(),
            ));
        }

        // Overwrite all fields based on CreateUserRequest, except groups: it's
        // invalid to change group memberships in a Users PUT.

        if let Some(active) = active {
            if !active {
                self.on_user_active_to_false_in_txn(conn, user_id).await?;
            }
        }

        let updated = diesel::update(dsl::silo_user)
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(user_id)))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::user_name.eq(name),
                dsl::active.eq(active),
                dsl::external_id.eq(external_id),
            ))
            .execute_async(conn)
            .await?;

        if updated != 1 {
            return Err(err.bail(ProviderStoreError::StoreError(anyhow!(
                "expected 1 row to be updated, not {updated}"
            ))));
        }

        let user = dsl::silo_user
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(user_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloUser::as_select())
            .first_async(conn)
            .await?;

        // Groups don't change, so query for what was previously there

        let groups = self
            .get_user_groups_for_user_in_txn(conn, user.identity.id.into())
            .await?;

        let SiloUser::Scim(user) = user.into() else {
            // With the user provision type filter, this should never be another
            // type.
            unreachable!();
        };

        Ok(convert_to_scim_user(user, groups))
    }

    async fn delete_user_by_id_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        user_id: SiloUserUuid,
    ) -> Result<bool, diesel::result::Error> {
        use nexus_db_schema::schema::silo_user::dsl;

        let maybe_user = dsl::silo_user
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(user_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloUser::as_select())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_user.is_none() {
            return Ok(false);
        }

        let updated = diesel::update(dsl::silo_user)
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(user_id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(conn)
            .await?;

        if updated != 1 {
            return Err(err.bail(ProviderStoreError::StoreError(anyhow!(
                "expected 1 row to be updated, not {updated}"
            ))));
        }

        {
            use nexus_db_schema::schema::silo_group_membership::dsl;

            diesel::delete(dsl::silo_group_membership)
                .filter(dsl::silo_user_id.eq(to_db_typed_uuid(user_id)))
                .execute_async(conn)
                .await?;
        }

        // Cleanup role assignment records for the deleted user
        {
            use nexus_db_schema::schema::role_assignment::dsl;

            diesel::delete(dsl::role_assignment)
                .filter(dsl::identity_type.eq(IdentityType::SiloUser))
                .filter(dsl::identity_id.eq(to_db_typed_uuid(user_id)))
                .execute_async(conn)
                .await?;
        }

        // Finally we should cleanup all auth related things like sessions and
        // tokens.
        self.remove_user_sessions_and_tokens_in_txn(conn, user_id).await?;

        Ok(true)
    }

    async fn get_group_by_id_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        _err: OptionalError<ProviderStoreError>,
        group_id: SiloGroupUuid,
    ) -> Result<Option<StoredParts<Group>>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group::dsl;

        let maybe_group = dsl::silo_group
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloGroup::as_returning())
            .first_async(conn)
            .await
            .optional()?;

        let Some(group) = maybe_group else {
            return Ok(None);
        };

        let members: Option<IdOrdMap<GroupMember>> =
            self.get_group_members_for_group_in_txn(conn, group_id).await?;

        let SiloGroup::Scim(group) = group.into() else {
            // With the user provision type filter, this should never be another
            // type.
            unreachable!();
        };

        Ok(Some(convert_to_scim_group(group, members)))
    }

    /// Returns User id, and the GroupMember object, if this member is valid
    async fn validate_group_member_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        member: &GroupMember,
    ) -> Result<(SiloUserUuid, GroupMember), diesel::result::Error> {
        let GroupMember { resource_type, value } = member;

        let Some(value) = value else {
            // The minimum that this code needs is the value field so complain
            // about that.
            return Err(err.bail(
                scim2_rs::Error::invalid_syntax(String::from(
                    "group member missing value field",
                ))
                .into(),
            ));
        };

        let id: Uuid = match value.parse() {
            Ok(v) => v,

            Err(_) => {
                return Err(err.bail(ProviderStoreError::StoreError(anyhow!(
                    "id must be uuid"
                ))));
            }
        };

        // Find the ID that this request is talking about, or 404
        let resource_type = if let Some(resource_type) = resource_type {
            let resource_type = match ResourceType::from_str(resource_type) {
                Ok(v) => v,
                Err(e) => Err(err.bail(
                    scim2_rs::Error::invalid_syntax(e.to_string()).into(),
                ))?,
            };

            match resource_type {
                ResourceType::User => {
                    use nexus_db_schema::schema::silo_user::dsl;

                    let maybe_user: Option<Uuid> = dsl::silo_user
                        .filter(dsl::silo_id.eq(self.authz_silo.id()))
                        .filter(
                            dsl::user_provision_type
                                .eq(model::UserProvisionType::Scim),
                        )
                        .filter(dsl::id.eq(id))
                        .filter(dsl::time_deleted.is_null())
                        .select(dsl::id)
                        .first_async(conn)
                        .await
                        .optional()?;

                    if maybe_user.is_none() {
                        return Err(err.bail(
                            scim2_rs::Error::not_found(value.to_string())
                                .into(),
                        ));
                    }
                }

                ResourceType::Group => {
                    return Err(err.bail(
                        scim2_rs::Error::internal_error(
                            "nested groups not supported".to_string(),
                        )
                        .into(),
                    ));
                }
            }

            resource_type
        } else {
            // If no resource type is supplied, then search for a matching user
            // or group.

            let maybe_user: Option<Uuid> = {
                use nexus_db_schema::schema::silo_user::dsl;

                dsl::silo_user
                    .filter(dsl::silo_id.eq(self.authz_silo.id()))
                    .filter(
                        dsl::user_provision_type
                            .eq(model::UserProvisionType::Scim),
                    )
                    .filter(dsl::id.eq(id))
                    .filter(dsl::time_deleted.is_null())
                    .select(dsl::id)
                    .first_async(conn)
                    .await
                    .optional()?
            };

            let maybe_group: Option<Uuid> = {
                use nexus_db_schema::schema::silo_group::dsl;

                dsl::silo_group
                    .filter(dsl::silo_id.eq(self.authz_silo.id()))
                    .filter(
                        dsl::user_provision_type
                            .eq(model::UserProvisionType::Scim),
                    )
                    .filter(dsl::id.eq(id))
                    .filter(dsl::time_deleted.is_null())
                    .select(dsl::id)
                    .first_async(conn)
                    .await
                    .optional()?
            };

            match (maybe_user, maybe_group) {
                (None, None) => {
                    // 404
                    return Err(err.bail(
                        scim2_rs::Error::not_found(value.clone()).into(),
                    ));
                }

                (Some(_), None) => ResourceType::User,

                (None, Some(_)) => {
                    return Err(err.bail(
                        scim2_rs::Error::internal_error(
                            "nested groups not supported".to_string(),
                        )
                        .into(),
                    ));
                }

                (Some(_), Some(_)) => {
                    return Err(err.bail(
                        scim2_rs::Error::internal_error(format!(
                            "{value} returned a user and group!"
                        ))
                        .into(),
                    ));
                }
            }
        };

        Ok((
            SiloUserUuid::from_untyped_uuid(id),
            GroupMember {
                resource_type: Some(resource_type.to_string()),
                value: Some(value.to_string()),
            },
        ))
    }

    async fn create_group_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        group_request: CreateGroupRequest,
    ) -> Result<StoredParts<Group>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group::dsl;

        let CreateGroupRequest { display_name, external_id, members } =
            group_request;

        // displayName is meant to be unique: If the group request is changing
        // the displayName to one that already exists, reject it.
        //
        // Note that the SCIM spec says that displayName's uniqueness is none -
        // but our / Nexus groups have to be unique due to the lookup by Name.

        let maybe_group = dsl::silo_group
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(lower(dsl::display_name).eq(lower(display_name.clone())))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloGroup::as_returning())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_group.is_some() {
            return Err(err.bail(
                scim2_rs::Error::conflict(format!(
                    "displayName {}",
                    display_name
                ))
                .into(),
            ));
        }

        let group_id = SiloGroupUuid::new_v4();

        let new_group = SiloGroupScim::new(
            self.authz_silo.id(),
            group_id,
            display_name.clone(),
            external_id,
        );

        let model: model::SiloGroup = new_group.clone().into();

        diesel::insert_into(dsl::silo_group)
            .values(model)
            .execute_async(conn)
            .await?;

        let members = if let Some(members) = &members {
            let mut returned_members = IdOrdMap::with_capacity(members.len());
            let mut memberships = Vec::with_capacity(members.len());

            // Validate the members arg, and insert silo group membership
            // records.
            for member in members {
                let (user_id, returned_member) = self
                    .validate_group_member_in_txn(conn, err.clone(), member)
                    .await?;

                match returned_members.insert_unique(returned_member) {
                    Ok(_) => {}

                    Err(e) => {
                        return Err(err.bail(ProviderStoreError::Scim(
                            scim2_rs::Error::conflict(format!(
                                "{:?}",
                                e.new_item()
                            )),
                        )));
                    }
                }

                memberships.push(model::SiloGroupMembership {
                    silo_group_id: group_id.into(),
                    silo_user_id: user_id.into(),
                });
            }

            use nexus_db_schema::schema::silo_group_membership::dsl;
            diesel::insert_into(dsl::silo_group_membership)
                .values(memberships)
                .execute_async(conn)
                .await?;

            Some(returned_members)
        } else {
            None
        };

        // If this group's name matches the silo's admin group name, then create
        // the appropriate policy granting members of that group the silo admin
        // role.

        {
            use nexus_db_schema::schema::silo::dsl;
            let silo = dsl::silo
                .filter(dsl::id.eq(self.authz_silo.id()))
                .select(model::Silo::as_select())
                .first_async(conn)
                .await?;

            if let Some(admin_group_name) = silo.admin_group_name {
                if admin_group_name.eq_ignore_ascii_case(&display_name) {
                    // XXX code copied from silo create

                    use nexus_db_schema::schema::role_assignment::dsl;

                    let new_assignment = model::RoleAssignment::new(
                        model::IdentityType::SiloGroup,
                        group_id.into_untyped_uuid(),
                        self.authz_silo.resource_type(),
                        self.authz_silo.resource_id(),
                        &SiloRole::Admin.to_database_string(),
                    );

                    diesel::insert_into(dsl::role_assignment)
                        .values(new_assignment)
                        .execute_async(conn)
                        .await?;
                }
            }
        }

        Ok(convert_to_scim_group(new_group, members))
    }

    async fn list_groups_with_members(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        filter: Option<FilterOp>,
    ) -> Result<Vec<StoredParts<Group>>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group::dsl as group_dsl;
        use nexus_db_schema::schema::silo_group_membership::dsl as membership_dsl;
        use std::collections::HashMap;

        let mut query = group_dsl::silo_group
            .left_join(
                membership_dsl::silo_group_membership
                    .on(group_dsl::id.eq(membership_dsl::silo_group_id)),
            )
            .filter(group_dsl::silo_id.eq(self.authz_silo.id()))
            .filter(
                group_dsl::user_provision_type
                    .eq(model::UserProvisionType::Scim),
            )
            .filter(group_dsl::time_deleted.is_null())
            .into_boxed();

        match filter {
            Some(FilterOp::DisplayNameEq(display_name)) => {
                // displayName is defined as `"caseExact" : false` in RFC 7643,
                // section 8.7.1
                query = query.filter(
                    lower(group_dsl::display_name)
                        .eq(lower(display_name.clone())),
                );
            }

            None => {
                // ok
            }

            Some(_) => {
                return Err(err.bail(
                    scim2_rs::Error::invalid_filter(
                        "invalid or unsupported filter".to_string(),
                    )
                    .into(),
                ));
            }
        }

        // Select group fields and optional member user_id
        type GroupRow = (model::SiloGroup, Option<Uuid>);
        type GroupsMap =
            HashMap<Uuid, (Option<model::SiloGroup>, Vec<SiloUserUuid>)>;

        let rows: Vec<GroupRow> = query
            .select((
                model::SiloGroup::as_select(),
                membership_dsl::silo_user_id.nullable(),
            ))
            .load_async(conn)
            .await?;

        // Group the results by group_id
        let mut groups_map: GroupsMap = HashMap::new();

        for (group, maybe_user_id) in rows {
            let group_id = group.identity.id.into_untyped_uuid();

            let entry = groups_map
                .entry(group_id)
                .or_insert_with(|| (None, Vec::new()));

            // Store the group on first occurrence
            if entry.0.is_none() {
                entry.0 = Some(group);
            }

            // If this row has a member, add it to the group's members
            if let Some(user_id) = maybe_user_id {
                entry.1.push(SiloUserUuid::from_untyped_uuid(user_id));
            }
        }

        // Convert to the expected return type
        let mut returned_groups = Vec::with_capacity(groups_map.len());

        for (_group_id, (maybe_group, members)) in groups_map {
            let group = maybe_group.expect("group should always be present");

            let members = if members.is_empty() {
                None
            } else {
                let mut id_ord_map = IdOrdMap::with_capacity(members.len());
                for user_id in members {
                    id_ord_map
                        .insert_unique(GroupMember {
                            resource_type: Some(ResourceType::User.to_string()),
                            value: Some(user_id.to_string()),
                        })
                        .expect("user_id should be unique");
                }
                Some(id_ord_map)
            };

            let SiloGroup::Scim(group) = group.into() else {
                // With the user provision type filter, this should never be
                // another type.
                unreachable!();
            };

            returned_groups.push(convert_to_scim_group(group, members));
        }

        Ok(returned_groups)
    }

    async fn replace_group_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        group_id: SiloGroupUuid,
        group_request: CreateGroupRequest,
    ) -> Result<StoredParts<Group>, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group::dsl;

        let maybe_group = dsl::silo_group
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloGroup::as_returning())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_group.is_none() {
            return Err(err.bail(
                scim2_rs::Error::not_found(group_id.to_string()).into(),
            ));
        };

        let CreateGroupRequest { display_name, external_id, members } =
            group_request;

        // displayName is meant to be unique: If the group request is changing
        // the displayName to one that already exists, reject it.

        let maybe_group = dsl::silo_group
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(lower(dsl::display_name).eq(lower(display_name.clone())))
            .filter(dsl::id.ne(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloGroup::as_returning())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_group.is_some() {
            return Err(err.bail(
                scim2_rs::Error::conflict(format!(
                    "displayName {}",
                    display_name
                ))
                .into(),
            ));
        }

        // Stash the group for later

        let existing_group = dsl::silo_group
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloGroup::as_select())
            .first_async(conn)
            .await?;

        let SiloGroup::Scim(existing_group) = existing_group.into() else {
            // With the user provision type filter, this should never be
            // another type.
            unreachable!();
        };

        // Overwrite all fields based on CreateGroupRequest.

        let updated = diesel::update(dsl::silo_group)
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::display_name.eq(display_name),
                dsl::external_id.eq(external_id),
            ))
            .execute_async(conn)
            .await?;

        if updated != 1 {
            return Err(err.bail(ProviderStoreError::StoreError(anyhow!(
                "expected 1 row to be updated, not {updated}"
            ))));
        }

        let group = dsl::silo_group
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloGroup::as_select())
            .first_async(conn)
            .await?;

        let SiloGroup::Scim(group) = group.into() else {
            // With the user provision type filter, this should never be
            // another type.
            unreachable!();
        };

        // Delete all existing group memberships for this group id

        {
            use nexus_db_schema::schema::silo_group_membership::dsl;

            diesel::delete(dsl::silo_group_membership)
                .filter(dsl::silo_group_id.eq(to_db_typed_uuid(group_id)))
                .execute_async(conn)
                .await?;
        }

        // Validate the members arg, and insert silo group membership records.

        let members = if let Some(members) = &members {
            let mut returned_members = IdOrdMap::with_capacity(members.len());
            let mut memberships = Vec::with_capacity(members.len());

            // Validate the members arg, and insert silo group membership
            // records.
            for member in members {
                let (user_id, returned_member) = self
                    .validate_group_member_in_txn(conn, err.clone(), member)
                    .await?;

                match returned_members.insert_unique(returned_member) {
                    Ok(_) => {}

                    Err(e) => {
                        return Err(err.bail(ProviderStoreError::Scim(
                            scim2_rs::Error::conflict(format!(
                                "{:?}",
                                e.new_item()
                            )),
                        )));
                    }
                }

                memberships.push(model::SiloGroupMembership {
                    silo_group_id: group_id.into(),
                    silo_user_id: user_id.into(),
                });
            }

            use nexus_db_schema::schema::silo_group_membership::dsl;
            diesel::insert_into(dsl::silo_group_membership)
                .values(memberships)
                .execute_async(conn)
                .await?;

            Some(returned_members)
        } else {
            None
        };

        // If displayName changes from the Silo admin group name to something
        // else, delete the Silo admin role assignment for this group ID. If it
        // changes _to_ the Silo admin group name, insert the appropriate Silo
        // admin role assignment.

        {
            use nexus_db_schema::schema::silo::dsl;
            let silo = dsl::silo
                .filter(dsl::id.eq(self.authz_silo.id()))
                .select(model::Silo::as_select())
                .first_async(conn)
                .await?;

            let authz_silo = authz::Silo::new(
                authz::FLEET,
                self.authz_silo.id(),
                LookupType::ById(self.authz_silo.id()),
            );

            if let Some(admin_group_name) = silo.admin_group_name {
                use nexus_db_schema::schema::role_assignment::dsl;

                // Did the group's name match the admin group name, and was it
                // changed?
                if existing_group
                    .display_name
                    .eq_ignore_ascii_case(&admin_group_name)
                {
                    if !group
                        .display_name
                        .eq_ignore_ascii_case(&admin_group_name)
                    {
                        // Scan for the matching role assignment, and delete
                        // that.

                        diesel::delete(dsl::role_assignment)
                            .filter(
                                dsl::identity_type
                                    .eq(model::IdentityType::SiloGroup),
                            )
                            .filter(
                                dsl::identity_id.eq(to_db_typed_uuid(group_id)),
                            )
                            .filter(
                                dsl::resource_type
                                    .eq(authz_silo.resource_type().to_string()),
                            )
                            .filter(dsl::resource_id.eq(authz_silo.id()))
                            .filter(
                                dsl::role_name
                                    .eq(SiloRole::Admin.to_database_string()),
                            )
                            .execute_async(conn)
                            .await?;
                    }
                } else {
                    // Did the group's name change _to_ the admin group name?
                    if group
                        .display_name
                        .eq_ignore_ascii_case(&admin_group_name)
                    {
                        // If so, insert a new assignment.

                        let new_assignment = model::RoleAssignment::new(
                            model::IdentityType::SiloGroup,
                            group_id.into_untyped_uuid(),
                            authz_silo.resource_type(),
                            authz_silo.resource_id(),
                            &SiloRole::Admin.to_database_string(),
                        );

                        // The ON CONFLICT + DO NOTHING is required to handle
                        // the case where the group was granted the silo admin
                        // role _before_ being renamed to match the silo's admin
                        // group name.

                        diesel::insert_into(dsl::role_assignment)
                            .values(new_assignment)
                            .on_conflict((
                                dsl::identity_type,
                                dsl::identity_id,
                                dsl::resource_type,
                                dsl::resource_id,
                                dsl::role_name,
                            ))
                            .do_nothing()
                            .execute_async(conn)
                            .await?;
                    }
                }
            }
        }

        Ok(convert_to_scim_group(group, members))
    }

    async fn delete_group_by_id_in_txn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ProviderStoreError>,
        group_id: SiloGroupUuid,
    ) -> Result<bool, diesel::result::Error> {
        use nexus_db_schema::schema::silo_group::dsl;

        let maybe_group = dsl::silo_group
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .select(model::SiloGroup::as_select())
            .first_async(conn)
            .await
            .optional()?;

        if maybe_group.is_none() {
            return Ok(false);
        }

        let updated = diesel::update(dsl::silo_group)
            .filter(dsl::silo_id.eq(self.authz_silo.id()))
            .filter(dsl::user_provision_type.eq(model::UserProvisionType::Scim))
            .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(conn)
            .await?;

        if updated != 1 {
            return Err(err.bail(ProviderStoreError::StoreError(anyhow!(
                "expected 1 row to be updated, not {updated}"
            ))));
        }

        {
            use nexus_db_schema::schema::silo_group_membership::dsl;

            diesel::delete(dsl::silo_group_membership)
                .filter(dsl::silo_group_id.eq(to_db_typed_uuid(group_id)))
                .execute_async(conn)
                .await?;
        }

        // Cleanup role assignment records for the deleted group
        {
            use nexus_db_schema::schema::role_assignment::dsl;

            diesel::delete(dsl::role_assignment)
                .filter(dsl::identity_id.eq(to_db_typed_uuid(group_id)))
                .filter(dsl::identity_type.eq(IdentityType::SiloGroup))
                .execute_async(conn)
                .await?;
        }

        Ok(true)
    }
}

fn convert_to_scim_user(
    silo_user: SiloUserScim,
    groups: Option<Vec<UserGroup>>,
) -> StoredParts<User> {
    StoredParts {
        resource: User {
            id: silo_user.id.to_string(),
            name: silo_user.user_name,
            active: silo_user.active,
            external_id: silo_user.external_id,
            groups,
        },

        meta: StoredMeta {
            created: silo_user.time_created,
            last_modified: silo_user.time_modified,
            version: "W/unimplemented".to_string(),
        },
    }
}

fn convert_to_scim_group(
    silo_group: SiloGroupScim,
    members: Option<IdOrdMap<GroupMember>>,
) -> StoredParts<Group> {
    StoredParts {
        resource: Group {
            id: silo_group.id.to_string(),
            display_name: silo_group.display_name,
            external_id: silo_group.external_id,
            members,
        },

        meta: StoredMeta {
            created: silo_group.time_created,
            last_modified: silo_group.time_modified,
            version: "W/unimplemented".to_string(),
        },
    }
}

impl<'a> ProviderStore for CrdbScimProviderStore<'a> {
    async fn get_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredParts<User>>, ProviderStoreError> {
        let user_id: SiloUserUuid = match user_id.parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(ProviderStoreError::StoreError(anyhow!(
                    "user id must be uuid"
                )));
            }
        };

        let authz_silo_user = authz::SiloUser::new(
            self.authz_silo.clone(),
            user_id,
            LookupType::by_id(user_id),
        );
        self.opctx
            .authorize(authz::Action::Read, &authz_silo_user)
            .await
            .map_err(external_error_to_provider_error)?;
        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let maybe_user =
            self.datastore
                .transaction_retry_wrapper("scim_get_user_by_id")
                .transaction(&conn, |conn| {
                    let err = err.clone();

                    async move {
                        self.get_user_by_id_in_txn(&conn, err, user_id).await
                    }
                })
                .await
                .map_err(|e| {
                    if let Some(e) = err.take() {
                        e
                    } else {
                        ProviderStoreError::StoreError(e.into())
                    }
                })?;

        Ok(maybe_user)
    }

    async fn create_user(
        &self,
        user_request: CreateUserRequest,
    ) -> Result<StoredParts<User>, ProviderStoreError> {
        let authz_silo_user_list =
            authz::SiloUserList::new(self.authz_silo.clone());
        self.opctx
            .authorize(authz::Action::CreateChild, &authz_silo_user_list)
            .await
            .map_err(external_error_to_provider_error)?;

        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let user = self
            .datastore
            .transaction_retry_wrapper("scim_create_user")
            .transaction(&conn, |conn| {
                let user_request = user_request.clone();
                let err = err.clone();

                async move {
                    self.create_user_in_txn(&conn, err, user_request).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        Ok(user)
    }

    async fn list_users(
        &self,
        filter: Option<FilterOp>,
    ) -> Result<Vec<StoredParts<User>>, ProviderStoreError> {
        self.opctx
            .authorize(
                authz::Action::ListChildren,
                &SiloUserList::new(self.authz_silo.clone()),
            )
            .await
            .map_err(external_error_to_provider_error)?;
        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let users = self
            .list_users_with_groups(&conn, err.clone(), filter)
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        Ok(users)
    }

    async fn replace_user(
        &self,
        user_id: &str,
        user_request: CreateUserRequest,
    ) -> Result<StoredParts<User>, ProviderStoreError> {
        let user_id: SiloUserUuid = match user_id.parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(ProviderStoreError::StoreError(anyhow!(
                    "user id must be uuid"
                )));
            }
        };

        let authz_silo_user = authz::SiloUser::new(
            self.authz_silo.clone(),
            user_id,
            LookupType::by_id(user_id),
        );
        self.opctx
            .authorize(authz::Action::Modify, &authz_silo_user)
            .await
            .map_err(external_error_to_provider_error)?;

        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let user = self
            .datastore
            .transaction_retry_wrapper("scim_replace_user")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let user_request = user_request.clone();

                async move {
                    self.replace_user_in_txn(&conn, err, user_id, user_request)
                        .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        Ok(user)
    }

    async fn delete_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<ProviderStoreDeleteResult, ProviderStoreError> {
        let user_id: SiloUserUuid = match user_id.parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(ProviderStoreError::StoreError(anyhow!(
                    "user id must be uuid"
                )));
            }
        };

        let authz_silo_user = authz::SiloUser::new(
            self.authz_silo.clone(),
            user_id,
            LookupType::by_id(user_id),
        );
        self.opctx
            .authorize(authz::Action::Delete, &authz_silo_user)
            .await
            .map_err(external_error_to_provider_error)?;
        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let deleted_user = self
            .datastore
            .transaction_retry_wrapper("scim_delete_user_by_id")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    self.delete_user_by_id_in_txn(&conn, err, user_id).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        if deleted_user {
            Ok(ProviderStoreDeleteResult::Deleted)
        } else {
            Ok(ProviderStoreDeleteResult::NotFound)
        }
    }

    async fn get_group_by_id(
        &self,
        group_id: &str,
    ) -> Result<Option<StoredParts<Group>>, ProviderStoreError> {
        let group_id: SiloGroupUuid = match group_id.parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(ProviderStoreError::StoreError(anyhow!(
                    "group id must be uuid"
                )));
            }
        };

        let authz_silo_group = authz::SiloGroup::new(
            self.authz_silo.clone(),
            group_id,
            LookupType::by_id(group_id),
        );
        self.opctx
            .authorize(authz::Action::Read, &authz_silo_group)
            .await
            .map_err(external_error_to_provider_error)?;
        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let maybe_group = self
            .datastore
            .transaction_retry_wrapper("scim_get_group_by_id")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    self.get_group_by_id_in_txn(&conn, err, group_id).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        Ok(maybe_group)
    }

    async fn create_group(
        &self,
        group_request: CreateGroupRequest,
    ) -> Result<StoredParts<Group>, ProviderStoreError> {
        let authz_silo_group_list =
            authz::SiloGroupList::new(self.authz_silo.clone());
        self.opctx
            .authorize(authz::Action::CreateChild, &authz_silo_group_list)
            .await
            .map_err(external_error_to_provider_error)?;
        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let group = self
            .datastore
            .transaction_retry_wrapper("scim_create_group")
            .transaction(&conn, |conn| {
                let group_request = group_request.clone();
                let err = err.clone();

                async move {
                    self.create_group_in_txn(&conn, err, group_request).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        Ok(group)
    }

    async fn list_groups(
        &self,
        filter: Option<FilterOp>,
    ) -> Result<Vec<StoredParts<Group>>, ProviderStoreError> {
        self.opctx
            .authorize(
                authz::Action::ListChildren,
                &SiloGroupList::new(self.authz_silo.clone()),
            )
            .await
            .map_err(external_error_to_provider_error)?;
        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let groups = self
            .list_groups_with_members(&conn, err.clone(), filter)
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        Ok(groups)
    }

    async fn replace_group(
        &self,
        group_id: &str,
        group_request: CreateGroupRequest,
    ) -> Result<StoredParts<Group>, ProviderStoreError> {
        let group_id: SiloGroupUuid = match group_id.parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(ProviderStoreError::StoreError(anyhow!(
                    "group id must be uuid"
                )));
            }
        };

        let authz_silo_group = authz::SiloGroup::new(
            self.authz_silo.clone(),
            group_id,
            LookupType::by_id(group_id),
        );
        self.opctx
            .authorize(authz::Action::Modify, &authz_silo_group)
            .await
            .map_err(external_error_to_provider_error)?;

        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let group = self
            .datastore
            .transaction_retry_wrapper("scim_replace_group")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let group_request = group_request.clone();

                async move {
                    self.replace_group_in_txn(
                        &conn,
                        err,
                        group_id,
                        group_request,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        Ok(group)
    }

    async fn delete_group_by_id(
        &self,
        group_id: &str,
    ) -> Result<ProviderStoreDeleteResult, ProviderStoreError> {
        let group_id: SiloGroupUuid = match group_id.parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(ProviderStoreError::StoreError(anyhow!(
                    "group id must be uuid"
                )));
            }
        };

        let authz_silo_group = authz::SiloGroup::new(
            self.authz_silo.clone(),
            group_id,
            LookupType::by_id(group_id),
        );
        self.opctx
            .authorize(authz::Action::Delete, &authz_silo_group)
            .await
            .map_err(external_error_to_provider_error)?;

        let conn = self
            .datastore
            .pool_connection_authorized(self.opctx)
            .await
            .map_err(|err| {
                ProviderStoreError::StoreError(anyhow!(
                    "Failed to access DB connection: {err}"
                ))
            })?;

        let err: OptionalError<ProviderStoreError> = OptionalError::new();

        let deleted_group = self
            .datastore
            .transaction_retry_wrapper("scim_delete_group_by_id")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    self.delete_group_by_id_in_txn(&conn, err, group_id).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(e) = err.take() {
                    e
                } else {
                    ProviderStoreError::StoreError(e.into())
                }
            })?;

        if deleted_group {
            Ok(ProviderStoreDeleteResult::Deleted)
        } else {
            Ok(ProviderStoreDeleteResult::NotFound)
        }
    }
}
