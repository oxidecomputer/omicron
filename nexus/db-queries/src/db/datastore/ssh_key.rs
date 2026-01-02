// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SshKey`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Resource;
use crate::db::model::Name;
use crate::db::model::SshKey;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use ref_cast::RefCast;
use uuid::Uuid;

impl DataStore {
    /// Resolves a list of names or IDs to a list of IDs that are validated to
    /// both exist and be owned by the current user.
    pub async fn ssh_keys_batch_lookup(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        keys: &Vec<NameOrId>,
    ) -> ListResultVec<Uuid> {
        opctx.authorize(authz::Action::ListChildren, authz_user).await?;

        let mut names: Vec<Name> = vec![];
        let mut ids: Vec<Uuid> = vec![];

        for key in keys.iter() {
            match key {
                NameOrId::Name(name) => names.push(name.clone().into()),
                NameOrId::Id(id) => ids.push(*id),
            }
        }

        use nexus_db_schema::schema::ssh_key::dsl;
        let result: Vec<(Uuid, Name)> = dsl::ssh_key
            .filter(dsl::id.eq_any(ids).or(dsl::name.eq_any(names)))
            .filter(dsl::silo_user_id.eq(to_db_typed_uuid(authz_user.id())))
            .filter(dsl::time_deleted.is_null())
            .select((dsl::id, dsl::name))
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // If a key isn't present in the result that was present in the input that means it either
        // doesn't exist or isn't owned by the user. Either way we want to give a specific lookup error
        // for at least the first result. It would be nice to include an aggregate error with all the missing
        // keys.
        for key in keys.iter() {
            match key {
                NameOrId::Name(name) => {
                    if !result
                        .iter()
                        .any(|(_, n)| n.clone() == name.clone().into())
                    {
                        return Err(Error::ObjectNotFound {
                            type_name: ResourceType::SshKey,
                            lookup_type: LookupType::ByName(name.to_string()),
                        });
                    }
                }
                NameOrId::Id(id) => {
                    if !result.iter().any(|&(i, _)| i == *id) {
                        return Err(Error::ObjectNotFound {
                            type_name: ResourceType::SshKey,
                            lookup_type: LookupType::ById(*id),
                        });
                    }
                }
            }
        }

        return Ok(result.iter().map(|&(id, _)| id).collect());
    }

    /// Given a list of IDs for SSH public keys, fetches the keys that belong to
    /// the user and aren't deleted. Does not fail if keys are missing.
    pub async fn ssh_keys_batch_fetch(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        keys: &Vec<Uuid>,
    ) -> ListResultVec<SshKey> {
        opctx.authorize(authz::Action::ListChildren, authz_user).await?;

        use nexus_db_schema::schema::ssh_key::dsl;
        dsl::ssh_key
            .filter(dsl::id.eq_any(keys.to_owned()))
            .filter(dsl::silo_user_id.eq(to_db_typed_uuid(authz_user.id())))
            .filter(dsl::time_deleted.is_null())
            .select(SshKey::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    // Associate a list of SSH public keys with an instance. This happens
    // during the instance create saga and does not fail if the the ssh keys
    // have been deleted as a race condition.
    pub async fn ssh_keys_batch_assign(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        instance_id: InstanceUuid,
        keys: &Option<Vec<Uuid>>,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::ListChildren, authz_user).await?;

        let instance_ssh_keys: Vec<db::model::InstanceSshKey> = match keys {
            // If the keys are None, use the fallback behavior of assigning all the users keys
            None => {
                use nexus_db_schema::schema::ssh_key::dsl;
                dsl::ssh_key
                    .filter(
                        dsl::silo_user_id.eq(to_db_typed_uuid(authz_user.id())),
                    )
                    .filter(dsl::time_deleted.is_null())
                    .select(dsl::id)
                    .get_results_async(
                        &*self.pool_connection_authorized(opctx).await?,
                    )
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?
                    .iter()
                    .map(|key| db::model::InstanceSshKey {
                        instance_id: instance_id.into_untyped_uuid(),
                        ssh_key_id: *key,
                    })
                    .collect()
            }
            // If the keys are Some and empty, opt out of assigning any ssh keys
            Some(vec) if vec.is_empty() => return Ok(()),
            // If the keys are Some and not-empty, assign the given keys
            Some(vec) => vec
                .iter()
                .map(|key| db::model::InstanceSshKey {
                    instance_id: instance_id.into_untyped_uuid(),
                    ssh_key_id: *key,
                })
                .collect(),
        };

        use nexus_db_schema::schema::instance_ssh_key::dsl;

        diesel::insert_into(dsl::instance_ssh_key)
            .values(instance_ssh_keys)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn instance_ssh_keys_list(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SshKey> {
        use nexus_db_schema::schema::instance_ssh_key::dsl as inst_dsl;
        use nexus_db_schema::schema::ssh_key::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::ssh_key, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::ssh_key,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .inner_join(
            nexus_db_schema::schema::instance_ssh_key::table
                .on(dsl::id.eq(inst_dsl::ssh_key_id)),
        )
        .filter(inst_dsl::instance_id.eq(authz_instance.id()))
        .filter(dsl::time_deleted.is_null())
        .select(SshKey::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn instance_ssh_keys_delete(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::instance_ssh_key::dsl;
        diesel::delete(dsl::instance_ssh_key)
            .filter(dsl::instance_id.eq(instance_id.into_untyped_uuid()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    pub async fn ssh_keys_list(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SshKey> {
        opctx.authorize(authz::Action::ListChildren, authz_user).await?;

        use nexus_db_schema::schema::ssh_key::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::ssh_key, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::ssh_key,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::silo_user_id.eq(to_db_typed_uuid(authz_user.id())))
        .filter(dsl::time_deleted.is_null())
        .select(SshKey::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Create a new SSH public key for a user.
    pub async fn ssh_key_create(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        ssh_key: SshKey,
    ) -> CreateResult<SshKey> {
        assert_eq!(authz_user.id(), ssh_key.silo_user_id());
        opctx.authorize(authz::Action::CreateChild, authz_user).await?;
        let name = ssh_key.name().to_string();

        use nexus_db_schema::schema::ssh_key::dsl;
        diesel::insert_into(dsl::ssh_key)
            .values(ssh_key)
            .returning(SshKey::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::SshKey, &name),
                )
            })
    }

    /// Delete an existing SSH public key.
    pub async fn ssh_key_delete(
        &self,
        opctx: &OpContext,
        authz_ssh_key: &authz::SshKey,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_ssh_key).await?;

        use nexus_db_schema::schema::ssh_key::dsl;
        diesel::update(dsl::ssh_key)
            .filter(dsl::id.eq(authz_ssh_key.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<SshKey>(authz_ssh_key.id())
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_ssh_key),
                )
            })?;
        Ok(())
    }
}
