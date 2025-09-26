// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Built-ins and roles

use crate::external_api::shared;
use anyhow::Context;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::SiloGroup;
use nexus_db_queries::db::datastore::SiloUser;
use nexus_db_queries::db::model::Name;
use nexus_types::external_api::params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::BuiltInUserUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloGroupUuid;
use ref_cast::RefCast;
use uuid::Uuid;

impl super::Nexus {
    // Global (fleet-wide) policy

    pub(crate) async fn fleet_fetch_policy(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<shared::Policy<shared::FleetRole>> {
        let role_assignments = self
            .db_datastore
            .role_assignment_fetch_visible(opctx, &authz::FLEET)
            .await?
            .into_iter()
            .map(|r| r.try_into().context("parsing database role assignment"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;
        Ok(shared::Policy { role_assignments })
    }

    pub(crate) async fn fleet_update_policy(
        &self,
        opctx: &OpContext,
        policy: &shared::Policy<shared::FleetRole>,
    ) -> UpdateResult<shared::Policy<shared::FleetRole>> {
        let role_assignments = self
            .db_datastore
            .role_assignment_replace_visible(
                opctx,
                &authz::FLEET,
                &policy.role_assignments,
            )
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(shared::Policy { role_assignments })
    }

    // Silo users

    /// List users in the current Silo
    pub(crate) async fn silo_users_list_current(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloUser> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing current silo's users")?;
        let authz_silo_user_list = authz::SiloUserList::new(authz_silo.clone());

        self.db_datastore
            .silo_users_list(opctx, &authz_silo_user_list, pagparams)
            .await
    }

    /// List users in the current Silo, filtered by group ID
    pub(crate) async fn current_silo_group_users_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
        group_id: &SiloGroupUuid,
    ) -> ListResultVec<SiloUser> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing current silo's users")?;
        let authz_silo_user_list = authz::SiloUserList::new(authz_silo.clone());

        let (.., authz_group, _db_group) =
            LookupPath::new(opctx, &self.db_datastore)
                .silo_group_id(*group_id)
                .fetch()
                .await?;

        self.db_datastore
            .silo_group_users_list(
                opctx,
                &authz_silo_user_list,
                pagparams,
                &authz_group,
            )
            .await
    }

    /// Fetch the currently-authenticated Silo user
    pub(crate) async fn silo_user_fetch_self(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SiloUser> {
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("loading current user")?;
        let (.., db_silo_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_actor(&actor)?
            .fetch()
            .await?;
        Ok(db_silo_user.into())
    }

    pub(crate) async fn silo_user_fetch_groups_for_self(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloGroup> {
        self.db_datastore.silo_groups_for_self(opctx, pagparams).await
    }

    // Silo groups

    pub(crate) async fn silo_groups_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloGroup> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing current silo's groups")?;
        self.db_datastore
            .silo_groups_list_by_id(opctx, &authz_silo, pagparams)
            .await
    }

    // Built-in users

    pub(crate) async fn users_builtin_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::UserBuiltin> {
        self.db_datastore.users_builtin_list_by_name(opctx, pagparams).await
    }

    pub fn user_builtin_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        user_selector: &'a params::UserBuiltinSelector,
    ) -> LookupResult<lookup::UserBuiltin<'a>> {
        let lookup_path = LookupPath::new(opctx, &self.db_datastore);
        let user = match user_selector {
            params::UserBuiltinSelector { user: NameOrId::Id(id) } => {
                lookup_path
                    .user_builtin_id(BuiltInUserUuid::from_untyped_uuid(*id))
            }
            params::UserBuiltinSelector { user: NameOrId::Name(name) } => {
                lookup_path.user_builtin_name(Name::ref_cast(name))
            }
        };
        Ok(user)
    }
}
