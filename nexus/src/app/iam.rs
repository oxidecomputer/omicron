// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Built-ins and roles

use anyhow::Context;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::SiloGroup;
use nexus_db_queries::db::datastore::SiloUser;
use nexus_db_queries::db::model::Name;
use nexus_types::external_api::policy;
use nexus_types::external_api::user;
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
use omicron_uuid_kinds::SiloUserUuid;
use ref_cast::RefCast;
use std::collections::HashMap;
use uuid::Uuid;

impl super::Nexus {
    // Global (fleet-wide) policy

    pub(crate) async fn fleet_fetch_policy(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<policy::Policy<policy::FleetRole>> {
        let role_assignments = self
            .db_datastore
            .role_assignment_fetch_visible(opctx, &authz::FLEET)
            .await?
            .into_iter()
            .map(|r| r.try_into().context("parsing database role assignment"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;
        Ok(policy::Policy { role_assignments })
    }

    pub(crate) async fn fleet_update_policy(
        &self,
        opctx: &OpContext,
        policy: &policy::Policy<policy::FleetRole>,
    ) -> UpdateResult<policy::Policy<policy::FleetRole>> {
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
        Ok(policy::Policy { role_assignments })
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

    /// Fetch the group memberships of each of the given users, keyed by user
    /// ID, for embedding in `User` views. Each user's groups are sorted by
    /// (display_name, id).
    async fn silo_user_groups(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        users: &[SiloUser],
    ) -> Result<HashMap<SiloUserUuid, Vec<user::UserGroup>>, Error> {
        let user_ids: Vec<SiloUserUuid> =
            users.iter().map(|u| u.id()).collect();
        let mut groups_by_user: HashMap<SiloUserUuid, Vec<user::UserGroup>> =
            HashMap::new();
        for (user_id, group) in self
            .db_datastore
            .silo_groups_for_users(opctx, authz_silo, &user_ids)
            .await?
        {
            groups_by_user.entry(user_id).or_default().push(group.into());
        }
        for groups in groups_by_user.values_mut() {
            groups.sort_by(|a, b| {
                (&a.display_name, a.id).cmp(&(&b.display_name, b.id))
            });
        }
        Ok(groups_by_user)
    }

    /// Convert users to `User` views, embedding each user's group memberships
    pub(crate) async fn silo_users_into_views(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        users: Vec<SiloUser>,
    ) -> ListResultVec<user::User> {
        let mut groups_by_user =
            self.silo_user_groups(opctx, authz_silo, &users).await?;
        Ok(users
            .into_iter()
            .map(|u| {
                let groups = groups_by_user.remove(&u.id()).unwrap_or_default();
                u.into_view(groups)
            })
            .collect())
    }

    /// Like [`Self::silo_users_into_views`], for users in the current silo
    pub(crate) async fn silo_users_into_views_current(
        &self,
        opctx: &OpContext,
        users: Vec<SiloUser>,
    ) -> ListResultVec<user::User> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("converting silo users to views")?;
        self.silo_users_into_views(opctx, &authz_silo, users).await
    }

    /// Like [`Self::silo_users_into_views`], for a single user in the given
    /// silo
    pub(crate) async fn silo_user_into_view_by_silo(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        user: SiloUser,
    ) -> Result<user::User, Error> {
        let (authz_silo,) = silo_lookup.lookup_for(authz::Action::Read).await?;
        let mut views =
            self.silo_users_into_views(opctx, &authz_silo, vec![user]).await?;
        Ok(views.pop().expect("one user in, one view out"))
    }

    /// Like [`Self::silo_users_into_views_current`], for a single user
    pub(crate) async fn silo_user_into_view_current(
        &self,
        opctx: &OpContext,
        user: SiloUser,
    ) -> Result<user::User, Error> {
        let mut views =
            self.silo_users_into_views_current(opctx, vec![user]).await?;
        Ok(views.pop().expect("one user in, one view out"))
    }

    /// Like [`Self::silo_users_into_views`], for users in the given silo
    pub(crate) async fn silo_users_into_views_by_silo(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        users: Vec<SiloUser>,
    ) -> ListResultVec<user::User> {
        let (authz_silo,) = silo_lookup.lookup_for(authz::Action::Read).await?;
        self.silo_users_into_views(opctx, &authz_silo, users).await
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
        user_selector: &'a user::UserBuiltinSelector,
    ) -> LookupResult<lookup::UserBuiltin<'a>> {
        let lookup_path = LookupPath::new(opctx, &self.db_datastore);
        let user = match user_selector {
            user::UserBuiltinSelector { user: NameOrId::Id(id) } => lookup_path
                .user_builtin_id(BuiltInUserUuid::from_untyped_uuid(*id)),
            user::UserBuiltinSelector { user: NameOrId::Name(name) } => {
                lookup_path.user_builtin_name(Name::ref_cast(name))
            }
        };
        Ok(user)
    }
}
