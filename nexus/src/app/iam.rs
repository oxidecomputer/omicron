// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Built-ins and roles

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::shared;
use anyhow::Context;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    // Global (fleet-wide) policy

    pub async fn fleet_fetch_policy(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<shared::Policy<authz::FleetRole>> {
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

    pub async fn fleet_update_policy(
        &self,
        opctx: &OpContext,
        policy: &shared::Policy<authz::FleetRole>,
    ) -> UpdateResult<shared::Policy<authz::FleetRole>> {
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
    pub async fn silo_users_list_current(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SiloUser> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing current silo's users")?;
        let authz_silo_user_list = authz::SiloUserList::new(authz_silo.clone());
        self.db_datastore
            .silo_users_list_by_id(opctx, &authz_silo_user_list, pagparams)
            .await
    }

    /// Fetch the currently-authenticated Silo user
    pub async fn silo_user_fetch_self(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::SiloUser> {
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("loading current user")?;
        let (.., db_silo_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(actor.actor_id())
            .fetch()
            .await?;
        Ok(db_silo_user)
    }

    // Built-in users

    pub async fn users_builtin_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::UserBuiltin> {
        self.db_datastore.users_builtin_list_by_name(opctx, pagparams).await
    }

    pub async fn user_builtin_fetch(
        &self,
        opctx: &OpContext,
        name: &Name,
    ) -> LookupResult<db::model::UserBuiltin> {
        let (.., db_user_builtin) = LookupPath::new(opctx, &self.db_datastore)
            .user_builtin_name(name)
            .fetch()
            .await?;
        Ok(db_user_builtin)
    }

    // Built-in roles

    pub async fn roles_builtin_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (String, String)>,
    ) -> ListResultVec<db::model::RoleBuiltin> {
        self.db_datastore.roles_builtin_list_by_name(opctx, pagparams).await
    }

    pub async fn role_builtin_fetch(
        &self,
        opctx: &OpContext,
        name: &str,
    ) -> LookupResult<db::model::RoleBuiltin> {
        let (.., db_role_builtin) = LookupPath::new(opctx, &self.db_datastore)
            .role_builtin_name(name)
            .fetch()
            .await?;
        Ok(db_role_builtin)
    }
}
