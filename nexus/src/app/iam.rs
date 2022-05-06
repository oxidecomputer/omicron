// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Built-ins and roles

use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;

impl super::Nexus {
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
