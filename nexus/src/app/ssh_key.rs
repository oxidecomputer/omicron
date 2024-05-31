// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SSH keys

use crate::external_api::params;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::Name;
use nexus_db_queries::db::{self, lookup};
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use ref_cast::RefCast;
use std::sync::Arc;
use uuid::Uuid;

/// Application level functionality for SSH keys
#[derive(Clone)]
pub struct SshKey {
    datastore: Arc<db::DataStore>,
}

impl SshKey {
    pub fn new(datastore: Arc<db::DataStore>) -> SshKey {
        SshKey { datastore }
    }

    // SSH Keys
    pub fn ssh_key_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        ssh_key_selector: &'a params::SshKeySelector,
    ) -> LookupResult<lookup::SshKey<'a>> {
        match ssh_key_selector {
            params::SshKeySelector {
                silo_user_id: _,
                ssh_key: NameOrId::Id(id),
            } => {
                let ssh_key =
                    LookupPath::new(opctx, &self.datastore).ssh_key_id(*id);
                Ok(ssh_key)
            }
            params::SshKeySelector {
                silo_user_id,
                ssh_key: NameOrId::Name(name),
            } => {
                let ssh_key = LookupPath::new(opctx, &self.datastore)
                    .silo_user_id(*silo_user_id)
                    .ssh_key_name(Name::ref_cast(name));
                Ok(ssh_key)
            }
        }
    }

    pub(crate) async fn ssh_key_create(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        params: params::SshKeyCreate,
    ) -> CreateResult<db::model::SshKey> {
        let ssh_key = db::model::SshKey::new(silo_user_id, params);
        let (.., authz_user) = LookupPath::new(opctx, &self.datastore)
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        assert_eq!(authz_user.id(), silo_user_id);
        self.datastore.ssh_key_create(opctx, &authz_user, ssh_key).await
    }

    pub(crate) async fn ssh_keys_list(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        page_params: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::SshKey> {
        let (.., authz_user) = LookupPath::new(opctx, &self.datastore)
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        assert_eq!(authz_user.id(), silo_user_id);
        self.datastore.ssh_keys_list(opctx, &authz_user, page_params).await
    }

    pub(crate) async fn instance_ssh_keys_list(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        page_params: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::SshKey> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.datastore
            .instance_ssh_keys_list(opctx, &authz_instance, page_params)
            .await
    }

    pub(crate) async fn ssh_key_delete(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        ssh_key_lookup: &lookup::SshKey<'_>,
    ) -> DeleteResult {
        let (.., authz_silo_user, authz_ssh_key) =
            ssh_key_lookup.lookup_for(authz::Action::Delete).await?;
        assert_eq!(authz_silo_user.id(), silo_user_id);
        self.datastore.ssh_key_delete(opctx, &authz_ssh_key).await
    }
}
