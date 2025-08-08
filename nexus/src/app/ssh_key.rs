use crate::external_api::params;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::model::Name;
use nexus_db_queries::db::model::SshKey;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::SiloUserUuid;
use ref_cast::RefCast;

impl super::Nexus {
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
                    LookupPath::new(opctx, &self.db_datastore).ssh_key_id(*id);
                Ok(ssh_key)
            }
            params::SshKeySelector {
                silo_user_id,
                ssh_key: NameOrId::Name(name),
            } => {
                let ssh_key = LookupPath::new(opctx, &self.db_datastore)
                    .silo_user_id(*silo_user_id)
                    .ssh_key_name(Name::ref_cast(name));
                Ok(ssh_key)
            }
        }
    }

    pub(crate) async fn ssh_key_create(
        &self,
        opctx: &OpContext,
        silo_user_id: SiloUserUuid,
        params: params::SshKeyCreate,
    ) -> CreateResult<db::model::SshKey> {
        let ssh_key = db::model::SshKey::new(silo_user_id, params);
        let (.., authz_user) = LookupPath::new(opctx, self.datastore())
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        assert_eq!(authz_user.id(), silo_user_id);
        self.db_datastore.ssh_key_create(opctx, &authz_user, ssh_key).await
    }

    pub(crate) async fn ssh_keys_list(
        &self,
        opctx: &OpContext,
        silo_user_id: SiloUserUuid,
        page_params: &PaginatedBy<'_>,
    ) -> ListResultVec<SshKey> {
        let (.., authz_user) = LookupPath::new(opctx, self.datastore())
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        assert_eq!(authz_user.id(), silo_user_id);
        self.db_datastore.ssh_keys_list(opctx, &authz_user, page_params).await
    }

    pub(crate) async fn instance_ssh_keys_list(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        page_params: &PaginatedBy<'_>,
    ) -> ListResultVec<SshKey> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .instance_ssh_keys_list(opctx, &authz_instance, page_params)
            .await
    }

    pub(crate) async fn ssh_key_delete(
        &self,
        opctx: &OpContext,
        silo_user_id: SiloUserUuid,
        ssh_key_lookup: &lookup::SshKey<'_>,
    ) -> DeleteResult {
        let (.., authz_silo_user, authz_ssh_key) =
            ssh_key_lookup.lookup_for(authz::Action::Delete).await?;
        assert_eq!(authz_silo_user.id(), silo_user_id);
        self.db_datastore.ssh_key_delete(opctx, &authz_ssh_key).await
    }
}
