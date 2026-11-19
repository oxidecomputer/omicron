// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use nexus_auth::authn;
use nexus_auth::authn::Reason;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::ServiceAccount;
use nexus_db_model::ServiceAccountKey;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::policy;
use nexus_types::external_api::project;
use nexus_types::external_api::service_account as views;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::SiloUuid;
use std::collections::BTreeSet;
use uuid::Uuid;

impl super::Nexus {
    pub fn service_account_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        service_account: NameOrId,
        project: Option<NameOrId>,
    ) -> LookupResult<lookup::ServiceAccount<'a>> {
        match (service_account, project) {
            (NameOrId::Id(id), None) => {
                Ok(LookupPath::new(opctx, &self.db_datastore)
                    .service_account_id(id))
            }
            (NameOrId::Name(name), Some(project)) => Ok(self
                .project_lookup(opctx, project::ProjectSelector { project })?
                .service_account_name_owned(name.into())),
            (NameOrId::Id(_), Some(_)) => Err(Error::invalid_request(
                "when providing service_account as an ID, project should not \
                 be specified",
            )),
            (NameOrId::Name(_), None) => Err(Error::invalid_request(
                "service_account should either be UUID or project should be \
                 specified",
            )),
        }
    }

    pub fn service_account_key_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        service_account: NameOrId,
        key: NameOrId,
        project: Option<NameOrId>,
    ) -> LookupResult<lookup::ServiceAccountKey<'a>> {
        match key {
            NameOrId::Id(id) => Ok(LookupPath::new(opctx, &self.db_datastore)
                .service_account_key_id(id)),
            NameOrId::Name(name) => Ok(self
                .service_account_lookup(opctx, service_account, project)?
                .service_account_key_name_owned(name.into())),
        }
    }

    pub(crate) async fn service_account_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: views::ServiceAccountCreate,
    ) -> CreateResult<views::ServiceAccount> {
        let (authz_silo, authz_project) =
            project_lookup.lookup_for(authz::Action::Modify).await?;
        let service_account = ServiceAccount::new(
            authz_silo.id(),
            authz_project.id(),
            params.identity,
        );
        self.db_datastore
            .service_account_create(opctx, &authz_project, service_account)
            .await
            .map(Into::into)
    }

    pub(crate) async fn service_account_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::ServiceAccount> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        Ok(self
            .db_datastore
            .service_account_list(opctx, &authz_project, pagparams)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(crate) async fn service_account_view(
        &self,
        lookup: &lookup::ServiceAccount<'_>,
    ) -> LookupResult<views::ServiceAccount> {
        let (.., db_service_account) = lookup.fetch().await?;
        Ok(db_service_account.into())
    }

    pub(crate) async fn service_account_delete(
        &self,
        opctx: &OpContext,
        lookup: &lookup::ServiceAccount<'_>,
    ) -> DeleteResult {
        let (.., authz_service_account) =
            lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .service_account_delete(opctx, &authz_service_account)
            .await
    }

    pub(crate) async fn service_account_key_create(
        &self,
        opctx: &OpContext,
        lookup: &lookup::ServiceAccount<'_>,
        params: views::ServiceAccountKeyCreate,
    ) -> CreateResult<views::ServiceAccountKey> {
        let (.., authz_service_account) =
            lookup.lookup_for(authz::Action::CreateChild).await?;
        if let Some(authn::Actor::ServiceAccount { .. }) = opctx.authn.actor() {
            return Err(Error::Forbidden);
        }

        let verifying_key =
            oxide_http_signatures::verifying_key_from_pem(&params.public_key)
                .map_err(|e| {
                Error::invalid_request(format!(
                    "public_key must be an Ed25519 public key in PEM \
                         (SubjectPublicKeyInfo) format: {e}"
                ))
            })?;

        let time_expires = params.ttl_seconds.map(|ttl| {
            Utc::now() + chrono::Duration::seconds(ttl.get().into())
        });

        let key = ServiceAccountKey::new(
            authz_service_account.id(),
            IdentityMetadataCreateParams {
                name: params.identity.name,
                description: params.identity.description,
            },
            oxide_http_signatures::verifying_key_to_der(&verifying_key),
            time_expires,
        );
        self.db_datastore
            .service_account_key_create(opctx, &authz_service_account, key)
            .await
            .map(Into::into)
    }

    pub(crate) async fn service_account_key_list(
        &self,
        opctx: &OpContext,
        lookup: &lookup::ServiceAccount<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::ServiceAccountKey> {
        let (.., authz_service_account) =
            lookup.lookup_for(authz::Action::ListChildren).await?;
        Ok(self
            .db_datastore
            .service_account_key_list(opctx, &authz_service_account, pagparams)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(crate) async fn service_account_key_view(
        &self,
        lookup: &lookup::ServiceAccountKey<'_>,
    ) -> LookupResult<views::ServiceAccountKey> {
        let (.., db_key) = lookup.fetch().await?;
        Ok(db_key.into())
    }

    pub(crate) async fn service_account_key_delete(
        &self,
        opctx: &OpContext,
        lookup: &lookup::ServiceAccountKey<'_>,
    ) -> DeleteResult {
        let (.., authz_key) = lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore.service_account_key_delete(opctx, &authz_key).await
    }

    pub(crate) async fn service_account_key_for_authn(
        &self,
        opctx: &OpContext,
        key_id: Uuid,
    ) -> Result<
        (
            authn::Actor,
            oxide_http_signatures::VerifyingKey,
            Option<DateTime<Utc>>,
        ),
        Reason,
    > {
        let (authz_silo, _, authz_service_account, _, key) =
            LookupPath::new(opctx, &self.db_datastore)
                .service_account_key_id(key_id)
                .fetch()
                .await
                .map_err(|e| match e {
                    Error::ObjectNotFound { .. } => {
                        Reason::UnknownActor { actor: key_id.to_string() }
                    }
                    e => Reason::UnknownError { source: e },
                })?;
        let actor = authn::Actor::ServiceAccount {
            service_account_id: authz_service_account.id(),
            silo_id: authz_silo.id(),
        };
        if let Some(time_expires) = key.time_expires {
            if time_expires < Utc::now() {
                return Err(Reason::BadCredentials {
                    actor,
                    source: anyhow!(
                        "service account key expired at {time_expires}"
                    ),
                });
            }
        }
        let verifying_key =
            oxide_http_signatures::verifying_key_from_der(&key.public_key)
                .map_err(|e| Reason::BadCredentials {
                    actor,
                    source: anyhow!("stored public key is invalid: {e}"),
                })?;
        Ok((actor, verifying_key, key.time_expires))
    }

    pub(crate) fn reject_if_service_accounts_present(
        current: &[nexus_db_model::RoleAssignment],
    ) -> Result<(), Error> {
        if current.iter().any(|r| {
            r.identity_type == nexus_db_model::IdentityType::ServiceAccount
        }) {
            return Err(Error::invalid_request(
                "this policy includes service account role assignments, \
                 which this API version cannot represent; use API version \
                 2026111700.0.0 or later",
            ));
        }
        Ok(())
    }

    pub(crate) async fn check_service_account_grants<R>(
        &self,
        opctx: &OpContext,
        target_silo_id: Option<SiloUuid>,
        new: &[policy::RoleAssignment<R>],
    ) -> Result<(), Error> {
        let ids: BTreeSet<Uuid> = new
            .iter()
            .filter(|r| r.identity_type == policy::IdentityType::ServiceAccount)
            .map(|r| r.identity_id)
            .collect();
        for id in ids {
            let service_account = self
                .db_datastore
                .service_account_fetch_by_id(opctx, id)
                .await
                .map_err(|_| {
                    Error::invalid_request(format!(
                        "service account {id} does not exist"
                    ))
                })?;
            if let Some(target_silo_id) = target_silo_id {
                if service_account.silo_id() != target_silo_id {
                    return Err(Error::invalid_request(format!(
                        "service account {id} belongs to a different silo"
                    )));
                }
            }
        }
        Ok(())
    }
}
