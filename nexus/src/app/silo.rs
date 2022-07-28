// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silos, Users, and SSH Keys.

use std::sync::Arc;

use crate::context::OpContext;
use crate::db;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::SshKey;
use crate::external_api::params;
use crate::external_api::shared;
use crate::{authn, authz};
use anyhow::Context;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    // Silos

    pub async fn silo_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        new_silo_params: params::SiloCreate,
    ) -> CreateResult<db::model::Silo> {
        self.datastore().silo_create(&opctx, new_silo_params).await
    }

    pub async fn silos_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Silo> {
        self.db_datastore.silos_list_by_name(opctx, pagparams).await
    }

    pub async fn silos_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Silo> {
        self.db_datastore.silos_list_by_id(opctx, pagparams).await
    }

    pub async fn silo_fetch(
        &self,
        opctx: &OpContext,
        name: &Name,
    ) -> LookupResult<db::model::Silo> {
        let (.., db_silo) = LookupPath::new(opctx, &self.db_datastore)
            .silo_name(name)
            .fetch()
            .await?;
        Ok(db_silo)
    }

    pub async fn silo_delete(
        &self,
        opctx: &OpContext,
        name: &Name,
    ) -> DeleteResult {
        let (.., authz_silo, db_silo) =
            LookupPath::new(opctx, &self.db_datastore)
                .silo_name(name)
                .fetch_for(authz::Action::Delete)
                .await?;
        self.db_datastore.silo_delete(opctx, &authz_silo, &db_silo).await
    }

    // Role assignments

    pub async fn silo_fetch_policy(
        &self,
        opctx: &OpContext,
        silo_name: &Name,
    ) -> LookupResult<shared::Policy<authz::SiloRole>> {
        let (.., authz_silo) = LookupPath::new(opctx, &self.db_datastore)
            .silo_name(silo_name)
            .lookup_for(authz::Action::ReadPolicy)
            .await?;
        let role_assignments = self
            .db_datastore
            .role_assignment_fetch_visible(opctx, &authz_silo)
            .await?
            .into_iter()
            .map(|r| r.try_into().context("parsing database role assignment"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;
        Ok(shared::Policy { role_assignments })
    }

    pub async fn silo_update_policy(
        &self,
        opctx: &OpContext,
        silo_name: &Name,
        policy: &shared::Policy<authz::SiloRole>,
    ) -> UpdateResult<shared::Policy<authz::SiloRole>> {
        let (.., authz_silo) = LookupPath::new(opctx, &self.db_datastore)
            .silo_name(silo_name)
            .lookup_for(authz::Action::ModifyPolicy)
            .await?;

        let role_assignments = self
            .db_datastore
            .role_assignment_replace_visible(
                opctx,
                &authz_silo,
                &policy.role_assignments,
            )
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(shared::Policy { role_assignments })
    }

    // Users

    pub async fn silo_user_fetch(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> LookupResult<db::model::SiloUser> {
        let (.., db_silo_user) = LookupPath::new(opctx, &self.datastore())
            .silo_user_id(silo_user_id)
            .fetch()
            .await?;
        Ok(db_silo_user)
    }

    /// Based on an authenticated subject, fetch or create a silo user
    pub async fn silo_user_from_authenticated_subject(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        db_silo: &db::model::Silo,
        authenticated_subject: &authn::silos::AuthenticatedSubject,
    ) -> LookupResult<Option<db::model::SiloUser>> {
        // XXX create user permission?
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        let existing_silo_user = self
            .datastore()
            .silo_user_fetch_by_external_id(
                opctx,
                &authz_silo,
                &authenticated_subject.external_id,
            )
            .await?;

        let silo_user = if let Some(existing_silo_user) = existing_silo_user {
            existing_silo_user
        } else {
            // In this branch, no user exists for the authenticated subject
            // external id. The next action depends on the silo's user provision
            // type.
            match db_silo.user_provision_type {
                // If the user provision type is fixed, do not a new user if one
                // does not exist.
                db::model::UserProvisionType::Fixed => {
                    return Ok(None);
                }

                // If the user provision type is JIT, then create the user if
                // one does not exist
                db::model::UserProvisionType::Jit => {
                    let silo_user = db::model::SiloUser::new(
                        authz_silo.id(),
                        Uuid::new_v4(),
                        authenticated_subject.external_id.clone(),
                    );

                    self.db_datastore.silo_user_create(silo_user).await?
                }
            }
        };

        // Gather a list of groups that the user is part of based on what the
        // IdP sent us. Also, if the silo user provision type is Jit, create
        // silo groups if new groups from the IdP are seen.

        let mut silo_user_group_ids: Vec<Uuid> =
            Vec::with_capacity(authenticated_subject.groups.len());

        for group in &authenticated_subject.groups {
            let silo_group = match db_silo.user_provision_type {
                db::model::UserProvisionType::Fixed => {
                    self.db_datastore
                        .silo_group_optional_lookup(
                            opctx,
                            &authz_silo,
                            group.clone(),
                        )
                        .await?
                }

                db::model::UserProvisionType::Jit => {
                    let silo_group = self
                        .silo_group_lookup_or_create_by_name(
                            opctx,
                            &authz_silo,
                            &group,
                        )
                        .await?;

                    Some(silo_group)
                }
            };

            if let Some(silo_group) = silo_group {
                silo_user_group_ids.push(silo_group.id());
            }
        }

        // Update the user's group memberships

        self.db_datastore
            .silo_group_membership_replace_for_user(
                opctx,
                silo_user.id(),
                silo_user_group_ids,
            )
            .await?;

        Ok(Some(silo_user))
    }

    // Silo groups

    pub async fn silo_group_lookup_or_create_by_name(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        external_id: &String,
    ) -> LookupResult<db::model::SiloGroup> {
        match self
            .db_datastore
            .silo_group_optional_lookup(opctx, authz_silo, external_id.clone())
            .await?
        {
            Some(silo_group) => Ok(silo_group),

            None => {
                self.db_datastore
                    .silo_group_create(
                        opctx,
                        authz_silo,
                        db::model::SiloGroup::new(
                            Uuid::new_v4(),
                            authz_silo.id(),
                            external_id.clone(),
                        ),
                    )
                    .await
            }
        }
    }

    // SSH Keys

    pub async fn ssh_key_create(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        params: params::SshKeyCreate,
    ) -> CreateResult<db::model::SshKey> {
        let ssh_key = db::model::SshKey::new(silo_user_id, params);
        let (.., authz_user) = LookupPath::new(opctx, &self.datastore())
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        assert_eq!(authz_user.id(), silo_user_id);
        self.db_datastore.ssh_key_create(opctx, &authz_user, ssh_key).await
    }

    pub async fn ssh_keys_list(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        page_params: &DataPageParams<'_, Name>,
    ) -> ListResultVec<SshKey> {
        let (.., authz_user) = LookupPath::new(opctx, &self.datastore())
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        assert_eq!(authz_user.id(), silo_user_id);
        self.db_datastore.ssh_keys_list(opctx, &authz_user, page_params).await
    }

    pub async fn ssh_key_fetch(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        ssh_key_name: &Name,
    ) -> LookupResult<SshKey> {
        let (.., ssh_key) = LookupPath::new(opctx, &self.datastore())
            .silo_user_id(silo_user_id)
            .ssh_key_name(ssh_key_name)
            .fetch()
            .await?;
        assert_eq!(ssh_key.name(), &ssh_key_name.0);
        Ok(ssh_key)
    }

    pub async fn ssh_key_delete(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        ssh_key_name: &Name,
    ) -> DeleteResult {
        let (.., authz_user, authz_ssh_key) =
            LookupPath::new(opctx, &self.datastore())
                .silo_user_id(silo_user_id)
                .ssh_key_name(ssh_key_name)
                .lookup_for(authz::Action::Delete)
                .await?;
        assert_eq!(authz_user.id(), silo_user_id);
        self.db_datastore.ssh_key_delete(opctx, &authz_ssh_key).await
    }

    // identity providers

    pub async fn identity_provider_list(
        &self,
        opctx: &OpContext,
        silo_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::IdentityProvider> {
        let (authz_silo, ..) = LookupPath::new(opctx, &self.db_datastore)
            .silo_name(silo_name)
            .fetch()
            .await?;
        self.db_datastore
            .identity_provider_list(opctx, &authz_silo, pagparams)
            .await
    }

    // Silo authn identity providers

    pub async fn saml_identity_provider_create(
        &self,
        opctx: &OpContext,
        silo_name: &Name,
        params: params::SamlIdentityProviderCreate,
    ) -> CreateResult<db::model::SamlIdentityProvider> {
        let (authz_silo, db_silo) = LookupPath::new(opctx, &self.db_datastore)
            .silo_name(silo_name)
            .fetch_for(authz::Action::CreateChild)
            .await?;

        let idp_metadata_document_string = match &params.idp_metadata_source {
            params::IdpMetadataSource::Url { url } => {
                // Download the SAML IdP descriptor, and write it into the DB. This is
                // so that it can be deserialized later.
                //
                // Importantly, do this only once and store it. It would introduce
                // attack surface to download it each time it was required.
                let dur = std::time::Duration::from_secs(5);
                let client = reqwest::ClientBuilder::new()
                    .connect_timeout(dur)
                    .timeout(dur)
                    .build()
                    .map_err(|e| {
                        Error::internal_error(&format!(
                            "failed to build reqwest client: {}",
                            e
                        ))
                    })?;

                let response = client.get(url).send().await.map_err(|e| {
                    Error::InvalidValue {
                        label: String::from("url"),
                        message: format!("error querying url: {}", e),
                    }
                })?;

                if !response.status().is_success() {
                    return Err(Error::InvalidValue {
                        label: String::from("url"),
                        message: format!(
                            "querying url returned: {}",
                            response.status()
                        ),
                    });
                }

                response.text().await.map_err(|e| Error::InvalidValue {
                    label: String::from("url"),
                    message: format!("error getting text from url: {}", e),
                })?
            }

            params::IdpMetadataSource::Base64EncodedXml { data } => {
                let bytes =
                    base64::decode(data).map_err(|e| Error::InvalidValue {
                        label: String::from("data"),
                        message: format!(
                            "error getting decoding base64 data: {}",
                            e
                        ),
                    })?;
                String::from_utf8_lossy(&bytes).into_owned()
            }
        };

        let provider = db::model::SamlIdentityProvider {
            identity: db::model::SamlIdentityProviderIdentity::new(
                Uuid::new_v4(),
                params.identity,
            ),
            silo_id: db_silo.id(),

            idp_metadata_document_string,

            idp_entity_id: params.idp_entity_id,
            sp_client_id: params.sp_client_id,
            acs_url: params.acs_url,
            slo_url: params.slo_url,
            technical_contact_email: params.technical_contact_email,
            public_cert: params
                .signing_keypair
                .as_ref()
                .map(|x| x.public_cert.clone()),
            private_key: params
                .signing_keypair
                .as_ref()
                .map(|x| x.private_key.clone()),

            group_attribute_name: params.group_attribute_name.clone(),
        };

        let _authn_provider: authn::silos::SamlIdentityProvider =
            provider.clone().try_into().map_err(|e: anyhow::Error|
                // If an error is encountered converting from the model to the
                // authn type here, this is a request error: something about the
                // parameters of this request doesn't work.
                Error::invalid_request(&e.to_string()))?;

        self.db_datastore
            .saml_identity_provider_create(opctx, &authz_silo, provider)
            .await
    }

    pub async fn saml_identity_provider_fetch(
        &self,
        opctx: &OpContext,
        silo_name: &Name,
        provider_name: &Name,
    ) -> LookupResult<db::model::SamlIdentityProvider> {
        let (.., saml_identity_provider) =
            LookupPath::new(opctx, &self.datastore())
                .silo_name(silo_name)
                .saml_identity_provider_name(provider_name)
                .fetch()
                .await?;
        Ok(saml_identity_provider)
    }
}
