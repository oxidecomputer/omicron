// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silos, Users, and SSH Keys.

use crate::external_api::params;
use crate::external_api::shared;
use anyhow::Context;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::SiloAuthSettings;
use nexus_db_model::{DnsGroup, UserProvisionType};
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::Discoverability;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_db_queries::{authn, authz};
use nexus_types::deployment::execution::blueprint_nexus_external_ips;
use nexus_types::internal_api::params::DnsRecord;
use nexus_types::silo::silo_dns_name;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{CreateResult, LookupType};
use omicron_common::api::external::{DataPageParams, ResourceType};
use omicron_common::api::external::{DeleteResult, NameOrId};
use omicron_common::api::external::{Error, InternalContext};
use omicron_common::bail_unless;
use std::net::IpAddr;
use std::str::FromStr;
use uuid::Uuid;

impl super::Nexus {
    // Silos
    pub fn current_silo_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
    ) -> LookupResult<lookup::Silo<'a>> {
        let silo = opctx
            .authn
            .silo_required()
            .internal_context("looking up current silo")?;
        let silo = self.silo_lookup(opctx, NameOrId::Id(silo.id()))?;
        Ok(silo)
    }
    pub fn silo_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        silo: NameOrId,
    ) -> LookupResult<lookup::Silo<'a>> {
        match silo {
            NameOrId::Id(id) => {
                let silo =
                    LookupPath::new(opctx, &self.db_datastore).silo_id(id);
                Ok(silo)
            }
            NameOrId::Name(name) => {
                let silo = LookupPath::new(opctx, &self.db_datastore)
                    .silo_name_owned(name.into());
                Ok(silo)
            }
        }
    }

    pub(crate) async fn silo_fq_dns_names(
        &self,
        opctx: &OpContext,
        silo_id: Uuid,
    ) -> ListResultVec<String> {
        let (_, silo) =
            self.silo_lookup(opctx, silo_id.into())?.fetch().await?;
        let silo_dns_name = silo_dns_name(&silo.name());
        let external_dns_zones = self
            .db_datastore
            .dns_zones_list_all(opctx, nexus_db_model::DnsGroup::External)
            .await?;

        Ok(external_dns_zones
            .into_iter()
            .map(|zone| format!("{silo_dns_name}.{}", zone.zone_name))
            .collect())
    }

    pub(crate) async fn silo_create(
        &self,
        opctx: &OpContext,
        new_silo_params: params::SiloCreate,
    ) -> CreateResult<db::model::Silo> {
        // Silo creation involves several operations that ordinary users cannot
        // generally do, like reading and modifying the fleet-wide external DNS
        // config.  Nexus assumes its own identity to do these operations in
        // this (very specific) context.
        let nexus_opctx = self.opctx_external_authn();
        let datastore = self.datastore();

        // Set up an external DNS name for this Silo's API and console
        // endpoints (which are the same endpoint).
        let nexus_external_dns_zones = datastore
            .dns_zones_list_all(nexus_opctx, DnsGroup::External)
            .await
            .internal_context("listing external DNS zones")?;
        let (_, target_blueprint) = datastore
            .blueprint_target_get_current_full(opctx)
            .await
            .internal_context("loading target blueprint")?;
        let nexus_external_ips =
            blueprint_nexus_external_ips(&target_blueprint);
        let dns_records: Vec<DnsRecord> = nexus_external_ips
            .into_iter()
            .map(|addr| match addr {
                IpAddr::V4(addr) => DnsRecord::A(addr),
                IpAddr::V6(addr) => DnsRecord::Aaaa(addr),
            })
            .collect();

        let silo_name = &new_silo_params.identity.name;
        let mut dns_update = DnsVersionUpdateBuilder::new(
            DnsGroup::External,
            format!("create silo: {:?}", silo_name.as_str()),
            self.id.to_string(),
        );
        let silo_dns_name = silo_dns_name(silo_name);
        let new_silo_dns_names = nexus_external_dns_zones
            .into_iter()
            .map(|zone| format!("{silo_dns_name}.{}", zone.zone_name))
            .collect::<Vec<_>>();

        dns_update.add_name(silo_dns_name, dns_records)?;

        let silo = datastore
            .silo_create(
                &opctx,
                &nexus_opctx,
                new_silo_params,
                &new_silo_dns_names,
                dns_update,
            )
            .await?;
        self.background_tasks
            .activate(&self.background_tasks.task_external_dns_config);
        self.background_tasks
            .activate(&self.background_tasks.task_external_endpoints);
        Ok(silo)
    }

    pub(crate) async fn silos_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Silo> {
        self.db_datastore
            .silos_list(opctx, pagparams, Discoverability::DiscoverableOnly)
            .await
    }

    pub(crate) async fn silo_delete(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> DeleteResult {
        let dns_opctx = self.opctx_external_authn();
        let datastore = self.datastore();
        let (.., authz_silo, db_silo) =
            silo_lookup.fetch_for(authz::Action::Delete).await?;
        let mut dns_update = DnsVersionUpdateBuilder::new(
            DnsGroup::External,
            format!("delete silo: {:?}", db_silo.name()),
            self.id.to_string(),
        );
        dns_update.remove_name(silo_dns_name(&db_silo.name()))?;
        datastore
            .silo_delete(opctx, &authz_silo, &db_silo, dns_opctx, dns_update)
            .await?;
        self.background_tasks
            .activate(&self.background_tasks.task_external_dns_config);
        self.background_tasks
            .activate(&self.background_tasks.task_external_endpoints);
        Ok(())
    }

    // Role assignments

    pub(crate) async fn silo_fetch_policy(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> LookupResult<shared::Policy<shared::SiloRole>> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::ReadPolicy).await?;
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

    pub(crate) async fn silo_update_policy(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        policy: &shared::Policy<shared::SiloRole>,
    ) -> UpdateResult<shared::Policy<shared::SiloRole>> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::ModifyPolicy).await?;

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

    pub(crate) async fn silo_fetch_auth_settings(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> LookupResult<SiloAuthSettings> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        self.db_datastore.silo_auth_settings_view(opctx, &authz_silo).await
    }

    pub(crate) async fn silo_update_auth_settings(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        settings: &params::SiloAuthSettingsUpdate,
    ) -> UpdateResult<SiloAuthSettings> {
        // TODO: modify seems fine, but look into why policy has its own
        // separate permission
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .silo_auth_settings_update(
                opctx,
                &authz_silo,
                settings.clone().into(),
            )
            .await
    }

    // Users

    /// Helper function for looking up a user in a Silo
    ///
    /// `LookupPath` lets you look up users directly, regardless of what Silo
    /// they're in.  This helper validates that they're in the expected Silo.
    async fn silo_user_lookup_by_id(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_user_id: Uuid,
        action: authz::Action,
    ) -> LookupResult<(authz::SiloUser, db::model::SiloUser)> {
        let (_, authz_silo_user, db_silo_user) =
            LookupPath::new(opctx, self.datastore())
                .silo_user_id(silo_user_id)
                .fetch_for(action)
                .await?;
        if db_silo_user.silo_id != authz_silo.id() {
            return Err(authz_silo_user.not_found());
        }

        Ok((authz_silo_user, db_silo_user))
    }

    /// List the users in a Silo
    pub(crate) async fn silo_list_users(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SiloUser> {
        let (authz_silo,) = silo_lookup.lookup_for(authz::Action::Read).await?;
        let authz_silo_user_list = authz::SiloUserList::new(authz_silo);
        self.db_datastore
            .silo_users_list(opctx, &authz_silo_user_list, pagparams)
            .await
    }

    /// Fetch a user in a Silo
    pub(crate) async fn silo_user_fetch(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        silo_user_id: Uuid,
    ) -> LookupResult<db::model::SiloUser> {
        let (authz_silo,) = silo_lookup.lookup_for(authz::Action::Read).await?;
        let (_, db_silo_user) = self
            .silo_user_lookup_by_id(
                opctx,
                &authz_silo,
                silo_user_id,
                authz::Action::Read,
            )
            .await?;
        Ok(db_silo_user)
    }

    /// Delete all of user's tokens and sessions
    pub(crate) async fn current_silo_user_logout(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> UpdateResult<()> {
        let (_, authz_silo_user, _) = LookupPath::new(opctx, self.datastore())
            .silo_user_id(silo_user_id)
            .fetch()
            .await?;

        let authz_token_list =
            authz::SiloUserTokenList::new(authz_silo_user.clone());
        self.datastore()
            .silo_user_tokens_delete(opctx, &authz_token_list)
            .await?;

        let authz_authn_list =
            authz::SiloUserSessionList::new(authz_silo_user.clone());
        self.datastore()
            .silo_user_sessions_delete(opctx, &authz_authn_list)
            .await?;

        Ok(())
    }

    /// Fetch a user in a Silo
    pub(crate) async fn current_silo_user_lookup(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> LookupResult<(authz::SiloUser, db::model::SiloUser)> {
        let (_, authz_silo_user, db_silo_user) =
            LookupPath::new(opctx, self.datastore())
                .silo_user_id(silo_user_id)
                .fetch()
                .await?;

        Ok((authz_silo_user, db_silo_user))
    }

    /// List device access tokens for a user in a Silo
    pub(crate) async fn silo_user_token_list(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::DeviceAccessToken> {
        let (_, authz_silo_user, _db_silo_user) =
            LookupPath::new(opctx, self.datastore())
                .silo_user_id(silo_user_id)
                .fetch()
                .await?;

        let authz_token_list = authz::SiloUserTokenList::new(authz_silo_user);

        self.datastore()
            .silo_user_token_list(opctx, authz_token_list, pagparams)
            .await
    }

    /// List console sessions for a user in a Silo
    pub(crate) async fn silo_user_session_list(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::ConsoleSession> {
        let (_, authz_silo_user, _db_silo_user) =
            LookupPath::new(opctx, self.datastore())
                .silo_user_id(silo_user_id)
                .fetch()
                .await?;

        let user_authn_list = authz::SiloUserSessionList::new(authz_silo_user);

        self.datastore()
            .silo_user_session_list(opctx, user_authn_list, pagparams)
            .await
    }

    // The "local" identity provider (available only in `LocalOnly` Silos)

    /// Helper function for looking up a LocalOnly Silo by name
    ///
    /// This is called from contexts that are trying to access the "local"
    /// identity provider.  On failure, it returns a 404 for that identity
    /// provider.
    async fn local_idp_fetch_silo(
        &self,
        silo_lookup: &lookup::Silo<'_>,
    ) -> LookupResult<(authz::Silo, db::model::Silo)> {
        let (authz_silo, db_silo) = silo_lookup.fetch().await?;
        if db_silo.user_provision_type != UserProvisionType::ApiOnly {
            return Err(Error::not_found_by_name(
                ResourceType::IdentityProvider,
                &omicron_common::api::external::Name::from_str("local")
                    .unwrap(),
            ));
        }
        Ok((authz_silo, db_silo))
    }

    /// Create a user in a Silo's local identity provider
    pub(crate) async fn local_idp_create_user(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        new_user_params: params::UserCreate,
    ) -> CreateResult<db::model::SiloUser> {
        let (authz_silo, db_silo) =
            self.local_idp_fetch_silo(silo_lookup).await?;
        let authz_silo_user_list = authz::SiloUserList::new(authz_silo.clone());
        // TODO-cleanup This authz check belongs in silo_user_create().
        opctx
            .authorize(authz::Action::CreateChild, &authz_silo_user_list)
            .await?;
        let silo_user = db::model::SiloUser::new(
            authz_silo.id(),
            Uuid::new_v4(),
            new_user_params.external_id.as_ref().to_owned(),
        );
        // TODO These two steps should happen in a transaction.
        let (_, db_silo_user) =
            self.datastore().silo_user_create(&authz_silo, silo_user).await?;
        let authz_silo_user = authz::SiloUser::new(
            authz_silo.clone(),
            db_silo_user.id(),
            LookupType::ById(db_silo_user.id()),
        );
        self.silo_user_password_set_internal(
            opctx,
            &db_silo,
            &authz_silo_user,
            &db_silo_user,
            new_user_params.password,
        )
        .await?;
        Ok(db_silo_user)
    }

    /// Delete a user in a Silo's local identity provider
    pub(crate) async fn local_idp_delete_user(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        silo_user_id: Uuid,
    ) -> DeleteResult {
        let (authz_silo, _) = self.local_idp_fetch_silo(silo_lookup).await?;

        let (authz_silo_user, _) = self
            .silo_user_lookup_by_id(
                opctx,
                &authz_silo,
                silo_user_id,
                authz::Action::Delete,
            )
            .await?;
        self.db_datastore.silo_user_delete(opctx, &authz_silo_user).await
    }

    /// Based on an authenticated subject, fetch or create a silo user
    pub async fn silo_user_from_authenticated_subject(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        db_silo: &db::model::Silo,
        authenticated_subject: &authn::silos::AuthenticatedSubject,
    ) -> LookupResult<db::model::SiloUser> {
        // XXX create user permission?
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        let fetch_result = self
            .datastore()
            .silo_user_fetch_by_external_id(
                opctx,
                &authz_silo,
                &authenticated_subject.external_id,
            )
            .await?;

        let (authz_silo_user, db_silo_user) = if let Some(existing_silo_user) =
            fetch_result
        {
            existing_silo_user
        } else {
            // In this branch, no user exists for the authenticated subject
            // external id. The next action depends on the silo's user
            // provision type.
            match db_silo.user_provision_type {
                // If the user provision type is ApiOnly, do not create a
                // new user if one does not exist.
                db::model::UserProvisionType::ApiOnly => {
                    return Err(Error::Unauthenticated {
                            internal_message: "User must exist before login when user provision type is ApiOnly".to_string(),
                    });
                }

                // If the user provision type is JIT, then create the user if
                // one does not exist
                db::model::UserProvisionType::Jit => {
                    let silo_user = db::model::SiloUser::new(
                        authz_silo.id(),
                        Uuid::new_v4(),
                        authenticated_subject.external_id.clone(),
                    );

                    self.db_datastore
                        .silo_user_create(&authz_silo, silo_user)
                        .await?
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
                db::model::UserProvisionType::ApiOnly => {
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
                &authz_silo_user,
                silo_user_group_ids,
            )
            .await?;

        Ok(db_silo_user)
    }

    // Silo user passwords

    /// Set or invalidate a Silo user's password
    ///
    /// If `password` is `UserPassword::Password`, the password is set to the
    /// requested value.  Otherwise, any existing password is invalidated so
    /// that it cannot be used for authentication any more.
    pub(crate) async fn local_idp_user_set_password(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        silo_user_id: Uuid,
        password_value: params::UserPassword,
    ) -> UpdateResult<()> {
        let (authz_silo, db_silo) =
            self.local_idp_fetch_silo(silo_lookup).await?;
        let (authz_silo_user, db_silo_user) = self
            .silo_user_lookup_by_id(
                opctx,
                &authz_silo,
                silo_user_id,
                authz::Action::Modify,
            )
            .await?;
        self.silo_user_password_set_internal(
            opctx,
            &db_silo,
            &authz_silo_user,
            &db_silo_user,
            password_value,
        )
        .await
    }

    /// Internal helper for setting a user's password
    ///
    /// The caller should have already verified that this is a `LocalOnly` Silo
    /// and that the specified user is in that Silo.
    async fn silo_user_password_set_internal(
        &self,
        opctx: &OpContext,
        db_silo: &db::model::Silo,
        authz_silo_user: &authz::SiloUser,
        db_silo_user: &db::model::SiloUser,
        password_value: params::UserPassword,
    ) -> UpdateResult<()> {
        let password_hash = match password_value {
            params::UserPassword::LoginDisallowed => None,
            params::UserPassword::Password(password) => {
                let mut hasher = omicron_passwords::Hasher::default();
                let password_hash = hasher
                    .create_password(password.as_ref())
                    .map_err(|e| {
                    Error::internal_error(&format!("setting password: {:#}", e))
                })?;
                Some(db::model::SiloUserPasswordHash::new(
                    authz_silo_user.id(),
                    nexus_db_model::PasswordHashString::from(password_hash),
                ))
            }
        };

        self.datastore()
            .silo_user_password_hash_set(
                opctx,
                db_silo,
                authz_silo_user,
                db_silo_user,
                password_hash,
            )
            .await
    }

    /// Verify a Silo user's password
    ///
    /// To prevent timing attacks that would allow an attacker to learn the
    /// identity of a valid user, it's important that password verification take
    /// the same amount of time whether a user exists or not.  To achieve that,
    /// callers are expected to invoke this function during authentication even
    /// if they've found no user to match the requested credentials.  That's why
    /// this function accepts `Option<SiloUser>` rather than just a `SiloUser`.
    pub(crate) async fn silo_user_password_verify(
        &self,
        opctx: &OpContext,
        maybe_authz_silo_user: Option<&authz::SiloUser>,
        password: &omicron_passwords::Password,
    ) -> Result<bool, Error> {
        let maybe_hash = match maybe_authz_silo_user {
            None => None,
            Some(authz_silo_user) => {
                self.datastore()
                    .silo_user_password_hash_fetch(opctx, authz_silo_user)
                    .await?
            }
        };

        let mut hasher = omicron_passwords::Hasher::default();
        match maybe_hash {
            None => {
                // If the user or their password hash does not exist, create a
                // dummy password hash anyway.  This avoids exposing a timing
                // attack where an attacker can learn that a user exists by
                // seeing how fast it took a login attempt to fail.
                let _ = hasher.create_password(password);
                Ok(false)
            }
            Some(silo_user_password_hash) => Ok(hasher
                .verify_password(password, &silo_user_password_hash.hash)
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "verifying password: {:#}",
                        e
                    ))
                })?),
        }
    }

    /// Given a silo name and username/password credentials, verify the
    /// credentials and return the corresponding SiloUser.
    pub(crate) async fn login_local(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        credentials: params::UsernamePasswordCredentials,
    ) -> Result<db::model::SiloUser, Error> {
        let (authz_silo, _) = self.local_idp_fetch_silo(silo_lookup).await?;

        // NOTE: It's very important that we not bail out early if we fail to
        // find a user with this external id.  See the note in
        // silo_user_password_verify().
        // TODO-security There may still be some vulnerability to timing attack
        // here, in that we'll do one fewer database lookup if a user does not
        // exist.  Rate limiting might help.  See omicron#2184.
        let fetch_user = self
            .datastore()
            .silo_user_fetch_by_external_id(
                opctx,
                &authz_silo,
                credentials.username.as_ref(),
            )
            .await?;
        let verified = self
            .silo_user_password_verify(
                opctx,
                fetch_user.as_ref().map(|(authz_silo_user, _)| authz_silo_user),
                credentials.password.as_ref(),
            )
            .await?;
        if verified {
            bail_unless!(
                fetch_user.is_some(),
                "passed password verification without a valid user"
            );
            let db_user = fetch_user.unwrap().1;
            Ok(db_user)
        } else {
            Err(Error::Unauthenticated {
                internal_message: "Failed password verification".to_string(),
            })
        }
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
                    .silo_group_ensure(
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

    // identity providers

    pub fn saml_identity_provider_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        saml_identity_provider_selector: params::SamlIdentityProviderSelector,
    ) -> LookupResult<lookup::SamlIdentityProvider<'a>> {
        match saml_identity_provider_selector {
            params::SamlIdentityProviderSelector {
                saml_identity_provider: NameOrId::Id(id),
                silo: None,
            } => {
                let saml_provider = LookupPath::new(opctx, &self.db_datastore)
                    .saml_identity_provider_id(id);
                Ok(saml_provider)
            }
            params::SamlIdentityProviderSelector {
                saml_identity_provider: NameOrId::Name(name),
                silo: Some(silo),
            } => {
                let saml_provider = self
                    .silo_lookup(opctx, silo)?
                    .saml_identity_provider_name_owned(name.into());
                Ok(saml_provider)
            }
            params::SamlIdentityProviderSelector {
                saml_identity_provider: NameOrId::Id(_),
                silo: _,
            } => Err(Error::invalid_request(
                "when providing provider as an ID, silo should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "provider should either be a UUID or silo should be specified",
            )),
        }
    }

    pub(crate) async fn identity_provider_list(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::IdentityProvider> {
        // TODO-security: This should likely be lookup_for ListChildren on the silo
        let (.., authz_silo, _) = silo_lookup.fetch().await?;
        let authz_idp_list = authz::SiloIdentityProviderList::new(authz_silo);
        self.db_datastore
            .identity_provider_list(opctx, &authz_idp_list, pagparams)
            .await
    }

    // Silo authn identity providers

    pub(crate) async fn saml_identity_provider_create(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        params: params::SamlIdentityProviderCreate,
    ) -> CreateResult<db::model::SamlIdentityProvider> {
        // TODO-security: This should likely be fetch_for CreateChild on the silo
        let (authz_silo, db_silo) = silo_lookup.fetch().await?;
        let authz_idp_list = authz::SiloIdentityProviderList::new(authz_silo);

        if db_silo.user_provision_type != UserProvisionType::Jit {
            return Err(Error::invalid_request(
                "cannot create identity providers in this kind of Silo",
            ));
        }

        // This check is not strictly necessary yet.  We'll check this
        // permission in the DataStore when we actually update the list.
        // But we check now to protect the code that fetches the descriptor from
        // an external source.
        opctx.authorize(authz::Action::CreateChild, &authz_idp_list).await?;

        // The authentication mode is immutable so it's safe to check this here
        // and bail out.
        if db_silo.authentication_mode
            != nexus_db_model::AuthenticationMode::Saml
        {
            return Err(Error::invalid_request(&format!(
                "cannot create SAML identity provider for this Silo type \
                (expected authentication mode {:?}, found {:?})",
                nexus_db_model::AuthenticationMode::Saml,
                &db_silo.authentication_mode,
            )));
        }

        let idp_metadata_document_string = match &params.idp_metadata_source {
            params::IdpMetadataSource::Url { url } => {
                // Download the SAML IdP descriptor, and write it into the DB.
                // This is so that it can be deserialized later.
                //
                // Importantly, do this only once and store it. It would
                // introduce attack surface to download it each time it was
                // required.
                let dur = std::time::Duration::from_secs(5);
                let client = reqwest::ClientBuilder::new()
                    .connect_timeout(dur)
                    .timeout(dur)
                    .dns_resolver(self.external_resolver.clone())
                    .build()
                    .map_err(|e| {
                        Error::internal_error(&format!(
                            "failed to build reqwest client: {}",
                            e
                        ))
                    })?;

                let response = client.get(url).send().await.map_err(|e| {
                    Error::invalid_value(
                        "url",
                        format!("error querying url: {e}"),
                    )
                })?;

                if !response.status().is_success() {
                    return Err(Error::invalid_value(
                        "url",
                        format!("querying url returned: {}", response.status()),
                    ));
                }

                response.text().await.map_err(|e| {
                    Error::invalid_value(
                        "url",
                        format!("error getting text from url: {e}"),
                    )
                })?
            }

            params::IdpMetadataSource::Base64EncodedXml { data } => {
                let bytes = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    data,
                )
                .map_err(|e| {
                    Error::invalid_value(
                        "data",
                        format!("error getting decoding base64 data: {e}"),
                    )
                })?;
                String::from_utf8_lossy(&bytes).into_owned()
            }
        };

        // Once the IDP metadata document is available, parse it into an
        // EntityDescriptor
        use samael::metadata::EntityDescriptor;
        let idp_metadata: EntityDescriptor =
            idp_metadata_document_string.parse().map_err(|e| {
                Error::invalid_request(&format!(
                    "idp_metadata_document_string could not be parsed as an EntityDescriptor! {}",
                    e
                ))
            })?;

        // Check for at least one signing key - do not accept IDPs that have
        // none!
        let mut found_signing_key = false;

        if let Some(idp_sso_descriptors) = idp_metadata.idp_sso_descriptors {
            for idp_sso_descriptor in &idp_sso_descriptors {
                for key_descriptor in &idp_sso_descriptor.key_descriptors {
                    // Key use is an optional attribute. If it's present, check
                    // if it's "signing". If it's not present, the contained key
                    // information could be used for either signing or
                    // encryption.
                    let is_signing_key =
                        if let Some(key_use) = &key_descriptor.key_use {
                            key_use == "signing"
                        } else {
                            true
                        };

                    if is_signing_key {
                        if let Some(x509_data) =
                            &key_descriptor.key_info.x509_data
                        {
                            if !x509_data.certificates.is_empty() {
                                found_signing_key = true;
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            return Err(Error::invalid_request(
                "no md:IDPSSODescriptor section",
            ));
        }

        if !found_signing_key {
            return Err(Error::invalid_request(
                "no signing key found in IDP metadata",
            ));
        }

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
            .saml_identity_provider_create(opctx, &authz_idp_list, provider)
            .await
    }

    pub fn silo_group_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        group_id: &'a Uuid,
    ) -> lookup::SiloGroup<'a> {
        LookupPath::new(opctx, &self.db_datastore).silo_group_id(*group_id)
    }
}
