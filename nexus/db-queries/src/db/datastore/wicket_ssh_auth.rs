// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Datastore methods for the operator-controlled wicket SSH auth settings.

use crate::authz;
use crate::context::OpContext;
use crate::db::DataStore;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::expression::SelectableHelper;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::WicketSshAuth as DbWicketSshAuth;
use nexus_db_model::WicketSshAuthKey as DbWicketSshAuthKey;
use nexus_db_model::to_db_typed_generation;
use nexus_types::deployment::Blueprint;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use omicron_generation_kinds::WicketSshAuthGeneration;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::inventory::ConfigReconcilerInventoryStatus;
use sled_agent_types::inventory::WicketSshAuth;
use sled_agent_types::inventory::WicketSshAuthKind;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::btree_map;

/// The set of distinct wicket SSH auth settings referenced by something about
/// to be inserted (a blueprint or an inventory collection).
#[derive(Debug)]
pub(crate) struct WicketSshAuthRefs {
    by_generation: BTreeMap<WicketSshAuthGeneration, WicketSshAuthRef>,
}

#[derive(Debug)]
struct WicketSshAuthRef {
    auth: WicketSshAuth,
    referenced_by: SledUuid,
}

impl WicketSshAuthRefs {
    /// Construct the set of wicket SSH auth settings referenced by `blueprint`.
    ///
    /// # Errors
    ///
    /// Fails if `blueprint` is self-inconsistent (i.e., contains two sleds with
    /// the same wicket SSH auth generation but different contents). This should
    /// be impossible.
    pub(crate) fn from_blueprint(
        blueprint: &Blueprint,
    ) -> Result<Self, WicketSshAuthValidateError> {
        let mut by_generation = BTreeMap::new();
        for (sled_id, sled) in &blueprint.sleds {
            Self::insert(&mut by_generation, &sled.wicket_ssh_auth, *sled_id)?;
        }
        Ok(Self { by_generation })
    }

    /// Construct the set of wicket SSH auth settings referenced by
    /// `collection`.
    ///
    /// # Errors
    ///
    /// Fails if `collection` is self-inconsistent (i.e., contains two sleds
    /// with the same wicket SSH auth generation but different contents). This
    /// should be impossible.
    pub(crate) fn from_inventory_collection(
        collection: &Collection,
    ) -> Result<Self, WicketSshAuthValidateError> {
        let mut by_generation = BTreeMap::new();
        for sled in &collection.sled_agents {
            // Each sled-agent can report up to three `OmicronSledConfig`s;
            // check each of them.
            if let Some(config) = &sled.ledgered_sled_config {
                Self::insert(
                    &mut by_generation,
                    &config.wicket_ssh_auth,
                    sled.sled_id,
                )?;
            }
            if let Some(config) = sled
                .last_reconciliation
                .as_ref()
                .map(|inv| &inv.last_reconciled_config)
            {
                Self::insert(
                    &mut by_generation,
                    &config.wicket_ssh_auth,
                    sled.sled_id,
                )?;
            }
            match &sled.reconciler_status {
                ConfigReconcilerInventoryStatus::NotYetRun
                | ConfigReconcilerInventoryStatus::Idle {
                    completed_at: _,
                    ran_for: _,
                } => (),
                ConfigReconcilerInventoryStatus::Running {
                    config,
                    started_at: _,
                    running_for: _,
                } => {
                    Self::insert(
                        &mut by_generation,
                        &config.wicket_ssh_auth,
                        sled.sled_id,
                    )?;
                }
            }
        }
        Ok(Self { by_generation })
    }

    fn insert(
        by_generation: &mut BTreeMap<WicketSshAuthGeneration, WicketSshAuthRef>,
        auth: &WicketSshAuth,
        referenced_by: SledUuid,
    ) -> Result<(), WicketSshAuthValidateError> {
        match by_generation.entry(auth.generation) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(WicketSshAuthRef {
                    auth: auth.clone(),
                    referenced_by,
                });
                Ok(())
            }
            btree_map::Entry::Occupied(entry) => {
                if entry.get().auth == *auth {
                    Ok(())
                } else {
                    Err(WicketSshAuthValidateError::Inconsistent {
                        generation: auth.generation,
                        first: entry.get().referenced_by,
                        second: referenced_by,
                    })
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WicketSshAuthValidateError {
    #[error("database error")]
    Diesel(#[from] DieselError),

    #[error(
        "wicket SSH auth generation {generation} (referenced by sled \
         {referenced_by}) does not exist"
    )]
    Missing { generation: WicketSshAuthGeneration, referenced_by: SledUuid },

    #[error(
        "wicket SSH auth generation {generation} (referenced by sled \
         {referenced_by}) has different contents than the stored setting \
         for that generation"
    )]
    ContentsDiffer {
        generation: WicketSshAuthGeneration,
        referenced_by: SledUuid,
    },

    #[error(
        "wicket SSH auth generation {generation} has different contents as \
         referenced by sleds {first} and {second}"
    )]
    Inconsistent {
        generation: WicketSshAuthGeneration,
        first: SledUuid,
        second: SledUuid,
    },
}

impl From<WicketSshAuthValidateError> for Error {
    fn from(err: WicketSshAuthValidateError) -> Self {
        match err {
            WicketSshAuthValidateError::Diesel(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
            // None of these errors should be reachable in practice, unless
            // something has gone very far off the rails: wicket SSH auth
            // generations are immutable (which should eliminate
            // `ContentsDiffer` and `Inconsistent`), and blueprints/inventory
            // collections can only report generations they've been told about
            // (which should eliminate `Missing`).
            e @ (WicketSshAuthValidateError::Missing { .. }
            | WicketSshAuthValidateError::ContentsDiffer { .. }
            | WicketSshAuthValidateError::Inconsistent { .. }) => {
                Error::internal_error(InlineErrorChain::new(&e).to_string())
            }
        }
    }
}

impl DataStore {
    /// Insert the initial wicket SSH auth setting (generation 1,
    /// unauthenticated) if it does not already exist.
    ///
    /// RSS initializes every sled to "generation 1, unauthenticated", and the
    /// db migration to the version that added this feature backfilled the same
    /// setting for all existing systems.
    //
    // TODO omicron#11821 Add note here about subsequent generations only being
    // created on operator requests, once that work is implemented.
    pub async fn load_builtin_wicket_ssh_auth(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::wicket_ssh_auth::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let initial = WicketSshAuth::unauthenticated_initial_generation();
        let row = DbWicketSshAuth::new(
            &initial,
            "initial setting (unauthenticated)".into(),
        );
        diesel::insert_into(dsl::wicket_ssh_auth)
            .values(row)
            .on_conflict(dsl::generation)
            .do_nothing()
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Check that every generation in `refs` exists in the database with
    /// exactly the contents recorded in `refs`.
    ///
    /// Blueprints and inventory collections do not store wicket SSH auth
    /// settings themselves; they store only a generation number and rely on the
    /// `wicket_ssh_auth*` tables to rehydrate the full setting on read. To
    /// avoid inserting a collection/blueprint that can't be read back, the
    /// inserts must call this method (inside the transaction performing the
    /// insert) to ensure they're storing references to a wicket SSH auth
    /// generation that actually exists with the expected contents.
    pub(crate) async fn wicket_ssh_auth_validate_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        refs: &WicketSshAuthRefs,
    ) -> Result<(), WicketSshAuthValidateError> {
        for (&generation, expected) in &refs.by_generation {
            let Some(stored) =
                Self::wicket_ssh_auth_read_optional_on_connection(
                    conn, generation,
                )
                .await?
            else {
                return Err(WicketSshAuthValidateError::Missing {
                    generation,
                    referenced_by: expected.referenced_by,
                });
            };
            if stored != expected.auth {
                return Err(WicketSshAuthValidateError::ContentsDiffer {
                    generation,
                    referenced_by: expected.referenced_by,
                });
            }
        }
        Ok(())
    }

    /// Read the full wicket SSH auth setting for `generation`.
    //
    // TODO omicron#11821 This returns the sled-agent `WicketSshAuth` type, for
    // now. As a part of the upcoming external API, we'll want to return a Nexus
    // type here instead (from which we can project to the sled-agent type as
    // needed).
    pub(crate) async fn wicket_ssh_auth_read_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        generation: WicketSshAuthGeneration,
    ) -> Result<WicketSshAuth, Error> {
        Self::wicket_ssh_auth_read_optional_on_connection(conn, generation)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .ok_or_else(|| {
                Error::internal_error(&format!(
                    "wicket SSH auth generation {generation} does not exist"
                ))
            })
    }

    /// Read the full wicket SSH auth setting for `generation`, or `None` if
    /// that generation does not exist.
    async fn wicket_ssh_auth_read_optional_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        generation: WicketSshAuthGeneration,
    ) -> Result<Option<WicketSshAuth>, DieselError> {
        use nexus_db_schema::schema::wicket_ssh_auth::dsl as auth_dsl;
        use nexus_db_schema::schema::wicket_ssh_auth_key::dsl as key_dsl;

        let generation = to_db_typed_generation(generation);
        let Some(db_auth) = auth_dsl::wicket_ssh_auth
            .filter(auth_dsl::generation.eq(generation))
            .select(DbWicketSshAuth::as_select())
            .first_async(conn)
            .await
            .optional()?
        else {
            return Ok(None);
        };

        let mut wicket_ssh_auth =
            db_auth.to_wicket_ssh_auth_without_operator_keys();
        match &mut wicket_ssh_auth.kind {
            WicketSshAuthKind::Unauthenticated => {
                // nothing to do - fall through and return as-is
            }
            WicketSshAuthKind::PublicKeys {
                authorized_keys,
                allow_support_keys: _,
            } => {
                // We need to query the `wicket_ssh_auth_key` table to fill in
                // `authorized_keys`.
                //
                // Pagination here is a little silly; we never expect to need to
                // fetch more than one batch. But it's easy enough to do
                // defensively anyway.
                let mut paginator = Paginator::new(
                    SQL_BATCH_SIZE,
                    dropshot::PaginationOrder::Ascending,
                );
                while let Some(p) = paginator.next() {
                    let batch = paginated(
                        key_dsl::wicket_ssh_auth_key,
                        key_dsl::authorized_key,
                        &p.current_pagparams(),
                    )
                    .filter(key_dsl::generation.eq(generation))
                    .select(DbWicketSshAuthKey::as_select())
                    .load_async(conn)
                    .await?;
                    paginator = p
                        .found_batch(&batch, &|row: &DbWicketSshAuthKey| {
                            row.authorized_key.clone()
                        });
                    for row in batch {
                        authorized_keys.insert(row.authorized_key);
                    }
                }
            }
        }

        Ok(Some(wicket_ssh_auth))
    }
}
