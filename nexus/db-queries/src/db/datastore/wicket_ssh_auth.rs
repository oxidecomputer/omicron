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
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::DbTypedGeneration;
use nexus_db_model::WicketSshAuth as DbWicketSshAuth;
use nexus_db_model::WicketSshAuthKey as DbWicketSshAuthKey;
use nexus_db_model::to_db_typed_generation;
use nexus_types::deployment::Blueprint;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use omicron_generation_kinds::WicketSshAuthGeneration;
use omicron_generation_kinds::WicketSshAuthGenerationKind;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::inventory::ConfigReconcilerInventoryStatus;
use sled_agent_types::inventory::WicketSshAuth;
use sled_agent_types::inventory::WicketSshAuthKind;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::btree_map;

/// Selector for a wicket SSH auth generation, used by
/// [`DataStore::wicket_ssh_auth_read()`].
#[derive(Debug, Clone, Copy)]
pub enum WicketSshAuthGenerationDescription {
    /// Read whatever the current latest generation is.
    ///
    /// This is inherently racy with any query that inserts a new generation.
    Latest,
    /// Read a specific generation.
    Specific(WicketSshAuthGeneration),
}

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
    ) -> Result<Self, WicketSshAuthRefError> {
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
    /// Fails if `collection` is self-inconsistent (i.e., contains two sled
    /// configs with the same wicket SSH auth generation but different
    /// contents). This should be impossible.
    pub(crate) fn from_inventory_collection(
        collection: &Collection,
    ) -> Result<Self, WicketSshAuthRefError> {
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
    ) -> Result<(), WicketSshAuthRefError> {
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
                    Err(WicketSshAuthRefError::Inconsistent {
                        generation: auth.generation,
                        first: entry.get().referenced_by,
                        second: referenced_by,
                    })
                }
            }
        }
    }
}

// None of these errors should be reachable in practice, unless something has
// gone very far off the rails: wicket SSH auth generations are immutable (which
// should eliminate `ContentsDiffer` and `Inconsistent`), and
// blueprints/inventory collections can only report generations they've been
// told about (which should eliminate `Missing`).
#[derive(Debug, thiserror::Error)]
pub(crate) enum WicketSshAuthRefError {
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

impl From<WicketSshAuthRefError> for Error {
    fn from(err: WicketSshAuthRefError) -> Self {
        Error::internal_error(InlineErrorChain::new(&err).to_string())
    }
}

impl DataStore {
    /// Insert the initial wicket SSH auth setting (generation 1,
    /// unauthenticated) if it does not already exist.
    ///
    /// RSS initializes every sled to "generation 1, unauthenticated", and this
    /// method is called by Nexus's startup db population code to ensure it
    /// always exists. Subsequent generations are written in response to
    /// operator requests to apply new settings.
    pub async fn load_builtin_wicket_ssh_auth(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::wicket_ssh_auth::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let row = DbWicketSshAuth::initial();
        diesel::insert_into(dsl::wicket_ssh_auth)
            .values(row)
            .on_conflict(dsl::generation)
            .do_nothing()
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    // Helper to read the latest generation recorded in `wicket_ssh_auth`.
    async fn wicket_ssh_auth_latest_generation_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WicketSshAuthGeneration, DieselError> {
        use nexus_db_schema::schema::wicket_ssh_auth::dsl;

        let latest_gen: DbTypedGeneration<WicketSshAuthGenerationKind> =
            dsl::wicket_ssh_auth
                .select(dsl::generation)
                .order_by(dsl::generation.desc())
                .limit(1)
                .first_async(conn)
                .await?;
        Ok(latest_gen.into())
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
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        refs: &WicketSshAuthRefs,
    ) -> Result<(), TransactionError<WicketSshAuthRefError>> {
        for (&generation, expected) in &refs.by_generation {
            let Some(stored) = self
                .wicket_ssh_auth_read_optional_on_connection(conn, generation)
                .await?
            else {
                return Err(TransactionError::CustomError(
                    WicketSshAuthRefError::Missing {
                        generation,
                        referenced_by: expected.referenced_by,
                    },
                ));
            };
            if stored != expected.auth {
                return Err(TransactionError::CustomError(
                    WicketSshAuthRefError::ContentsDiffer {
                        generation,
                        referenced_by: expected.referenced_by,
                    },
                ));
            }
        }
        Ok(())
    }

    /// Read the full wicket SSH auth setting for `generation`.
    ///
    /// Fails if `generation` refers to a generation that doesn't exist.
    //
    // TODO omicron#11821 This returns the sled-agent `WicketSshAuth` type, for
    // now. As a part of the upcoming external API, we'll want to return a Nexus
    // type here instead (from which we can project to the sled-agent type as
    // needed).
    pub async fn wicket_ssh_auth_read(
        &self,
        opctx: &OpContext,
        generation: WicketSshAuthGenerationDescription,
    ) -> Result<WicketSshAuth, Error> {
        opctx.authorize(authz::Action::Read, &authz::WICKET_SSH_AUTH).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        // Read the latest generation, then we'll use that to look up the full
        // setting. These do not need to be in the same transaction: wicket SSH
        // auth settings are immutable, so once we've read the latest
        // generation, we can (separately) read the setting associated with it.
        let generation = match generation {
            WicketSshAuthGenerationDescription::Latest => {
                Self::wicket_ssh_auth_latest_generation_on_connection(&conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?
            }
            WicketSshAuthGenerationDescription::Specific(generation) => {
                generation
            }
        };

        self.wicket_ssh_auth_read_optional_on_connection(&conn, generation)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .ok_or_else(|| {
                Error::non_resourcetype_not_found(&format!(
                    "wicket SSH auth generation {generation} does not exist"
                ))
            })
    }

    /// Read the full wicket SSH auth setting for `generation`, or `None` if
    /// that generation does not exist.
    pub(crate) async fn wicket_ssh_auth_read_optional_on_connection(
        &self,
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

#[cfg(test)]
impl DataStore {
    // TODO omicron#11821 Remove this method once we add a real datastore method
    // for inserting `WicketSshAuth`s, and update the tests currently calling
    // this to use that instead.
    pub(crate) async fn wicket_ssh_auth_insert_for_tests(
        &self,
        auth: WicketSshAuth,
    ) {
        use nexus_db_schema::schema::wicket_ssh_auth::dsl as auth_dsl;
        use nexus_db_schema::schema::wicket_ssh_auth_key::dsl as key_dsl;

        let conn = self.pool_connection_for_tests().await.unwrap();
        let description =
            format!("test setting for generation {}", auth.generation);
        let (row, keys) = DbWicketSshAuth::new(auth, description);
        diesel::insert_into(auth_dsl::wicket_ssh_auth)
            .values(row)
            .execute_async(&*conn)
            .await
            .expect("inserted wicket_ssh_auth row");

        if !keys.is_empty() {
            diesel::insert_into(key_dsl::wicket_ssh_auth_key)
                .values(keys)
                .execute_async(&*conn)
                .await
                .expect("inserted wicket_ssh_auth_key rows");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use assert_matches::assert_matches;
    use nexus_auth::authn;
    use omicron_test_utils::dev;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn public_keys(
        generation: u32,
        keys: &[&str],
        allow_support_keys: bool,
    ) -> WicketSshAuth {
        WicketSshAuth {
            generation: generation.into(),
            kind: WicketSshAuthKind::PublicKeys {
                authorized_keys: keys.iter().map(|k| k.to_string()).collect(),
                allow_support_keys,
            },
        }
    }

    #[tokio::test]
    async fn test_wicket_ssh_auth_read() {
        let logctx = dev::test_setup_log("test_wicket_ssh_auth_read");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let read = |generation| async move {
            datastore.wicket_ssh_auth_read(opctx, generation).await
        };

        // Before we add anything, the only setting is the initial one.
        let initial = WicketSshAuth::initial();
        assert_eq!(
            read(WicketSshAuthGenerationDescription::Latest).await.unwrap(),
            initial,
        );

        let gen2 = public_keys(2, &["fake key 1", "fake key 2"], false);
        let gen3 = public_keys(3, &[], true);
        let gen4 = public_keys(4, &["fake key 3"], true);

        // Insert out of order; `Latest` must not depend on insertion order.
        for auth in [&gen4, &gen2, &gen3] {
            datastore.wicket_ssh_auth_insert_for_tests(auth.clone()).await;
        }

        for expected in [&initial, &gen2, &gen3, &gen4] {
            let found = read(WicketSshAuthGenerationDescription::Specific(
                expected.generation,
            ))
            .await
            .unwrap();
            assert_eq!(found, *expected);
        }
        assert_eq!(
            read(WicketSshAuthGenerationDescription::Latest).await.unwrap(),
            gen4,
        );

        // A generation that doesn't exist produces a "not found".
        let err = read(WicketSshAuthGenerationDescription::Specific(5.into()))
            .await
            .unwrap_err();
        assert_matches!(err, Error::NotFound { .. });

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // This is a silly amount of keys (and Nexus will presumably put a cap on it
    // much lower than `SQL_BATCH_SIZE`). But it costs us very little to
    // paginate, so smoke test that we've done that correctly.
    #[tokio::test]
    async fn test_wicket_ssh_auth_read_paginates_keys() {
        let logctx =
            dev::test_setup_log("test_wicket_ssh_auth_read_paginates_keys");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let nkeys = usize::try_from(SQL_BATCH_SIZE.get()).unwrap() + 1;
        let authorized_keys: BTreeSet<String> =
            (0..nkeys).map(|i| format!("ssh-ed25519 key{i}")).collect();
        assert_eq!(authorized_keys.len(), nkeys);
        let auth = WicketSshAuth {
            generation: 2.into(),
            kind: WicketSshAuthKind::PublicKeys {
                authorized_keys,
                allow_support_keys: false,
            },
        };
        datastore.wicket_ssh_auth_insert_for_tests(auth.clone()).await;

        let found = datastore
            .wicket_ssh_auth_read(
                opctx,
                WicketSshAuthGenerationDescription::Specific(auth.generation),
            )
            .await
            .unwrap();
        assert_eq!(found, auth);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_wicket_ssh_auth_validate_contents() {
        let logctx =
            dev::test_setup_log("test_wicket_ssh_auth_validate_contents");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        const KEY_A: &str = "fake key A";
        const KEY_B: &str = "fake key B";
        let stored = public_keys(2, &[KEY_A], false);
        datastore.wicket_ssh_auth_insert_for_tests(stored.clone()).await;

        let validate = |auths: Vec<WicketSshAuth>| {
            let conn = &conn;
            async move {
                let mut by_generation = BTreeMap::new();
                for auth in &auths {
                    WicketSshAuthRefs::insert(
                        &mut by_generation,
                        auth,
                        SledUuid::new_v4(),
                    )
                    .expect("refs are self-consistent");
                }
                datastore
                    .wicket_ssh_auth_validate_on_connection(
                        conn,
                        &WicketSshAuthRefs { by_generation },
                    )
                    .await
            }
        };

        validate(vec![WicketSshAuth::initial(), stored.clone()])
            .await
            .expect("matching contents should validate");

        // Same generation and kind but a different set of keys or a different
        // `allow_support_keys` should fail validation.
        for bad in [
            public_keys(2, &[KEY_A, KEY_B], false),
            public_keys(2, &[KEY_B], false),
            public_keys(2, &[], false),
            public_keys(2, &[KEY_A], true),
            WicketSshAuth {
                generation: 2.into(),
                kind: WicketSshAuthKind::Unauthenticated,
            },
        ] {
            let err = validate(vec![WicketSshAuth::initial(), bad.clone()])
                .await
                .expect_err("differing contents should not validate");
            assert_matches!(
                err,
                TransactionError::CustomError(
                    WicketSshAuthRefError::ContentsDiffer { generation, .. }
                ) if generation == bad.generation,
                "unexpected error for {bad:?}"
            );
        }

        // A generation that doesn't exist in the db should also fail validation.
        let missing = public_keys(9, &[KEY_A], false);
        let err = validate(vec![stored.clone(), missing.clone()])
            .await
            .expect_err("missing generation should not validate");
        assert_matches!(
            err,
            TransactionError::CustomError(WicketSshAuthRefError::Missing {
                generation,
                ..
            }) if generation == missing.generation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_load_builtin_wicket_ssh_auth_idempotent() {
        let logctx =
            dev::test_setup_log("test_load_builtin_wicket_ssh_auth_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // new_with_datastore() populates the datastore just like Nexus, so
        // `load_builtin_wicket_ssh_auth()` has already run; the initial setting
        // should already exist.
        let latest = datastore
            .wicket_ssh_auth_read(
                opctx,
                WicketSshAuthGenerationDescription::Latest,
            )
            .await
            .unwrap();
        assert_eq!(latest, WicketSshAuth::initial());

        // Confirm we can run it again without error, and that it doesn't insert
        // any new generations. (We have to construct an opctx that's valid; the
        // default test opctx won't pass `load_builtin_wicket_ssh_auth()`'s
        // authz check.)
        let populate_opctx = OpContext::for_background(
            logctx.log.clone(),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::internal_db_init(),
            Arc::clone(datastore) as Arc<_>,
        );
        for _ in 0..2 {
            datastore
                .load_builtin_wicket_ssh_auth(&populate_opctx)
                .await
                .expect("loaded builtin wicket SSH auth");
        }

        let latest = datastore
            .wicket_ssh_auth_read(
                opctx,
                WicketSshAuthGenerationDescription::Latest,
            )
            .await
            .unwrap();
        assert_eq!(latest, WicketSshAuth::initial());
        let err = datastore
            .wicket_ssh_auth_read(
                opctx,
                WicketSshAuthGenerationDescription::Specific(2.into()),
            )
            .await
            .unwrap_err();
        assert_matches!(err, Error::NotFound { .. });

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
