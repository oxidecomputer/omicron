// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::DataStore;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::expression::SelectableHelper;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::WicketSshAuth as DbWicketSshAuth;
use nexus_db_model::WicketSshAuthKey as DbWicketSshAuthKey;
use nexus_db_model::to_db_typed_generation;
use omicron_common::api::external::Error;
use omicron_generation_kinds::WicketSshAuthGeneration;
use sled_agent_types::inventory::WicketSshAuth;
use sled_agent_types::inventory::WicketSshAuthKind;

impl DataStore {
    // TODO This returns the sled-agent `WicketSshAuth` type, for now. As a part
    // of the external API work for omicron#11821, we'll want to return a Nexus
    // type here instead (from which we can project to the sled-agent type as
    // needed).
    pub(crate) async fn wicket_ssh_auth_read_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        generation: WicketSshAuthGeneration,
    ) -> Result<WicketSshAuth, Error> {
        use nexus_db_schema::schema::wicket_ssh_auth::dsl as auth_dsl;
        use nexus_db_schema::schema::wicket_ssh_auth_key::dsl as key_dsl;

        let generation = to_db_typed_generation(generation);
        let db_auth = auth_dsl::wicket_ssh_auth
            .filter(auth_dsl::generation.eq(generation))
            .select(DbWicketSshAuth::as_select())
            .first_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

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
                // We need to query the `wicket_ssh_auth_key` to fill in
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
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;
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

        Ok(wicket_ssh_auth)
    }
}
