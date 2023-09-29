// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::IntoSql;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::HwPowerState;
use nexus_db_model::HwPowerStateEnum;
use nexus_db_model::InvCollection;
use nexus_db_model::InvCollectionError;
use nexus_db_model::SwCaboose;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;

impl DataStore {
    /// Store a complete inventory collection into the database
    pub async fn inventory_insert_collection(
        &self,
        opctx: &OpContext,
        collection: &Collection,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::INVENTORY).await?;

        // It's not critical that this be done in one transaction.  But it keeps
        // the database a little tidier because we won't have half-inserted
        // collections in there.
        //
        // Even better would be a large hunk of SQL sent over at once.  None of
        // this needs to be interactive.  But we don't yet have support for
        // this.  See oxidecomputer/omicron#973.
        let pool = self.pool_authorized(opctx).await?;
        pool.transaction_async(|conn| async move {
            let row_collection = InvCollection::from(collection);
            let collection_id = row_collection.id;

            // Insert a record describing the collection itself.
            {
                use db::schema::inv_collection::dsl;
                let _ = diesel::insert_into(dsl::inv_collection)
                    .values(row_collection)
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel_pool(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context(
                                "inserting new inventory collection",
                            ),
                        )
                    })?;
            }

            // Insert records (and generate ids) for any baseboards that do not
            // already exist in the database.
            {
                use db::schema::hw_baseboard_id::dsl;
                let baseboards = collection
                    .baseboards
                    .iter()
                    .map(|b| HwBaseboardId::from(b.as_ref()))
                    .collect::<Vec<_>>();
                let _ = diesel::insert_into(dsl::hw_baseboard_id)
                    .values(baseboards)
                    .on_conflict_do_nothing()
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel_pool(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting baseboards"),
                        )
                    });
            }

            // Insert records (and generate ids) for each distinct caboose that
            // we've found.  Like baseboards, these might already be present.
            {
                use db::schema::sw_caboose::dsl;
                let cabooses = collection
                    .cabooses
                    .iter()
                    .map(|s| SwCaboose::from(s.as_ref()))
                    .collect::<Vec<_>>();
                let _ = diesel::insert_into(dsl::sw_caboose)
                    .values(cabooses)
                    .on_conflict_do_nothing()
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel_pool(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting cabooses"),
                        )
                    });
            }

            // Now insert rows for the service processors we found.  These have
            // a foreign key into the hw_baseboard_id table.  We don't have
            // those values.  We may have just inserted them, or maybe not (if
            // they previously existed).  To avoid dozens of unnecessary
            // round-trips, we use INSERT INTO ... SELECT, which looks like
            // this:
            //
            //   INSERT INTO inv_service_processor
            //       SELECT
            //           id
            //           [other service_processor column values as literals]
            //         FROM hw_baseboard_id
            //         WHERE part_number = ... AND serial_number = ...;
            //
            // This way, we don't need to know the id.  The database looks it up
            // for us as it does the INSERT.
            {
                use db::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use db::schema::inv_service_processor::dsl as sp_dsl;

                for (_, sp) in &collection.sps {
                    let selection =
                        db::schema::hw_baseboard_id::table.select((
                            collection_id.into_sql::<diesel::sql_types::Uuid>(),
                            baseboard_dsl::id,
                            sp.time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            sp.source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            i64::from(sp.baseboard_revision)
                                .into_sql::<diesel::sql_types::Int8>(),
                            sp.hubris_archive
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            HwPowerState::from(sp.power_state)
                                .into_sql::<HwPowerStateEnum>(),
                        ));

                    let _ = diesel::insert_into(
                        db::schema::inv_service_processor::table,
                    )
                    .values(selection)
                    .into_columns((
                        sp_dsl::inv_collection_id,
                        sp_dsl::hw_baseboard_id,
                        sp_dsl::time_collected,
                        sp_dsl::source,
                        sp_dsl::baseboard_revision,
                        sp_dsl::hubris_archive_id,
                        sp_dsl::power_state,
                    ))
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel_pool(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting service processor"),
                        )
                    });
                }
            }

            // XXX-dap Insert the root-of-trust information.
            // XXX-dap Insert the "found cabooses" information.
            // XXX-dap the various loops here could probably be INSERTs of Vecs

            // Finally, insert the list of errors.
            {
                use db::schema::inv_collection_error::dsl as errors_dsl;
                let error_values = collection
                    .errors
                    .iter()
                    .enumerate()
                    .map(|(i, error)| {
                        // XXX-dap unwrap
                        let index = i32::try_from(i).unwrap();
                        let message = format!("{:#}", error);
                        InvCollectionError::new(collection_id, index, message)
                    })
                    .collect::<Vec<_>>();
                let _ = diesel::insert_into(errors_dsl::inv_collection_error)
                    .values(error_values)
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel_pool(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting errors"),
                        )
                    });
            }

            Ok(())
        })
        .await
        .map_err(|error| match error {
            TransactionError::CustomError(e) => e,
            TransactionError::Pool(e) => {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            }
        })
    }
}
