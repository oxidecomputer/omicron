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
use diesel::sql_types::Nullable;
use diesel::ExpressionMethods;
use diesel::IntoSql;
use diesel::QueryDsl;
use nexus_db_model::CabooseWhichEnum;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::HwPowerState;
use nexus_db_model::HwPowerStateEnum;
use nexus_db_model::HwRotSlot;
use nexus_db_model::HwRotSlotEnum;
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

                for (baseboard_id, sp) in &collection.sps {
                    let selection = db::schema::hw_baseboard_id::table
                        .select((
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
                        ))
                        .filter(
                            baseboard_dsl::part_number
                                .eq(baseboard_id.part_number.clone()),
                        )
                        .filter(
                            baseboard_dsl::serial_number
                                .eq(baseboard_id.serial_number.clone()),
                        );

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

            // Insert rows for the roots of trust that we found.  Like service
            // processors, we do this using INSERT INTO ... SELECT.
            {
                use db::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use db::schema::inv_root_of_trust::dsl as rot_dsl;

                // XXX-dap is there some way to make this fail to column if,
                // say, we add another column to one of these tables?  Maybe a
                // match on a struct and make sure we get all the fields?
                for (baseboard_id, rot) in &collection.rots {
                    let selection = db::schema::hw_baseboard_id::table
                        .select((
                            collection_id.into_sql::<diesel::sql_types::Uuid>(),
                            baseboard_dsl::id,
                            rot.time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            rot.source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            HwRotSlot::from(rot.active_slot)
                                .into_sql::<HwRotSlotEnum>(),
                            HwRotSlot::from(rot.persistent_boot_preference)
                                .into_sql::<HwRotSlotEnum>(),
                            rot.pending_persistent_boot_preference
                                .map(HwRotSlot::from)
                                .into_sql::<Nullable<HwRotSlotEnum>>(),
                            rot.transient_boot_preference
                                .map(HwRotSlot::from)
                                .into_sql::<Nullable<HwRotSlotEnum>>(),
                            rot.slot_a_sha3_256_digest
                                .clone()
                                .into_sql::<Nullable<diesel::sql_types::Text>>(
                                ),
                            rot.slot_b_sha3_256_digest
                                .clone()
                                .into_sql::<Nullable<diesel::sql_types::Text>>(
                                ),
                        ))
                        .filter(
                            baseboard_dsl::part_number
                                .eq(baseboard_id.part_number.clone()),
                        )
                        .filter(
                            baseboard_dsl::serial_number
                                .eq(baseboard_id.serial_number.clone()),
                        );

                    let _ = diesel::insert_into(
                        db::schema::inv_root_of_trust::table,
                    )
                    .values(selection)
                    .into_columns((
                        rot_dsl::inv_collection_id,
                        rot_dsl::hw_baseboard_id,
                        rot_dsl::time_collected,
                        rot_dsl::source,
                        rot_dsl::rot_slot_active,
                        rot_dsl::rot_slot_boot_pref_persistent,
                        rot_dsl::rot_slot_boot_pref_persistent_pending,
                        rot_dsl::rot_slot_boot_pref_transient,
                        rot_dsl::rot_slot_a_sha3_256,
                        rot_dsl::rot_slot_b_sha3_256,
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

            // Insert rows for the cabooses that we found.  Like service
            // processors and roots of trust, we do this using INSERT INTO ...
            // SELECT.
            // XXX-dap I want a block similar to the above, but it's not clear
            // how to extend it to two different dependent tables.  I can do it
            // with a CTE:
            // https://www.db-fiddle.com/f/aaegKd3RXqaqyuxBLXSxaJ/0
            {
                use db::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use db::schema::inv_caboose::dsl as inv_caboose_dsl;
                use db::schema::sw_caboose::dsl as sw_caboose_dsl;

                for (which, tree) in &collection.cabooses_found {
                    let db_which = nexus_db_model::CabooseWhich::from(*which);
                    for (baseboard_id, found_caboose) in tree {
                        // XXX-dap
                        todo!();
                        // let selection = db::schema::hw_baseboard_id::table
                        //     .select((
                        //         collection_id.into_sql::<diesel::sql_types::Uuid>(),
                        //         baseboard_dsl::id,
                        //         found_caboose.time_collected
                        //             .into_sql::<diesel::sql_types::Timestamptz>(),
                        //         found_caboose.source
                        //             .clone()
                        //             .into_sql::<diesel::sql_types::Text>(),
                        //         db_which.into_sql::<CabooseWhichEnum>(),
                        //     ))
                        //     .filter(
                        //         baseboard_dsl::part_number
                        //             .eq(baseboard_id.part_number.clone()),
                        //     )
                        //     .filter(
                        //         baseboard_dsl::serial_number
                        //             .eq(baseboard_id.serial_number.clone()),
                        //     )
                        //     .left_join(db::schema::sw_caboose::table
                        //         .select(sw_caboose_dsl::id)
                        //         .filter(sw_caboose_dsl::board.eq(found_caboose.board))
                        //         .filter(sw_caboose_dsl::git_commit.eq(found_caboose.git_commit))
                        //         .filter(sw_caboose_dsl::name.eq(found_caboose.name))
                        //         .filter(sw_caboose_dsl::version.eq(found_caboose.version)));

                        // let _ = diesel::insert_into(
                        //     db::schema::inv_root_of_trust::table,
                        // )
                        // .values(selection)
                        // .into_columns((
                        //     inv_caboose_dsl::inv_collection_id,
                        //     inv_caboose_dsl::hw_baseboard_id,
                        //     inv_caboose_dsl::time_collected,
                        //     inv_caboose_dsl::source,
                        //     inv_caboose_dsl::which,
                        //     inv_caboose_dsl::sw_caboose_id,
                        // ))
                        // .execute_async(&conn)
                        // .await
                        // .map_err(|e| {
                        //     TransactionError::CustomError(
                        //         public_error_from_diesel_pool(
                        //             e.into(),
                        //             ErrorHandler::Server,
                        //         )
                        //         .internal_context("inserting service processor"),
                        //     )
                        // });
                    }
                }
            }

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
