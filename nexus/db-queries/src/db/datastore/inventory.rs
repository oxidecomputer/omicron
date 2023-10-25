// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::sql_types::Nullable;
use diesel::ExpressionMethods;
use diesel::IntoSql;
use diesel::NullableExpressionMethods;
use diesel::QueryDsl;
use diesel::Table;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::HwPowerState;
use nexus_db_model::HwPowerStateEnum;
use nexus_db_model::HwRotSlot;
use nexus_db_model::HwRotSlotEnum;
use nexus_db_model::InvCollection;
use nexus_db_model::InvCollectionError;
use nexus_db_model::SpType;
use nexus_db_model::SpTypeEnum;
use nexus_db_model::SwCaboose;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use uuid::Uuid;

impl DataStore {
    /// Store a complete inventory collection into the database
    pub async fn inventory_insert_collection(
        &self,
        opctx: &OpContext,
        collection: &Collection,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::INVENTORY).await?;

        // In the database, the collection is represented essentially as a tree
        // rooted at an `inv_collection` row.  Other nodes in the tree point
        // back at the `inv_collection` via `inv_collection_id`.
        //
        // It's helpful to assemble some values before entering the transaction
        // so that we can produce the `Error` type that we want here.
        let row_collection = InvCollection::from(collection);
        let collection_id = row_collection.id;
        let baseboards = collection
            .baseboards
            .iter()
            .map(|b| HwBaseboardId::from(b.as_ref()))
            .collect::<Vec<_>>();
        let cabooses = collection
            .cabooses
            .iter()
            .map(|s| SwCaboose::from(s.as_ref()))
            .collect::<Vec<_>>();
        let cabooses_found: Vec<_> = collection
            .sps
            .iter()
            .flat_map(|(_, sp)| [&sp.slot0_caboose, &sp.slot1_caboose])
            .chain(collection.rots.iter().flat_map(|(_, rot)| {
                [&rot.slot_a_caboose, &rot.slot_b_caboose]
            }))
            .flatten()
            .collect();
        let error_values = collection
            .errors
            .iter()
            .enumerate()
            .map(|(i, error)| {
                let index = u16::try_from(i).map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to convert error index to u16 (too \
                                many errors in inventory collection?): {}",
                        e
                    ))
                })?;
                let message = format!("{:#}", error);
                Ok(InvCollectionError::new(collection_id, index, message))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // This implementation inserts all records associated with the
        // collection in one transaction.  This is primarily for simplicity.  It
        // means we don't have to worry about other readers seeing a
        // half-inserted collection, nor leaving detritus around if we start
        // inserting records and then crash.  However, it does mean this is
        // likely to be a big transaction and if that becomes a problem we could
        // break this up as long as we address those problems.
        //
        // The SQL here is written so that it doesn't have to be an
        // *interactive* transaction.  That is, it should in principle be
        // possible to generate all this SQL up front and send it as one big
        // batch rather than making a bunch of round-trips to the database.
        // We'd do that if we had an interface for doing that with bound
        // parameters, etc.  See oxidecomputer/omicron#973.
        let pool = self.pool_connection_authorized(opctx).await?;
        pool.transaction_async(|conn| async move {
            // Insert records (and generate ids) for any baseboards that do not
            // already exist in the database.  These rows are not scoped to a
            // particular collection.  They contain only immutable data --
            // they're just a mapping between hardware-provided baseboard
            // identifiers (part number and model number) and an
            // Omicron-specific primary key (a UUID).
            {
                use db::schema::hw_baseboard_id::dsl;
                let _ = diesel::insert_into(dsl::hw_baseboard_id)
                    .values(baseboards)
                    .on_conflict_do_nothing()
                    .execute_async(&conn)
                    .await?;
            }

            // Insert records (and generate ids) for each distinct caboose that
            // we've found.  Like baseboards, these might already be present and
            // rows in this table are not scoped to a particular collection
            // because they only map (immutable) identifiers to UUIDs.
            {
                use db::schema::sw_caboose::dsl;
                let _ = diesel::insert_into(dsl::sw_caboose)
                    .values(cabooses)
                    .on_conflict_do_nothing()
                    .execute_async(&conn)
                    .await?;
            }

            // Insert a record describing the collection itself.
            {
                use db::schema::inv_collection::dsl;
                let _ = diesel::insert_into(dsl::inv_collection)
                    .values(row_collection)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert rows for the service processors we found.  These have a
            // foreign key into the hw_baseboard_id table.  We don't have those
            // id values, though.  We may have just inserted them, or maybe not
            // (if they previously existed).  To avoid dozens of unnecessary
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
                            SpType::from(sp.sp_type).into_sql::<SpTypeEnum>(),
                            i32::from(sp.sp_slot)
                                .into_sql::<diesel::sql_types::Int4>(),
                            i64::from(sp.baseboard_revision)
                                .into_sql::<diesel::sql_types::Int8>(),
                            sp.hubris_archive
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            HwPowerState::from(sp.power_state)
                                .into_sql::<HwPowerStateEnum>(),
                            sp.slot0_caboose
                                .as_ref()
                                .map(|c| c.id)
                                .into_sql::<diesel::sql_types::Nullable<
                                diesel::sql_types::Uuid,
                            >>(),
                            sp.slot1_caboose
                                .as_ref()
                                .map(|c| c.id)
                                .into_sql::<diesel::sql_types::Nullable<
                                diesel::sql_types::Uuid,
                            >>(),
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
                        sp_dsl::sp_type,
                        sp_dsl::sp_slot,
                        sp_dsl::baseboard_revision,
                        sp_dsl::hubris_archive_id,
                        sp_dsl::power_state,
                        sp_dsl::slot0_inv_caboose_id,
                        sp_dsl::slot1_inv_caboose_id,
                    ))
                    .execute_async(&conn)
                    .await?;

                    // This statement is just here to force a compilation error
                    // if the set of columns in `inv_service_processor` changes.
                    // The code above attempts to insert a row into
                    // `inv_service_processor` using an explicit list of columns
                    // and values.  Without the following statement, If a new
                    // required column were added, this would only fail at
                    // runtime.
                    //
                    // If you're here because of a compile error, you might be
                    // changing the `inv_service_processor` table.  Update the
                    // statement below and be sure to update the code above,
                    // too!
                    //
                    // See also similar comments in blocks below, near other
                    // uses of `all_columns().
                    let (
                        _inv_collection_id,
                        _hw_baseboard_id,
                        _time_collected,
                        _source,
                        _sp_type,
                        _sp_slot,
                        _baseboard_revision,
                        _hubris_archive_id,
                        _power_state,
                        _slot0_inv_caboose_id,
                        _slot1_inv_caboose_id,
                    ) = sp_dsl::inv_service_processor::all_columns();
                }
            }

            // Insert rows for the roots of trust that we found.  Like service
            // processors, we do this using INSERT INTO ... SELECT.
            {
                use db::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use db::schema::inv_root_of_trust::dsl as rot_dsl;

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
                            rot.slot_a_caboose
                                .as_ref()
                                .map(|c| c.id)
                                .into_sql::<diesel::sql_types::Nullable<
                                diesel::sql_types::Uuid,
                            >>(),
                            rot.slot_b_caboose
                                .as_ref()
                                .map(|c| c.id)
                                .into_sql::<diesel::sql_types::Nullable<
                                diesel::sql_types::Uuid,
                            >>(),
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
                        rot_dsl::slot_active,
                        rot_dsl::slot_boot_pref_persistent,
                        rot_dsl::slot_boot_pref_persistent_pending,
                        rot_dsl::slot_boot_pref_transient,
                        rot_dsl::slot_a_sha3_256,
                        rot_dsl::slot_b_sha3_256,
                        rot_dsl::slot_a_inv_caboose_id,
                        rot_dsl::slot_b_inv_caboose_id,
                    ))
                    .execute_async(&conn)
                    .await?;

                    // See the comment in the previous block (where we use
                    // `inv_service_processor::all_columns()`).  The same
                    // applies here.
                    let (
                        _inv_collection_id,
                        _hw_baseboard_id,
                        _time_collected,
                        _source,
                        _slot_active,
                        _slot_boot_pref_persistent,
                        _slot_boot_pref_persistent_pending,
                        _slot_boot_pref_transient,
                        _slot_a_sha3_256,
                        _slot_b_sha3_256,
                        _slot_a_inv_caboose_id,
                        _slot_b_inv_caboose_id,
                    ) = rot_dsl::inv_root_of_trust::all_columns();
                }
            }

            // Insert records for cabooses found.  Like the others, we do this
            // using INSERT INTO ... SELECT because we need ids from the
            // `sw_caboose` table that we may not have.
            {
                use db::schema::inv_caboose::dsl as inv_dsl;
                use db::schema::sw_caboose::dsl as sw_dsl;

                for caboose_found in &cabooses_found {
                    let selection = db::schema::sw_caboose::table
                        .select((
                            caboose_found
                                .id
                                .into_sql::<diesel::sql_types::Uuid>(),
                            collection_id.into_sql::<diesel::sql_types::Uuid>(),
                            caboose_found
                                .time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            caboose_found
                                .source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            sw_dsl::id,
                        ))
                        .filter(
                            sw_dsl::board
                                .eq(caboose_found.caboose.board.clone()),
                        )
                        .filter(
                            sw_dsl::git_commit
                                .eq(caboose_found.caboose.git_commit.clone()),
                        )
                        .filter(
                            sw_dsl::name.eq(caboose_found.caboose.name.clone()),
                        )
                        .filter(
                            sw_dsl::version
                                .eq(caboose_found.caboose.version.clone()),
                        );

                    let _ = diesel::insert_into(db::schema::inv_caboose::table)
                        .values(selection)
                        .into_columns((
                            inv_dsl::id,
                            inv_dsl::inv_collection_id,
                            inv_dsl::time_collected,
                            inv_dsl::source,
                            inv_dsl::sw_caboose_id,
                        ))
                        .execute_async(&conn)
                        .await?;
                }

                let (
                    _id,
                    _inv_collection_id,
                    _time_collected,
                    _source,
                    _sw_caboose_id,
                ) = inv_dsl::inv_caboose::all_columns();
            }

            // Finally, insert the list of errors.
            {
                use db::schema::inv_collection_error::dsl as errors_dsl;
                let _ = diesel::insert_into(errors_dsl::inv_collection_error)
                    .values(error_values)
                    .execute_async(&conn)
                    .await?;
            }

            Ok(())
        })
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        info!(
            &opctx.log,
            "inserted inventory collection";
            "collection_id" => collection.id.to_string(),
        );

        Ok(())
    }

    /// Prune inventory collections stored in the database, keeping at least
    /// `nkeep`.
    ///
    /// This function removes as many collections as possible while preserving
    /// the last `nkeep`.  This will also preserve at least one "complete"
    /// collection (i.e., one having zero errors).
    // It might seem surprising that such a high-level application policy is
    // embedded in the DataStore.  The reason is that we want to push a bunch of
    // the logic into the SQL to avoid interactive queries.
    pub async fn inventory_prune_collections(
        &self,
        opctx: &OpContext,
        nkeep: u32,
    ) -> Result<(), Error> {
        // Assumptions:
        //
        // - Most of the time, there will be about `nkeep + 1` collections in
        //   the database.  That's because the normal expected case is: we had
        //   `nkeep`, we created another one, and now we're pruning the oldest
        //   one.
        //
        // - There could be fewer collections in the database, early in the
        //   system's lifetime (before we've accumulated `nkeep` of them).
        //
        // - There could be many more collections in the database, if something
        //   has gone wrong and we've fallen behind in our cleanup.
        //
        // - Due to transient errors during the collection process, it's
        //   possible that a collection is known to be potentially incomplete.
        //   We can tell this because it has rows in `inv_collection_errors`.
        //   (It's possible that a collection can be incomplete with zero
        //   errors, but we can't know that here and so we can't do anything
        //   about it.)
        //
        // Goals:
        //
        // - When this function returns without error, there were at most
        //   `nkeep` collections in the database.
        //
        // - If we have to remove any collections, we want to start from the
        //   oldest ones.  (We want to maintain a window of the last `nkeep`,
        //   not the first `nkeep - 1` from the beginning of time plus the most
        //   recent one.)
        //
        // - We want to avoid removing the last collection that had zero errors.
        //   (If we weren't careful, we might do this if there were `nkeep`
        //   collections with errors that were newer than the last complete
        //   collection.)
        //
        // Here's the plan:
        //
        // - Select from the database the `nkeep + 1` oldest collections and the
        //   number of errors associated with each one.
        //
        // - If we got fewer than `nkeep + 1` back, we're done.  We shouldn't
        //   prune anything.
        //
        // - Otherwise, if the oldest collection is the only complete one,
        //   remove the next-oldest collection and go back to the top (repeat).
        //
        // - Otherwise, remove the oldest collection and go back to the top
        //   (repeat).
        //
        // This seems surprisingly complicated.  It's designed to meet the above
        // goals.
        //
        // Is this going to work if multiple Nexuses are doing it concurrently?
        // This cannot remove the last complete collection because a given Nexus
        // will only remove a complete collection if it has seen a newer
        // complete one.  This cannot result in keeping fewer than "nkeep"
        // collections because any Nexus will only remove a collection if there
        // are "nkeep" newer ones.  In both of these cases, another Nexus might
        // remove one of the ones that the first Nexus was counting on keeping,
        // but only if there was a newer one to replace it.

        opctx.authorize(authz::Action::Modify, &authz::INVENTORY).await?;

        loop {
            match self.inventory_find_pruneable(opctx, nkeep).await? {
                None => break,
                Some(collection_id) => {
                    self.inventory_delete_collection(opctx, collection_id)
                        .await?
                }
            }
        }

        Ok(())
    }

    /// Return the oldest inventory collection that's eligible for pruning,
    /// if any
    ///
    /// The caller of this (non-pub) function is responsible for authz.
    async fn inventory_find_pruneable(
        &self,
        opctx: &OpContext,
        nkeep: u32,
    ) -> Result<Option<Uuid>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        // Diesel requires us to use aliases in order to refer to the
        // `inv_collection` table twice in the same query.
        let (inv_collection1, inv_collection2) = diesel::alias!(
            db::schema::inv_collection as inv_collection1,
            db::schema::inv_collection as inv_collection2
        );

        // This subquery essentially generates:
        //
        //    SELECT id FROM inv_collection ORDER BY time_started" ASC LIMIT $1
        //
        // where $1 becomes `nkeep + 1`.  This just lists the `nkeep + 1` oldest
        // collections.
        let subquery = inv_collection1
            .select(inv_collection1.field(db::schema::inv_collection::id))
            .order_by(
                inv_collection1
                    .field(db::schema::inv_collection::time_started)
                    .asc(),
            )
            .limit(i64::from(nkeep) + 1);

        // This essentially generates:
        //
        //     SELECT
        //         inv_collection.id,
        //         count(inv_collection_error.inv_collection_id)
        //     FROM (
        //             inv_collection
        //         LEFT OUTER JOIN
        //             inv_collection_error
        //         ON (
        //             inv_collection_error.inv_collection_id = inv_collection.id
        //         )
        //     ) WHERE (
        //         inv_collection.id = ANY( <<subquery above>> )
        //     )
        //     GROUP BY inv_collection.id
        //     ORDER BY inv_collection.time_started ASC
        //
        // This looks a lot scarier than it is.  The goal is to produce a
        // two-column table that looks like this:
        //
        //     collection_id1     count of errors from collection_id1
        //     collection_id2     count of errors from collection_id2
        //     collection_id3     count of errors from collection_id3
        //     ...
        //
        let candidates: Vec<(Uuid, i64)> = inv_collection2
            .left_outer_join(db::schema::inv_collection_error::table)
            .filter(
                inv_collection2
                    .field(db::schema::inv_collection::id)
                    .eq_any(subquery),
            )
            .group_by(inv_collection2.field(db::schema::inv_collection::id))
            .select((
                inv_collection2.field(db::schema::inv_collection::id),
                diesel::dsl::count(
                    db::schema::inv_collection_error::inv_collection_id
                        .nullable(),
                ),
            ))
            .order_by(
                inv_collection2
                    .field(db::schema::inv_collection::time_started)
                    .asc(),
            )
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .internal_context("listing oldest collections")?;

        if u32::try_from(candidates.len()).unwrap() <= nkeep {
            debug!(
                &opctx.log,
                "inventory_prune_one: nothing eligible for removal (too few)";
                "candidates" => ?candidates,
            );
            return Ok(None);
        }

        // We've now got up to "nkeep + 1" oldest collections, starting with the
        // very oldest.  We can get rid of the oldest one unless it's the only
        // complete one.  Another way to think about it: find the _last_
        // complete one.  Remove it from the list of candidates.  Now mark the
        // first item in the remaining list for deletion.
        let last_completed_idx = candidates
            .iter()
            .enumerate()
            .rev()
            .find(|(_i, (_collection_id, nerrors))| *nerrors == 0);
        let candidate = match last_completed_idx {
            Some((i, _)) if i == 0 => candidates.iter().skip(1).next(),
            _ => candidates.iter().next(),
        }
        .map(|(collection_id, _nerrors)| *collection_id);
        if let Some(c) = candidate {
            debug!(
                &opctx.log,
                "inventory_prune_one: eligible for removal";
                "collection_id" => c.to_string(),
                "candidates" => ?candidates,
            );
        } else {
            debug!(
                &opctx.log,
                "inventory_prune_one: nothing eligible for removal";
                "candidates" => ?candidates,
            );
        }
        Ok(candidate)
    }

    /// Removes an inventory collection from the database
    ///
    /// The caller of this (non-pub) function is responsible for authz.
    async fn inventory_delete_collection(
        &self,
        opctx: &OpContext,
        collection_id: Uuid,
    ) -> Result<(), Error> {
        // As with inserting a whole collection, we remove it in one big
        // transaction for simplicity.  Similar considerations apply.  We could
        // break it up if these transactions become too big.  But we'd need a
        // way to stop other clients from discovering a collection after we
        // start removing it and we'd also need to make sure we didn't leak a
        // collection if we crash while deleting it.
        let conn = self.pool_connection_authorized(opctx).await?;
        let (ncollections, nsps, nrots, ncabooses, nerrors) = conn
            .transaction_async(|conn| async move {
                // Remove the record describing the collection itself.
                let ncollections = {
                    use db::schema::inv_collection::dsl;
                    diesel::delete(
                        dsl::inv_collection.filter(dsl::id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await?;
                };

                // Remove rows for service processors.
                let nsps = {
                    use db::schema::inv_service_processor::dsl;
                    diesel::delete(
                        dsl::inv_service_processor
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await?;
                };

                // Remove rows for roots of trust.
                let nrots = {
                    use db::schema::inv_root_of_trust::dsl;
                    diesel::delete(
                        dsl::inv_root_of_trust
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await?;
                };

                // Remove rows for cabooses found.
                let ncabooses = {
                    use db::schema::inv_caboose::dsl;
                    diesel::delete(
                        dsl::inv_caboose
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await?;
                };

                // Remove rows for errors encountered.
                let nerrors = {
                    use db::schema::inv_collection_error::dsl;
                    diesel::delete(
                        dsl::inv_collection_error
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await?;
                };

                Ok((ncollections, nsps, nrots, ncabooses, nerrors))
            })
            .await
            .map_err(|error| match error {
                TransactionError::CustomError(e) => e,
                TransactionError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        info!(&opctx.log, "removed inventory collection";
            "collection_id" => collection_id.to_string(),
            "ncollections" => ncollections,
            "nsps" => nsps,
            "nrots" => nrots,
            "ncabooses" => ncabooses,
            "nerrors" => nerrors,
        );

        Ok(())
    }
}
