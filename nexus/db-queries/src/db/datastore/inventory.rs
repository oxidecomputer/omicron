// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use crate::db::TransactionError;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use diesel::expression::SelectableHelper;
use diesel::sql_types::Nullable;
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::IntoSql;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::QueryDsl;
use diesel::Table;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::CabooseWhichEnum;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::HwPowerState;
use nexus_db_model::HwPowerStateEnum;
use nexus_db_model::HwRotSlot;
use nexus_db_model::HwRotSlotEnum;
use nexus_db_model::InvCaboose;
use nexus_db_model::InvCollection;
use nexus_db_model::InvCollectionError;
use nexus_db_model::InvRootOfTrust;
use nexus_db_model::InvServiceProcessor;
use nexus_db_model::SpType;
use nexus_db_model::SpTypeEnum;
use nexus_db_model::SwCaboose;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;
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
        let error_values = collection
            .errors
            .iter()
            .enumerate()
            .map(|(i, message)| {
                let index = u16::try_from(i).map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to convert error index to u16 (too \
                                many errors in inventory collection?): {}",
                        e
                    ))
                })?;
                Ok(InvCollectionError::new(
                    collection_id,
                    index,
                    message.clone(),
                ))
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
                    ) = rot_dsl::inv_root_of_trust::all_columns();
                }
            }

            // Insert rows for the cabooses that we found.  Like service
            // processors and roots of trust, we do this using INSERT INTO ...
            // SELECT.  This one's a little more complicated because there are
            // two foreign keys.  Concretely, we have these three tables:
            //
            // - `hw_baseboard` with an "id" primary key and lookup columns
            //   "part_number" and "serial_number"
            // - `sw_caboose` with an "id" primary key and lookup columns
            //   "board", "git_commit", "name", and "version"
            // - `inv_caboose` with foreign keys "hw_baseboard_id",
            //   "sw_caboose_id", and various other columns
            //
            // We want to INSERT INTO `inv_caboose` a row with:
            //
            // - hw_baseboard_id (foreign key) the result of looking up an
            //   hw_baseboard row by a specific part number and serial number
            //
            // - sw_caboose_id (foreign key) the result of looking up a
            //   specific sw_caboose row by board, git_commit, name, and version
            //
            // - the other columns being literals
            //
            // To achieve this, we're going to generate something like:
            //
            //     INSERT INTO
            //         inv_caboose (
            //             hw_baseboard_id,
            //             sw_caboose_id,
            //             inv_collection_id,
            //             time_collected,
            //             source,
            //             which,
            //         )
            //         SELECT (
            //             hw_baseboard_id.id,
            //             sw_caboose.id,
            //             ...              /* literal collection id */
            //             ...              /* literal time collected */
            //             ...              /* literal source */
            //             ...              /* literal 'which' */
            //         )
            //         FROM
            //             hw_baseboard
            //         INNER JOIN
            //             sw_caboose
            //         ON  hw_baseboard.part_number = ...
            //         AND hw_baseboard.serial_number = ...
            //         AND sw_caboose.board = ...
            //         AND sw_caboose.git_commit = ...
            //         AND sw_caboose.name = ...
            //         AND sw_caboose.version = ...;
            //
            // Again, the whole point is to avoid back-and-forth between the
            // client and the database.  Those back-and-forth interactions can
            // significantly increase latency and the probability of transaction
            // conflicts.  See RFD 192 for details.  (Unfortunately, we still
            // _are_ going back and forth here to issue each of these queries.
            // But that's an artifact of the interface we currently have for
            // sending queries.  It should be possible to send all of these in
            // one batch.
            for (which, tree) in &collection.cabooses_found {
                let db_which = nexus_db_model::CabooseWhich::from(*which);
                for (baseboard_id, found_caboose) in tree {
                    use db::schema::hw_baseboard_id::dsl as dsl_baseboard_id;
                    use db::schema::inv_caboose::dsl as dsl_inv_caboose;
                    use db::schema::sw_caboose::dsl as dsl_sw_caboose;

                    let selection = db::schema::hw_baseboard_id::table
                        .inner_join(
                            db::schema::sw_caboose::table.on(
                                dsl_baseboard_id::part_number
                                    .eq(baseboard_id.part_number.clone())
                                    .and(
                                        dsl_baseboard_id::serial_number.eq(
                                            baseboard_id.serial_number.clone(),
                                        ),
                                    )
                                    .and(dsl_sw_caboose::board.eq(
                                        found_caboose.caboose.board.clone(),
                                    ))
                                    .and(
                                        dsl_sw_caboose::git_commit.eq(
                                            found_caboose
                                                .caboose
                                                .git_commit
                                                .clone(),
                                        ),
                                    )
                                    .and(
                                        dsl_sw_caboose::name.eq(found_caboose
                                            .caboose
                                            .name
                                            .clone()),
                                    )
                                    .and(dsl_sw_caboose::version.eq(
                                        found_caboose.caboose.version.clone(),
                                    )),
                            ),
                        )
                        .select((
                            dsl_baseboard_id::id,
                            dsl_sw_caboose::id,
                            collection_id.into_sql::<diesel::sql_types::Uuid>(),
                            found_caboose
                                .time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            found_caboose
                                .source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            db_which.into_sql::<CabooseWhichEnum>(),
                        ));

                    let _ = diesel::insert_into(db::schema::inv_caboose::table)
                        .values(selection)
                        .into_columns((
                            dsl_inv_caboose::hw_baseboard_id,
                            dsl_inv_caboose::sw_caboose_id,
                            dsl_inv_caboose::inv_collection_id,
                            dsl_inv_caboose::time_collected,
                            dsl_inv_caboose::source,
                            dsl_inv_caboose::which,
                        ))
                        .execute_async(&conn)
                        .await?;

                    // See the comments above.  The same applies here.  If you
                    // update the statement below because the schema for
                    // `inv_caboose` has changed, be sure to update the code
                    // above, too!
                    let (
                        _hw_baseboard_id,
                        _sw_caboose_id,
                        _inv_collection_id,
                        _time_collected,
                        _source,
                        _which,
                    ) = dsl_inv_caboose::inv_caboose::all_columns();
                }
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

/// A SQL common table expression (CTE) used to insert into `inv_caboose`
///
/// Concretely, we have these three tables:
///
/// - `hw_baseboard` with an "id" primary key and lookup columns "part_number"
///    and "serial_number"
/// - `sw_caboose` with an "id" primary key and lookup columns "board",
///    "git_commit", "name", and "version"
/// - `inv_caboose` with foreign keys "hw_baseboard_id", "sw_caboose_id", and
///    various other columns
///
/// We want to INSERT INTO `inv_caboose` a row with:
///
/// - hw_baseboard_id (foreign key) the result of looking up an hw_baseboard row
///   by part number and serial number provided by the caller
///
/// - sw_caboose_id (foreign key) the result of looking up a sw_caboose row by
///   board, git_commit, name, and version provided by the caller
///
/// - the other columns being literals provided by the caller
///
/// To achieve this, we're going to generate something like:
///
/// WITH
///     my_new_row
/// AS (
///     SELECT
///         hw_baseboard.id, /* `hw_baseboard` foreign key */
///         sw_caboose.id,   /* `sw_caboose` foreign key */
///         ...              /* caller-provided literal values for the rest */
///                          /* of the new inv_caboose row */
///     FROM
///         hw_baseboard,
///         sw_caboose
///     WHERE
///         hw_baseboard.part_number = ...   /* caller-provided part number */
///         hw_baseboard.serial_number = ... /* caller-provided serial number */
///         sw_caboose.board = ...           /* caller-provided board */
///         sw_caboose.git_commit = ...      /* caller-provided git_commit */
///         sw_caboose.name = ...            /* caller-provided name */
///         sw_caboose.version = ...         /* caller-provided version */
/// ) INSERT INTO
///     inv_caboose (... /* inv_caboose columns */)
///     SELECT * from my_new_row;
///
/// The whole point is to avoid back-and-forth between the client and the
/// database.  Those back-and-forth interactions can significantly increase
/// latency and the probability of transaction conflicts.  See RFD 192 for
/// details.

/// Extra interfaces that are not intended (and potentially unsafe) for use in
/// Nexus, but useful for testing and `omdb`
pub trait DataStoreInventoryTest: Send + Sync {
    /// List all collections
    ///
    /// This does not paginate.
    fn inventory_collections(&self) -> BoxFuture<anyhow::Result<Vec<Uuid>>>;

    /// Make a best effort to read the given collection while limiting queries
    /// to `limit` results.  Returns as much as it was able to get.  The
    /// returned bool indicates whether the returned collection might be
    /// incomplete because the limit was reached.
    fn inventory_collection_read_best_effort(
        &self,
        id: Uuid,
        limit: NonZeroU32,
    ) -> BoxFuture<anyhow::Result<(Collection, bool)>>;

    /// Attempt to read the given collection while limiting queries to `limit`
    /// records
    fn inventory_collection_read_all_or_nothing(
        &self,
        id: Uuid,
        limit: NonZeroU32,
    ) -> BoxFuture<anyhow::Result<Collection>> {
        async move {
            let (collection, limit_reached) =
                self.inventory_collection_read_best_effort(id, limit).await?;
            anyhow::ensure!(
                !limit_reached,
                "hit limit of {} records while loading collection",
                limit
            );
            Ok(collection)
        }
        .boxed()
    }
}

impl DataStoreInventoryTest for DataStore {
    fn inventory_collections(&self) -> BoxFuture<anyhow::Result<Vec<Uuid>>> {
        async {
            let conn = self
                .pool_connection_for_tests()
                .await
                .context("getting connectoin")?;
            conn.transaction_async(|conn| async move {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                    .await
                    .context("failed to allow table scan")?;

                use db::schema::inv_collection::dsl;
                dsl::inv_collection
                    .select(dsl::id)
                    .order_by(dsl::time_started)
                    .load_async(&conn)
                    .await
                    .context("failed to list collections")
            })
            .await
        }
        .boxed()
    }

    // This function could move into the datastore if it proves helpful.  We'd
    // need to work out how to report the usual type of Error.  For now we don't
    // need it so we limit its scope to the test suite.
    fn inventory_collection_read_best_effort(
        &self,
        id: Uuid,
        limit: NonZeroU32,
    ) -> BoxFuture<anyhow::Result<(Collection, bool)>> {
        async move {
            let conn = &self
                .pool_connection_for_tests()
                .await
                .context("getting connection")?;
            let sql_limit = i64::from(u32::from(limit));
            let usize_limit = usize::try_from(u32::from(limit)).unwrap();
            let mut limit_reached = false;
            let (time_started, time_done, collector) = {
                use db::schema::inv_collection::dsl;

                let collections = dsl::inv_collection
                    .filter(dsl::id.eq(id))
                    .limit(2)
                    .select(InvCollection::as_select())
                    .load_async(&**conn)
                    .await
                    .context("loading collection")?;
                anyhow::ensure!(collections.len() == 1);
                let collection = collections.into_iter().next().unwrap();
                (
                    collection.time_started,
                    collection.time_done,
                    collection.collector,
                )
            };

            let errors: Vec<String> = {
                use db::schema::inv_collection_error::dsl;
                dsl::inv_collection_error
                    .filter(dsl::inv_collection_id.eq(id))
                    .order_by(dsl::idx)
                    .limit(sql_limit)
                    .select(InvCollectionError::as_select())
                    .load_async(&**conn)
                    .await
                    .context("loading collection errors")?
                    .into_iter()
                    .map(|e| e.message)
                    .collect()
            };
            limit_reached = limit_reached || errors.len() == usize_limit;

            let sps: BTreeMap<_, _> = {
                use db::schema::inv_service_processor::dsl;
                dsl::inv_service_processor
                    .filter(dsl::inv_collection_id.eq(id))
                    .limit(sql_limit)
                    .select(InvServiceProcessor::as_select())
                    .load_async(&**conn)
                    .await
                    .context("loading service processors")?
                    .into_iter()
                    .map(|sp_row| {
                        let baseboard_id = sp_row.hw_baseboard_id;
                        (
                            baseboard_id,
                            nexus_types::inventory::ServiceProcessor::from(
                                sp_row,
                            ),
                        )
                    })
                    .collect()
            };
            limit_reached = limit_reached || sps.len() == usize_limit;

            let rots: BTreeMap<_, _> = {
                use db::schema::inv_root_of_trust::dsl;
                dsl::inv_root_of_trust
                    .filter(dsl::inv_collection_id.eq(id))
                    .limit(sql_limit)
                    .select(InvRootOfTrust::as_select())
                    .load_async(&**conn)
                    .await
                    .context("loading roots of trust")?
                    .into_iter()
                    .map(|rot_row| {
                        let baseboard_id = rot_row.hw_baseboard_id;
                        (
                            baseboard_id,
                            nexus_types::inventory::RotState::from(rot_row),
                        )
                    })
                    .collect()
            };
            limit_reached = limit_reached || rots.len() == usize_limit;

            // Collect the unique baseboard ids referenced by SPs and RoTs.
            let baseboard_id_ids: BTreeSet<_> =
                sps.keys().chain(rots.keys()).cloned().collect();
            // Fetch the corresponding baseboard records.
            let baseboards_by_id: BTreeMap<_, _> = {
                use db::schema::hw_baseboard_id::dsl;
                dsl::hw_baseboard_id
                    .filter(dsl::id.eq_any(baseboard_id_ids))
                    .limit(sql_limit)
                    .select(HwBaseboardId::as_select())
                    .load_async(&**conn)
                    .await
                    .context("loading baseboards")?
                    .into_iter()
                    .map(|bb| {
                        (
                            bb.id,
                            Arc::new(
                                nexus_types::inventory::BaseboardId::from(bb),
                            ),
                        )
                    })
                    .collect()
            };
            limit_reached =
                limit_reached || baseboards_by_id.len() == usize_limit;

            // Having those, we can replace the keys in the maps above with
            // references to the actual baseboard rather than the uuid.
            let sps = sps
                .into_iter()
                .map(|(id, sp)| {
                    baseboards_by_id
                        .get(&id)
                        .map(|bb| (bb.clone(), sp))
                        .ok_or_else(|| {
                            anyhow!(
                                "missing baseboard that we should have fetched"
                            )
                        })
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let rots =
                rots.into_iter()
                    .map(|(id, rot)| {
                        baseboards_by_id
                    .get(&id)
                    .map(|bb| (bb.clone(), rot))
                    .ok_or_else(|| {
                        anyhow!("missing baseboard that we should have fetched")
                    })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;

            // Fetch records of cabooses found.
            let inv_caboose_rows = {
                use db::schema::inv_caboose::dsl;
                dsl::inv_caboose
                    .filter(dsl::inv_collection_id.eq(id))
                    .limit(sql_limit)
                    .select(InvCaboose::as_select())
                    .load_async(&**conn)
                    .await
                    .context("loading inv_cabooses")?
            };
            limit_reached =
                limit_reached || inv_caboose_rows.len() == usize_limit;

            // Collect the unique sw_caboose_ids for those cabooses.
            let sw_caboose_ids: BTreeSet<_> = inv_caboose_rows
                .iter()
                .map(|inv_caboose| inv_caboose.sw_caboose_id)
                .collect();
            // Fetch the corresponing records.
            let cabooses_by_id: BTreeMap<_, _> = {
                use db::schema::sw_caboose::dsl;
                dsl::sw_caboose
                    .filter(dsl::id.eq_any(sw_caboose_ids))
                    .limit(sql_limit)
                    .select(SwCaboose::as_select())
                    .load_async(&**conn)
                    .await
                    .context("loading sw_cabooses")?
                    .into_iter()
                    .map(|sw_caboose_row| {
                        (
                            sw_caboose_row.id,
                            Arc::new(nexus_types::inventory::Caboose::from(
                                sw_caboose_row,
                            )),
                        )
                    })
                    .collect()
            };
            limit_reached =
                limit_reached || cabooses_by_id.len() == usize_limit;

            // Assemble the lists of cabooses found.
            let mut cabooses_found = BTreeMap::new();
            for c in inv_caboose_rows {
                let by_baseboard = cabooses_found
                    .entry(nexus_types::inventory::CabooseWhich::from(c.which))
                    .or_insert_with(BTreeMap::new);
                let Some(bb) = baseboards_by_id.get(&c.hw_baseboard_id) else {
                    bail!(
                        "unknown baseboard found in inv_caboose: {}",
                        c.hw_baseboard_id
                    );
                };
                let Some(sw_caboose) = cabooses_by_id.get(&c.sw_caboose_id)
                else {
                    bail!(
                        "unknown caboose found in inv_caboose: {}",
                        c.sw_caboose_id
                    );
                };

                let previous = by_baseboard.insert(
                    bb.clone(),
                    nexus_types::inventory::CabooseFound {
                        time_collected: c.time_collected,
                        source: c.source,
                        caboose: sw_caboose.clone(),
                    },
                );
                anyhow::ensure!(
                    previous.is_none(),
                    "duplicate caboose found: {:?} baseboard {:?}",
                    c.which,
                    c.hw_baseboard_id
                );
            }

            Ok((
                Collection {
                    id,
                    errors,
                    time_started,
                    time_done,
                    collector,
                    baseboards: baseboards_by_id.values().cloned().collect(),
                    cabooses: cabooses_by_id.values().cloned().collect(),
                    sps,
                    rots,
                    cabooses_found,
                },
                limit_reached,
            ))
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::db::datastore::datastore_test;
    use crate::db::datastore::inventory::DataStoreInventoryTest;
    use crate::db::datastore::DataStore;
    use crate::db::datastore::DataStoreConnection;
    use crate::db::schema;
    use anyhow::Context;
    use async_bb8_diesel::AsyncConnection;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::QueryDsl;
    use gateway_client::types::SpType;
    use nexus_inventory::examples::representative;
    use nexus_inventory::examples::Representative;
    use nexus_test_utils::db::test_setup_database;
    use nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL;
    use nexus_types::inventory::CabooseWhich;
    use nexus_types::inventory::Collection;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    async fn read_collection(
        datastore: &DataStore,
        id: Uuid,
    ) -> anyhow::Result<Collection> {
        let limit = NonZeroU32::new(1000).unwrap();
        datastore.inventory_collection_read_all_or_nothing(id, limit).await
    }

    async fn count_baseboards_cabooses(
        conn: &DataStoreConnection<'_>,
    ) -> anyhow::Result<(usize, usize)> {
        conn.transaction_async(|conn| async move {
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await.unwrap();
            let bb_count = schema::hw_baseboard_id::dsl::hw_baseboard_id
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .context("failed to count baseboards")?;
            let caboose_count = schema::sw_caboose::dsl::sw_caboose
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .context("failed to count cabooses")?;
            let bb_count_usize = usize::try_from(bb_count)
                .context("failed to convert baseboard count to usize")?;
            let caboose_count_usize = usize::try_from(caboose_count)
                .context("failed to convert caboose count to usize")?;
            Ok((bb_count_usize, caboose_count_usize))
        })
        .await
    }

    /// Tests inserting several collections, reading them back, and making sure
    /// they look the same.
    #[tokio::test]
    async fn test_inventory_insert() {
        // Setup
        let logctx = dev::test_setup_log("inventory_insert");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create an empty collection and write it to the database.
        let builder = nexus_inventory::CollectionBuilder::new("test");
        let collection1 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection1)
            .await
            .expect("failed to insert collection");

        // Read it back.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let collection_read = read_collection(&datastore, collection1.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection1, collection_read);

        // There ought to be no baseboards or cabooses in the databases from
        // that collection.
        assert_eq!(collection1.baseboards.len(), 0);
        assert_eq!(collection1.cabooses.len(), 0);
        let (nbaseboards, ncabooses) =
            count_baseboards_cabooses(&conn).await.unwrap();
        assert_eq!(collection1.baseboards.len(), nbaseboards);
        assert_eq!(collection1.cabooses.len(), ncabooses);

        // Now insert a more complex collection, write it to the database, and
        // read it back.
        let Representative { builder, .. } = representative();
        let collection2 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection2)
            .await
            .expect("failed to insert collection");
        let collection_read = read_collection(&datastore, collection2.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection2, collection_read);
        // Verify that we have exactly the set of cabooses and baseboards in the
        // databases that came from this first non-empty collection.
        assert_ne!(collection2.baseboards.len(), collection1.baseboards.len());
        assert_ne!(collection2.cabooses.len(), collection1.cabooses.len());
        let (nbaseboards, ncabooses) =
            count_baseboards_cabooses(&conn).await.unwrap();
        assert_eq!(collection2.baseboards.len(), nbaseboards);
        assert_eq!(collection2.cabooses.len(), ncabooses);

        // Now insert an equivalent collection again.  Verify the distinct
        // baseboards and cabooses again.  This is important: the insertion
        // process should re-use the baseboards and cabooses from the previous
        // collection.
        let Representative { builder, .. } = representative();
        let collection3 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection3)
            .await
            .expect("failed to insert collection");
        let collection_read = read_collection(&datastore, collection3.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection3, collection_read);
        // Verify that we have the same number of cabooses and baseboards, since
        // those didn't change.
        assert_eq!(collection3.baseboards.len(), collection2.baseboards.len());
        assert_eq!(collection3.cabooses.len(), collection2.cabooses.len());
        let (nbaseboards, ncabooses) =
            count_baseboards_cabooses(&conn).await.unwrap();
        assert_eq!(collection3.baseboards.len(), nbaseboards);
        assert_eq!(collection3.cabooses.len(), ncabooses);

        // Now insert a collection that's almost equivalent, but has an extra
        // couple of baseboards and caboose.  Verify that we re-use the existing
        // ones, but still insert the new ones.
        let Representative { mut builder, .. } = representative();
        builder.found_sp_state(
            "test suite",
            SpType::Switch,
            1,
            nexus_inventory::examples::sp_state("2"),
        );
        let bb = builder
            .found_sp_state(
                "test suite",
                SpType::Power,
                1,
                nexus_inventory::examples::sp_state("3"),
            )
            .unwrap();
        builder
            .found_caboose(
                &bb,
                CabooseWhich::SpSlot0,
                "dummy",
                nexus_inventory::examples::caboose("dummy"),
            )
            .unwrap();
        let collection4 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection4)
            .await
            .expect("failed to insert collection");
        let collection_read = read_collection(&datastore, collection4.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection4, collection_read);
        // Verify the number of baseboards and collections again.
        assert_eq!(
            collection4.baseboards.len(),
            collection3.baseboards.len() + 2
        );
        assert_eq!(
            collection4.cabooses.len(),
            collection3.baseboards.len() + 1
        );
        let (nbaseboards, ncabooses) =
            count_baseboards_cabooses(&conn).await.unwrap();
        assert_eq!(collection4.baseboards.len(), nbaseboards);
        assert_eq!(collection4.cabooses.len(), ncabooses);

        // This time, go back to our earlier collection.  This logically removes
        // some baseboards.  They should still be present in the database, but
        // not in the collection.
        let Representative { builder, .. } = representative();
        let collection5 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection5)
            .await
            .expect("failed to insert collection");
        let collection_read = read_collection(&datastore, collection5.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection5, collection_read);
        assert_eq!(collection5.baseboards.len(), collection3.baseboards.len());
        assert_eq!(collection5.cabooses.len(), collection3.cabooses.len());
        assert_ne!(collection5.baseboards.len(), collection4.baseboards.len());
        assert_ne!(collection5.cabooses.len(), collection4.cabooses.len());
        let (nbaseboards, ncabooses) =
            count_baseboards_cabooses(&conn).await.unwrap();
        assert_eq!(collection4.baseboards.len(), nbaseboards);
        assert_eq!(collection4.cabooses.len(), ncabooses);

        // Try to insert the same collection again and make sure it fails.
        let error = datastore
            .inventory_insert_collection(&opctx, &collection5)
            .await
            .expect_err("unexpectedly succeeded in inserting collection");
        assert!(format!("{:#}", error)
            .contains("duplicate key value violates unique constraint"));

        // Now that we've inserted a bunch of collections, we can test pruning.
        //
        // The datastore should start by pruning the oldest collection, unless
        // it's the only collection with no errors.  The oldest one is
        // `collection1`, which _is_ the only one with no errors.  So we should
        // get back `collection2`.
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[
                collection1.id,
                collection2.id,
                collection3.id,
                collection4.id,
                collection5.id,
            ]
        );
        println!(
            "all collections: {:?}\n",
            &[
                collection1.id,
                collection2.id,
                collection3.id,
                collection4.id,
                collection5.id,
            ]
        );
        datastore
            .inventory_prune_collections(&opctx, 4)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection1.id, collection3.id, collection4.id, collection5.id,]
        );
        // Again, we should skip over collection1 and delete the next oldest:
        // collection3.
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection1.id, collection4.id, collection5.id,]
        );
        // At this point, if we're keeping 3, we don't need to prune anything.
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection1.id, collection4.id, collection5.id,]
        );

        // If we then insert an empty collection (which has no errors),
        // collection1 becomes pruneable.
        let builder = nexus_inventory::CollectionBuilder::new("test");
        let collection6 = builder.build();
        println!(
            "collection 6: {} ({:?})",
            collection6.id, collection6.time_started
        );
        datastore
            .inventory_insert_collection(&opctx, &collection6)
            .await
            .expect("failed to insert collection");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection1.id, collection4.id, collection5.id, collection6.id,]
        );
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection4.id, collection5.id, collection6.id,]
        );
        // Again, at this point, we should not prune anything.
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection4.id, collection5.id, collection6.id,]
        );

        // If we insert another collection with errors, then prune, we should
        // end up pruning collection 4.
        let Representative { builder, .. } = representative();
        let collection7 = builder.build();
        println!(
            "collection 7: {} ({:?})",
            collection7.id, collection7.time_started
        );
        datastore
            .inventory_insert_collection(&opctx, &collection7)
            .await
            .expect("failed to insert collection");
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection5.id, collection6.id, collection7.id,]
        );

        // If we try to fetch a pruned collection, we should get nothing.
        let _ = read_collection(&datastore, collection4.id)
            .await
            .expect_err("unexpectedly read pruned collection");

        // But we should still be able to fetch the collections that do exist.
        let collection_read =
            read_collection(&datastore, collection5.id).await.unwrap();
        assert_eq!(collection5, collection_read);
        let collection_read =
            read_collection(&datastore, collection6.id).await.unwrap();
        assert_eq!(collection6, collection_read);
        let collection_read =
            read_collection(&datastore, collection7.id).await.unwrap();
        assert_eq!(collection7, collection_read);

        // We should prune more than one collection, if needed.  We'll wind up
        // with just collection6 because that's the latest one with no errors.
        datastore
            .inventory_prune_collections(&opctx, 1)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore.inventory_collections().await.unwrap(),
            &[collection6.id,]
        );

        // Remove the remaining collection and make sure the inventory tables
        // are empty (i.e., we got everything).
        datastore
            .inventory_delete_collection(&opctx, collection6.id)
            .await
            .expect("failed to delete collection");
        assert_eq!(datastore.inventory_collections().await.unwrap(), &[]);

        conn.transaction_async(|conn| async move {
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await.unwrap();
            let count = schema::inv_collection::dsl::inv_collection
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_collection_error::dsl::inv_collection_error
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count =
                schema::inv_service_processor::dsl::inv_service_processor
                    .select(diesel::dsl::count_star())
                    .first_async::<i64>(&conn)
                    .await
                    .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_root_of_trust::dsl::inv_root_of_trust
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_caboose::dsl::inv_caboose
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            Ok::<(), anyhow::Error>(())
        })
        .await
        .expect("failed to check that tables were empty");

        // We currently keep the baseboard ids and sw_cabooses around.
        let (nbaseboards, ncabooses) =
            count_baseboards_cabooses(&conn).await.unwrap();
        assert_ne!(nbaseboards, 0);
        assert_ne!(ncabooses, 0);

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
