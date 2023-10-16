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
use chrono::DateTime;
use chrono::Utc;
use diesel::sql_types;
use diesel::sql_types::Nullable;
use diesel::Column;
use diesel::ExpressionMethods;
use diesel::IntoSql;
use diesel::NullableExpressionMethods;
use diesel::QueryDsl;
use diesel::QuerySource;
use diesel::Table;
use nexus_db_model::CabooseWhich;
use nexus_db_model::CabooseWhichEnum;
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
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseFound;
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

        // It's not critical that this be done in one transaction.  But it keeps
        // the database a little tidier because we won't have half-inserted
        // collections in there.
        //
        // Even better would be a large hunk of SQL sent over at once.  That is
        // in principle easy to do because none of this needs to be interactive.
        // But we don't yet have support for this.  See
        // oxidecomputer/omicron#973.
        let pool = self.pool_connection_authorized(opctx).await?;
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
                            public_error_from_diesel(
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
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting baseboards"),
                        )
                    })?;
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
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting cabooses"),
                        )
                    })?;
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
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting service processor"),
                        )
                    })?;

                    // This statement is just here to force a compilation error
                    // if the set of columns in `inv_service_processor` changes.
                    // The code above attempts to insert a row into
                    // `inv_service_processor` using an explicit list of columns
                    // and values.  Without the following statement, If a new
                    // required column were added, this would fail at runtime.
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
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting service processor"),
                        )
                    })?;

                    // See the comment in the previous block (where we use
                    // `inv_service_processor::all_columns()`).  The same
                    // applies here.
                    let (
                        _inv_collection_id,
                        _hw_baseboard_id,
                        _time_collected,
                        _source,
                        _rot_slot_active,
                        _rot_slot_boot_pref_persistent,
                        _rot_slot_boot_pref_persistent_pending,
                        _rot_slot_boot_pref_transient,
                        _rot_slot_a_sha3_256,
                        _rot_slot_b_sha3_256,
                    ) = rot_dsl::inv_root_of_trust::all_columns();
                }
            }

            // Insert rows for the cabooses that we found.  Like service
            // processors and roots of trust, we do this using INSERT INTO ...
            // SELECT.  But because there are two foreign keys, we need a more
            // complicated `SELECT`, which requires using a CTE.
            for (which, tree) in &collection.cabooses_found {
                let db_which = nexus_db_model::CabooseWhich::from(*which);
                for (baseboard_id, found_caboose) in tree {
                    InvCabooseInsert::new(
                        collection_id,
                        baseboard_id,
                        found_caboose,
                        db_which,
                    )
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting found caboose"),
                        )
                    })?;
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
                        let index =
                            u16::try_from(i).map_err(|e| {
                                Error::internal_error(&format!(
                                "failed to convert error index to u16 (too \
                                many errors in inventory collection?): {}", e))
                            })?;
                        let message = format!("{:#}", error);
                        Ok(InvCollectionError::new(
                            collection_id,
                            index,
                            message,
                        ))
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                let _ = diesel::insert_into(errors_dsl::inv_collection_error)
                    .values(error_values)
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("inserting errors"),
                        )
                    })?;
            }

            Ok(())
        })
        .await
        .map_err(|error| match error {
            TransactionError::CustomError(e) => e,
            TransactionError::Connection(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        })?;

        info!(
            &opctx.log,
            "inserted inventory collection";
            "collection_id" => collection.id.to_string(),
        );

        Ok(())
    }

    // XXX-dap TODO-doc
    // This seems like a high-level function, but we want to push some of the
    // logic into the SQL.
    pub async fn inventory_prune_collections(
        &self,
        opctx: &OpContext,
        nkeep: u32,
    ) -> Result<(), Error> {
        // There could be any number of collections in the database: 0, 1, ...,
        // nkeep, nkeep + 1, ..., up to a very large number.  Of these, some of
        // these are potentially incomplete.  We use a non-zero error count as a
        // proxy for that.  We never want to remove the last successful
        // collection, even if there have been a lot of more recent (incomplete)
        // collections.
        //
        // Here's how we'll do it:
        //
        // - From the database, select the latest-started collection that has no
        //   errors associated with it.  We're going to hang onto this one.
        // - Select the oldest `nkeep` collections that *aren't* the one we want
        //   to hang onto.
        // - Remove the oldest collection in the returned set.
        // - Repeat until the returned set is empty.
        //
        // This admittedly feels overcomplicated.  If we were willing to start
        // by pruning the *latest* collection eligible for removal, we could
        // simply select collections most recent collections and use `OFFSET` to
        // skip the last "nkeep".  But if for some reason we're falling behind,
        // it seems much better to prune from the oldest first.
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

    async fn inventory_find_pruneable(
        &self,
        opctx: &OpContext,
        nkeep: u32,
    ) -> Result<Option<Uuid>, Error> {
        // The caller of this (non-pub) function is responsible for authz.

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
        // where $1 becomes `nkeep + 1`.  This just lists the N oldest
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
            .map_err(|e| {
                public_error_from_diesel(e.into(), ErrorHandler::Server)
            })
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

    async fn inventory_delete_collection(
        &self,
        opctx: &OpContext,
        collection_id: Uuid,
    ) -> Result<(), Error> {
        // The caller of this (non-pub) function is responsible for authz.

        // We do this in a transaction for simplicity.  If these transactions
        // got too big, we could break it up, but we'd need a way to update the
        // collection to say it's being deleted.  (We can't delete it first
        // because then we'd have no efficient way to identify records in the
        // other tables that need to be deleted.)
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
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context(
                                "removing inventory collection record",
                            ),
                        )
                    })?;
                };

                // Remove rows for service processors.
                let nsps = {
                    use db::schema::inv_service_processor::dsl;
                    diesel::delete(
                        dsl::inv_service_processor
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context(
                                "removing service processor records",
                            ),
                        )
                    })?;
                };

                // Remove rows for service processors.
                let nrots = {
                    use db::schema::inv_root_of_trust::dsl;
                    diesel::delete(
                        dsl::inv_root_of_trust
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("removing root of trust records"),
                        )
                    })?;
                };

                // Remove rows for cabooses found.
                let ncabooses = {
                    use db::schema::inv_caboose::dsl;
                    diesel::delete(
                        dsl::inv_caboose
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context(
                                "removing cabooses-found records",
                            ),
                        )
                    })?;
                };

                // Remove rows for errors encountered.
                let nerrors = {
                    use db::schema::inv_collection_error::dsl;
                    diesel::delete(
                        dsl::inv_collection_error
                            .filter(dsl::inv_collection_id.eq(collection_id)),
                    )
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        // XXX-dap do we really have to do this mapping in every
                        // one of these?
                        TransactionError::CustomError(
                            public_error_from_diesel(
                                e.into(),
                                ErrorHandler::Server,
                            )
                            .internal_context("removing error records"),
                        )
                    })?;
                };

                Ok((ncollections, nsps, nrots, ncabooses, nerrors))
            })
            .await
            .map_err(|error| match error {
                TransactionError::CustomError(e) => e,
                TransactionError::Connection(e) => {
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

/// A CTE used to insert into `inv_caboose`
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
#[must_use = "Queries must be executed"]
struct InvCabooseInsert {
    // fields used to look up baseboard id
    baseboard_part_number: String,
    baseboard_serial_number: String,

    // fields used to look up caboose id
    caboose_board: String,
    caboose_git_commit: String,
    caboose_name: String,
    caboose_version: String,

    // literal values for the rest of the inv_caboose columns
    collection_id: Uuid,
    time_collected: DateTime<Utc>,
    source: String,
    which: CabooseWhich,

    // These are Diesel structures representing table names in the "from" or
    // "into" parts of queries (e.g., "SELECT FROM tablename" or "INSERT INTO
    // tablename").  We need this in `walk_ast()` below, but they must outlive
    // `walk_ast()`, so they need to be created ahead of time.
    //
    // TODO-cleanup These Diesel-internal types are nasty.  It's not clear how
    // else to do this.
    from_hw_baseboard_id:
        diesel::internal::table_macro::StaticQueryFragmentInstance<
            db::schema::hw_baseboard_id::table,
        >,
    from_sw_caboose: diesel::internal::table_macro::StaticQueryFragmentInstance<
        db::schema::sw_caboose::table,
    >,
    into_inv_caboose:
        diesel::internal::table_macro::StaticQueryFragmentInstance<
            db::schema::inv_caboose::table,
        >,
}

impl InvCabooseInsert {
    pub fn new(
        collection_id: Uuid,
        baseboard: &BaseboardId,
        found_caboose: &CabooseFound,
        which: CabooseWhich,
    ) -> InvCabooseInsert {
        InvCabooseInsert {
            baseboard_part_number: baseboard.part_number.clone(),
            baseboard_serial_number: baseboard.serial_number.clone(),
            caboose_board: found_caboose.caboose.board.clone(),
            caboose_git_commit: found_caboose.caboose.git_commit.clone(),
            caboose_name: found_caboose.caboose.name.clone(),
            caboose_version: found_caboose.caboose.version.clone(),
            collection_id,
            time_collected: found_caboose.time_collected,
            source: found_caboose.source.clone(),
            which,
            from_hw_baseboard_id: db::schema::hw_baseboard_id::table
                .from_clause(),
            from_sw_caboose: db::schema::sw_caboose::table.from_clause(),
            // It sounds a little goofy to use "from_clause()" when this is
            // really part of an INSERT.  But really this just produces the
            // table name as an identifier.  This is the same for both "FROM"
            // and "INSERT" clauses.  And diesel internally does the same thing
            // here (see the type of `InsertStatement::into_clause`).
            into_inv_caboose: db::schema::inv_caboose::table.from_clause(),
        }
    }
}

impl diesel::query_builder::QueryFragment<diesel::pg::Pg> for InvCabooseInsert {
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, diesel::pg::Pg>,
    ) -> diesel::QueryResult<()> {
        use db::schema::hw_baseboard_id::dsl as dsl_baseboard_id;
        use db::schema::inv_caboose::dsl as dsl_inv_caboose;
        use db::schema::sw_caboose::dsl as dsl_sw_caboose;

        pass.unsafe_to_cache_prepared();
        pass.push_sql("WITH my_new_row AS (");

        pass.push_sql("SELECT ");

        // Emit the values that we're going to insert into `inv_caboose`.
        // First, emit the looked-up foreign keys.
        self.from_hw_baseboard_id.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_baseboard_id::id::NAME)?;
        pass.push_sql(", ");
        self.from_sw_caboose.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_sw_caboose::id::NAME)?;
        pass.push_sql(", ");
        // Next, emit the literal values used for the rest of the columns.
        pass.push_bind_param::<sql_types::Uuid, _>(&self.collection_id)?;
        pass.push_sql(", ");
        pass.push_bind_param::<sql_types::Timestamptz, _>(
            &self.time_collected,
        )?;
        pass.push_sql(", ");
        pass.push_bind_param::<sql_types::Text, _>(&self.source)?;
        pass.push_sql(", ");
        pass.push_bind_param::<CabooseWhichEnum, _>(&self.which)?;

        // Finish the SELECT by adding the list of tables and the WHERE to pick
        // out only the relevant row from each tables.
        pass.push_sql(" FROM ");

        self.from_hw_baseboard_id.walk_ast(pass.reborrow())?;
        pass.push_sql(", ");
        self.from_sw_caboose.walk_ast(pass.reborrow())?;

        pass.push_sql(" WHERE ");
        self.from_hw_baseboard_id.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_baseboard_id::part_number::NAME)?;
        pass.push_sql(" = ");
        pass.push_bind_param::<sql_types::Text, _>(
            &self.baseboard_part_number,
        )?;
        pass.push_sql(" AND ");
        self.from_hw_baseboard_id.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_baseboard_id::serial_number::NAME)?;
        pass.push_sql(" = ");
        pass.push_bind_param::<sql_types::Text, _>(
            &self.baseboard_serial_number,
        )?;
        pass.push_sql(" AND ");
        self.from_sw_caboose.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_sw_caboose::board::NAME)?;
        pass.push_sql(" = ");
        pass.push_bind_param::<sql_types::Text, _>(&self.caboose_board)?;
        pass.push_sql(" AND ");
        self.from_sw_caboose.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_sw_caboose::git_commit::NAME)?;
        pass.push_sql(" = ");
        pass.push_bind_param::<sql_types::Text, _>(&self.caboose_git_commit)?;
        pass.push_sql(" AND ");
        self.from_sw_caboose.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_sw_caboose::name::NAME)?;
        pass.push_sql(" = ");
        pass.push_bind_param::<sql_types::Text, _>(&self.caboose_name)?;
        pass.push_sql(" AND ");
        self.from_sw_caboose.walk_ast(pass.reborrow())?;
        pass.push_sql(".");
        pass.push_identifier(dsl_sw_caboose::version::NAME)?;
        pass.push_sql(" = ");
        pass.push_bind_param::<sql_types::Text, _>(&self.caboose_version)?;

        pass.push_sql(")\n"); // end of the SELECT query within the WITH

        pass.push_sql("INSERT INTO ");
        self.into_inv_caboose.walk_ast(pass.reborrow())?;

        pass.push_sql("(");
        pass.push_identifier(dsl_inv_caboose::hw_baseboard_id::NAME)?;
        pass.push_sql(", ");
        pass.push_identifier(dsl_inv_caboose::sw_caboose_id::NAME)?;
        pass.push_sql(", ");
        pass.push_identifier(dsl_inv_caboose::inv_collection_id::NAME)?;
        pass.push_sql(", ");
        pass.push_identifier(dsl_inv_caboose::time_collected::NAME)?;
        pass.push_sql(", ");
        pass.push_identifier(dsl_inv_caboose::source::NAME)?;
        pass.push_sql(", ");
        pass.push_identifier(dsl_inv_caboose::which::NAME)?;
        pass.push_sql(")\n");
        pass.push_sql("SELECT * FROM my_new_row");

        // See the comment in inventory_insert_collection() where we use
        // `inv_service_processor::all_columns()`.  The same applies here.
        // If you update the statement below because the schema for
        // `inv_caboose` has changed, be sure to update the code above, too!
        let (
            _hw_baseboard_id,
            _sw_caboose_id,
            _inv_collection_id,
            _time_collected,
            _source,
            _which,
        ) = dsl_inv_caboose::inv_caboose::all_columns();

        Ok(())
    }
}

// This is required to be able to call `inv_caboose_insert.execute_async()`.
impl diesel::RunQueryDsl<db::pool::DbConnection> for InvCabooseInsert {}

// This is required to be able to call `inv_caboose_insert.execute_async()`.
impl diesel::query_builder::QueryId for InvCabooseInsert {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
