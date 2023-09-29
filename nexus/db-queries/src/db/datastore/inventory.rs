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
use async_bb8_diesel::PoolError;
use diesel::expression::AsExpression;
use diesel::query_dsl::methods::SelectDsl;
use diesel::IntoSql;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::HwPowerState;
use nexus_db_model::HwPowerStateEnum;
use nexus_db_model::InvCollection;
use nexus_types::inventory::BaseboardId;
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

            // Insert records for any baseboards that do not already exist in
            // the database.
            {
                use db::schema::hw_baseboard_id::dsl;
                for baseboard_id in &collection.baseboards {
                    let _ = diesel::insert_into(dsl::hw_baseboard_id)
                        .values(HwBaseboardId::from(baseboard_id.as_ref()))
                        .on_conflict_do_nothing()
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            TransactionError::CustomError(
                                public_error_from_diesel_pool(
                                    e.into(),
                                    ErrorHandler::Server,
                                )
                                .internal_context("inserting baseboard"),
                            )
                        });
                }
            }

            // XXX-dap insert the errors

            // XXX-dap remove this
            // We now need to fetch the ids for those baseboards.  We need these
            // ids in order to insert, for example, inv_service_processor
            // records, which have a foreign key into the hw_baseboard_id table.
            //
            // This approach sucks.  With raw SQL, we could do something like
            //
            // INSERT INTO inv_service_processor
            //     SELECT
            //         id
            //         [other service_processor column values]
            //       FROM hw_baseboard_id
            //       WHERE part_number = ... AND serial_number = ...;
            //
            // This way, we don't need to know the id directly.

            // XXX-dap
            //self.inventory_insert_cabooses(opctx, &collection.cabooses, &conn)
            //    .await?;

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
                            sp.source.into_sql::<diesel::sql_types::Text>(),
                            i64::from(sp.baseboard_revision)
                                .into_sql::<diesel::sql_types::Int8>(),
                            sp.hubris_archive
                                .into_sql::<diesel::sql_types::Text>(),
                            HwPowerState::from(sp.power_state).into_sql(),
                        ));

                    diesel::insert_into(
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

            //self.inventory_insert_sps(opctx, &collection.sps, &conn).await?;
            //self.inventory_insert_cabooses_found(
            //    opctx,
            //    &collection.cabooses,
            //    &collection.sps,
            //    &conn,
            //)
            //.await?;
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

    //async fn inventory_insert_baseboards<ConnErr>(
    //    &self,
    //    baseboards: impl Iterator<Item = &'_ BaseboardId>,
    //    conn: &(impl async_bb8_diesel::AsyncConnection<
    //        crate::db::pool::DbConnection,
    //        ConnErr,
    //    > + Sync),
    //) -> Result<(), TransactionError<Error>>
    //where
    //    ConnErr: From<diesel::result::Error> + Send + 'static,
    //    ConnErr: Into<PoolError>,
    //{
    //    use db::schema::hw_baseboard_id::dsl;

    //    for baseboard_id in baseboards {
    //        let _ = diesel::insert_into(dsl::hw_baseboard_id)
    //            .values(HwBaseboardId::from(baseboard_id))
    //            .on_conflict_do_nothing()
    //            .execute_async(conn)
    //            .await
    //            .map_err(|e| {
    //                TransactionError::CustomError(
    //                    public_error_from_diesel_pool(
    //                        e.into(),
    //                        ErrorHandler::Server,
    //                    )
    //                    .internal_context("inserting baseboard"),
    //                )
    //            });
    //    }

    //    Ok(())
    //}
}
