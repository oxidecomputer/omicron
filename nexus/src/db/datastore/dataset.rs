// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Dataset`]s.

use super::DataStore;
use super::RunnableQuery;
use super::REGION_REDUNDANCY_THRESHOLD;
use crate::authz;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::DatasetRedundancy;
use crate::db::datastore::OpContext;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::DatasetKind;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::pool::DbConnection;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use std::net::SocketAddrV6;
use uuid::Uuid;

impl DataStore {
    /// Stores a new dataset in the database.
    pub async fn dataset_upsert(
        &self,
        dataset: Dataset,
    ) -> CreateResult<Dataset> {
        use db::schema::dataset::dsl;

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::dataset)
                .values(dataset.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::pool_id.eq(excluded(dsl::pool_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::port.eq(excluded(dsl::port)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Zpool,
                lookup_type: LookupType::ById(zpool_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Dataset,
                        &dataset.id().to_string(),
                    ),
                )
            }
        })
    }

    /// Stores a new dataset in the database.
    async fn dataset_upsert_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        dataset: Dataset,
    ) -> CreateResult<Dataset> {
        use db::schema::dataset::dsl;

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::dataset)
                .values(dataset.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::pool_id.eq(excluded(dsl::pool_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::port.eq(excluded(dsl::port)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Zpool,
                lookup_type: LookupType::ById(zpool_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            }
        })
    }

    pub async fn dataset_list(
        &self,
        opctx: &OpContext,
        zpool_id: Uuid,
    ) -> Result<Vec<Dataset>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::dataset::dsl;
        dsl::dataset
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::pool_id.eq(zpool_id))
            .select(Dataset::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    async fn sled_zpool_and_dataset_list_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: Uuid,
        kind: DatasetKind,
    ) -> Result<Vec<(Sled, Zpool, Option<Dataset>)>, Error> {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::sled::dsl as sled_dsl;
        use db::schema::zpool::dsl as zpool_dsl;

        db::schema::sled::table
            .filter(sled_dsl::time_deleted.is_null())
            .filter(sled_dsl::rack_id.eq(rack_id))
            .inner_join(
                db::schema::zpool::table.on(zpool_dsl::sled_id
                    .eq(sled_dsl::id)
                    .and(zpool_dsl::time_deleted.is_null())),
            )
            .left_outer_join(
                db::schema::dataset::table.on(dataset_dsl::pool_id
                    .eq(zpool_dsl::id)
                    .and(dataset_dsl::kind.eq(kind))
                    .and(dataset_dsl::time_deleted.is_null())),
            )
            .select(<(Sled, Zpool, Option<Dataset>)>::as_select())
            .get_results_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e.into(), ErrorHandler::Server)
            })
    }

    pub async fn ensure_rack_dataset(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        kind: DatasetKind,
        redundancy: DatasetRedundancy,
    ) -> Result<Vec<(Sled, Zpool, Dataset)>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        #[derive(Debug)]
        enum DatasetError {
            NotEnoughZpools,
            Other(Error),
        }
        type TxnError = TransactionError<DatasetError>;

        self.pool()
            .transaction_async(|conn| async move {
                let sleds_zpools_and_maybe_datasets =
                    Self::sled_zpool_and_dataset_list_on_connection(
                        &conn, rack_id, kind,
                    )
                    .await
                    .map_err(|e| {
                        TxnError::CustomError(DatasetError::Other(e.into()))
                    })?;

                // Split the set of returned zpools into "those with" and "those
                // without" the requested dataset.
                let (zpools_with_dataset, zpools_without_dataset): (
                    Vec<_>,
                    Vec<_>,
                ) = sleds_zpools_and_maybe_datasets
                    .into_iter()
                    .partition(|(_, _, maybe_dataset)| maybe_dataset.is_some());
                let mut zpools_without_dataset = zpools_without_dataset
                    .into_iter()
                    .map(|(sled, zpool, _)| (sled, zpool))
                    .peekable();

                let mut datasets: Vec<_> = zpools_with_dataset
                    .into_iter()
                    .map(|(sled, zpool, maybe_dataset)| {
                        (
                            sled,
                            zpool,
                            maybe_dataset.expect("Dataset should exist"),
                        )
                    })
                    .collect();

                // Add datasets to zpools, in-order, until we've met a
                // number sufficient for our redundancy.
                //
                // The selection of "which zpools contain this dataset" is completely
                // arbitrary.
                loop {
                    match redundancy {
                        DatasetRedundancy::OnAll => {
                            if zpools_without_dataset.peek().is_none() {
                                break;
                            }
                        }
                        DatasetRedundancy::PerRack(desired) => {
                            if datasets.len() >= (desired as usize) {
                                break;
                            }
                        }
                    };

                    let (sled, zpool) =
                        zpools_without_dataset.next().ok_or_else(|| {
                            TxnError::CustomError(DatasetError::NotEnoughZpools)
                        })?;
                    let dataset_id = Uuid::new_v4();
                    let address =
                        Self::next_ipv6_address_on_connection(&conn, sled.id())
                            .await
                            .map_err(|e| {
                                TxnError::CustomError(DatasetError::Other(
                                    e.into(),
                                ))
                            })
                            .map(|ip| {
                                SocketAddrV6::new(ip, kind.port(), 0, 0)
                            })?;

                    let dataset = db::model::Dataset::new(
                        dataset_id,
                        zpool.id(),
                        address,
                        kind,
                    );

                    let dataset =
                        Self::dataset_upsert_on_connection(&conn, dataset)
                            .await
                            .map_err(|e| {
                                TxnError::CustomError(DatasetError::Other(
                                    e.into(),
                                ))
                            })?;
                    datasets.push((sled, zpool, dataset));
                }

                return Ok(datasets);
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(DatasetError::NotEnoughZpools) => {
                    Error::unavail("Not enough zpools for dataset allocation")
                }
                TxnError::CustomError(DatasetError::Other(e)) => e,
                TxnError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    pub(super) fn get_allocatable_datasets_query() -> impl RunnableQuery<Dataset>
    {
        use db::schema::dataset::dsl;

        dsl::dataset
            // We look for valid datasets (non-deleted crucible datasets).
            .filter(dsl::size_used.is_not_null())
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::kind.eq(DatasetKind::Crucible))
            .order(dsl::size_used.asc())
            .select(Dataset::as_select())
            .limit(REGION_REDUNDANCY_THRESHOLD.try_into().unwrap())
    }
}
