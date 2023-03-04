// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::PgConnection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::Name;
use crate::db::model::{AddressLot, AddressLotBlock, AddressLotRsvdBlock};
use crate::db::pagination::paginated;
use async_bb8_diesel::{
    AsyncConnection, AsyncRunQueryDsl, Connection, ConnectionError, PoolError,
};
use chrono::Utc;
use diesel::result::Error as DieselError;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_dtrace::DTraceConnection;
use ipnetwork::IpNetwork;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, LookupType, NameOrId, ResourceType,
};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AddressLotCreateResult {
    pub lot: AddressLot,
    pub blocks: Vec<AddressLotBlock>,
}

impl DataStore {
    pub async fn address_lot_create(
        &self,
        opctx: &OpContext,
        params: &params::AddressLotCreate,
    ) -> CreateResult<AddressLotCreateResult> {
        use db::schema::address_lot::dsl as lot_dsl;
        use db::schema::address_lot_block::dsl as block_dsl;

        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let lot = AddressLot::new(&params.identity, params.kind.into());

                let db_lot: AddressLot =
                    diesel::insert_into(lot_dsl::address_lot)
                        .values(lot)
                        .returning(AddressLot::as_returning())
                        .get_result_async(&conn)
                        .await?;

                let blocks: Vec<AddressLotBlock> = params
                    .blocks
                    .iter()
                    .map(|b| {
                        AddressLotBlock::new(
                            db_lot.id(),
                            b.first_address.into(),
                            b.last_address.into(),
                        )
                    })
                    .collect();

                let db_blocks =
                    diesel::insert_into(block_dsl::address_lot_block)
                        .values(blocks)
                        .returning(AddressLotBlock::as_returning())
                        .get_results_async(&conn)
                        .await?;

                Ok(AddressLotCreateResult { lot: db_lot, blocks: db_blocks })
            })
            .await
            .map_err(|e| match e {
                PoolError::Connection(ConnectionError::Query(
                    DieselError::DatabaseError(_, _),
                )) => public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::AddressLot,
                        &params.identity.name.as_str(),
                    ),
                ),
                _ => public_error_from_diesel_pool(e, ErrorHandler::Server),
            })
    }

    pub async fn address_lot_delete(
        &self,
        opctx: &OpContext,
        name_or_id: &Option<NameOrId>,
    ) -> DeleteResult {
        use db::schema::address_lot;
        use db::schema::address_lot::dsl as lot_dsl;
        use db::schema::address_lot_block::dsl as block_dsl;
        use db::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;

        let pool = self.pool_authorized(opctx).await?;

        #[derive(Debug)]
        enum AddressLotDeleteError {
            NameOrIdRequired,
            LotNotFound,
            LotInUse,
        }

        type TxnError = TransactionError<AddressLotDeleteError>;

        pool.transaction_async(|conn| async move {
            let id = match name_or_id {
                None => Err(TxnError::CustomError(
                    AddressLotDeleteError::NameOrIdRequired,
                ))?,
                Some(NameOrId::Id(id)) => *id,
                Some(NameOrId::Name(name)) => {
                    let name = name.to_string();
                    lot_dsl::address_lot
                        .filter(address_lot::time_deleted.is_null())
                        .filter(address_lot::name.eq(name))
                        .select(address_lot::id)
                        .limit(1)
                        .first_async::<Uuid>(&conn)
                        .await
                        .map_err(|e| match e {
                            ConnectionError::Query(_) => TxnError::CustomError(
                                AddressLotDeleteError::LotNotFound,
                            ),
                            e => e.into(),
                        })?
                }
            };

            let rsvd: Vec<AddressLotRsvdBlock> =
                rsvd_block_dsl::address_lot_rsvd_block
                    .filter(rsvd_block_dsl::address_lot_id.eq(id))
                    .select(AddressLotRsvdBlock::as_select())
                    .limit(1)
                    .load_async(&conn)
                    .await?;

            if !rsvd.is_empty() {
                Err(TxnError::CustomError(AddressLotDeleteError::LotInUse))?;
            }

            let now = Utc::now();
            diesel::update(lot_dsl::address_lot)
                .filter(lot_dsl::time_deleted.is_null())
                .filter(lot_dsl::id.eq(id))
                .set(lot_dsl::time_deleted.eq(now))
                .execute_async(&conn)
                .await?;

            diesel::delete(block_dsl::address_lot_block)
                .filter(block_dsl::address_lot_id.eq(id))
                .execute_async(&conn)
                .await?;

            Ok(())
        })
        .await
        .map_err(|e| match e {
            TxnError::Pool(e) => {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            }
            TxnError::CustomError(AddressLotDeleteError::LotNotFound) => {
                Error::invalid_request("address lot not found")
            }
            TxnError::CustomError(AddressLotDeleteError::NameOrIdRequired) => {
                Error::invalid_request("name or id required")
            }
            TxnError::CustomError(AddressLotDeleteError::LotInUse) => {
                Error::invalid_request("lot is in use")
            }
        })
    }

    pub async fn address_lot_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AddressLot> {
        use db::schema::address_lot::dsl;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::address_lot, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::address_lot,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .select(AddressLot::as_select())
        .load_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn address_lot_block_list(
        &self,
        opctx: &OpContext,
        address_block_id: &NameOrId,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<AddressLotBlock> {
        use db::schema::address_lot;
        use db::schema::address_lot::dsl as lot_dsl;
        use db::schema::address_lot_block::dsl as block_dsl;

        let pool = self.pool_authorized(opctx).await?;

        let id = match &address_block_id {
            NameOrId::Id(id) => *id,
            NameOrId::Name(name) => {
                let name = name.to_string();
                lot_dsl::address_lot
                    .filter(address_lot::time_deleted.is_null())
                    .filter(address_lot::name.eq(name.clone()))
                    .select(address_lot::id)
                    .limit(1)
                    .first_async::<Uuid>(pool)
                    .await
                    .map_err(|e| {
                        if let PoolError::Connection(ConnectionError::Query(
                            DieselError::NotFound,
                        )) = e
                        {
                            public_error_from_diesel_pool(
                                e,
                                ErrorHandler::NotFoundByLookup(
                                    ResourceType::AddressLot,
                                    LookupType::ByName(name),
                                ),
                            )
                        } else {
                            public_error_from_diesel_pool(
                                e,
                                ErrorHandler::Server,
                            )
                        }
                    })?
            }
        };

        paginated(block_dsl::address_lot_block, block_dsl::id, &pagparams)
            .filter(block_dsl::address_lot_id.eq(id))
            .select(AddressLotBlock::as_select())
            .load_async(pool)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn address_lot_id_for_block_id(
        &self,
        opctx: &OpContext,
        address_lot_block_id: Uuid,
    ) -> LookupResult<Uuid> {
        let pool = self.pool_authorized(opctx).await?;

        use db::schema::address_lot_block;
        use db::schema::address_lot_block::dsl as block_dsl;

        let address_lot_id = block_dsl::address_lot_block
            .filter(address_lot_block::id.eq(address_lot_block_id))
            .select(address_lot_block::address_lot_id)
            .limit(1)
            .first_async::<Uuid>(pool)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        Ok(address_lot_id)
    }
}

#[derive(Debug)]
pub(crate) enum ReserveBlockError {
    AddressUnavailable,
    AddressNotInLot,
}
pub(crate) type ReserveBlockTxnError = TransactionError<ReserveBlockError>;

pub(crate) async fn try_reserve_block(
    lot_id: Uuid,
    inet: IpNetwork,
    conn: &Connection<DTraceConnection<PgConnection>>,
) -> Result<(AddressLotBlock, AddressLotRsvdBlock), ReserveBlockTxnError> {
    use db::schema::address_lot_block;
    use db::schema::address_lot_block::dsl as block_dsl;
    use db::schema::address_lot_rsvd_block;
    use db::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;

    // Ensure a lot block exists with the requested address.

    let block = block_dsl::address_lot_block
        .filter(address_lot_block::address_lot_id.eq(lot_id))
        .filter(address_lot_block::first_address.le(inet))
        .filter(address_lot_block::last_address.ge(inet))
        .select(AddressLotBlock::as_select())
        .limit(1)
        .first_async::<AddressLotBlock>(conn)
        .await
        .map_err(|e| match e {
            ConnectionError::Query(_) => ReserveBlockTxnError::CustomError(
                ReserveBlockError::AddressNotInLot,
            ),
            e => e.into(),
        })?;

    // Ensure the address is not already taken.

    let results: Vec<Uuid> = rsvd_block_dsl::address_lot_rsvd_block
        .filter(address_lot_rsvd_block::address_lot_id.eq(lot_id))
        .filter(address_lot_rsvd_block::first_address.le(inet))
        .filter(address_lot_rsvd_block::last_address.ge(inet))
        .select(address_lot_rsvd_block::id)
        .get_results_async(conn)
        .await?;

    if !results.is_empty() {
        return Err(ReserveBlockTxnError::CustomError(
            ReserveBlockError::AddressUnavailable,
        ));
    }

    // 3. Mark the address as in use.

    let rsvd_block = AddressLotRsvdBlock {
        id: Uuid::new_v4(),
        address_lot_id: lot_id,
        first_address: inet,
        last_address: inet,
    };

    diesel::insert_into(rsvd_block_dsl::address_lot_rsvd_block)
        .values(rsvd_block.clone())
        .execute_async(conn)
        .await?;

    Ok((block, rsvd_block))
}
