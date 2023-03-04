// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use super::DataStore;

use crate::context::OpContext;
use crate::db;
use crate::db::datastore::address_lot::{
    ReserveBlockError, ReserveBlockTxnError,
};
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::LoopbackAddress;
use crate::db::pagination::paginated;
use async_bb8_diesel::{
    AsyncConnection, AsyncRunQueryDsl, ConnectionError, PoolError,
};
use diesel::result::Error as DieselError;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_types::external_api::params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::{
    CreateResult, DeleteResult, ListResultVec, LookupResult, NameOrId,
};
use uuid::Uuid;

impl DataStore {
    pub async fn loopback_address_create(
        &self,
        opctx: &OpContext,
        params: &params::LoopbackAddressCreate,
        id: Option<Uuid>,
    ) -> CreateResult<LoopbackAddress> {
        use db::schema::address_lot;
        use db::schema::address_lot::dsl as lot_dsl;
        use db::schema::loopback_address::dsl;

        #[derive(Debug)]
        enum LoopbackAddressCreateError {
            LotNotFound,
            RackNotFound,
            ReserveBlock(ReserveBlockError),
        }

        type TxnError = TransactionError<LoopbackAddressCreateError>;

        let pool = self.pool_authorized(opctx).await?;

        let inet = IpNetwork::new(params.address, params.mask)
            .map_err(|_| Error::invalid_request("invalid address"))?;

        pool.transaction_async(|conn| async move {
            let lot_id = match &params.address_lot {
                NameOrId::Id(id) => *id,
                NameOrId::Name(name) => {
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
                                LoopbackAddressCreateError::LotNotFound,
                            ),
                            e => e.into(),
                        })?
                }
            };

            let (block, rsvd_block) =
                crate::db::datastore::address_lot::try_reserve_block(
                    lot_id,
                    inet.ip().into(),
                    &conn,
                )
                .await
                .map_err(|e| match e {
                    ReserveBlockTxnError::CustomError(err) => {
                        TxnError::CustomError(
                            LoopbackAddressCreateError::ReserveBlock(err),
                        )
                    }
                    ReserveBlockTxnError::Pool(err) => TxnError::Pool(err),
                })?;

            use db::schema::rack;
            use db::schema::rack::dsl as rack_dsl;
            rack_dsl::rack
                .filter(rack::id.eq(params.rack_id))
                .select(rack::id)
                .limit(1)
                .first_async::<Uuid>(&conn)
                .await
                .map_err(|e| match e {
                    ConnectionError::Query(_) => TxnError::CustomError(
                        LoopbackAddressCreateError::RackNotFound,
                    ),
                    e => e.into(),
                })?;

            // Address block reserved, now create the loopback address.

            let addr = LoopbackAddress::new(
                id,
                block.id,
                rsvd_block.id,
                params.rack_id,
                params.switch_location.to_string(),
                inet,
            );

            let db_addr: LoopbackAddress =
                diesel::insert_into(dsl::loopback_address)
                    .values(addr)
                    .returning(LoopbackAddress::as_returning())
                    .get_result_async(&conn)
                    .await?;

            Ok(db_addr)
        })
        .await
        .map_err(|e| match e {
            TxnError::CustomError(LoopbackAddressCreateError::LotNotFound) => {
                Error::invalid_request("address lot not found")
            }
            TxnError::CustomError(LoopbackAddressCreateError::RackNotFound) => {
                Error::invalid_request("rack not found")
            }
            TxnError::CustomError(
                LoopbackAddressCreateError::ReserveBlock(
                    ReserveBlockError::AddressUnavailable,
                ),
            ) => Error::invalid_request("address unavailable"),
            TxnError::CustomError(
                LoopbackAddressCreateError::ReserveBlock(
                    ReserveBlockError::AddressNotInLot,
                ),
            ) => Error::invalid_request("address not in lot"),
            TxnError::Pool(e) => match e {
                PoolError::Connection(ConnectionError::Query(
                    DieselError::DatabaseError(_, _),
                )) => public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::LoopbackAddress,
                        &format!("lo {}", inet),
                    ),
                ),
                _ => public_error_from_diesel_pool(e, ErrorHandler::Server),
            },
        })
    }

    pub async fn loopback_address_delete(
        &self,
        opctx: &OpContext,
        params: &params::LoopbackAddressSelector,
    ) -> DeleteResult {
        use db::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;
        use db::schema::loopback_address::dsl;

        let addr = params.address;
        let inet: IpNetwork = addr.into();
        let pool = self.pool_authorized(opctx).await?;

        pool.transaction_async(|conn| async move {
            let la = diesel::delete(dsl::loopback_address)
                .filter(dsl::address.eq(inet))
                .returning(LoopbackAddress::as_returning())
                .get_result_async(&conn)
                .await?;

            diesel::delete(rsvd_block_dsl::address_lot_rsvd_block)
                .filter(rsvd_block_dsl::id.eq(la.rsvd_address_lot_block_id))
                .execute_async(&conn)
                .await?;

            Ok(())
        })
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn loopback_address_get(
        &self,
        opctx: &OpContext,
        params: &params::LoopbackAddressSelector,
    ) -> LookupResult<LoopbackAddress> {
        use db::schema::loopback_address;
        use db::schema::loopback_address::dsl as loopback_dsl;

        let pool = self.pool_authorized(opctx).await?;
        let inet: IpNetwork = params.address.into();

        loopback_dsl::loopback_address
            .filter(loopback_address::rack_id.eq(params.rack_id))
            .filter(
                loopback_address::switch_location
                    .eq(params.switch_location.to_string()),
            )
            .filter(loopback_address::address.eq(inet))
            .select(LoopbackAddress::as_select())
            .limit(1)
            .first_async::<LoopbackAddress>(pool)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn loopback_address_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<LoopbackAddress> {
        use db::schema::loopback_address::dsl;

        paginated(dsl::loopback_address, dsl::id, &pagparams)
            .select(LoopbackAddress::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}
