// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use super::DataStore;

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::address_lot::{
    ReserveBlockError, ReserveBlockTxnError,
};
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::LoopbackAddress;
use crate::db::pagination::paginated;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl, ConnectionError};
use diesel::result::Error as DieselError;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_types::external_api::params::LoopbackAddressCreate;
use omicron_common::api::external::{
    CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, ResourceType,
};
use uuid::Uuid;

impl DataStore {
    pub async fn loopback_address_create(
        &self,
        opctx: &OpContext,
        params: &LoopbackAddressCreate,
        id: Option<Uuid>,
        authz_address_lot: &authz::AddressLot,
    ) -> CreateResult<LoopbackAddress> {
        use db::schema::loopback_address::dsl;

        #[derive(Debug)]
        enum LoopbackAddressCreateError {
            ReserveBlock(ReserveBlockError),
        }

        type TxnError = TransactionError<LoopbackAddressCreateError>;

        let conn = self.pool_connection_authorized(opctx).await?;

        let inet = IpNetwork::new(params.address, params.mask)
            .map_err(|_| Error::invalid_request("invalid address"))?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        conn.transaction_async(|conn| async move {
            let lot_id = authz_address_lot.id();
            let (block, rsvd_block) =
                crate::db::datastore::address_lot::try_reserve_block(
                    lot_id,
                    inet.ip().into(),
                    params.anycast,
                    &conn,
                )
                .await
                .map_err(|e| match e {
                    ReserveBlockTxnError::CustomError(err) => {
                        TxnError::CustomError(
                            LoopbackAddressCreateError::ReserveBlock(err),
                        )
                    }
                    ReserveBlockTxnError::Connection(err) => {
                        TxnError::Connection(err)
                    }
                })?;

            // Address block reserved, now create the loopback address.

            let addr = LoopbackAddress::new(
                id,
                block.id,
                rsvd_block.id,
                params.rack_id,
                params.switch_location.to_string(),
                inet,
                params.anycast,
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
            TxnError::Connection(e) => match e {
                ConnectionError::Query(DieselError::DatabaseError(_, _)) => {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::LoopbackAddress,
                            &format!("lo {}", inet),
                        ),
                    )
                }
                _ => public_error_from_diesel(e, ErrorHandler::Server),
            },
        })
    }

    pub async fn loopback_address_delete(
        &self,
        opctx: &OpContext,
        authz_loopback_address: &authz::LoopbackAddress,
    ) -> DeleteResult {
        use db::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;
        use db::schema::loopback_address::dsl;

        let id = authz_loopback_address.id();

        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        conn.transaction_async(|conn| async move {
            let la = diesel::delete(dsl::loopback_address)
                .filter(dsl::id.eq(id))
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
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn loopback_address_get(
        &self,
        opctx: &OpContext,
        authz_loopback_address: &authz::LoopbackAddress,
    ) -> LookupResult<LoopbackAddress> {
        use db::schema::loopback_address;
        use db::schema::loopback_address::dsl as loopback_dsl;

        let id = authz_loopback_address.id();

        let conn = self.pool_connection_authorized(opctx).await?;

        loopback_dsl::loopback_address
            .filter(loopback_address::id.eq(id))
            .select(LoopbackAddress::as_select())
            .limit(1)
            .first_async::<LoopbackAddress>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn loopback_address_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<LoopbackAddress> {
        use db::schema::loopback_address::dsl;

        paginated(dsl::loopback_address, dsl::id, &pagparams)
            .select(LoopbackAddress::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
