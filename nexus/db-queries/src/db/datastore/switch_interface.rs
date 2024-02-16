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
use crate::db::model::LoopbackAddress;
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_db_model::to_db_typed_uuid;
use nexus_types::external_api::params::LoopbackAddressCreate;
use omicron_common::api::external::{
    CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, ResourceType,
};
use omicron_uuid_kinds::LoopbackAddressKind;
use omicron_uuid_kinds::TypedUuid;
use uuid::Uuid;

impl DataStore {
    pub async fn loopback_address_create(
        &self,
        opctx: &OpContext,
        params: &LoopbackAddressCreate,
        id: Option<TypedUuid<LoopbackAddressKind>>,
        authz_address_lot: &authz::AddressLot,
    ) -> CreateResult<LoopbackAddress> {
        use db::schema::loopback_address::dsl;

        #[derive(Debug)]
        enum LoopbackAddressCreateError {
            ReserveBlock(ReserveBlockError),
        }

        let conn = self.pool_connection_authorized(opctx).await?;

        let inet = IpNetwork::new(params.address, params.mask)
            .map_err(|_| Error::invalid_request("invalid address"))?;

        let err = OptionalError::new();

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("loopback_address_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
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
                            ReserveBlockTxnError::CustomError(e) => err.bail(
                                LoopbackAddressCreateError::ReserveBlock(e),
                            ),
                            ReserveBlockTxnError::Database(e) => e,
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
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        LoopbackAddressCreateError::ReserveBlock(
                            ReserveBlockError::AddressUnavailable,
                        ) => Error::invalid_request("address unavailable"),
                        LoopbackAddressCreateError::ReserveBlock(
                            ReserveBlockError::AddressNotInLot,
                        ) => Error::invalid_request("address not in lot"),
                    }
                } else {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::LoopbackAddress,
                            &format!("lo {}", inet),
                        ),
                    )
                }
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
        self.transaction_retry_wrapper("loopback_address_delete")
            .transaction(&conn, |conn| async move {
                let la = diesel::delete(dsl::loopback_address)
                    .filter(dsl::id.eq(to_db_typed_uuid(id)))
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
            .filter(loopback_address::id.eq(to_db_typed_uuid(id)))
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
