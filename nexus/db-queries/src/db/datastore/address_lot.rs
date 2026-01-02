// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::PgConnection;
use crate::db::model::Name;
use crate::db::model::{AddressLot, AddressLotBlock, AddressLotReservedBlock};
use crate::db::pagination::paginated;
use async_bb8_diesel::{AsyncRunQueryDsl, Connection};
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_dtrace::DTraceConnection;
use ipnetwork::IpNetwork;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, ResourceType,
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
        use nexus_db_schema::schema::address_lot::dsl as lot_dsl;
        use nexus_db_schema::schema::address_lot_block::dsl as block_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("address_lot_create")
            .transaction(&conn, |conn| async move {
                let lot = AddressLot::new(&params.identity, params.kind.into());

                // @internet-diglett says:
                // I hate this. I know how to replace this transaction with
                // CTEs but for the life of me I can't get it to work in
                // diesel. I gave up and just extended the logic inside
                // of the transaction instead chasing diesel trait bound errors.
                let found_lot: Option<AddressLot> = lot_dsl::address_lot
                    .filter(
                        lot_dsl::name
                            .eq(Name::from(params.identity.name.clone())),
                    )
                    .filter(lot_dsl::time_deleted.is_null())
                    .select(AddressLot::as_select())
                    .limit(1)
                    .first_async(&conn)
                    .await
                    .ok();

                let db_lot = match found_lot {
                    Some(v) => v,
                    None => {
                        diesel::insert_into(lot_dsl::address_lot)
                            .values(lot)
                            .returning(AddressLot::as_returning())
                            .get_result_async(&conn)
                            .await?
                    }
                };

                let desired_blocks: Vec<AddressLotBlock> = params
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

                let found_blocks: Vec<AddressLotBlock> =
                    block_dsl::address_lot_block
                        .filter(block_dsl::address_lot_id.eq(db_lot.id()))
                        .filter(
                            block_dsl::first_address.eq_any(
                                desired_blocks
                                    .iter()
                                    .map(|b| b.first_address)
                                    .collect::<Vec<_>>(),
                            ),
                        )
                        .filter(
                            block_dsl::last_address.eq_any(
                                desired_blocks
                                    .iter()
                                    .map(|b| b.last_address)
                                    .collect::<Vec<_>>(),
                            ),
                        )
                        .get_results_async(&conn)
                        .await?;

                let mut blocks = vec![];

                // If the block is found in the database, use the found block.
                // If the block is not found in the database, insert it.
                for desired_block in desired_blocks {
                    let block = match found_blocks.iter().find(|db_b| {
                        db_b.first_address == desired_block.first_address
                            && db_b.last_address == desired_block.last_address
                    }) {
                        Some(block) => block.clone(),
                        None => {
                            diesel::insert_into(block_dsl::address_lot_block)
                                .values(desired_block)
                                .returning(AddressLotBlock::as_returning())
                                .get_results_async(&conn)
                                .await?[0]
                                .clone()
                        }
                    };
                    blocks.push(block);
                }

                Ok(AddressLotCreateResult { lot: db_lot, blocks })
            })
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::AddressLot,
                        params.identity.name.as_str(),
                    ),
                )
            })
    }

    pub async fn address_lot_delete(
        &self,
        opctx: &OpContext,
        authz_address_lot: &authz::AddressLot,
    ) -> DeleteResult {
        use nexus_db_schema::schema::address_lot::dsl as lot_dsl;
        use nexus_db_schema::schema::address_lot_block::dsl as block_dsl;
        use nexus_db_schema::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;

        opctx.authorize(authz::Action::Delete, authz_address_lot).await?;

        let id = authz_address_lot.id();

        let conn = self.pool_connection_authorized(opctx).await?;

        #[derive(Debug)]
        enum AddressLotDeleteError {
            LotInUse,
        }

        let err = OptionalError::new();

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("address_lot_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let rsvd: Vec<AddressLotReservedBlock> =
                        rsvd_block_dsl::address_lot_rsvd_block
                            .filter(rsvd_block_dsl::address_lot_id.eq(id))
                            .select(AddressLotReservedBlock::as_select())
                            .limit(1)
                            .load_async(&conn)
                            .await?;

                    if !rsvd.is_empty() {
                        return Err(err.bail(AddressLotDeleteError::LotInUse));
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
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        AddressLotDeleteError::LotInUse => {
                            Error::invalid_request("lot is in use")
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn address_lot_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AddressLot> {
        use nexus_db_schema::schema::address_lot::dsl;

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
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn address_lot_block_list(
        &self,
        opctx: &OpContext,
        authz_address_lot: &authz::AddressLot,
        pagparams: Option<&DataPageParams<'_, Uuid>>,
    ) -> ListResultVec<AddressLotBlock> {
        use nexus_db_schema::schema::address_lot_block::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let table = match pagparams {
            Some(params) => paginated(dsl::address_lot_block, dsl::id, &params),
            None => dsl::address_lot_block.into_boxed(),
        };

        table
            .filter(dsl::address_lot_id.eq(authz_address_lot.id()))
            .select(AddressLotBlock::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn address_lot_id_for_block_id(
        &self,
        opctx: &OpContext,
        address_lot_block_id: Uuid,
    ) -> LookupResult<Uuid> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::address_lot_block;
        use nexus_db_schema::schema::address_lot_block::dsl as block_dsl;

        let address_lot_id = block_dsl::address_lot_block
            .filter(address_lot_block::id.eq(address_lot_block_id))
            .select(address_lot_block::address_lot_id)
            .limit(1)
            .first_async::<Uuid>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(address_lot_id)
    }

    // Take the name of an address lot and look up its blocks
    pub async fn address_lot_blocks_by_name(
        &self,
        opctx: &OpContext,
        name: String,
    ) -> LookupResult<Vec<AddressLotBlock>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::address_lot::dsl as lot_dsl;
        use nexus_db_schema::schema::address_lot_block::dsl as block_dsl;

        let address_lot_id = lot_dsl::address_lot
            .filter(lot_dsl::name.eq(name))
            .select(lot_dsl::id)
            .limit(1)
            .first_async::<Uuid>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let blocks = block_dsl::address_lot_block
            .filter(block_dsl::address_lot_id.eq(address_lot_id))
            .select(AddressLotBlock::as_select())
            .load_async::<AddressLotBlock>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(blocks)
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
    anycast: bool,
    conn: &Connection<DTraceConnection<PgConnection>>,
) -> Result<(AddressLotBlock, AddressLotReservedBlock), ReserveBlockTxnError> {
    use nexus_db_schema::schema::address_lot_block;
    use nexus_db_schema::schema::address_lot_block::dsl as block_dsl;
    use nexus_db_schema::schema::address_lot_rsvd_block;
    use nexus_db_schema::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;

    // Ensure a lot block exists with the requested address.

    let block = block_dsl::address_lot_block
        .filter(address_lot_block::address_lot_id.eq(lot_id))
        .filter(address_lot_block::first_address.le(inet))
        .filter(address_lot_block::last_address.ge(inet))
        .select(AddressLotBlock::as_select())
        .limit(1)
        .first_async::<AddressLotBlock>(conn)
        .await
        .map_err(|_e| {
            ReserveBlockTxnError::CustomError(
                ReserveBlockError::AddressNotInLot,
            )
        })?;

    // Ensure the address is not already taken.

    let results: Vec<Uuid> = if anycast {
        // Ensure that a non-anycast reservation has not already been made
        rsvd_block_dsl::address_lot_rsvd_block
            .filter(address_lot_rsvd_block::address_lot_id.eq(lot_id))
            .filter(address_lot_rsvd_block::first_address.le(inet))
            .filter(address_lot_rsvd_block::last_address.ge(inet))
            .filter(address_lot_rsvd_block::anycast.eq(false))
            .select(address_lot_rsvd_block::id)
            .get_results_async(conn)
            .await?
    } else {
        // Ensure that a reservation of any kind has not already been made
        rsvd_block_dsl::address_lot_rsvd_block
            .filter(address_lot_rsvd_block::address_lot_id.eq(lot_id))
            .filter(address_lot_rsvd_block::first_address.le(inet))
            .filter(address_lot_rsvd_block::last_address.ge(inet))
            .select(address_lot_rsvd_block::id)
            .get_results_async(conn)
            .await?
    };

    if !results.is_empty() {
        return Err(ReserveBlockTxnError::CustomError(
            ReserveBlockError::AddressUnavailable,
        ));
    }

    // 3. Mark the address as in use.

    let rsvd_block = AddressLotReservedBlock {
        id: Uuid::new_v4(),
        address_lot_id: lot_id,
        first_address: inet,
        last_address: inet,
        anycast,
    };

    diesel::insert_into(rsvd_block_dsl::address_lot_rsvd_block)
        .values(rsvd_block.clone())
        .execute_async(conn)
        .await?;

    Ok((block, rsvd_block))
}
