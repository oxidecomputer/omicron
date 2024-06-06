// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Address Lots

use crate::external_api::params;
use db::model::AddressLotBlock;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::AddressLotCreateResult;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::{
    CreateResult, DeleteResult, Error, ListResultVec,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on Address Lots
#[derive(Clone)]
pub struct AddressLot {
    datastore: Arc<db::DataStore>,
}

impl AddressLot {
    pub fn new(datastore: Arc<db::DataStore>) -> AddressLot {
        AddressLot { datastore }
    }

    pub fn lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        address_lot: NameOrId,
    ) -> LookupResult<lookup::AddressLot<'a>> {
        match address_lot {
            NameOrId::Id(id) => {
                let lot =
                    LookupPath::new(opctx, &self.datastore).address_lot_id(id);
                Ok(lot)
            }
            NameOrId::Name(name) => {
                let lot = LookupPath::new(opctx, &self.datastore)
                    .address_lot_name_owned(name.into());
                Ok(lot)
            }
        }
    }

    pub(crate) async fn create(
        &self,
        opctx: &OpContext,
        params: params::AddressLotCreate,
    ) -> CreateResult<AddressLotCreateResult> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        validate_blocks(&params)?;
        self.datastore.address_lot_create(opctx, &params).await
    }

    pub(crate) async fn delete(
        &self,
        opctx: &OpContext,
        address_lot_lookup: &lookup::AddressLot<'_>,
    ) -> DeleteResult {
        let (.., authz_address_lot) =
            address_lot_lookup.lookup_for(authz::Action::Delete).await?;
        self.datastore.address_lot_delete(opctx, &authz_address_lot).await
    }

    pub(crate) async fn list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::AddressLot> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.datastore.address_lot_list(opctx, pagparams).await
    }

    pub(crate) async fn block_list(
        &self,
        opctx: &OpContext,
        address_lot: &lookup::AddressLot<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<AddressLotBlock> {
        let (.., authz_address_lot) =
            address_lot.lookup_for(authz::Action::ListChildren).await?;
        self.datastore
            .address_lot_block_list(opctx, &authz_address_lot, pagparams)
            .await
    }
}

fn validate_blocks(lot: &params::AddressLotCreate) -> Result<(), Error> {
    for b in &lot.blocks {
        match (&b.first_address, &b.last_address) {
            (IpAddr::V4(first), IpAddr::V4(last)) => {
                validate_v4_block(first, last)?
            }
            (IpAddr::V6(first), IpAddr::V6(last)) => {
                validate_v6_block(first, last)?
            }
            _ => {
                return Err(Error::invalid_request(
                    "Block bounds must be in same address family",
                ));
            }
        }
    }
    Ok(())
}

fn validate_v4_block(first: &Ipv4Addr, last: &Ipv4Addr) -> Result<(), Error> {
    if first > last {
        return Err(Error::invalid_request(
            "Invalid range, first must be <= last",
        ));
    }
    Ok(())
}

fn validate_v6_block(first: &Ipv6Addr, last: &Ipv6Addr) -> Result<(), Error> {
    if first > last {
        return Err(Error::invalid_request(
            "Invalid range, first must be <= last",
        ));
    }
    Ok(())
}
