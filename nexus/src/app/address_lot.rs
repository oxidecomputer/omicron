// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::db;
use crate::db::datastore::AddressLotCreateResult;
use crate::external_api::params;
use db::model::{AddressLot, AddressLotBlock};
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::{
    CreateResult, DeleteResult, Error, ListResultVec,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub async fn address_lot_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::AddressLotCreate,
    ) -> CreateResult<AddressLotCreateResult> {
        validate_blocks(&params)?;
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        self.db_datastore.address_lot_create(opctx, &params).await
    }

    pub async fn address_lot_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        name_or_id: &Option<NameOrId>,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        self.db_datastore.address_lot_delete(opctx, name_or_id).await
    }

    pub async fn address_lot_list(
        self: &Arc<Self>,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AddressLot> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.address_lot_list(opctx, pagparams).await
    }

    pub async fn address_lot_block_list(
        self: &Arc<Self>,
        opctx: &OpContext,
        address_lot: &NameOrId,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<AddressLotBlock> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore
            .address_lot_block_list(opctx, address_lot, pagparams)
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
                return Err(Error::InvalidRequest {
                    message: "Block bounds must be in same address family"
                        .into(),
                })
            }
        }
    }
    Ok(())
}

fn validate_v4_block(first: &Ipv4Addr, last: &Ipv4Addr) -> Result<(), Error> {
    if first > last {
        return Err(Error::InvalidRequest {
            message: "Invalid range, first must be <= last".into(),
        });
    }
    Ok(())
}

fn validate_v6_block(first: &Ipv6Addr, last: &Ipv6Addr) -> Result<(), Error> {
    if first > last {
        return Err(Error::InvalidRequest {
            message: "Invalid range, first must be <= last".into(),
        });
    }
    Ok(())
}
