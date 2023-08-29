// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::db;
use crate::db::datastore::AddressLotCreateResult;
use crate::external_api::params;
use db::model::{AddressLot, AddressLotBlock};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::{
    CreateResult, DeleteResult, ListResultVec,
};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub fn address_lot_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        address_lot: NameOrId,
    ) -> LookupResult<lookup::AddressLot<'a>> {
        match address_lot {
            NameOrId::Id(id) => {
                let lot = LookupPath::new(opctx, &self.db_datastore)
                    .address_lot_id(id);
                Ok(lot)
            }
            NameOrId::Name(name) => {
                let lot = LookupPath::new(opctx, &self.db_datastore)
                    .address_lot_name_owned(name.into());
                Ok(lot)
            }
        }
    }

    pub async fn address_lot_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::AddressLotCreate,
    ) -> CreateResult<AddressLotCreateResult> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        self.db_datastore.address_lot_create(opctx, &params).await
    }

    pub async fn address_lot_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        address_lot_lookup: &lookup::AddressLot<'_>,
    ) -> DeleteResult {
        let (.., authz_address_lot) =
            address_lot_lookup.lookup_for(authz::Action::Delete).await?;
        self.db_datastore.address_lot_delete(opctx, &authz_address_lot).await
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
        address_lot: &lookup::AddressLot<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<AddressLotBlock> {
        let (.., authz_address_lot) =
            address_lot.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .address_lot_block_list(opctx, &authz_address_lot, pagparams)
            .await
    }
}
