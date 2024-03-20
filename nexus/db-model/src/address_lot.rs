// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impl_enum_type;
use crate::schema::{address_lot, address_lot_block, address_lot_rsvd_block};
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const INFRA_LOT: &str = "initial-infra";

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy, QueryId)]
    #[diesel(postgres_type(name = "address_lot_kind", schema = "public"))]
    pub struct AddressLotKindEnum;

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialEq,
        Serialize,
        Deserialize,
    )]
    #[diesel(sql_type = AddressLotKindEnum)]
    pub enum AddressLotKind;

    Infra => b"infra"
    Pool => b"pool"
);

/// An address lot contains addresses that may be allocated for assignment to
/// network infrastructure such as rack switch interfaces, or to IP pools.
#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = address_lot)]
pub struct AddressLot {
    #[diesel(embed)]
    pub identity: AddressLotIdentity,

    /// Indicates whether this address lot is for use with infrastructure or IP
    /// pools.
    pub kind: AddressLotKind,
}

impl Into<external::AddressLot> for AddressLot {
    fn into(self) -> external::AddressLot {
        external::AddressLot {
            identity: self.identity(),
            kind: self.kind.into(),
        }
    }
}

impl From<external::AddressLotKind> for AddressLotKind {
    fn from(k: external::AddressLotKind) -> AddressLotKind {
        match k {
            external::AddressLotKind::Infra => AddressLotKind::Infra,
            external::AddressLotKind::Pool => AddressLotKind::Pool,
        }
    }
}

impl From<AddressLotKind> for external::AddressLotKind {
    fn from(value: AddressLotKind) -> Self {
        match value {
            AddressLotKind::Infra => external::AddressLotKind::Infra,
            AddressLotKind::Pool => external::AddressLotKind::Pool,
        }
    }
}

impl AddressLot {
    pub fn new(
        id: &external::IdentityMetadataCreateParams,
        kind: AddressLotKind,
    ) -> Self {
        Self {
            identity: AddressLotIdentity::new(Uuid::new_v4(), id.clone()),
            kind,
        }
    }
}

/// An address lot block is a set of contiguous addresses that may be fully or
/// partially reserved.
#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = address_lot_block)]
pub struct AddressLotBlock {
    pub id: Uuid,
    pub address_lot_id: Uuid,
    pub first_address: IpNetwork,
    pub last_address: IpNetwork,
}

impl AddressLotBlock {
    pub fn new(
        address_lot_id: Uuid,
        first_address: IpNetwork,
        last_address: IpNetwork,
    ) -> Self {
        Self { id: Uuid::new_v4(), address_lot_id, first_address, last_address }
    }
}

impl Into<external::AddressLotBlock> for AddressLotBlock {
    fn into(self) -> external::AddressLotBlock {
        external::AddressLotBlock {
            id: self.id,
            first_address: self.first_address.ip(),
            last_address: self.last_address.ip(),
        }
    }
}

/// A reservation made against an address lot. A reservation may take up part of
/// a address lot block or the whole block. Reservations for a particular lot
/// may not overlap. This is checked in
/// `nexus_db_queries::db:datastore::address_lot::try_reserve_block`. If a new
/// reservation is requested that overlaps by any amount with an existing
/// reservation for the same lot, the reservation fails.
#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = address_lot_rsvd_block)]
pub struct AddressLotReservedBlock {
    /// A unique identifier for this reservation.
    pub id: Uuid,

    /// The address lot this reservation is associated with.
    pub address_lot_id: Uuid,

    /// The first address in the reservation (inclusive).
    pub first_address: IpNetwork,

    /// The last address in the reservation (inclusive).
    pub last_address: IpNetwork,

    /// Address is an anycast address.
    /// This allows the address to be assigned to multiple locations simultaneously.
    pub anycast: bool,
}
