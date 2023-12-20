// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the hardware/software inventory in the database

use crate::schema::{
    hw_baseboard_id, inv_caboose, inv_collection, inv_collection_error,
    inv_omicron_zone, inv_omicron_zone_nic, inv_root_of_trust,
    inv_root_of_trust_page, inv_service_processor, inv_sled_agent,
    inv_sled_omicron_zones, sw_caboose, sw_root_of_trust_page,
};
use crate::{
    impl_enum_type, ipv6, ByteCount, Generation, MacAddr, Name, SqlU16, SqlU32,
    SqlU8,
};
use anyhow::{anyhow, ensure};
use anyhow::{bail, Context};
use chrono::DateTime;
use chrono::Utc;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::serialize::ToSql;
use diesel::{serialize, sql_types};
use ipnetwork::IpNetwork;
use nexus_types::inventory::{
    BaseboardId, Caboose, Collection, OmicronZoneType, PowerState, RotPage,
    RotSlot,
};
use std::net::SocketAddrV6;
use uuid::Uuid;

// See [`nexus_types::inventory::PowerState`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "hw_power_state"))]
    pub struct HwPowerStateEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = HwPowerStateEnum)]
    pub enum HwPowerState;

    // Enum values
    A0 => b"A0"
    A1 => b"A1"
    A2 => b"A2"
);

impl From<PowerState> for HwPowerState {
    fn from(p: PowerState) -> Self {
        match p {
            PowerState::A0 => HwPowerState::A0,
            PowerState::A1 => HwPowerState::A1,
            PowerState::A2 => HwPowerState::A2,
        }
    }
}

impl From<HwPowerState> for PowerState {
    fn from(value: HwPowerState) -> Self {
        match value {
            HwPowerState::A0 => PowerState::A0,
            HwPowerState::A1 => PowerState::A1,
            HwPowerState::A2 => PowerState::A2,
        }
    }
}

// See [`nexus_types::inventory::RotSlot`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "hw_rot_slot"))]
    pub struct HwRotSlotEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = HwRotSlotEnum)]
    pub enum HwRotSlot;

    // Enum values
    A => b"A"
    B => b"B"
);

impl From<RotSlot> for HwRotSlot {
    fn from(value: RotSlot) -> Self {
        match value {
            RotSlot::A => HwRotSlot::A,
            RotSlot::B => HwRotSlot::B,
        }
    }
}

impl From<HwRotSlot> for RotSlot {
    fn from(value: HwRotSlot) -> RotSlot {
        match value {
            HwRotSlot::A => RotSlot::A,
            HwRotSlot::B => RotSlot::B,
        }
    }
}

// See [`nexus_types::inventory::CabooseWhich`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "caboose_which"))]
    pub struct CabooseWhichEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = CabooseWhichEnum)]
    pub enum CabooseWhich;

    // Enum values
    SpSlot0 => b"sp_slot_0"
    SpSlot1 => b"sp_slot_1"
    RotSlotA => b"rot_slot_A"
    RotSlotB => b"rot_slot_B"
);

impl From<nexus_types::inventory::CabooseWhich> for CabooseWhich {
    fn from(c: nexus_types::inventory::CabooseWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match c {
            nexus_inventory::CabooseWhich::SpSlot0 => CabooseWhich::SpSlot0,
            nexus_inventory::CabooseWhich::SpSlot1 => CabooseWhich::SpSlot1,
            nexus_inventory::CabooseWhich::RotSlotA => CabooseWhich::RotSlotA,
            nexus_inventory::CabooseWhich::RotSlotB => CabooseWhich::RotSlotB,
        }
    }
}

impl From<CabooseWhich> for nexus_types::inventory::CabooseWhich {
    fn from(row: CabooseWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match row {
            CabooseWhich::SpSlot0 => nexus_inventory::CabooseWhich::SpSlot0,
            CabooseWhich::SpSlot1 => nexus_inventory::CabooseWhich::SpSlot1,
            CabooseWhich::RotSlotA => nexus_inventory::CabooseWhich::RotSlotA,
            CabooseWhich::RotSlotB => nexus_inventory::CabooseWhich::RotSlotB,
        }
    }
}

// See [`nexus_types::inventory::RotPageWhich`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "root_of_trust_page_which"))]
    pub struct RotPageWhichEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = RotPageWhichEnum)]
    pub enum RotPageWhich;

    // Enum values
    Cmpa => b"cmpa"
    CfpaActive => b"cfpa_active"
    CfpaInactive => b"cfpa_inactive"
    CfpaScratch => b"cfpa_scratch"
);

impl From<nexus_types::inventory::RotPageWhich> for RotPageWhich {
    fn from(c: nexus_types::inventory::RotPageWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match c {
            nexus_inventory::RotPageWhich::Cmpa => RotPageWhich::Cmpa,
            nexus_inventory::RotPageWhich::CfpaActive => {
                RotPageWhich::CfpaActive
            }
            nexus_inventory::RotPageWhich::CfpaInactive => {
                RotPageWhich::CfpaInactive
            }
            nexus_inventory::RotPageWhich::CfpaScratch => {
                RotPageWhich::CfpaScratch
            }
        }
    }
}

impl From<RotPageWhich> for nexus_types::inventory::RotPageWhich {
    fn from(row: RotPageWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match row {
            RotPageWhich::Cmpa => nexus_inventory::RotPageWhich::Cmpa,
            RotPageWhich::CfpaActive => {
                nexus_inventory::RotPageWhich::CfpaActive
            }
            RotPageWhich::CfpaInactive => {
                nexus_inventory::RotPageWhich::CfpaInactive
            }
            RotPageWhich::CfpaScratch => {
                nexus_inventory::RotPageWhich::CfpaScratch
            }
        }
    }
}

// See [`nexus_types::inventory::SpType`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sp_type"))]
    pub struct SpTypeEnum;

    #[derive(
        Copy,
        Clone,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialOrd,
        Ord,
        PartialEq,
        Eq
    )]
    #[diesel(sql_type = SpTypeEnum)]
    pub enum SpType;

    // Enum values
    Sled => b"sled"
    Switch =>  b"switch"
    Power => b"power"
);

impl From<nexus_types::inventory::SpType> for SpType {
    fn from(value: nexus_types::inventory::SpType) -> Self {
        match value {
            nexus_types::inventory::SpType::Sled => SpType::Sled,
            nexus_types::inventory::SpType::Power => SpType::Power,
            nexus_types::inventory::SpType::Switch => SpType::Switch,
        }
    }
}

impl From<SpType> for nexus_types::inventory::SpType {
    fn from(value: SpType) -> Self {
        match value {
            SpType::Sled => nexus_types::inventory::SpType::Sled,
            SpType::Switch => nexus_types::inventory::SpType::Switch,
            SpType::Power => nexus_types::inventory::SpType::Power,
        }
    }
}

/// See [`nexus_types::inventory::Collection`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_collection)]
pub struct InvCollection {
    pub id: Uuid,
    pub time_started: DateTime<Utc>,
    pub time_done: DateTime<Utc>,
    pub collector: String,
}

impl<'a> From<&'a Collection> for InvCollection {
    fn from(c: &'a Collection) -> Self {
        InvCollection {
            id: c.id,
            time_started: c.time_started,
            time_done: c.time_done,
            collector: c.collector.clone(),
        }
    }
}

/// See [`nexus_types::inventory::BaseboardId`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = hw_baseboard_id)]
pub struct HwBaseboardId {
    pub id: Uuid,
    pub part_number: String,
    pub serial_number: String,
}

impl From<BaseboardId> for HwBaseboardId {
    fn from(c: BaseboardId) -> Self {
        HwBaseboardId {
            id: Uuid::new_v4(),
            part_number: c.part_number,
            serial_number: c.serial_number,
        }
    }
}

impl From<HwBaseboardId> for BaseboardId {
    fn from(row: HwBaseboardId) -> Self {
        BaseboardId {
            part_number: row.part_number,
            serial_number: row.serial_number,
        }
    }
}

/// See [`nexus_types::inventory::Caboose`].
#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
)]
#[diesel(table_name = sw_caboose)]
pub struct SwCaboose {
    pub id: Uuid,
    pub board: String,
    pub git_commit: String,
    pub name: String,
    pub version: String,
}

impl From<Caboose> for SwCaboose {
    fn from(c: Caboose) -> Self {
        SwCaboose {
            id: Uuid::new_v4(),
            board: c.board,
            git_commit: c.git_commit,
            name: c.name,
            version: c.version,
        }
    }
}

impl From<SwCaboose> for Caboose {
    fn from(row: SwCaboose) -> Self {
        Self {
            board: row.board,
            git_commit: row.git_commit,
            name: row.name,
            version: row.version,
        }
    }
}

/// See [`nexus_types::inventory::RotPage`].
#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
)]
#[diesel(table_name = sw_root_of_trust_page)]
pub struct SwRotPage {
    pub id: Uuid,
    pub data_base64: String,
}

impl From<RotPage> for SwRotPage {
    fn from(p: RotPage) -> Self {
        Self { id: Uuid::new_v4(), data_base64: p.data_base64 }
    }
}

impl From<SwRotPage> for RotPage {
    fn from(row: SwRotPage) -> Self {
        Self { data_base64: row.data_base64 }
    }
}

/// See [`nexus_types::inventory::Collection`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_collection_error)]
pub struct InvCollectionError {
    pub inv_collection_id: Uuid,
    pub idx: SqlU16,
    pub message: String,
}

impl InvCollectionError {
    pub fn new(inv_collection_id: Uuid, idx: u16, message: String) -> Self {
        InvCollectionError {
            inv_collection_id,
            idx: SqlU16::from(idx),
            message,
        }
    }
}

/// See [`nexus_types::inventory::ServiceProcessor`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_service_processor)]
pub struct InvServiceProcessor {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,

    pub baseboard_revision: BaseboardRevision,
    pub hubris_archive_id: String,
    pub power_state: HwPowerState,
}

impl From<InvServiceProcessor> for nexus_types::inventory::ServiceProcessor {
    fn from(row: InvServiceProcessor) -> Self {
        Self {
            time_collected: row.time_collected,
            source: row.source,
            sp_type: nexus_types::inventory::SpType::from(row.sp_type),
            sp_slot: **row.sp_slot,
            baseboard_revision: **row.baseboard_revision,
            hubris_archive: row.hubris_archive_id,
            power_state: PowerState::from(row.power_state),
        }
    }
}

/// Newtype wrapping the MGS-reported slot number for an SP
///
/// Current racks only have 32 slots for any given SP type.  MGS represents the
/// slot number with a u32.  We truncate it to a u16 (which still requires
/// storing it as an i32 in the database, since the database doesn't natively
/// support signed integers).
#[derive(Copy, Clone, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Int4)]
pub struct SpMgsSlot(SqlU16);

NewtypeFrom! { () pub struct SpMgsSlot(SqlU16); }
NewtypeDeref! { () pub struct SpMgsSlot(SqlU16); }
NewtypeDisplay! { () pub struct SpMgsSlot(SqlU16); }

impl ToSql<sql_types::Int4, Pg> for SpMgsSlot {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <SqlU16 as ToSql<sql_types::Int4, Pg>>::to_sql(
            &self.0,
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Int4, DB> for SpMgsSlot
where
    DB: Backend,
    SqlU16: FromSql<sql_types::Int4, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(SpMgsSlot(SqlU16::from_sql(bytes)?))
    }
}

/// Newtype wrapping the revision number for a particular baseboard
///
/// MGS reports this as a u32 and we represent it the same way, though that
/// would be quite a lot of hardware revisions to go through!
#[derive(Copy, Clone, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Int8)]
pub struct BaseboardRevision(SqlU32);

NewtypeFrom! { () pub struct BaseboardRevision(SqlU32); }
NewtypeDeref! { () pub struct BaseboardRevision(SqlU32); }
NewtypeDisplay! { () pub struct BaseboardRevision(SqlU32); }

impl ToSql<sql_types::Int8, Pg> for BaseboardRevision {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <SqlU32 as ToSql<sql_types::Int8, Pg>>::to_sql(
            &self.0,
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Int8, DB> for BaseboardRevision
where
    DB: Backend,
    SqlU32: FromSql<sql_types::Int8, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(BaseboardRevision(SqlU32::from_sql(bytes)?))
    }
}

/// See [`nexus_types::inventory::RotState`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_root_of_trust)]
pub struct InvRootOfTrust {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub slot_active: HwRotSlot,
    pub slot_boot_pref_transient: Option<HwRotSlot>,
    pub slot_boot_pref_persistent: HwRotSlot,
    pub slot_boot_pref_persistent_pending: Option<HwRotSlot>,
    pub slot_a_sha3_256: Option<String>,
    pub slot_b_sha3_256: Option<String>,
}

impl From<InvRootOfTrust> for nexus_types::inventory::RotState {
    fn from(row: InvRootOfTrust) -> Self {
        Self {
            time_collected: row.time_collected,
            source: row.source,
            active_slot: RotSlot::from(row.slot_active),
            persistent_boot_preference: RotSlot::from(
                row.slot_boot_pref_persistent,
            ),
            pending_persistent_boot_preference: row
                .slot_boot_pref_persistent_pending
                .map(RotSlot::from),
            transient_boot_preference: row
                .slot_boot_pref_transient
                .map(RotSlot::from),
            slot_a_sha3_256_digest: row.slot_a_sha3_256,
            slot_b_sha3_256_digest: row.slot_b_sha3_256,
        }
    }
}

/// See [`nexus_types::inventory::CabooseFound`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_caboose)]
pub struct InvCaboose {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub which: CabooseWhich,
    pub sw_caboose_id: Uuid,
}

/// See [`nexus_types::inventory::RotPageFound`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_root_of_trust_page)]
pub struct InvRotPage {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub which: RotPageWhich,
    pub sw_root_of_trust_page_id: Uuid,
}

// See [`nexus_types::inventory::SledRole`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sled_role"))]
    pub struct SledRoleEnum;

    #[derive(
        Copy,
        Clone,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialOrd,
        Ord,
        PartialEq,
        Eq
    )]
    #[diesel(sql_type = SledRoleEnum)]
    pub enum SledRole;

    // Enum values
    Gimlet => b"gimlet"
    Scrimlet =>  b"scrimlet"
);

impl From<nexus_types::inventory::SledRole> for SledRole {
    fn from(value: nexus_types::inventory::SledRole) -> Self {
        match value {
            nexus_types::inventory::SledRole::Gimlet => SledRole::Gimlet,
            nexus_types::inventory::SledRole::Scrimlet => SledRole::Scrimlet,
        }
    }
}

impl From<SledRole> for nexus_types::inventory::SledRole {
    fn from(value: SledRole) -> Self {
        match value {
            SledRole::Gimlet => nexus_types::inventory::SledRole::Gimlet,
            SledRole::Scrimlet => nexus_types::inventory::SledRole::Scrimlet,
        }
    }
}

/// See [`nexus_types::inventory::SledAgent`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_agent)]
pub struct InvSledAgent {
    pub inv_collection_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub sled_id: Uuid,
    pub hw_baseboard_id: Option<Uuid>,
    pub sled_agent_ip: ipv6::Ipv6Addr,
    pub sled_agent_port: SqlU16,
    pub sled_role: SledRole,
    pub usable_hardware_threads: SqlU32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,
}

impl InvSledAgent {
    pub fn new_without_baseboard(
        collection_id: Uuid,
        sled_agent: &nexus_types::inventory::SledAgent,
    ) -> Result<InvSledAgent, anyhow::Error> {
        // It's irritating to have to check this case at runtime.  The challenge
        // is that if this sled agent does have a baseboard id, we don't know
        // what it's (SQL) id is.  The only way to get it is to query it from
        // the database.  As a result, the caller takes a wholly different code
        // path for that case that doesn't even involve constructing one of
        // these objects.  (In fact, we never see the id in Rust.)
        //
        // To check this at compile time, we'd have to bifurcate
        // `nexus_types::inventory::SledAgent` into an enum with two variants:
        // one with a baseboard id and one without.  This would muck up all the
        // other consumers of this type, just for a highly database-specific
        // concern.
        if sled_agent.baseboard_id.is_some() {
            Err(anyhow!(
                "attempted to directly insert InvSledAgent with \
                non-null baseboard id"
            ))
        } else {
            Ok(InvSledAgent {
                inv_collection_id: collection_id,
                time_collected: sled_agent.time_collected,
                source: sled_agent.source.clone(),
                sled_id: sled_agent.sled_id,
                hw_baseboard_id: None,
                sled_agent_ip: ipv6::Ipv6Addr::from(
                    *sled_agent.sled_agent_address.ip(),
                ),
                sled_agent_port: SqlU16(sled_agent.sled_agent_address.port()),
                sled_role: SledRole::from(sled_agent.sled_role),
                usable_hardware_threads: SqlU32(
                    sled_agent.usable_hardware_threads,
                ),
                usable_physical_ram: ByteCount::from(
                    sled_agent.usable_physical_ram,
                ),
                reservoir_size: ByteCount::from(sled_agent.reservoir_size),
            })
        }
    }
}

/// See [`nexus_types::inventory::OmicronZonesFound`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_omicron_zones)]
pub struct InvSledOmicronZones {
    pub inv_collection_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub sled_id: Uuid,
    pub generation: Generation,
}

impl InvSledOmicronZones {
    pub fn new(
        inv_collection_id: Uuid,
        zones_found: &nexus_types::inventory::OmicronZonesFound,
    ) -> InvSledOmicronZones {
        InvSledOmicronZones {
            inv_collection_id,
            time_collected: zones_found.time_collected,
            source: zones_found.source.clone(),
            sled_id: zones_found.sled_id,
            generation: Generation(zones_found.zones.generation.clone().into()),
        }
    }

    pub fn into_uninit_zones_found(
        self,
    ) -> nexus_types::inventory::OmicronZonesFound {
        nexus_types::inventory::OmicronZonesFound {
            time_collected: self.time_collected,
            source: self.source,
            sled_id: self.sled_id,
            zones: nexus_types::inventory::OmicronZonesConfig {
                generation: self.generation.0.into(),
                zones: Vec::new(),
            },
        }
    }
}

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "zone_type"))]
    pub struct ZoneTypeEnum;

    #[derive(Clone, Copy, Debug, Eq, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = ZoneTypeEnum)]
    pub enum ZoneType;

    // Enum values
    BoundaryNtp => b"boundary_ntp"
    Clickhouse => b"clickhouse"
    ClickhouseKeeper => b"clickhouse_keeper"
    CockroachDb => b"cockroach_db"
    Crucible => b"crucible"
    CruciblePantry => b"crucible_pantry"
    ExternalDns => b"external_dns"
    InternalDns => b"internal_dns"
    InternalNtp => b"internal_ntp"
    Nexus => b"nexus"
    Oximeter => b"oximeter"
);

/// See [`nexus_types::inventory::OmicronZoneConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_zone)]
pub struct InvOmicronZone {
    pub inv_collection_id: Uuid,
    pub sled_id: Uuid,
    pub id: Uuid,
    pub underlay_address: ipv6::Ipv6Addr,
    pub zone_type: ZoneType,
    pub primary_service_ip: ipv6::Ipv6Addr,
    pub primary_service_port: SqlU16,
    pub second_service_ip: Option<IpNetwork>,
    pub second_service_port: Option<SqlU16>,
    pub dataset_zpool_name: Option<String>,
    pub nic_id: Option<Uuid>,
    pub dns_gz_address: Option<ipv6::Ipv6Addr>,
    pub dns_gz_address_index: Option<SqlU32>,
    pub ntp_ntp_servers: Option<Vec<String>>,
    pub ntp_dns_servers: Option<Vec<IpNetwork>>,
    pub ntp_ntp_domain: Option<String>,
    pub nexus_external_tls: Option<bool>,
    pub nexus_external_dns_servers: Option<Vec<IpNetwork>>,
    pub snat_ip: Option<ipv6::Ipv6Addr>,
    pub snat_first_port: Option<SqlU16>,
    pub snat_last_port: Option<SqlU16>,
}

impl InvOmicronZone {
    pub fn new(
        inv_collection_id: Uuid,
        sled_id: Uuid,
        zone: &nexus_types::inventory::OmicronZoneConfig,
    ) -> Result<InvOmicronZone, anyhow::Error> {
        let id = zone.id;
        let underlay_address = ipv6::Ipv6Addr::from(zone.underlay_address);
        let mut nic_id = None;
        let mut dns_gz_address = None;
        let mut dns_gz_address_index = None;
        let mut ntp_ntp_servers = None;
        let mut ntp_dns_servers = None;
        let mut ntp_ntp_domain = None;
        let mut nexus_external_tls = None;
        let mut nexus_external_dns_servers = None;
        let mut snat_ip = None;
        let mut snat_first_port = None;
        let mut snat_last_port = None;
        let mut second_service_ip = None;
        let mut second_service_port = None;

        let (zone_type, primary_service_sockaddr_str, dataset) = match &zone
            .zone_type
        {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => {
                ntp_ntp_servers = Some(ntp_servers.clone());
                ntp_dns_servers = Some(dns_servers.clone());
                ntp_ntp_domain = domain.clone();
                snat_ip = Some(ipv6::Ipv6Addr::from(match snat_cfg.ip {
                    std::net::IpAddr::V6(a) => Ok(a),
                    std::net::IpAddr::V4(bad) => Err(anyhow!(
                        "expected source NAT IP to be IPv6, found IPv4 {:?}",
                        bad
                    )),
                }?));
                snat_first_port = Some(SqlU16::from(snat_cfg.first_port));
                snat_last_port = Some(SqlU16::from(snat_cfg.last_port));
                nic_id = Some(nic.id);
                (ZoneType::BoundaryNtp, address, None)
            }
            OmicronZoneType::Clickhouse { address, dataset } => {
                (ZoneType::Clickhouse, address, Some(dataset))
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                (ZoneType::ClickhouseKeeper, address, Some(dataset))
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                (ZoneType::CockroachDb, address, Some(dataset))
            }
            OmicronZoneType::Crucible { address, dataset } => {
                (ZoneType::Crucible, address, Some(dataset))
            }
            OmicronZoneType::CruciblePantry { address } => {
                (ZoneType::CruciblePantry, address, None)
            }
            OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => {
                nic_id = Some(nic.id);
                let sockaddr = dns_address
                    .parse::<std::net::SocketAddr>()
                    .with_context(|| {
                        format!(
                            "parsing address for external DNS server {:?}",
                            dns_address
                        )
                    })?;
                second_service_ip = Some(sockaddr.ip());
                second_service_port = Some(SqlU16::from(sockaddr.port()));
                (ZoneType::ExternalDns, http_address, Some(dataset))
            }
            OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => {
                dns_gz_address = Some(ipv6::Ipv6Addr::from(gz_address));
                dns_gz_address_index = Some(SqlU32::from(*gz_address_index));
                let sockaddr = dns_address
                    .parse::<std::net::SocketAddr>()
                    .with_context(|| {
                        format!(
                            "parsing address for internal DNS server {:?}",
                            dns_address
                        )
                    })?;
                second_service_ip = Some(sockaddr.ip());
                second_service_port = Some(SqlU16::from(sockaddr.port()));
                (ZoneType::InternalDns, http_address, Some(dataset))
            }
            OmicronZoneType::InternalNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
            } => {
                ntp_ntp_servers = Some(ntp_servers.clone());
                ntp_dns_servers = Some(dns_servers.clone());
                ntp_ntp_domain = domain.clone();
                (ZoneType::InternalNtp, address, None)
            }
            OmicronZoneType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => {
                nic_id = Some(nic.id);
                nexus_external_tls = Some(*external_tls);
                nexus_external_dns_servers = Some(external_dns_servers.clone());
                second_service_ip = Some(*external_ip);
                (ZoneType::Nexus, internal_address, None)
            }
            OmicronZoneType::Oximeter { address } => {
                (ZoneType::Oximeter, address, None)
            }
        };

        let dataset_zpool_name =
            dataset.map(|d| d.pool_name.as_str().to_string());
        let primary_service_sockaddr = primary_service_sockaddr_str
            .parse::<std::net::SocketAddrV6>()
            .with_context(|| {
                format!(
                    "parsing socket address for primary IP {:?}",
                    primary_service_sockaddr_str
                )
            })?;
        let (primary_service_ip, primary_service_port) = (
            ipv6::Ipv6Addr::from(*primary_service_sockaddr.ip()),
            SqlU16::from(primary_service_sockaddr.port()),
        );

        Ok(InvOmicronZone {
            inv_collection_id,
            sled_id,
            id,
            underlay_address,
            zone_type,
            primary_service_ip,
            primary_service_port,
            second_service_ip: second_service_ip.map(IpNetwork::from),
            second_service_port,
            dataset_zpool_name,
            nic_id,
            dns_gz_address,
            dns_gz_address_index,
            ntp_ntp_servers,
            ntp_dns_servers: ntp_dns_servers
                .map(|list| list.into_iter().map(IpNetwork::from).collect()),
            ntp_ntp_domain,
            nexus_external_tls,
            nexus_external_dns_servers: nexus_external_dns_servers
                .map(|list| list.into_iter().map(IpNetwork::from).collect()),
            snat_ip,
            snat_first_port,
            snat_last_port,
        })
    }

    pub fn into_omicron_zone_config(
        self,
        nic_row: Option<InvOmicronZoneNic>,
    ) -> Result<nexus_types::inventory::OmicronZoneConfig, anyhow::Error> {
        let address = SocketAddrV6::new(
            std::net::Ipv6Addr::from(self.primary_service_ip),
            *self.primary_service_port,
            0,
            0,
        )
        .to_string();

        // Assemble a value that we can use to extract the NIC _if necessary_
        // and report an error if it was needed but not found.
        //
        // Any error here should be impossible.  By the time we get here, the
        // caller should have provided `nic_row` iff there's a corresponding
        // `nic_id` in this row, and the ids should match up.  And whoever
        // created this row ought to have provided a nic_id iff this type of
        // zone needs a NIC.  This last issue is not under our control, though,
        // so we definitely want to handle that as an operational error.  The
        // others could arguably be programmer errors (i.e., we could `assert`),
        // but it seems excessive to crash here.
        //
        // Note that we immediately return for any of the caller errors here.
        // For the other error, we will return only later, if some code path
        // below tries to use `nic` when it's not present.
        let nic = match (self.nic_id, nic_row) {
            (Some(expected_id), Some(nic_row)) => {
                ensure!(expected_id == nic_row.id, "caller provided wrong NIC");
                // XXX-dap move to InvOmicronZoneNic method
                Ok(nexus_types::inventory::NetworkInterface {
                    id: nic_row.id,
                    ip: nic_row.ip.ip(),
                    kind: nexus_types::inventory::NetworkInterfaceKind::Service(
                        self.id,
                    ),
                    mac: (*nic_row.mac).into(),
                    name: (&(*nic_row.name)).into(),
                    primary: nic_row.is_primary,
                    slot: *nic_row.slot,
                    vni: nexus_types::inventory::Vni::from(*nic_row.vni),
                    subnet: nic_row.subnet.into(),
                })
            }
            (None, None) => Err(anyhow!(
                "expected zone to have an associated NIC, but it doesn't"
            )),
            (Some(_), None) => bail!("caller provided no NIC"),
            (None, Some(_)) => bail!("caller unexpectedly provided a NIC"),
        };

        // Similarly, assemble a value that we can use to extract the dataset,
        // if necessary.  We only return this error if code below tries to use
        // this value.
        let dataset = self
            .dataset_zpool_name
            .map(|zpool_name| -> Result<_, anyhow::Error> {
                Ok(nexus_types::inventory::OmicronZoneDataset {
                    pool_name: zpool_name.parse().map_err(|e| {
                        anyhow!("parsing zpool name {:?}: {}", zpool_name, e)
                    })?,
                })
            })
            .transpose()?
            .ok_or_else(|| anyhow!("expected dataset zpool name, found none"));

        // Do the same for the DNS server address.
        let dns_address =
            match (self.second_service_ip, self.second_service_port) {
                (Some(dns_ip), Some(dns_port)) => {
                    Ok(std::net::SocketAddr::new(dns_ip.ip(), *dns_port)
                        .to_string())
                }
                _ => Err(anyhow!(
                    "expected second service IP and port, \
                            found one missing"
                )),
            };

        // Do the same for NTP zone properties.
        let ntp_dns_servers = self
            .ntp_dns_servers
            .ok_or_else(|| anyhow!("expected list of DNS servers, found null"))
            .map(|list| {
                list.into_iter().map(|ipnetwork| ipnetwork.ip()).collect()
            });
        let ntp_ntp_servers =
            self.ntp_ntp_servers.ok_or_else(|| anyhow!("expected ntp_servers"));

        let zone_type = match self.zone_type {
            ZoneType::BoundaryNtp => {
                let snat_cfg = match (
                    self.snat_ip,
                    self.snat_first_port,
                    self.snat_last_port,
                ) {
                    (Some(ip), Some(first_port), Some(last_port)) => {
                        nexus_types::inventory::SourceNatConfig {
                            ip: std::net::IpAddr::from(*ip),
                            first_port: *first_port,
                            last_port: *last_port,
                        }
                    }
                    _ => bail!(
                        "expected non-NULL snat properties, \
                        found at least one NULL"
                    ),
                };
                OmicronZoneType::BoundaryNtp {
                    address,
                    dns_servers: ntp_dns_servers?,
                    domain: self.ntp_ntp_domain,
                    nic: nic?,
                    ntp_servers: ntp_ntp_servers?,
                    snat_cfg,
                }
            }
            ZoneType::Clickhouse => {
                OmicronZoneType::Clickhouse { address, dataset: dataset? }
            }
            ZoneType::ClickhouseKeeper => {
                OmicronZoneType::ClickhouseKeeper { address, dataset: dataset? }
            }
            ZoneType::CockroachDb => {
                OmicronZoneType::CockroachDb { address, dataset: dataset? }
            }
            ZoneType::Crucible => {
                OmicronZoneType::Crucible { address, dataset: dataset? }
            }
            ZoneType::CruciblePantry => {
                OmicronZoneType::CruciblePantry { address }
            }
            ZoneType::ExternalDns => OmicronZoneType::ExternalDns {
                dataset: dataset?,
                dns_address: dns_address?,
                http_address: address,
                nic: nic?,
            },
            ZoneType::InternalDns => OmicronZoneType::InternalDns {
                dataset: dataset?,
                dns_address: dns_address?,
                http_address: address,
                gz_address: *self.dns_gz_address.ok_or_else(|| {
                    anyhow!("expected dns_gz_address, found none")
                })?,
                gz_address_index: *self.dns_gz_address_index.ok_or_else(
                    || anyhow!("expected dns_gz_address_index, found none"),
                )?,
            },
            ZoneType::InternalNtp => OmicronZoneType::InternalNtp {
                address,
                dns_servers: ntp_dns_servers?,
                domain: self.ntp_ntp_domain,
                ntp_servers: ntp_ntp_servers?,
            },
            ZoneType::Nexus => OmicronZoneType::Nexus {
                internal_address: address,
                nic: nic?,
                external_tls: self
                    .nexus_external_tls
                    .ok_or_else(|| anyhow!("expected 'external_tls'"))?,
                external_ip: self
                    .second_service_ip
                    .ok_or_else(|| anyhow!("expected second service IP"))?
                    .ip(),
                external_dns_servers: self
                    .nexus_external_dns_servers
                    .ok_or_else(|| anyhow!("expected 'external_dns_servers'"))?
                    .into_iter()
                    .map(|i| i.ip())
                    .collect(),
            },
            ZoneType::Oximeter => OmicronZoneType::Oximeter { address },
        };
        Ok(nexus_types::inventory::OmicronZoneConfig {
            id: self.id,
            underlay_address: std::net::Ipv6Addr::from(self.underlay_address),
            zone_type,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_zone_nic)]
pub struct InvOmicronZoneNic {
    inv_collection_id: Uuid,
    pub id: Uuid,
    name: Name,
    ip: IpNetwork,
    mac: MacAddr,
    subnet: IpNetwork,
    vni: SqlU32,
    is_primary: bool,
    slot: SqlU8,
}

impl InvOmicronZoneNic {
    pub fn new(
        inv_collection_id: Uuid,
        zone: &nexus_types::inventory::OmicronZoneConfig,
    ) -> Result<Option<InvOmicronZoneNic>, anyhow::Error> {
        match &zone.zone_type {
            OmicronZoneType::ExternalDns { nic, .. }
            | OmicronZoneType::BoundaryNtp { nic, .. }
            | OmicronZoneType::Nexus { nic, .. } => {
                // We do not bother storing the NIC's kind and associated id
                // because it should be inferrable from the other information
                // that we have.  Verify that here.
                ensure!(
                    matches!(
                        nic.kind,
                        nexus_types::inventory::NetworkInterfaceKind::Service(
                            id
                        ) if id == zone.id
                    ),
                    "expected zone's NIC kind to be \"service\" and the \
                    id to match the zone's id ({})",
                    zone.id
                );

                Ok(Some(InvOmicronZoneNic {
                    inv_collection_id,
                    id: nic.id,
                    name: Name::from(
                        omicron_common::api::external::Name::from(
                            nic.name.clone(),
                        ),
                    ),
                    ip: IpNetwork::from(nic.ip),
                    mac: MacAddr::from(
                        omicron_common::api::external::MacAddr::from(
                            nic.mac.clone(),
                        ),
                    ),
                    subnet: IpNetwork::from(nic.subnet.clone()),
                    vni: SqlU32::from(nic.vni.0),
                    is_primary: nic.primary,
                    slot: SqlU8::from(nic.slot),
                }))
            }
            _ => Ok(None),
        }
    }
}
