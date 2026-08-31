// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types for the Nexus external API.

use crate::v2025_11_20_00::support_bundle::SupportBundleInfo;
use crate::v2026_08_27_00;
use crate::v2026_08_27_00::support_bundle::SupportBundleEreports;
use crate::v2026_08_27_00::support_bundle::SupportBundleSledSelection;
use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// A type of zone whose logs a support bundle can collect.
///
/// A zone's type is derived from its name, which cannot distinguish
/// boundary from internal NTP zones; `ntp` covers both.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SupportBundleZoneType {
    /// The global zone.
    Global,
    /// The switch zone.
    Switch,
    /// Instance zones (propolis-server).
    Propolis,
    /// NTP zones, boundary and internal alike.
    Ntp,
    /// Single-node ClickHouse zones.
    Clickhouse,
    /// ClickHouse keeper zones.
    ClickhouseKeeper,
    /// Replicated ClickHouse server zones.
    ClickhouseServer,
    /// CockroachDB zones.
    #[serde(rename = "cockroachdb")]
    CockroachDb,
    /// Crucible zones.
    Crucible,
    /// Crucible pantry zones.
    CruciblePantry,
    /// External DNS zones.
    ExternalDns,
    /// Internal DNS zones.
    InternalDns,
    /// Nexus zones.
    Nexus,
    /// Oximeter zones.
    Oximeter,
}

/// The zones whose logs host info collects, selected by zone type.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SupportBundleZoneSelection {
    /// Collect logs from every zone, including zones of types not listed
    /// in `SupportBundleZoneType`.
    #[default]
    All,
    /// Collect logs only from zones of the listed types. An empty list
    /// collects from none of them. The list is treated as a set: duplicates
    /// are ignored, and a bundle's view reports it in a canonical order.
    Specific { types: Vec<SupportBundleZoneType> },
}

/// Host info collection: diagnostic commands and zone logs from sleds.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct SupportBundleHostInfo {
    /// The sleds to collect from. Every sled if omitted.
    #[serde(default)]
    pub sleds: SupportBundleSledSelection,
    /// The zones to collect logs from, by zone type. Every zone if omitted.
    /// Diagnostic commands are sled-wide and are not affected.
    #[serde(default)]
    pub zones: SupportBundleZoneSelection,
}

impl From<v2026_08_27_00::support_bundle::SupportBundleHostInfo>
    for SupportBundleHostInfo
{
    fn from(
        old: v2026_08_27_00::support_bundle::SupportBundleHostInfo,
    ) -> Self {
        Self { sleds: old.sleds, zones: SupportBundleZoneSelection::All }
    }
}

impl From<SupportBundleHostInfo>
    for v2026_08_27_00::support_bundle::SupportBundleHostInfo
{
    fn from(new: SupportBundleHostInfo) -> Self {
        Self { sleds: new.sleds }
    }
}

/// The data a support bundle collects.
///
/// Each category's settings live within that category, so settings for a
/// category that is not being collected cannot be expressed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SupportBundleData {
    /// Collect every category of data, from every sled, unfiltered.
    All,
    /// Collect exactly what is specified here. A category that is omitted is
    /// not collected.
    Explicit {
        /// Collect reconfigurator state: recent blueprints and information
        /// about the target blueprint.
        #[serde(default)]
        reconfigurator: bool,
        /// Collect sled serial numbers, cubby numbers, and UUIDs.
        #[serde(default)]
        sled_cubby_info: bool,
        /// Collect task dumps from service processors.
        #[serde(default)]
        sp_dumps: bool,
        /// Collect diagnostic commands and zone logs from sleds.
        host_info: Option<SupportBundleHostInfo>,
        /// Collect ereports.
        ereports: Option<SupportBundleEreports>,
    },
}

impl Default for SupportBundleData {
    fn default() -> Self {
        Self::All {}
    }
}

impl From<v2026_08_27_00::support_bundle::SupportBundleData>
    for SupportBundleData
{
    fn from(old: v2026_08_27_00::support_bundle::SupportBundleData) -> Self {
        match old {
            v2026_08_27_00::support_bundle::SupportBundleData::All => Self::All,
            v2026_08_27_00::support_bundle::SupportBundleData::Explicit {
                reconfigurator,
                sled_cubby_info,
                sp_dumps,
                host_info,
                ereports,
            } => Self::Explicit {
                reconfigurator,
                sled_cubby_info,
                sp_dumps,
                host_info: host_info.map(Into::into),
                ereports,
            },
        }
    }
}

impl From<SupportBundleData>
    for v2026_08_27_00::support_bundle::SupportBundleData
{
    fn from(new: SupportBundleData) -> Self {
        match new {
            SupportBundleData::All => Self::All,
            SupportBundleData::Explicit {
                reconfigurator,
                sled_cubby_info,
                sp_dumps,
                host_info,
                ereports,
            } => Self::Explicit {
                reconfigurator,
                sled_cubby_info,
                sp_dumps,
                host_info: host_info.map(Into::into),
                ereports,
            },
        }
    }
}

/// What a support bundle collects.
///
/// When creating a bundle, an omitted field takes its default: everything,
/// from every sled, unfiltered. When viewing a bundle, every field describes
/// what was actually recorded for it.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct SupportBundleDataSelection {
    /// The data to collect. Everything if omitted.
    #[serde(default)]
    pub data: SupportBundleData,
    /// The inclusive start of the window bounding time-bounded data: zone
    /// logs and ereports. Ereports are filtered on their timestamp; a log
    /// file is included when the interval it spans overlaps the window.
    ///
    /// When creating a bundle, an omitted start defaults to seven days
    /// before the end of the window (or seven days ago, when the window has
    /// no end). When viewing a bundle, an omitted start means the bundle was
    /// created before time windows were recorded, and was collected without
    /// a lower bound.
    pub start_time: Option<DateTime<Utc>>,
    /// The inclusive end of that window. Unbounded if omitted.
    pub end_time: Option<DateTime<Utc>>,
}

impl From<v2026_08_27_00::support_bundle::SupportBundleDataSelection>
    for SupportBundleDataSelection
{
    fn from(
        old: v2026_08_27_00::support_bundle::SupportBundleDataSelection,
    ) -> Self {
        Self {
            data: old.data.into(),
            start_time: old.start_time,
            end_time: old.end_time,
        }
    }
}

impl From<SupportBundleDataSelection>
    for v2026_08_27_00::support_bundle::SupportBundleDataSelection
{
    fn from(new: SupportBundleDataSelection) -> Self {
        Self {
            data: new.data.into(),
            start_time: new.start_time,
            end_time: new.end_time,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleCreate {
    /// User comment for the support bundle
    pub user_comment: Option<String>,

    /// What the bundle should collect. Everything, over the last seven days,
    /// if omitted.
    pub data_selection: Option<SupportBundleDataSelection>,
}

impl From<v2026_08_27_00::support_bundle::SupportBundleCreate>
    for SupportBundleCreate
{
    fn from(old: v2026_08_27_00::support_bundle::SupportBundleCreate) -> Self {
        Self {
            user_comment: old.user_comment,
            data_selection: old.data_selection.map(Into::into),
        }
    }
}

/// A support bundle, along with what it collects.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleView {
    #[serde(flatten)]
    pub bundle: SupportBundleInfo,

    /// What this bundle collects.
    pub data_selection: SupportBundleDataSelection,
}

impl From<SupportBundleView>
    for v2026_08_27_00::support_bundle::SupportBundleView
{
    fn from(new: SupportBundleView) -> Self {
        Self { bundle: new.bundle, data_selection: new.data_selection.into() }
    }
}

impl From<SupportBundleView> for SupportBundleInfo {
    fn from(new: SupportBundleView) -> Self {
        new.bundle
    }
}
