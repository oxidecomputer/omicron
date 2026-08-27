// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types for the Nexus external API.

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::support_bundle::SupportBundleInfo;
use chrono::DateTime;
use chrono::Utc;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// The sleds to collect host info from.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SupportBundleSledSelection {
    /// Collect from every sled.
    All {},
    /// Collect only from the listed sleds. An empty list collects from none
    /// of them.
    Specific {
        #[schemars(with = "Vec<Uuid>")]
        sleds: Vec<SledUuid>,
    },
}

impl Default for SupportBundleSledSelection {
    fn default() -> Self {
        Self::All {}
    }
}

/// Host info collection: diagnostic commands and zone logs from sleds.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleHostInfo {
    /// The sleds to collect from. Every sled if omitted.
    #[serde(default)]
    pub sleds: SupportBundleSledSelection,
}

/// Ereport collection.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleEreports {
    /// Collect only ereports reported by systems with these serial numbers.
    /// Unfiltered when empty.
    #[serde(default)]
    pub only_serials: Vec<String>,
    /// Collect only ereports with these class strings. Unfiltered when empty.
    #[serde(default)]
    pub only_classes: Vec<String>,
}

/// The data a support bundle collects.
///
/// Each category's settings live within that category, so settings for a
/// category that is not being collected cannot be expressed.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SupportBundleData {
    /// Collect every category of data, from every sled, unfiltered.
    All {},
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

/// What a support bundle collects.
///
/// When creating a bundle, an omitted field takes its default: everything,
/// from every sled, unfiltered. When viewing a bundle, every field describes
/// what was actually recorded for it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleCreate {
    /// User comment for the support bundle
    pub user_comment: Option<String>,

    /// What the bundle should collect. Everything, over the last seven days,
    /// if omitted.
    pub data_selection: Option<SupportBundleDataSelection>,
}

impl From<v2025_11_20_00::support_bundle::SupportBundleCreate>
    for SupportBundleCreate
{
    fn from(old: v2025_11_20_00::support_bundle::SupportBundleCreate) -> Self {
        Self { user_comment: old.user_comment, data_selection: None }
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

impl From<SupportBundleView> for SupportBundleInfo {
    fn from(new: SupportBundleView) -> Self {
        new.bundle
    }
}
