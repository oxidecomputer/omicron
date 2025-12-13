// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle request types and data selection

use nexus_db_queries::db::datastore::EreportFilters;
use omicron_uuid_kinds::SledUuid;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroU64;

/// We use "/var/tmp" to use Nexus' filesystem for temporary storage,
/// rather than "/tmp", which would keep this collected data in-memory.
pub const TEMPDIR: &str = "/var/tmp";

/// The size of piece of a support bundle to transfer to the sled agent
/// within a single streaming request.
pub const CHUNK_SIZE: NonZeroU64 = NonZeroU64::new(1024 * 1024 * 1024).unwrap();

/// Describes the category of support bundle data.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum BundleDataCategory {
    /// Collects reconfigurator state (some of the latest blueprints,
    /// information about the target blueprint).
    Reconfigurator,
    /// Collects info from sled agents, running a handful of
    /// diagnostic commands (e.g., zoneadm, dladm, etc).
    HostInfo,
    /// Collects sled serial numbers, cubby numbers, and UUIDs.
    SledCubbyInfo,
    /// Saves task dumps from SPs.
    SpDumps,
    /// Collects ereports
    Ereports,
}

/// Specifies what data to collect for a bundle data category.
///
/// Each variant corresponds to a BundleDataCategory.
/// For categories without additional parameters, the variant is a unit variant.
/// For categories that can be filtered or configured, the variant contains
/// that configuration data.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BundleData {
    Reconfigurator,
    HostInfo(HashSet<SledSelection>),
    SledCubbyInfo,
    SpDumps,
    Ereports(EreportFilters),
}

impl BundleData {
    fn category(&self) -> BundleDataCategory {
        match self {
            Self::Reconfigurator => BundleDataCategory::Reconfigurator,
            Self::HostInfo(_) => BundleDataCategory::HostInfo,
            Self::SledCubbyInfo => BundleDataCategory::SledCubbyInfo,
            Self::SpDumps => BundleDataCategory::SpDumps,
            Self::Ereports(_) => BundleDataCategory::Ereports,
        }
    }
}

/// A collection of bundle data specifications.
///
/// This wrapper ensures that categories and data always match - you can't
/// insert (BundleDataCategory::Reconfigurator, BundleData::SpDumps)
/// because each BundleData determines its own category.
#[derive(Debug, Clone)]
pub struct BundleDataSelection {
    data: HashMap<BundleDataCategory, BundleData>,
}

impl BundleDataSelection {
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }

    /// Inserts BundleData to be queried for a particular category within the
    /// bundle.
    ///
    /// Each category of data can only be specified once (e.g., inserting
    /// BundleData::HostInfo multiple times will only use the most-recently
    /// inserted specification)
    pub fn insert(&mut self, bundle_data: BundleData) {
        self.data.insert(bundle_data.category(), bundle_data);
    }

    pub fn contains(&self, category: BundleDataCategory) -> bool {
        self.data.contains_key(&category)
    }

    pub fn get(&self, category: BundleDataCategory) -> Option<&BundleData> {
        self.data.get(&category)
    }
}

impl FromIterator<BundleData> for BundleDataSelection {
    fn from_iter<T: IntoIterator<Item = BundleData>>(iter: T) -> Self {
        let mut selection = Self::new();
        for bundle_data in iter {
            selection.insert(bundle_data);
        }
        selection
    }
}

impl Default for BundleDataSelection {
    fn default() -> Self {
        [
            BundleData::Reconfigurator,
            BundleData::HostInfo(HashSet::from([SledSelection::All])),
            BundleData::SledCubbyInfo,
            BundleData::SpDumps,
            BundleData::Ereports(EreportFilters {
                start_time: Some(chrono::Utc::now() - chrono::Days::new(7)),
                ..EreportFilters::default()
            }),
        ]
        .into_iter()
        .collect()
    }
}

/// The set of sleds to include
///
/// Multiple values of this enum are joined together into a HashSet.
/// Therefore "SledSelection::All" overrides specific sleds.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum SledSelection {
    All,
    Specific(SledUuid),
}

/// Specifies the data to be collected within the Support Bundle.
#[derive(Clone)]
pub struct BundleRequest {
    /// The size of chunks to use when transferring a bundle from Nexus
    /// to a sled agent.
    ///
    /// Typically, this is CHUNK_SIZE, but can be modified for testing.
    pub transfer_chunk_size: NonZeroU64,

    /// The set of data to be included within this bundle.
    ///
    /// Maps each category to its filter. If a category is not in the map,
    /// it is excluded from the bundle.
    pub data_selection: BundleDataSelection,
}

impl BundleRequest {
    pub fn include_reconfigurator_data(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::Reconfigurator)
    }

    pub fn include_host_info(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::HostInfo)
    }

    pub fn include_sled_host_info(&self, id: SledUuid) -> bool {
        let selection =
            match self.data_selection.get(BundleDataCategory::HostInfo) {
                Some(BundleData::HostInfo(selection)) => selection,
                _ => return false,
            };

        selection.contains(&SledSelection::Specific(id))
            || selection.contains(&SledSelection::All)
    }

    pub fn get_ereport_filters(&self) -> Option<&EreportFilters> {
        match self.data_selection.get(BundleDataCategory::Ereports) {
            Some(BundleData::Ereports(filters)) => Some(filters),
            _ => None,
        }
    }

    pub fn include_sled_cubby_info(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::SledCubbyInfo)
    }

    pub fn include_sp_dumps(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::SpDumps)
    }
}

impl Default for BundleRequest {
    fn default() -> Self {
        Self {
            transfer_chunk_size: CHUNK_SIZE,
            data_selection: BundleDataSelection::default(),
        }
    }
}
