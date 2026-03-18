// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle data selection types.
//!
//! These types specify what data to collect in a support bundle.
//! They are shared between the support bundle collector and FM case types.

use crate::fm::ereport::EreportFilters;
use itertools::Itertools;
use omicron_uuid_kinds::SledUuid;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

/// Describes the category of support bundle data.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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
    /// Collects ereports.
    Ereports,
}

/// Specifies what data to collect for a bundle data category.
///
/// Each variant corresponds to a BundleDataCategory.
/// For categories without additional parameters, the variant is a unit variant.
/// For categories that can be filtered or configured, the variant contains
/// that configuration data.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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

impl fmt::Display for BundleData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reconfigurator => write!(f, "reconfigurator"),
            Self::HostInfo(sleds) => {
                write!(f, "host_info({})", sleds.iter().format(", "))
            }
            Self::SledCubbyInfo => write!(f, "sled_cubby_info"),
            Self::SpDumps => write!(f, "sp_dumps"),
            Self::Ereports(filters) => write!(f, "ereports({filters})"),
        }
    }
}

/// A collection of bundle data specifications.
///
/// This wrapper ensures that categories and data always match - you can't
/// insert (BundleDataCategory::Reconfigurator, BundleData::SpDumps)
/// because each BundleData determines its own category.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleDataSelection {
    data: HashMap<BundleDataCategory, BundleData>,
}

impl BundleDataSelection {
    /// Creates an empty selection with no data categories.
    ///
    /// This is distinct from [`Self::default`], which returns a selection
    /// containing all categories (i.e. "collect everything").
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

    /// Iterate over all data entries in this selection.
    pub fn iter(&self) -> impl Iterator<Item = &BundleData> {
        self.data.values()
    }
}

impl fmt::Display for BundleDataSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data.values().format(", "))
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
    /// Returns a selection containing all data categories (i.e. "collect
    /// everything"). This is distinct from [`Self::new`], which returns an
    /// empty selection.
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

/// The set of sleds to include.
///
/// Multiple values of this enum are joined together into a HashSet.
/// Therefore "SledSelection::All" overrides specific sleds.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum SledSelection {
    All,
    Specific(SledUuid),
}

impl fmt::Display for SledSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Specific(id) => write!(f, "{id}"),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use proptest::prelude::*;

    impl Arbitrary for BundleDataSelection {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop::collection::vec(any::<BundleData>(), 0..=5)
                .prop_map(|data| data.into_iter().collect())
                .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use test_strategy::proptest;

    #[proptest]
    fn bundle_data_selection_serde_round_trip(selection: BundleDataSelection) {
        let json = serde_json::to_string(&selection).unwrap();
        let deserialized: BundleDataSelection =
            serde_json::from_str(&json).unwrap();
        prop_assert_eq!(selection, deserialized);
    }
}
