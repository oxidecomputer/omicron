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
    HostInfo(SledSelection),
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

/// Displayer for pretty-printing [`BundleData`].
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplayBundleData<'a> {
    data: &'a BundleData,
}

impl BundleData {
    pub fn display(&self) -> DisplayBundleData<'_> {
        DisplayBundleData { data: self }
    }
}

impl fmt::Display for DisplayBundleData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.data {
            BundleData::Reconfigurator => write!(f, "reconfigurator"),
            BundleData::HostInfo(selection) => {
                write!(f, "host_info({})", selection.display())
            }
            BundleData::SledCubbyInfo => write!(f, "sled_cubby_info"),
            BundleData::SpDumps => write!(f, "sp_dumps"),
            BundleData::Ereports(filters) => {
                write!(f, "ereports({})", filters.display())
            }
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

    /// Adds reconfigurator state collection.
    pub fn with_reconfigurator(self) -> Self {
        self.with(BundleData::Reconfigurator)
    }

    /// Adds sled cubby info collection.
    pub fn with_sled_cubby_info(self) -> Self {
        self.with(BundleData::SledCubbyInfo)
    }

    /// Adds SP dump collection.
    pub fn with_sp_dumps(self) -> Self {
        self.with(BundleData::SpDumps)
    }

    /// Adds host info collection from all sleds.
    pub fn with_all_sleds(self) -> Self {
        self.with(BundleData::HostInfo(SledSelection::All))
    }

    /// Adds host info collection from specific sleds.
    pub fn with_specific_sleds(
        self,
        sleds: impl IntoIterator<Item = SledUuid>,
    ) -> Self {
        self.with(BundleData::HostInfo(SledSelection::Specific(
            sleds.into_iter().collect(),
        )))
    }

    /// Adds ereport collection with the given filters.
    pub fn with_ereports(self, filters: EreportFilters) -> Self {
        self.with(BundleData::Ereports(filters))
    }

    /// Builder-style method that inserts a [`BundleData`] value and returns
    /// `self`. If multiple BundleData entries with the same type are inserted,
    /// the last write wins.
    pub fn with(mut self, bundle_data: BundleData) -> Self {
        self.data.insert(bundle_data.category(), bundle_data);
        self
    }

    pub fn contains(&self, category: BundleDataCategory) -> bool {
        self.data.contains_key(&category)
    }

    pub fn get(&self, category: BundleDataCategory) -> Option<&BundleData> {
        self.data.get(&category)
    }

    /// Iterates over the data entries in this selection.
    pub fn iter(&self) -> impl Iterator<Item = &BundleData> {
        self.data.values()
    }
}

/// Displayer for pretty-printing [`BundleDataSelection`].
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplayBundleDataSelection<'a> {
    selection: &'a BundleDataSelection,
    indent: usize,
}

impl BundleDataSelection {
    pub fn display(&self, indent: usize) -> DisplayBundleDataSelection<'_> {
        DisplayBundleDataSelection { selection: self, indent }
    }
}

impl fmt::Display for DisplayBundleDataSelection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let indent = self.indent;
        for (i, item) in self.selection.data.values().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{:>indent$}- {}", "", item.display())?;
        }
        Ok(())
    }
}

impl FromIterator<BundleData> for BundleDataSelection {
    fn from_iter<T: IntoIterator<Item = BundleData>>(iter: T) -> Self {
        iter.into_iter().fold(Self::new(), |sel, data| sel.with(data))
    }
}

impl Default for BundleDataSelection {
    /// Returns a selection containing all data categories (i.e. "collect
    /// everything"). This is distinct from [`Self::new`], which returns an
    /// empty selection.
    fn default() -> Self {
        Self::new()
            .with_reconfigurator()
            .with_all_sleds()
            .with_sled_cubby_info()
            .with_sp_dumps()
            .with_ereports(
                EreportFilters::new()
                    .with_start_time(chrono::Utc::now() - chrono::Days::new(7))
                    .expect("no end time set, cannot fail"),
            )
    }
}

/// The set of sleds to include. This can either be all sleds, or a set of
/// specific sleds.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum SledSelection {
    All,
    Specific(HashSet<SledUuid>),
}

/// Displayer for pretty-printing [`SledSelection`].
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplaySledSelection<'a> {
    selection: &'a SledSelection,
}

impl SledSelection {
    pub fn display(&self) -> DisplaySledSelection<'_> {
        DisplaySledSelection { selection: self }
    }
}

impl fmt::Display for DisplaySledSelection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.selection {
            SledSelection::All => write!(f, "all"),
            SledSelection::Specific(ids) => {
                write!(f, "{}", ids.iter().format(", "))
            }
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
