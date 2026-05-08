// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle data selection types.
//!
//! These types specify what data to collect in a support bundle.
//! They are shared between the support bundle collector and FM case types.

use crate::fm::ereport::EreportFilters;
use chrono::DateTime;
use chrono::Utc;
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

/// Inclusive time bound applied bundle-wide to time-bounded categories
/// (currently host-info logs and ereports).
///
/// `None` on either side means unbounded on that side. Stored as a
/// single field on [`BundleDataSelection`] rather than smeared across
/// per-category filters; the `time_range()` accessor surfaces it to
/// consumers.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleTimeRange {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
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
            BundleData::HostInfo(sleds) => {
                write!(f, "host_info({})", sleds.display())
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
///
/// `time_range`, when set, bounds every time-bounded category's
/// collection (host-info logs and ereports). Stored as one field
/// here rather than copied into per-category filters.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleDataSelection {
    data: HashMap<BundleDataCategory, BundleData>,
    time_range: Option<BundleTimeRange>,
}

impl BundleDataSelection {
    /// Creates an empty selection with no data categories.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a selection containing all default data categories
    /// (i.e. "collect everything") with a bundle-wide 7-day time
    /// window applied to host-info logs and ereports.
    pub fn all() -> Self {
        Self::new()
            .with_reconfigurator()
            .with_all_sleds()
            .with_sled_cubby_info()
            .with_sp_dumps()
            .with_ereports(EreportFilters::new())
            .with_time_range(BundleTimeRange {
                start: Some(
                    omicron_common::now_db_precision() - chrono::Days::new(7),
                ),
                end: None,
            })
    }

    /// Adds reconfigurator state collection.
    pub fn with_reconfigurator(mut self) -> Self {
        self.insert(BundleData::Reconfigurator);
        self
    }

    /// Adds sled cubby info collection.
    pub fn with_sled_cubby_info(mut self) -> Self {
        self.insert(BundleData::SledCubbyInfo);
        self
    }

    /// Adds SP dump collection.
    pub fn with_sp_dumps(mut self) -> Self {
        self.insert(BundleData::SpDumps);
        self
    }

    /// Adds host info collection from all sleds.
    pub fn with_all_sleds(mut self) -> Self {
        self.insert(BundleData::HostInfo(SledSelection::All));
        self
    }

    /// Adds host info collection from specific sleds.
    pub fn with_specific_sleds(
        mut self,
        sleds: impl IntoIterator<Item = SledUuid>,
    ) -> Self {
        self.insert(BundleData::HostInfo(SledSelection::Specific(
            sleds.into_iter().collect(),
        )));
        self
    }

    /// Adds ereport collection with the given filters.
    pub fn with_ereports(mut self, filters: EreportFilters) -> Self {
        self.insert(BundleData::Ereports(filters));
        self
    }

    /// Sets the bundle-wide time range. Affects every time-bounded
    /// category (host-info logs and ereports) at collection time.
    pub fn with_time_range(mut self, range: BundleTimeRange) -> Self {
        self.time_range = Some(range);
        self
    }

    /// Inserts a [`BundleData`] value. If a value with the same category
    /// already exists, the last write wins.
    pub fn insert(&mut self, bundle_data: BundleData) {
        self.data.insert(bundle_data.category(), bundle_data);
    }

    /// Sets the bundle-wide time range in place (used by code paths
    /// that build the selection incrementally, e.g. database read
    /// paths).
    pub fn set_time_range(&mut self, range: Option<BundleTimeRange>) {
        self.time_range = range;
    }

    /// Returns `true` if reconfigurator state should be collected.
    pub fn contains_reconfigurator(&self) -> bool {
        self.data.contains_key(&BundleDataCategory::Reconfigurator)
    }

    /// Returns the sled selection for host info, or `None` if host info
    /// is not in the selection.
    pub fn sled_selection(&self) -> Option<&SledSelection> {
        match self.data.get(&BundleDataCategory::HostInfo) {
            Some(BundleData::HostInfo(sel)) => Some(sel),
            _ => None,
        }
    }

    /// Returns `true` if sled cubby info should be collected.
    pub fn contains_sled_cubby_info(&self) -> bool {
        self.data.contains_key(&BundleDataCategory::SledCubbyInfo)
    }

    /// Returns `true` if SP dumps should be collected.
    pub fn contains_sp_dumps(&self) -> bool {
        self.data.contains_key(&BundleDataCategory::SpDumps)
    }

    /// Returns the ereport filters, or `None` if ereports are not in
    /// the selection.
    pub fn ereport_filters(&self) -> Option<&EreportFilters> {
        match self.data.get(&BundleDataCategory::Ereports) {
            Some(BundleData::Ereports(filters)) => Some(filters),
            _ => None,
        }
    }

    /// Returns the bundle-wide time range, if any was set. Applies
    /// to every time-bounded category at collection time.
    pub fn time_range(&self) -> Option<&BundleTimeRange> {
        self.time_range.as_ref()
    }
}

impl IntoIterator for BundleDataSelection {
    type Item = BundleData;
    type IntoIter =
        std::collections::hash_map::IntoValues<BundleDataCategory, BundleData>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_values()
    }
}

impl<'a> IntoIterator for &'a BundleDataSelection {
    type Item = &'a BundleData;
    type IntoIter =
        std::collections::hash_map::Values<'a, BundleDataCategory, BundleData>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.values()
    }
}

impl FromIterator<BundleData> for BundleDataSelection {
    fn from_iter<T: IntoIterator<Item = BundleData>>(iter: T) -> Self {
        let mut sel = Self::new();
        for data in iter {
            sel.insert(data);
        }
        sel
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
    /// Returns `true` if this selection includes the given sled.
    pub fn contains(&self, id: SledUuid) -> bool {
        match self {
            Self::All => true,
            Self::Specific(sleds) => sleds.contains(&id),
        }
    }

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

    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        // Span the full representable range of `DateTime<Utc>` so
        // round-trip tests exercise far-past and far-future times,
        // not just a hand-picked window that drifts out of date.
        let min = DateTime::<Utc>::MIN_UTC.timestamp();
        let max = DateTime::<Utc>::MAX_UTC.timestamp();
        (min..=max).prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap())
    }

    impl Arbitrary for BundleTimeRange {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            (prop::option::of(arb_datetime()), prop::option::of(arb_datetime()))
                .prop_map(|(start, end)| BundleTimeRange { start, end })
                .boxed()
        }
    }

    impl Arbitrary for BundleDataSelection {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            (
                prop::collection::vec(any::<BundleData>(), 0..=5),
                prop::option::of(any::<BundleTimeRange>()),
            )
                .prop_map(|(data, time_range)| {
                    let mut sel: BundleDataSelection =
                        data.into_iter().collect();
                    sel.set_time_range(time_range);
                    sel
                })
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
