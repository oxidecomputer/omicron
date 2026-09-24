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
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt;

/// Describes the category of support bundle data.
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    clap::ValueEnum,
)]
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

/// Inclusive time bound applied bundle-wide to time-bounded categories
/// (currently host-info logs and ereports).
///
/// Ereports have a single timestamp, and are filtered exactly.
/// Zone logs are created within an interval of time, and the time bound
/// includes a log file if it overlaps with this interval.
///
/// `None` on either side means unbounded on that side. When both bounds are
/// set, `start <= end` holds: construction and deserialization both reject
/// inverted ranges, and persistence backstops the invariant with a database
/// CHECK constraint.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedBundleTimeRange")]
pub struct BundleTimeRange {
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

impl BundleTimeRange {
    /// Creates a range, rejecting `start > end` when both bounds are set.
    pub fn new(
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Self> {
        if let (Some(start), Some(end)) = (start, end) {
            anyhow::ensure!(
                start <= end,
                "time range start ({start}) must not be later than \
                 its end ({end})"
            );
        }
        Ok(Self { start, end })
    }

    pub fn start(&self) -> Option<DateTime<Utc>> {
        self.start
    }

    pub fn end(&self) -> Option<DateTime<Utc>> {
        self.end
    }

    /// Returns `true` when at least one bound is set.
    pub fn has_bounds(&self) -> bool {
        self.start.is_some() || self.end.is_some()
    }
}

/// Mirror of [`BundleTimeRange`] that deserialization goes through so
/// inverted ranges are rejected there as well.
#[derive(Deserialize)]
struct UncheckedBundleTimeRange {
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

impl TryFrom<UncheckedBundleTimeRange> for BundleTimeRange {
    type Error = String;

    fn try_from(value: UncheckedBundleTimeRange) -> Result<Self, String> {
        Self::new(value.start, value.end).map_err(|e| e.to_string())
    }
}

/// A collection of bundle data specifications.
///
/// This wrapper ensures that categories and data always match - you can't
/// insert (BundleDataCategory::Reconfigurator, BundleData::SpDumps)
/// because each BundleData determines its own category.
///
/// `time_range` bounds every time-bounded category's collection (host-info
/// logs and ereports); the default range is unbounded on both sides and
/// applies no filter.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleDataSelection {
    // Ordered so iteration (and therefore display output) is deterministic.
    data: BTreeMap<BundleDataCategory, BundleData>,
    #[serde(default)]
    time_range: BundleTimeRange,
}

impl BundleDataSelection {
    /// The default collection lookback, filled in for a selection that does
    /// not specify a start bound for its time range.
    ///
    /// This is applied (via [`Self::ensure_start_bound`]) where a selection
    /// enters the system: when Nexus persists it at bundle creation, or by
    /// omdb immediately before an unpersisted collection. Collection itself
    /// uses the selection exactly as given.
    pub const DEFAULT_LOOKBACK: chrono::Days = chrono::Days::new(7);

    /// Creates an empty selection with no data categories.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a selection containing all default data categories
    /// (i.e. "collect everything") with no explicit time range.
    pub fn all() -> Self {
        Self::new()
            .with_reconfigurator()
            .with_all_sleds()
            .with_sled_cubby_info()
            .with_sp_dumps()
            .with_ereports(EreportFilters::new())
    }

    /// Ensures the bundle-wide time range has a start bound.
    ///
    /// When the range has no start bound, the start is filled in `lookback`
    /// before the range's end bound, or before `now` when the range has no
    /// end bound either. Existing bounds are preserved.
    pub fn ensure_start_bound(
        &mut self,
        now: DateTime<Utc>,
        lookback: chrono::Days,
    ) {
        let range = &mut self.time_range;
        if range.start.is_none() {
            let anchor = range.end.unwrap_or(now);
            // Checked subtraction: an end bound near the minimum
            // representable time would otherwise underflow.
            range.start = Some(
                anchor
                    .checked_sub_days(lookback)
                    .unwrap_or(DateTime::<Utc>::MIN_UTC),
            );
        }
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

    /// Sets the bundle-wide time range. Affects every time-bounded category
    /// (host-info logs and ereports) at collection time.
    pub fn with_time_range(mut self, range: BundleTimeRange) -> Self {
        self.time_range = range;
        self
    }

    /// Inserts a [`BundleData`] value. If a value with the same category
    /// already exists, the last write wins.
    pub fn insert(&mut self, bundle_data: BundleData) {
        self.data.insert(bundle_data.category(), bundle_data);
    }

    /// Sets the bundle-wide time range in place (used by code paths that
    /// build the selection incrementally, e.g. database read paths).
    pub fn set_time_range(&mut self, range: BundleTimeRange) {
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

    /// Returns the bundle-wide time range. Applies to every time-bounded
    /// category at collection time; the default range is unbounded and
    /// applies no filter.
    pub fn time_range(&self) -> &BundleTimeRange {
        &self.time_range
    }
}

impl IntoIterator for BundleDataSelection {
    type Item = BundleData;
    type IntoIter =
        std::collections::btree_map::IntoValues<BundleDataCategory, BundleData>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_values()
    }
}

impl<'a> IntoIterator for &'a BundleDataSelection {
    type Item = &'a BundleData;
    type IntoIter =
        std::collections::btree_map::Values<'a, BundleDataCategory, BundleData>;

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
        let range = self.selection.time_range();
        if range.has_bounds() {
            if !self.selection.data.is_empty() {
                writeln!(f)?;
            }
            let bound = |b: &Option<DateTime<Utc>>| match b {
                Some(ts) => {
                    ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                }
                None => "unbounded".to_string(),
            };
            write!(
                f,
                "{:>indent$}- time_range(start: {}, end: {})",
                "",
                bound(&range.start),
                bound(&range.end),
            )?;
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
            // Generated bounds are ordered (start <= end when both are
            // set) because `BundleTimeRange::new` rejects inverted ranges.
            // The timestamps span chrono's full range, which exceeds what
            // CockroachDB's TIMESTAMPTZ can store: fine for serde tests,
            // unsuitable for persistence tests.
            (prop::option::of(arb_datetime()), prop::option::of(arb_datetime()))
                .prop_map(|(a, b)| {
                    let (start, end) = match (a, b) {
                        (Some(a), Some(b)) => (Some(a.min(b)), Some(a.max(b))),
                        (start, end) => (start, end),
                    };
                    BundleTimeRange::new(start, end)
                        .expect("bounds are ordered")
                })
                .boxed()
        }
    }

    impl Arbitrary for BundleDataSelection {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            (
                prop::collection::vec(any::<BundleData>(), 0..=5),
                any::<BundleTimeRange>(),
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

    #[test]
    fn ensure_start_bound_fills_only_missing_start() {
        let ts = |secs| DateTime::<Utc>::from_timestamp(secs, 0).unwrap();
        const WEEK_SECS: i64 = 7 * 24 * 60 * 60;
        let now = ts(10 * WEEK_SECS);
        let lookback = chrono::Days::new(7);

        // The default (unbounded) range: the lookback anchors to `now`.
        let mut selection = BundleDataSelection::new();
        selection.ensure_start_bound(now, lookback);
        let range = selection.time_range();
        assert_eq!(range.start(), Some(ts(9 * WEEK_SECS)));
        assert_eq!(range.end(), None);

        // With only an end bound, the lookback anchors to it instead, and
        // it is preserved.
        let mut selection = BundleDataSelection::new().with_time_range(
            BundleTimeRange::new(None, Some(ts(5 * WEEK_SECS))).unwrap(),
        );
        selection.ensure_start_bound(now, lookback);
        let range = selection.time_range();
        assert_eq!(range.start(), Some(ts(4 * WEEK_SECS)));
        assert_eq!(range.end(), Some(ts(5 * WEEK_SECS)));

        // An existing start bound is left alone.
        let mut selection = BundleDataSelection::new()
            .with_time_range(BundleTimeRange::new(Some(ts(50)), None).unwrap());
        selection.ensure_start_bound(now, lookback);
        let range = selection.time_range();
        assert_eq!(range.start(), Some(ts(50)));
        assert_eq!(range.end(), None);

        // An end bound at the minimum representable time saturates rather
        // than underflowing.
        let mut selection = BundleDataSelection::new().with_time_range(
            BundleTimeRange::new(None, Some(DateTime::<Utc>::MIN_UTC)).unwrap(),
        );
        selection.ensure_start_bound(now, lookback);
        let range = selection.time_range();
        assert_eq!(range.start(), Some(DateTime::<Utc>::MIN_UTC));
        assert_eq!(range.end(), Some(DateTime::<Utc>::MIN_UTC));
    }

    #[test]
    fn bundle_time_range_rejects_inverted_bounds() {
        let ts = |secs| DateTime::<Utc>::from_timestamp(secs, 0).unwrap();

        assert!(BundleTimeRange::new(Some(ts(200)), Some(ts(100))).is_err());
        assert!(BundleTimeRange::new(Some(ts(100)), Some(ts(100))).is_ok());
        assert!(BundleTimeRange::new(Some(ts(100)), Some(ts(200))).is_ok());
        assert!(BundleTimeRange::new(None, None).is_ok());

        // Deserialization rejects inverted ranges too.
        let inverted =
            r#"{"start":"2024-01-02T00:00:00Z","end":"2024-01-01T00:00:00Z"}"#;
        assert!(serde_json::from_str::<BundleTimeRange>(inverted).is_err());
        let ordered =
            r#"{"start":"2024-01-01T00:00:00Z","end":"2024-01-02T00:00:00Z"}"#;
        assert!(serde_json::from_str::<BundleTimeRange>(ordered).is_ok());
    }
}
