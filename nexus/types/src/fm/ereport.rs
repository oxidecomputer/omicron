// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ereports

use crate::inventory::SpType;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

pub use ereport_types::{Ena, EreportId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ereport {
    #[serde(flatten)]
    pub data: EreportData,
    #[serde(flatten)]
    pub reporter: Reporter,
}

impl Ereport {
    pub fn id(&self) -> &EreportId {
        &self.data.id
    }
}

impl core::ops::Deref for Ereport {
    type Target = EreportData;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl iddqd::IdOrdItem for Ereport {
    type Key<'a> = &'a EreportId;
    fn key(&self) -> Self::Key<'_> {
        self.id()
    }

    iddqd::id_upcast!();
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EreportData {
    #[serde(flatten)]
    pub id: EreportId,
    pub time_collected: DateTime<Utc>,
    pub collector_id: OmicronZoneUuid,
    pub serial_number: Option<String>,
    pub part_number: Option<String>,
    pub class: Option<String>,
    #[serde(flatten)]
    pub report: serde_json::Value,
}

impl EreportData {
    /// Interpret a service processor ereport from a raw JSON blobule, plus the
    /// restart ID and collection metadata.
    ///
    /// This conversion is lossy; if some information is not present in the raw
    /// ereport JSON, such as the SP's VPD identity, we log a warning, rather
    /// than returning an error, and leave those fields empty in the returned
    /// `EreportData`. This is because, if we receive an ereport that is
    /// incomplete, we would still like to preserve whatever information *is*
    /// present, rather than throwing the whole thing away. Thus, this function
    /// also takes a `slog::Logger` for logging warnings if some expected fields
    /// are not present or malformed.
    pub fn from_sp_ereport(
        log: &slog::Logger,
        restart_id: EreporterRestartUuid,
        ereport: ereport_types::Ereport,
        time_collected: DateTime<Utc>,
        collector_id: OmicronZoneUuid,
    ) -> Self {
        const MISSING_VPD: &str = " (perhaps the SP doesn't know its own VPD?)";
        let part_number = get_sp_metadata_string(
            "baseboard_part_number",
            &ereport,
            &restart_id,
            &log,
            MISSING_VPD,
        );
        let serial_number = get_sp_metadata_string(
            "baseboard_serial_number",
            &ereport,
            &restart_id,
            &log,
            MISSING_VPD,
        );
        let ena = ereport.ena;
        let class = ereport
            .data
            // "k" (for "kind") is used as an abbreviation of
            // "class" to save 4 bytes of ereport.
            .get("k")
            .or_else(|| ereport.data.get("class"));
        let class = match (class, ereport.data.get("lost")) {
            (Some(serde_json::Value::String(class)), _) => {
                Some(class.to_string())
            }
            (Some(v), _) => {
                slog::warn!(
                    &log,
                    "malformed ereport: value for 'k'/'class' \
                     should be a string, but found: {v:?}";
                    "ena" => ?ena,
                    "restart_id" => ?restart_id,
                );
                None
            }
            // This is a loss record! I know this!
            (None, Some(serde_json::Value::Null)) => {
                Some("ereport.data_loss.possible".to_string())
            }
            (None, Some(serde_json::Value::Number(_))) => {
                Some("ereport.data_loss.certain".to_string())
            }
            (None, _) => {
                slog::warn!(
                    &log,
                    "ereport missing 'k'/'class' key";
                    "ena" => ?ena,
                    "restart_id" => ?restart_id,
                );
                None
            }
        };

        EreportData {
            id: EreportId { restart_id, ena },
            time_collected,
            collector_id,
            part_number,
            serial_number,
            class,
            report: serde_json::Value::Object(ereport.data),
        }
    }
}

/// Describes the source of an ereport.
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Hash,
)]
#[serde(tag = "reporter")]
pub enum Reporter {
    Sp { sp_type: SpType, slot: u16 },
    HostOs { sled: SledUuid, slot: Option<u16> },
}

impl Reporter {
    /// Returns the slot type occupied by this reporter.
    ///
    /// N.B. that while the return type of this method is [`SpType`], this is
    /// technically a bit of a misnomer, as it's really the type of the physical
    /// slot in the rack --- `SpType::Sled` is returned for both sled SP *and*
    /// sled host OS reporters.
    pub fn slot_type(&self) -> SpType {
        match self {
            Self::Sp { sp_type, .. } => *sp_type,
            Self::HostOs { .. } => SpType::Sled,
        }
    }
}

impl fmt::Display for Reporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display format based on:
        // https://rfd.shared.oxide.computer/rfd/200#_labeling
        match self {
            Self::Sp { sp_type: sp_type @ SpType::Sled, slot } => {
                write!(f, "{sp_type} {slot:<2} (SP)")
            }
            Self::HostOs { sled, slot: Some(slot) } => {
                write!(f, "{} {slot:<2} (OS) ({sled})", SpType::Sled)
            }
            Self::HostOs { sled, slot: None } => {
                write!(f, "{} ?? (OS) ({sled})", SpType::Sled)
            }
            Self::Sp { sp_type, slot } => {
                write!(f, "{sp_type} {slot}")
            }
        }
    }
}

/// Attempt to extract a VPD metadata from an SP ereport, logging a warning if
/// it's missing. We still want to keep such ereports, as the error condition
/// could be that the SP couldn't determine the metadata field, but it's
/// uncomfortable, so we ought to complain about it.
fn get_sp_metadata_string(
    key: &str,
    ereport: &ereport_types::Ereport,
    restart_id: &EreporterRestartUuid,
    log: &slog::Logger,
    extra_context: &'static str,
) -> Option<String> {
    match ereport.data.get(key) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(v) => {
            slog::warn!(
                &log,
                "malformed ereport: value for '{key}' should be a string, \
                 but found: {v:?}";
                "ena" => ?ereport.ena,
                "restart_id" => ?restart_id,
            );
            None
        }
        None => {
            slog::warn!(
                &log,
                "ereport missing '{key}'{extra_context}";
                "ena" => ?ereport.ena,
                "restart_id" => ?restart_id,
            );
            None
        }
    }
}

/// A set of filters for fetching ereports.
///
/// Construct using [`EreportFilters::new`] and the builder methods:
///
/// ```
/// # use nexus_types::fm::ereport::EreportFilters;
/// let filters = EreportFilters::new()
///     .with_start_time(chrono::Utc::now() - chrono::Days::new(7))
///     .expect("no end time set")
///     .with_serials(["BRM6900420"])
///     .with_classes(["hw.pwr.*"]);
/// ```
///
/// Note: JSON deserialization validates the start_time/end_time constraints
/// using `TryFrom<EreportFiltersUnvalidated>`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "EreportFiltersUnvalidated")]
pub struct EreportFilters {
    /// If present, include only ereports that were collected at the specified
    /// timestamp or later.
    ///
    /// If `end_time` is also present, this value *must* be at or before
    /// `end_time`. This invariant is enforced by [`Self::with_start_time`].
    start_time: Option<DateTime<Utc>>,
    /// If present, include only ereports that were collected at the specified
    /// timestamp or before.
    ///
    /// If `start_time` is also present, this value *must* be at or after
    /// `start_time`. This invariant is enforced by [`Self::with_end_time`].
    end_time: Option<DateTime<Utc>>,
    /// If this list is non-empty, include only ereports that were reported by
    /// systems with the provided serial numbers.
    only_serials: Vec<String>,
    /// If this list is non-empty, include only ereports with the provided class
    /// strings.
    // TODO(eliza): globbing could be nice to add here eventually...
    only_classes: Vec<String>,
}

/// Private deserialization helper for [`EreportFilters`] that validates the
/// `start_time <= end_time` invariant.
#[derive(Deserialize)]
struct EreportFiltersUnvalidated {
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    only_serials: Vec<String>,
    only_classes: Vec<String>,
}

impl TryFrom<EreportFiltersUnvalidated> for EreportFilters {
    type Error = Error;

    fn try_from(
        unvalidated: EreportFiltersUnvalidated,
    ) -> Result<Self, Self::Error> {
        let mut f = Self::new();
        if let Some(t) = unvalidated.start_time {
            f = f.with_start_time(t)?;
        }
        if let Some(t) = unvalidated.end_time {
            f = f.with_end_time(t)?;
        }
        f = f
            .with_serials(unvalidated.only_serials)
            .with_classes(unvalidated.only_classes);
        Ok(f)
    }
}

/// Displayer for pretty-printing [`EreportFilters`].
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplayEreportFilters<'a> {
    filters: &'a EreportFilters,
}

impl EreportFilters {
    /// Creates an empty set of filters (no filtering).
    pub fn new() -> Self {
        Self::default()
    }

    /// Includes only ereports collected at or after `time`.
    ///
    /// Returns an error if `time` is after a previously set end time.
    pub fn with_start_time(
        mut self,
        time: DateTime<Utc>,
    ) -> Result<Self, Error> {
        if let Some(end) = self.end_time {
            if time > end {
                return Err(Error::invalid_request(
                    "start time must be before end time",
                ));
            }
        }
        self.start_time = Some(time);
        Ok(self)
    }

    /// Includes only ereports collected at or before `time`.
    ///
    /// Returns an error if `time` is before a previously set start time.
    pub fn with_end_time(mut self, time: DateTime<Utc>) -> Result<Self, Error> {
        if let Some(start) = self.start_time {
            if start > time {
                return Err(Error::invalid_request(
                    "start time must be before end time",
                ));
            }
        }
        self.end_time = Some(time);
        Ok(self)
    }

    /// Adds serial numbers to the inclusion filter.
    ///
    /// When one or more serials are present, only ereports reported by
    /// systems with those serial numbers are included.
    pub fn with_serials(
        mut self,
        serials: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.only_serials.extend(serials.into_iter().map(Into::into));
        self
    }

    /// Adds ereport classes to the inclusion filter.
    ///
    /// When one or more classes are present, only ereports with those
    /// class strings are included.
    pub fn with_classes(
        mut self,
        classes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.only_classes.extend(classes.into_iter().map(Into::into));
        self
    }

    /// Returns the start time filter, if set.
    pub fn start_time(&self) -> Option<DateTime<Utc>> {
        self.start_time
    }

    /// Returns the end time filter, if set.
    pub fn end_time(&self) -> Option<DateTime<Utc>> {
        self.end_time
    }

    /// Returns the serial number inclusion filter.
    pub fn only_serials(&self) -> &[String] {
        &self.only_serials
    }

    /// Returns the ereport class inclusion filter.
    pub fn only_classes(&self) -> &[String] {
        &self.only_classes
    }

    pub fn display(&self) -> DisplayEreportFilters<'_> {
        DisplayEreportFilters { filters: self }
    }
}

impl fmt::Display for DisplayEreportFilters<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use itertools::Itertools;

        let filters = self.filters;

        // Writes a semicolon-separated part to the formatter, tracking whether
        // we've written anything yet.
        let mut empty = true;
        let mut fmt_part =
            |f: &mut fmt::Formatter, args: fmt::Arguments| -> fmt::Result {
                if !empty {
                    write!(f, "; ")?;
                }
                empty = false;
                f.write_fmt(args)
            };

        if let Some(start) = filters.start_time() {
            fmt_part(f, format_args!("start: {start}"))?;
        }
        if let Some(end) = filters.end_time() {
            fmt_part(f, format_args!("end: {end}"))?;
        }
        if !filters.only_serials().is_empty() {
            fmt_part(
                f,
                format_args!(
                    "serials: {}",
                    filters.only_serials().iter().format(", ")
                ),
            )?;
        }
        if !filters.only_classes().is_empty() {
            fmt_part(
                f,
                format_args!(
                    "classes: {}",
                    filters.only_classes().iter().format(", ")
                ),
            )?;
        }

        // If no filters are set, display "none" rather than empty output.
        if empty {
            write!(f, "none")?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use proptest::prelude::*;

    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        // Generate timestamps in a reasonable range (2020-2030).
        (1577836800i64..1893456000i64)
            .prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap())
    }

    impl Arbitrary for EreportFilters {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            (
                prop::option::of(arb_datetime()),
                prop::option::of(arb_datetime()),
                prop::collection::vec(".*", 0..=3),
                prop::collection::vec(".*", 0..=3),
            )
                .prop_map(|(time_a, time_b, only_serials, only_classes)| {
                    // Ensure start <= end when both are present.
                    let (start_time, end_time) = match (time_a, time_b) {
                        (Some(a), Some(b)) if a > b => (Some(b), Some(a)),
                        other => other,
                    };
                    let mut filters = EreportFilters::new();
                    if let Some(t) = start_time {
                        filters = filters
                            .with_start_time(t)
                            .expect("no end time set yet");
                    }
                    if let Some(t) = end_time {
                        filters = filters
                            .with_end_time(t)
                            .expect("start <= end by construction");
                    }
                    filters
                        .with_serials(only_serials)
                        .with_classes(only_classes)
                })
                .boxed()
        }
    }
}
