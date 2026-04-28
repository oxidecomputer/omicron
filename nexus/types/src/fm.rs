// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management types.
//!
//! Of particular importance is the [`Sitrep`], which is the top-level data
//! structure containing fault management state.

pub mod analysis_reports;
pub mod ereport;
pub use ereport::{Ereport, EreportId};
pub mod case;
pub use case::Case;

use case::AlertRequest;
use chrono::{DateTime, Utc};
use iddqd::IdOrdMap;
use omicron_uuid_kinds::{
    CaseUuid, CollectionUuid, OmicronZoneUuid, SitrepUuid,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A fault management situation report, or _sitrep_.
///
/// The sitrep is a data structure that represents a snapshot of the state of
/// the system as understood by the control plane's fault management subsystem.
/// At any point in time, a single sitrep is considered the "current" sitrep.
/// Each sitrep records a _parent sitrep ID_, which indicates the sitrep that
/// was current at the time that the sitrep was created.
/// A sitrep may only be made current if its parent is the current sitrep.
/// This ensures that there is a sequentially consistent history of sitreps.
/// The fault management subsystem only considers data from the current sitrep
/// when making decisions and diagnoses.
///
/// The sitrep, how it is represented in the database, and how the fault
/// management subsystem creates and interacts with sitreps, is described in
/// detail in [RFD 603](https://rfd.shared.oxide.computer/rfd/0603).
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Sitrep {
    /// Metadata describing this sitrep, when it was created, its parent sitrep
    /// ID, and which Nexus produced it.
    pub metadata: SitrepMetadata,
    pub cases: IdOrdMap<Case>,
    /// A map of all ereports associated with cases in this sitrep.
    pub ereports_by_id: IdOrdMap<Arc<Ereport>>,
    //
    // NOTE FOR FUTURE GENERATIONS: If you add more database tables whose
    // records are top-level children of a sitrep (i.e., like cases), please
    // make sure to update the tests in `nexus_db_queries::db::datastore::fm` to
    // also include those records. In particular, make sure to update the
    // `assert_sitreps_eq` function to also assert that your new records are
    // contained in both sitreps. Also, the tests for sitrep deletion and for
    // roundtripping sitreps through the database should also
    // create/delete/assert any new records added.
    //
    // Thank you for your cooperation!
    //
}

impl Sitrep {
    pub fn id(&self) -> SitrepUuid {
        self.metadata.id
    }

    pub fn parent_id(&self) -> Option<SitrepUuid> {
        self.metadata.parent_sitrep_id
    }

    /// Iterate over all the open cases in this sitrep.
    ///
    /// All cases returned by this iterator will be copied forward into any
    /// child sitreps that descend from this one.
    pub fn open_cases(&self) -> impl Iterator<Item = &Case> + '_ {
        self.cases.iter().filter(|c| c.is_open())
    }

    /// Iterate over all alerts requested by cases in this sitrep.
    pub fn alerts_requested(
        &self,
    ) -> impl Iterator<Item = (CaseUuid, &'_ AlertRequest)> + '_ {
        self.cases.iter().flat_map(|case| {
            let case_id = *case.id();
            case.alerts_requested.iter().map(move |alert| (case_id, alert))
        })
    }

    /// Iterate over all support bundles requested by cases in this sitrep.
    pub fn support_bundles_requested(
        &self,
    ) -> impl Iterator<Item = (&Case, &case::SupportBundleRequest)> {
        self.cases.iter().flat_map(|case| {
            case.support_bundles_requested.iter().map(move |req| (case, req))
        })
    }

    /// Returns a value which implements `PartialEq<Sitrep>` and can be used to
    /// test whether another sitrep represents the same state of the system as
    /// this sitrep.
    ///
    /// This is distinct than comparing two `Sitrep`s directly using their
    /// `PartialEq` implementation, as that comparison will test whether *all
    /// fields of the sitreps* are exactly the same, while this comparison type
    /// will compare equal to *any other sitrep which represents an identical
    /// system state*. Two sitreps with different metadata (ID, creator ID,
    /// timestamp, etc) can still represent identical states.
    pub fn compare_state(&self) -> SitrepStateComparison<'_> {
        SitrepStateComparison { sitrep: self }
    }
}

/// Metadata describing a sitrep.
///
/// This corresponds to the records stored in the `fm_sitrep` database table.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SitrepMetadata {
    /// The ID of this sitrep.
    pub id: SitrepUuid,

    /// The ID of the parent sitrep.
    ///
    /// A sitrep's _parent_ is the sitrep that was current when the planning
    /// phase that produced that sitrep ran. The parent sitrep is a planning
    /// input that produced this sitrep.
    ///
    /// The parent sitrep ID is optional, because this sitrep _may_ be the first
    /// sitrep ever generated by the system. However, once a current sitrep has
    /// been set, no subsequent sitrep should be created without a parent.
    pub parent_sitrep_id: Option<SitrepUuid>,

    /// The ID of the inventory collection that was used as planning input to
    /// this sitrep.
    ///
    /// When generating a new sitrep, the fault manager should ensure that the
    /// inventory collection it uses as input is at least as new as the parent
    /// sitrep's inventory collection.
    pub inv_collection_id: CollectionUuid,

    /// The minimum start time for an inventory collection to be considered
    /// "newer" than the one included in this sitrep.
    ///
    /// This is used when beginning FM analysis in order to ensure that a
    /// potential input inventory collection is not older than the one used in
    /// the analysis step that produced this sitrep.
    pub next_inv_min_time_started: DateTime<Utc>,

    /// The Omicron zone UUID of the Nexus that generated this sitrep.
    ///
    /// This is intended for debugging purposes.
    pub creator_id: OmicronZoneUuid,

    /// A human-readable (but mechanically generated) string describing the
    /// reason(s) this sitrep was created.
    ///
    /// This is intended for debugging purposes.
    pub comment: String,

    /// The time at which this sitrep was created.
    pub time_created: DateTime<Utc>,
}

pub struct SitrepStateComparison<'sitrep> {
    sitrep: &'sitrep Sitrep,
}

impl PartialEq<Sitrep> for SitrepStateComparison<'_> {
    fn eq(&self, other: &Sitrep) -> bool {
        let Sitrep {
            // Ignore the metadata, because two sitreps with different IDs,
            // creator IDs, and timestamps can represent the same state.
            metadata: _,
            // Ignore the map of ereports by ID, as it's just an additional
            // index of ereports that are already contained in the `cases` map.
            // The state we want to compare is just whether or not the same
            // ereports are assigned to the same cases, and it is unnecessary to
            // also test that the by-ID indices have the same contents, since
            // they always will if the two sitreps' cases contain the same
            // ereports.
            ereports_by_id: _,
            cases,
        } = self.sitrep;

        cases == &other.cases
        // TO FUTURE GENERATIONS: if other top-level sitrep objects which
        // represent the state of the system are added, compare them here!
    }
}

impl PartialEq for SitrepStateComparison<'_> {
    fn eq(&self, other: &Self) -> bool {
        self == other.sitrep
    }
}

/// An entry in the sitrep version history.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SitrepVersion {
    pub id: SitrepUuid,
    pub version: u32,
    pub time_made_current: DateTime<Utc>,
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DiagnosisEngineKind {
    PowerShelf,
}
