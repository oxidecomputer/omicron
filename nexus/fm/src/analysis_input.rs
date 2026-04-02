// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inputs to fault management analysis.

use iddqd::IdOrdMap;
use nexus_types::fm::{self, ClosedCaseReport, Sitrep, SitrepVersion};
use nexus_types::inventory;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

pub use nexus_types::fm::AnalysisInputReport as Report;

/// A complete set of inputs to a fault management analysis phase.
///
/// This struct bundles together all the inputs to analysis, including:
///
/// - The [parent sitrep](Input::parent_sitrep)
/// - The current [inventory collection](Input::inventory)
/// - Any [new ereports](Input::new_ereports) which were received since when
///   the parent sitrep was produced
/// - The set of [cases](Input::cases) which must be copied forwards into
///   the next sitrep
///
/// This type represents the outputs of the analysis preparation phase. Once it
/// is constructed, the inputs are immutable and cannot be modified. To
/// construct a new `Input` as part of a preparaation phase, use
/// [`Input::builder`].
pub struct Input {
    parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
    inv: Arc<inventory::Collection>,
    /// Ereports which are new and should be input to analysis in the next
    /// sitrep.
    new_ereports: IdOrdMap<fm::Ereport>,
    open_cases: IdOrdMap<fm::Case>,
    closed_cases_copied_forward: IdOrdMap<fm::Case>,
}

impl Input {
    pub fn parent_sitrep(&self) -> Option<&Sitrep> {
        self.parent_sitrep.as_ref().map(|s| &s.1)
    }

    pub fn inventory(&self) -> &inventory::Collection {
        &self.inv
    }

    pub fn new_ereports(&self) -> &IdOrdMap<fm::Ereport> {
        &self.new_ereports
    }

    pub fn cases(&self) -> &IdOrdMap<fm::Case> {
        &self.open_cases
    }

    pub(crate) fn closed_cases_copied_forward(&self) -> &IdOrdMap<fm::Case> {
        &self.closed_cases_copied_forward
    }

    /// Returns a [`Builder`] for constructing a new `Input` from the provided
    /// `parent_sitrep` and inventory collection.
    pub fn builder(
        parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
        inv: Arc<inventory::Collection>,
    ) -> Builder {
        Builder {
            parent_sitrep,
            inv,
            new_ereports: IdOrdMap::default(),
            unmarked_seen_ereports: BTreeSet::default(),
        }
    }
}

#[must_use]
pub struct Builder {
    parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
    inv: Arc<inventory::Collection>,
    /// Ereports which are new and should be input to analysis in the next
    /// sitrep.
    new_ereports: IdOrdMap<fm::Ereport>,

    /// The IDs of any ereports which have been included in the parent sitrep,
    /// but which have *not* yet been marked as seen in the database.
    ///
    /// These must be tracked in order to determine which closed cases must be
    /// copied forwards due to containing unmarked ereports.
    unmarked_seen_ereports: BTreeSet<fm::EreportId>,
}

impl Builder {
    /// Adds a set of ereports which have not been marked as "seen" in the
    /// database to the inputs under construction.
    ///
    /// This will filter out any ereports which are present in the parent sitrep
    /// and have not yet been marked in the database, and then add any ereports
    /// which remain to the set of ereports which are actually new and should be
    /// included in the inputs to the next sitrep.
    pub fn add_unmarked_ereports(
        &mut self,
        ereports: impl IntoIterator<Item = fm::Ereport>,
    ) {
        let parent_sitrep = self.parent_sitrep.as_ref().map(|s| &s.1);
        self.new_ereports.extend(ereports.into_iter().filter_map(|ereport| {
            if let Some(sitrep) = parent_sitrep {
                let id = ereport.id();
                if sitrep.ereports_by_id.contains_key(&id) {
                    self.unmarked_seen_ereports.insert(*id);
                    return None;
                }
            }

            Some(ereport)
        }))
    }

    pub fn num_ereports(&self) -> usize {
        self.new_ereports.len()
    }

    /// Finish constructing the [`Input`] and return it, along with a [`Report`]
    /// that provides a human-readable summary of how the inputs were
    /// constructed.
    pub fn build(self) -> (Input, Report) {
        let parent_sitrep = self.parent_sitrep.as_ref().map(|s| &s.1);
        let (parent_sitrep_id, parent_inv_id) = match parent_sitrep {
            Some(sitrep) => {
                let id = sitrep.id();
                let inv_id = sitrep.metadata.inv_collection_id;
                (Some(id), Some(inv_id))
            }
            None => (None, None),
        };

        let mut report = Report {
            parent_sitrep_id,
            parent_inv_id,
            inv_id: self.inv.id,
            new_ereport_ids: self
                .new_ereports
                .iter()
                .map(|e| *e.id())
                .collect(),
            open_cases: BTreeMap::new(),
            closed_cases_copied_forward: BTreeMap::new(),
        };

        // Determine which cases must be copied forwards into the next sitrep.
        // Cases from the parent sitrep should be copied forwards if:
        // - The case is still open
        let mut open_cases = IdOrdMap::new();
        // - The case has been closed, but it contains an ereport which has not
        //   yet been marked as "seen" in the database.
        let mut closed_cases_copied_forward = IdOrdMap::new();
        for case in parent_sitrep.iter().flat_map(|s| s.cases.iter()) {
            if case.is_open() {
                report.open_cases.insert(case.id, case.metadata.clone());
                open_cases.insert_unique(case.clone());
            } else {
                let unmarked_ereports = case
                    .ereports
                    .iter()
                    .filter_map(|ereport| {
                        let id = ereport.ereport_id();
                        if self.unmarked_seen_ereports.contains(&id) {
                            Some(*id)
                        } else {
                            None
                        }
                    })
                    .collect::<BTreeSet<_>>();
                if !unmarked_ereports.is_empty() {
                    report.closed_cases_copied_forward.insert(
                        case.id,
                        ClosedCaseReport {
                            metadata: case.metadata.clone(),
                            unmarked_ereports,
                        },
                    );
                    closed_cases_copied_forward.insert_unique(case.clone());
                }
            }
        }
        let input = Input {
            parent_sitrep: self.parent_sitrep.clone(),
            inv: self.inv.clone(),
            new_ereports: self.new_ereports,
            open_cases,
            closed_cases_copied_forward,
        };

        (input, report)
    }
}
