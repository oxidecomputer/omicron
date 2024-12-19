// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blippy::Note;
use core::fmt;
use nexus_types::deployment::Blueprint;

#[derive(Debug, Clone, Copy)]
pub enum BlippyReportSortKey {
    Kind,
    Severity,
}

#[derive(Debug)]
pub struct BlippyReport<'a> {
    blueprint: &'a Blueprint,
    notes: Vec<Note<'a>>,
    sort_key: BlippyReportSortKey,
}

impl<'a> BlippyReport<'a> {
    pub(crate) fn new(
        blueprint: &'a Blueprint,
        notes: Vec<Note<'a>>,
        sort_key: BlippyReportSortKey,
    ) -> Self {
        let mut slf = Self { blueprint, notes, sort_key };
        slf.sort_notes_by_key(sort_key);
        slf
    }

    pub fn sort_notes_by_key(&mut self, key: BlippyReportSortKey) {
        match key {
            BlippyReportSortKey::Kind => {
                self.notes.sort_unstable_by(|a, b| {
                    let a = (&a.kind, &a.severity);
                    let b = (&b.kind, &b.severity);
                    a.cmp(&b)
                });
            }
            BlippyReportSortKey::Severity => {
                self.notes.sort_unstable_by(|a, b| {
                    let a = (&a.severity, &a.kind);
                    let b = (&b.severity, &b.kind);
                    a.cmp(&b)
                });
            }
        }
        self.sort_key = key;
    }

    pub fn blueprint(&self) -> &'a Blueprint {
        self.blueprint
    }

    pub fn notes(&self) -> &[Note<'a>] {
        &self.notes
    }

    pub fn display(&self) -> BlippyReportDisplay<'_> {
        BlippyReportDisplay { report: self }
    }
}

#[derive(Debug)]
pub struct BlippyReportDisplay<'a> {
    report: &'a BlippyReport<'a>,
}

impl fmt::Display for BlippyReportDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pluralize = if self.report.notes.len() == 1 { "" } else { "s" };
        writeln!(
            f,
            "blippy report for blueprint {}: {} note{pluralize}",
            self.report.blueprint.id,
            self.report.notes.len(),
        )?;
        for note in self.report.notes() {
            writeln!(f, "  {}", note.display(self.report.sort_key))?;
        }
        Ok(())
    }
}
