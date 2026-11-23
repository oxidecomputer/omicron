// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::slippy::Note;
use core::fmt;
use nexus_types::fm::Sitrep;

#[derive(Debug, Clone, Copy)]
pub enum SlippyReportSortKey {
    Kind,
    Severity,
}

#[derive(Debug)]
pub struct SlippyReport<'a> {
    sitrep: &'a Sitrep,
    notes: Vec<Note>,
    sort_key: SlippyReportSortKey,
}

impl<'a> SlippyReport<'a> {
    pub(crate) fn new(
        sitrep: &'a Sitrep,
        notes: Vec<Note>,
        sort_key: SlippyReportSortKey,
    ) -> Self {
        let mut slf = Self { sitrep, notes, sort_key };
        slf.sort_notes_by_key(sort_key);
        slf
    }

    pub fn sort_notes_by_key(&mut self, key: SlippyReportSortKey) {
        match key {
            SlippyReportSortKey::Kind => {
                self.notes.sort_unstable_by(|a, b| {
                    let a = (&a.kind, &a.severity);
                    let b = (&b.kind, &b.severity);
                    a.cmp(&b)
                });
            }
            SlippyReportSortKey::Severity => {
                self.notes.sort_unstable_by(|a, b| {
                    let a = (&a.severity, &a.kind);
                    let b = (&b.severity, &b.kind);
                    a.cmp(&b)
                });
            }
        }
        self.sort_key = key;
    }

    pub fn sitrep(&self) -> &'a Sitrep {
        self.sitrep
    }

    pub fn notes(&self) -> &[Note] {
        &self.notes
    }

    pub fn display(&self) -> SlippyReportDisplay<'_> {
        SlippyReportDisplay { report: self }
    }
}

#[derive(Debug)]
pub struct SlippyReportDisplay<'a> {
    report: &'a SlippyReport<'a>,
}

impl fmt::Display for SlippyReportDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pluralize = if self.report.notes.len() == 1 { "" } else { "s" };
        writeln!(
            f,
            "slippy report for sitrep {}: {} note{pluralize}",
            self.report.sitrep.id(),
            self.report.notes.len(),
        )?;
        for note in self.report.notes() {
            writeln!(f, "  {}", note.display(self.report.sort_key))?;
        }
        Ok(())
    }
}
