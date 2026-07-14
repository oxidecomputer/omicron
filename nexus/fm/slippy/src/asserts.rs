// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Assertion helpers for tests that produce or load sitreps.

use crate::report::SlippyReportSortKey;
use crate::slippy::Severity;
use crate::slippy::Slippy;
use nexus_types::fm::Sitrep;

/// Panic unless `sitrep` produces an empty slippy report.
///
/// Pass the parent sitrep when it is available so that parent cross-checks
/// also run.
#[track_caller]
pub fn assert_sitrep_is_slippy_clean(sitrep: &Sitrep, parent: Option<&Sitrep>) {
    let report =
        Slippy::new(sitrep, parent).into_report(SlippyReportSortKey::Kind);
    if !report.notes().is_empty() {
        panic!(
            "expected sitrep {} to have no slippy notes:\n{}",
            sitrep.id(),
            report.display(),
        );
    }
}

/// Panic if `sitrep` produces any `Fatal` slippy note.
///
/// This is the bar for "a well-formed sitrep": `Quarantined` notes are
/// allowed, since a sitrep legitimately carries contained violations on
/// closed cases until their rendezvous work drains.
#[track_caller]
pub fn assert_sitrep_has_no_fatal_notes(
    sitrep: &Sitrep,
    parent: Option<&Sitrep>,
) {
    let report =
        Slippy::new(sitrep, parent).into_report(SlippyReportSortKey::Severity);
    if report.notes().iter().any(|n| n.severity == Severity::Fatal) {
        panic!(
            "expected sitrep {} to have no fatal slippy notes:\n{}",
            sitrep.id(),
            report.display(),
        );
    }
}
