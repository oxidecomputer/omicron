// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Slippy: a linter for fault management sitreps.
//!
//! Slippy examines a [`Sitrep`](nexus_types::fm::Sitrep) for states that
//! should be impossible given how sitreps are constructed, or that would be
//! problematic for the fault management subsystem to consume. It's the
//! sitrep counterpart of `nexus-reconfigurator-blippy`, the blueprint
//! linter, and works the same way: constructing a [`Slippy`] runs every
//! check, and [`Slippy::into_report`] returns the findings.
//!
//! Some checks are structural and apply to any sitrep; others belong to a
//! particular diagnosis engine and validate the semantics of that engine's
//! cases (for example, that every fact on a physical-disk case references
//! the same disk).

mod asserts;
mod checks;
mod report;
mod slippy;

pub use asserts::assert_sitrep_has_no_fatal_notes;
pub use asserts::assert_sitrep_is_slippy_clean;
pub use report::SlippyReport;
pub use report::SlippyReportSortKey;
pub use slippy::CaseKind as SlippyCaseKind;
pub use slippy::Kind as SlippyKind;
pub use slippy::Note as SlippyNote;
pub use slippy::PhysicalDiskCaseKind as SlippyPhysicalDiskCaseKind;
pub use slippy::Severity as SlippySeverity;
pub use slippy::SitrepKind as SlippySitrepKind;
pub use slippy::Slippy;
