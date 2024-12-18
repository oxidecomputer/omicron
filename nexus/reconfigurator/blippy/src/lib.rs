// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blippy: the blueprint checker

mod blippy;
mod checks;
mod report;

pub use blippy::Blippy;
pub use blippy::Kind as BlippyKind;
pub use blippy::Note as BlippyNote;
pub use blippy::Severity as BlippySeverity;
pub use report::BlippyReport;
pub use report::BlippyReportSortKey;
