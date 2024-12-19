// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blippy: the blueprint checker
//!
//! [`Blippy`] performs a variety of checks on blueprints to ensure they are
//! internally-consistent (e.g., "every in-service zone that should have one or
//! more datasets do", or "any given external IP address is used by at most one
//! in-service zone"). It emits [`BlippyReport`]s in the form of a list of
//! [`BlippyNote`]s, each of which has an associated severity and parent
//! component (typically a sled).

mod blippy;
mod checks;
mod report;

pub use blippy::Blippy;
pub use blippy::Kind as BlippyKind;
pub use blippy::Note as BlippyNote;
pub use blippy::Severity as BlippySeverity;
pub use report::BlippyReport;
pub use report::BlippyReportSortKey;
