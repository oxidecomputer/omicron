// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

mod execution;
mod filesystem;
mod planning;
mod rules;
#[cfg(test)]
mod test_helpers;

pub use planning::ArchiveKind;
pub use planning::ArchivePlanner;
