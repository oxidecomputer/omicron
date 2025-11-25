// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management

pub mod alert;
pub mod builder;
pub mod diagnosis;
pub mod ereport_analysis;
pub use builder::{CaseBuilder, SitrepBuilder};
