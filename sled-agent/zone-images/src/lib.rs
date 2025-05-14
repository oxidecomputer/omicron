// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of Omicron zone images within sled-agent.
//!
//! This contains a subset of zone image code at the moment: you're encouraged
//! to move more code into this crate as appropriate.

mod mupdate_override;
mod source_resolver;

pub(crate) use mupdate_override::*;
pub use source_resolver::*;
