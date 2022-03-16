// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interactions with the Oxide Packet Transformation Engine (OPTE)

#[cfg_attr(target_os = "illumos", path = "opte.rs")]
#[cfg_attr(not(target_os = "illumos"), path = "mock_opte.rs")]
mod inner;

pub use inner::*;
