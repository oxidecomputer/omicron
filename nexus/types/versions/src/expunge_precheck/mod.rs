// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `EXPUNGE_PRECHECK` of the Nexus external API.
//!
//! Adds an endpoint that checks for conditions that make it risky to expunge a
//! sled or physical disk.

pub mod expunge_precheck;
