// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The stable wicketd commissioning API.
//!
//! This API is used by automated tooling such as rkdeploy to commission new
//! racks. It is a reduced subset of the full, unstable wicketd API.
//!
//! **Automation must always use the stable API!**
//!
//! For more information, see RFD 710.

mod conversions;
mod http_entrypoints;
mod progress;

pub use http_entrypoints::api;
