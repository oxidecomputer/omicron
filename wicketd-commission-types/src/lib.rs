// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A facade over the latest versions of the wicketd commissioning API types.
//!
//! Business logic and the wicketd implementation use this crate so they do not
//! have to depend on `wicketd-commission-types-versions` directly. See RFD 619.

pub mod rack_setup;
pub mod update;
