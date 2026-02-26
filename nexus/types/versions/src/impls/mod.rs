// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for the latest versions of types.
//!
//! This module contains inherent methods, trait implementations (e.g.,
//! `Display`, `FromStr`), and other functional code attached to versioned
//! types. Per RFD 619, such code must be implemented on the latest versions
//! of each type and live in this module.
//!
//! Within this module, types are referred to using `latest::` identifiers.

mod alert;
mod disk;
mod hardware;
mod instance;
pub(crate) mod multicast;
mod networking;
mod physical_disk;
mod policy;
mod saml;
mod silo;
mod sled;
mod switch;
mod user;
