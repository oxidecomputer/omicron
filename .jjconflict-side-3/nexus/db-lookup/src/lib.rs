// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types to look up entries in the Nexus database by their primary key.
//!
//! This crate does not depend on the Nexus datastore directly. Rather, it
//! exposes a generic trait that the datastore implements so that more of the
//! build process can be parallelized in the future.

mod datastore_interface;
pub mod lookup;
mod lookup_path;

pub use datastore_interface::*;
pub use lookup_path::*;
