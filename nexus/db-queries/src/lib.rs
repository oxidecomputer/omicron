// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for working with the Omicron database

pub mod authn;
pub mod authz;
pub mod context;
pub mod db;
pub mod provisioning;

#[macro_use]
extern crate slog;
#[macro_use]
extern crate newtype_derive;
#[cfg(test)]
#[macro_use]
extern crate diesel;
