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

#[usdt::provider(provider = "nexus__db__queries")]
mod probes {
    // Fires before we start a search over a range for a VNI.
    //
    // Includes the starting VNI and the size of the range being searched.
    fn vni__search__range__start(
        _: &usdt::UniqueId,
        start_vni: u32,
        size: u32,
    ) {
    }

    // Fires when we successfully find a VNI.
    fn vni__search__range__found(_: &usdt::UniqueId, vni: u32) {}

    // Fires when we fail to find a VNI in the provided range.
    fn vni__search__range__empty(_: &usdt::UniqueId) {}
}
