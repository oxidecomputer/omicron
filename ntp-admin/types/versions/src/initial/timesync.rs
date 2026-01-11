// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct TimeSync {
    /// The synchronization state of the sled, true when the system clock
    /// and the NTP clock are in sync (to within a small window).
    pub sync: bool,
    /// The NTP reference ID.
    pub ref_id: u32,
    /// The NTP reference IP address.
    pub ip_addr: IpAddr,
    /// The NTP stratum (our upstream's stratum plus one).
    pub stratum: u8,
    /// The NTP reference time (i.e. what chrony thinks the current time is, not
    /// necessarily the current system time).
    pub ref_time: f64,
    // This could be f32, but there is a problem with progenitor/typify
    // where, although the f32 correctly becomes "float" (and not "double") in
    // the API spec, that "float" gets converted back to f64 when generating
    // the client.
    /// The current offset between the NTP clock and system clock.
    pub correction: f64,
}
