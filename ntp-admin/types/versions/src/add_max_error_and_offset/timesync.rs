// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v1;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

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
    /// The NTP reference time (i.e. what chrony thinks the current time is,
    /// not necessarily the current system time).
    pub ref_time: f64,
    // This could be f32, but there is a problem with progenitor/typify
    // where, although the f32 correctly becomes "float" (and not "double") in
    // the API spec, that "float" gets converted back to f64 when generating
    // the client.
    /// The current offset between the NTP clock and system clock, in seconds.
    pub correction: f64,
    /// The offset of the NTP clock from the reference clock in the last
    /// measurement, in seconds.
    pub last_offset: f64,
    /// The root-mean-square (RMS) of recent NTP clock offsets from the
    /// reference clock, in seconds.
    pub rms_offset: f64,
    /// The total round-trip delay to the reference clock, in seconds.
    pub root_delay: f64,
    /// The root dispersion: uncertainty in the NTP clock due to accumulated
    /// frequency errors since the last reference update, in seconds.
    pub root_dispersion: f64,
    /// The maximum estimated clock error: root_delay / 2 + root_dispersion,
    /// in seconds.
    pub max_error: f64,
}

impl From<TimeSync> for v1::timesync::TimeSync {
    fn from(s: TimeSync) -> Self {
        v1::timesync::TimeSync {
            sync: s.sync,
            ref_id: s.ref_id,
            ip_addr: s.ip_addr,
            stratum: s.stratum,
            ref_time: s.ref_time,
            correction: s.correction,
        }
    }
}
