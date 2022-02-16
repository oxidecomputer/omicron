// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod server;
mod sidecar;

use std::net::IpAddr;

pub struct Config {
    pub ip: IpAddr,
    pub port: u16,
}

pub use sidecar::Sidecar;
