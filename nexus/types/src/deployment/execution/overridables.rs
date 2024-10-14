// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::address::get_switch_zone_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGD_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::SLED_PREFIX;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;

/// Override values used during blueprint execution
///
/// Blueprint execution assumes certain values about production systems that
/// differ in the simulated testing environment and cannot be easily derived
/// from anything else in the environment. To accommodate this, this structure
/// provides access to these values. Everywhere except the test suite, this
/// structure is empty and returns the default (production) values. The test
/// suite overrides these values.
#[derive(Debug, Default)]
pub struct Overridables {
    /// map: sled id -> TCP port on which that sled's Dendrite is listening
    pub dendrite_ports: BTreeMap<SledUuid, u16>,
    /// map: sled id -> TCP port on which that sled's MGS is listening
    pub mgs_ports: BTreeMap<SledUuid, u16>,
    /// map: sled id -> TCP port on which that sled's MGD is listening
    pub mgd_ports: BTreeMap<SledUuid, u16>,
    /// map: sled id -> IP address of the sled's switch zone
    pub switch_zone_ips: BTreeMap<SledUuid, Ipv6Addr>,
}

impl Overridables {
    /// Specify the TCP port on which this sled's Dendrite is listening
    pub fn override_dendrite_port(&mut self, sled_id: SledUuid, port: u16) {
        self.dendrite_ports.insert(sled_id, port);
    }

    /// Returns the TCP port on which this sled's Dendrite is listening
    pub fn dendrite_port(&self, sled_id: SledUuid) -> u16 {
        self.dendrite_ports.get(&sled_id).copied().unwrap_or(DENDRITE_PORT)
    }

    /// Specify the TCP port on which this sled's MGS is listening
    pub fn override_mgs_port(&mut self, sled_id: SledUuid, port: u16) {
        self.mgs_ports.insert(sled_id, port);
    }

    /// Returns the TCP port on which this sled's MGS is listening
    pub fn mgs_port(&self, sled_id: SledUuid) -> u16 {
        self.mgs_ports.get(&sled_id).copied().unwrap_or(MGS_PORT)
    }

    /// Specify the TCP port on which this sled's MGD is listening
    pub fn override_mgd_port(&mut self, sled_id: SledUuid, port: u16) {
        self.mgd_ports.insert(sled_id, port);
    }

    /// Returns the TCP port on which this sled's MGD is listening
    pub fn mgd_port(&self, sled_id: SledUuid) -> u16 {
        self.mgd_ports.get(&sled_id).copied().unwrap_or(MGD_PORT)
    }

    /// Specify the IP address of this switch zone
    pub fn override_switch_zone_ip(
        &mut self,
        sled_id: SledUuid,
        addr: Ipv6Addr,
    ) {
        self.switch_zone_ips.insert(sled_id, addr);
    }

    /// Returns the IP address of this sled's switch zone
    pub fn switch_zone_ip(
        &self,
        sled_id: SledUuid,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
    ) -> Ipv6Addr {
        self.switch_zone_ips
            .get(&sled_id)
            .copied()
            .unwrap_or_else(|| get_switch_zone_address(sled_subnet))
    }
}
