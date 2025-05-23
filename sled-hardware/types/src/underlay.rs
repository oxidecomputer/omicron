// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_utils::dladm;
use illumos_utils::dladm::Dladm;
use illumos_utils::dladm::PhysicalLink;
use omicron_common::api::external::MacAddr;
use std::net::Ipv6Addr;

/// Initial octet of IPv6 for bootstrap addresses.
pub const BOOTSTRAP_PREFIX: u16 = 0xfdb0;

/// IPv6 prefix mask for bootstrap addresses.
pub const BOOTSTRAP_MASK: u8 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapInterface {
    GlobalZone,
    SwitchZone,
}

impl BootstrapInterface {
    pub fn interface_id(self) -> u64 {
        match self {
            BootstrapInterface::GlobalZone => 1,
            BootstrapInterface::SwitchZone => 2,
        }
    }

    // TODO(https://github.com/oxidecomputer/omicron/issues/945): This address
    // could be randomly generated when it no longer needs to be durable.
    pub async fn ip(
        self,
        link: &PhysicalLink,
    ) -> Result<Ipv6Addr, dladm::GetMacError> {
        let mac = Dladm::get_mac(link).await?;
        Ok(mac_to_bootstrap_ip(mac, self.interface_id()))
    }
}

fn mac_to_bootstrap_ip(mac: MacAddr, interface_id: u64) -> Ipv6Addr {
    let mac_bytes = mac.into_array();
    assert_eq!(6, mac_bytes.len());

    Ipv6Addr::new(
        BOOTSTRAP_PREFIX,
        (u16::from(mac_bytes[0]) << 8) | u16::from(mac_bytes[1]),
        (u16::from(mac_bytes[2]) << 8) | u16::from(mac_bytes[3]),
        (u16::from(mac_bytes[4]) << 8) | u16::from(mac_bytes[5]),
        ((interface_id >> 48) & 0xffff).try_into().unwrap(),
        ((interface_id >> 32) & 0xffff).try_into().unwrap(),
        ((interface_id >> 16) & 0xffff).try_into().unwrap(),
        (interface_id & 0xfff).try_into().unwrap(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use macaddr::MacAddr6;

    #[test]
    fn test_mac_to_bootstrap_ip() {
        let mac = MacAddr("a8:40:25:10:00:01".parse::<MacAddr6>().unwrap());

        assert_eq!(
            mac_to_bootstrap_ip(mac, 1),
            "fdb0:a840:2510:1::1".parse::<Ipv6Addr>().unwrap(),
        );
    }
}
