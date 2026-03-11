// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Finding the underlay network physical links and address objects,
//! and deriving bootstrap IP addresses.

use std::net::Ipv6Addr;

use crate::is_oxide_sled;
use illumos_utils::addrobj;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm;
use illumos_utils::dladm::CHELSIO_LINK_PREFIX;
use illumos_utils::dladm::Dladm;
use illumos_utils::dladm::FindPhysicalLinkError;
use illumos_utils::dladm::GetLinkpropError;
use illumos_utils::dladm::PhysicalLink;
use illumos_utils::dladm::SetLinkpropError;
use illumos_utils::zone::Zones;
use omicron_common::api::external::MacAddr;

#[doc(inline)]
pub use sled_hardware_types::underlay::BOOTSTRAP_MASK;
#[doc(inline)]
pub use sled_hardware_types::underlay::BOOTSTRAP_PREFIX;
#[doc(inline)]
pub use sled_hardware_types::underlay::BootstrapInterface;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(
        "Failed to create an IPv6 link-local address for underlay devices: {0}"
    )]
    UnderlayDeviceAddress(#[from] illumos_utils::ExecutionError),

    #[error(transparent)]
    BadAddrObj(#[from] addrobj::ParseError),

    #[error("Could not determine if host is an Oxide sled: {0}")]
    SystemDetection(#[source] anyhow::Error),

    #[error("Could not enumerate physical links: {0}")]
    FindLinks(#[from] FindPhysicalLinkError),

    #[error("Could not set linkprop: {0}")]
    SetLinkprop(#[from] SetLinkpropError),

    #[error("Could not get linkprop: {0}")]
    GetLinkprop(#[from] GetLinkpropError),
}

/// Convenience function that calls
/// `ensure_links_have_global_zone_link_local_v6_addresses()` with the links
/// returned by `find_chelsio_links()`.
pub async fn find_nics(
    config_data_links: &[String; 2],
) -> Result<Vec<AddrObject>, Error> {
    let underlay_nics = find_chelsio_links(config_data_links).await?;

    // Before these links have any consumers (eg. IP interfaces), set the MTU.
    // If we have previously set the MTU, do not attempt to re-set.
    const MTU: &str = "9000";
    for link in &underlay_nics {
        let existing_mtu =
            Dladm::get_linkprop(&link.to_string(), "mtu").await?;

        if existing_mtu != MTU {
            Dladm::set_linkprop(&link.to_string(), "mtu", MTU).await?;
        }
    }

    ensure_links_have_global_zone_link_local_v6_addresses(&underlay_nics).await
}

/// Return the Chelsio links on the system.
///
/// For a real Oxide sled, this should return the devices like `cxgbeN`. For a
/// developer machine, or generally a non-sled, this will return the
/// VNICs we use to emulate those Chelsio links.
pub async fn find_chelsio_links(
    config_data_links: &[String; 2],
) -> Result<Vec<PhysicalLink>, Error> {
    if is_oxide_sled().map_err(Error::SystemDetection)? {
        Dladm::list_physical().await.map_err(Error::FindLinks).map(|links| {
            links
                .into_iter()
                .filter(|link| link.0.starts_with(CHELSIO_LINK_PREFIX))
                .collect()
        })
    } else {
        Ok(config_data_links
            .into_iter()
            .map(|name| PhysicalLink(name.to_string()))
            .collect())
    }
}

/// Ensure each of the `PhysicalLink`s has a link local IPv6 address in the
/// global zone.
pub async fn ensure_links_have_global_zone_link_local_v6_addresses(
    links: &[PhysicalLink],
) -> Result<Vec<AddrObject>, Error> {
    let mut addr_objs = Vec::with_capacity(links.len());

    for link in links {
        let addrobj = AddrObject::link_local(&link.0)?;
        Zones::ensure_has_link_local_v6_address(None, &addrobj).await?;
        addr_objs.push(addrobj);
    }

    Ok(addr_objs)
}

// TODO(https://github.com/oxidecomputer/omicron/issues/945): This address
// could be randomly generated when it no longer needs to be durable.
/// Derive the bootstrap IP address for the given interface by reading
/// the MAC address of the given physical link.
pub async fn bootstrap_ip(
    interface: BootstrapInterface,
    link: &PhysicalLink,
) -> Result<Ipv6Addr, dladm::GetMacError> {
    let mac = Dladm::get_mac(link).await?;
    Ok(mac_to_bootstrap_ip(mac, interface.interface_id()))
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
