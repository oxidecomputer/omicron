// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Finding the underlay network physical links and address objects.

use illumos_utils::addrobj;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm::Dladm;
use illumos_utils::dladm::FindPhysicalLinkError;
use illumos_utils::dladm::PhysicalLink;
use illumos_utils::dladm::CHELSIO_LINK_PREFIX;
use illumos_utils::zone::Zones;
use sled_hardware::is_gimlet;

// Names of VNICs used as underlay devices for the xde driver.
//
// NOTE: These are only used on non-Gimlet systems. In that case, they are
// expected to have been created by a run of
// `./tools/create_virtual_hardware.sh`.
const XDE_VNIC_NAMES: [&str; 2] = ["net0", "net1"];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(
        "Failed to create an IPv6 link-local address for underlay devices: {0}"
    )]
    UnderlayDeviceAddress(#[from] illumos_utils::ExecutionError),

    #[error(transparent)]
    BadAddrObj(#[from] addrobj::ParseError),

    #[error("Could not determine if host is a Gimlet: {0}")]
    SystemDetection(#[source] anyhow::Error),

    #[error("Could not enumerate physical links")]
    FindLinks(#[from] FindPhysicalLinkError),
}

pub fn find_nics() -> Result<Vec<AddrObject>, Error> {
    let underlay_nics = find_chelsio_links()?;

    let mut addr_objs = Vec::with_capacity(underlay_nics.len());
    for nic in underlay_nics {
        let addrobj = AddrObject::link_local(&nic.0)?;
        Zones::ensure_has_link_local_v6_address(None, &addrobj)?;
        addr_objs.push(addrobj);
    }

    Ok(addr_objs)
}

/// Return the Chelsio links on the system.
///
/// For a real Gimlet, this should return the devices like `cxgbeN`. For a
/// developer machine, or generally a non-Gimlet, this will return the
/// VNICs we use to emulate those Chelsio links.
pub(crate) fn find_chelsio_links() -> Result<Vec<PhysicalLink>, Error> {
    if is_gimlet().map_err(Error::SystemDetection)? {
        Dladm::list_physical().map_err(Error::FindLinks).map(|links| {
            links
                .into_iter()
                .filter(|link| link.0.starts_with(CHELSIO_LINK_PREFIX))
                .collect()
        })
    } else {
        Ok(XDE_VNIC_NAMES
            .into_iter()
            .map(|name| PhysicalLink(name.to_string()))
            .collect())
    }
}
