// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Finding the underlay network physical links and address objects.

use crate::illumos::addrobj;
use crate::illumos::addrobj::AddrObject;
use crate::illumos::dladm::PhysicalLink;
use crate::zone::Zones;

// Names of VNICs used as underlay devices for the xde driver.
const XDE_VNIC_NAMES: [&str; 2] = ["net0", "net1"];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(
        "Failed to create an IPv6 link-local address for underlay devices: {0}"
    )]
    UnderlayDeviceAddress(#[from] crate::illumos::ExecutionError),

    #[error(transparent)]
    BadAddrObj(#[from] addrobj::ParseError),
}

pub fn find_nics() -> Result<Vec<AddrObject>, Error> {
    let underlay_nics = find_chelsio_links()?;

    let mut addr_objs = Vec::with_capacity(underlay_nics.len());
    for nic in underlay_nics {
        let addrobj = AddrObject::new(&nic.0, "linklocal")?;
        Zones::ensure_has_link_local_v6_address(None, &addrobj)?;
        addr_objs.push(addrobj);
    }

    Ok(addr_objs)
}

fn find_chelsio_links() -> Result<Vec<PhysicalLink>, Error> {
    // TODO-correctness: This should eventually be determined by a call to
    // `Dladm` to get the real Chelsio links on a Gimlet. These will likely be
    // called `cxgbeN`, but we explicitly call them `netN` to be clear that
    // they're likely VNICs for the time being.
    Ok(XDE_VNIC_NAMES
        .into_iter()
        .map(|name| PhysicalLink(name.to_string()))
        .collect())
}
