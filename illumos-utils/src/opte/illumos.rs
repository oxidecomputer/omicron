// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interactions with the Oxide Packet Transformation Engine (OPTE)

use crate::dladm;
use camino::Utf8Path;
use helios_fusion::addrobj::AddrObject;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use opte_ioctl::OpteHdl;
use slog::info;
use slog::Logger;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failure interacting with the OPTE ioctl(2) interface: {0}")]
    Opte(#[from] opte_ioctl::Error),

    #[error("Failed to wrap OPTE port in a VNIC: {0}")]
    CreateVnic(#[from] dladm::CreateVnicError),

    #[error(
        "No xde driver configuration file exists at '/kernel/drv/xde.conf'"
    )]
    NoXdeConf,

    #[error(
        "The OS kernel does not support the xde driver. Please update the OS \
        using `./tools/install_opte.sh` to provide kernel bits and the xde \
        driver which are compatible."
    )]
    IncompatibleKernel,

    #[error(transparent)]
    BadAddrObj(#[from] helios_fusion::addrobj::ParseError),

    #[error(transparent)]
    SetLinkpropError(#[from] crate::dladm::SetLinkpropError),

    #[error(transparent)]
    ResetLinkpropError(#[from] crate::dladm::ResetLinkpropError),

    #[error("Invalid IP configuration for port")]
    InvalidPortIpConfig,

    #[error("Tried to release non-existent port ({0}, {1:?})")]
    ReleaseMissingPort(uuid::Uuid, NetworkInterfaceKind),
}

/// Delete all xde devices on the system.
pub fn delete_all_xde_devices(log: &Logger) -> Result<(), Error> {
    let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;
    for port_info in hdl.list_ports()?.ports.into_iter() {
        let name = &port_info.name;
        info!(
            log,
            "deleting existing OPTE port and xde device";
            "device_name" => name
        );
        hdl.delete_xde(name)?;
    }
    Ok(())
}

/// Initialize the underlay devices required for the xde kernel module.
///
/// The xde driver needs information about the physical devices out which it can
/// send traffic from the guests.
pub fn initialize_xde_driver(
    log: &Logger,
    underlay_nics: &[AddrObject],
) -> Result<(), Error> {
    const XDE_CONF: &str = "/kernel/drv/xde.conf";
    let xde_conf = Utf8Path::new(XDE_CONF);
    if !xde_conf.exists() {
        return Err(Error::NoXdeConf);
    }

    info!(log, "using '{:?}' as data links for xde driver", underlay_nics);
    if underlay_nics.len() < 2 {
        const MESSAGE: &str = concat!(
            "There must be at least two underlay NICs for the xde ",
            "driver to operate. These are currently created by ",
            "`./tools/create_virtual_hardware.sh`. Please ensure that ",
            "script has been run, and that two VNICs named `net{0,1}` ",
            "exist on the system."
        );
        return Err(Error::Opte(opte_ioctl::Error::InvalidArgument(
            String::from(MESSAGE),
        )));
    }
    match OpteHdl::open(OpteHdl::XDE_CTL)?.set_xde_underlay(
        underlay_nics[0].interface(),
        underlay_nics[1].interface(),
    ) {
        Ok(_) => Ok(()),
        // Handle the specific case where the kernel appears to be unaware of
        // xde at all. This implies the developer has not installed the correct
        // helios-netdev kernel bits.
        //
        // TODO-correctness: This error should never occur in the product. Both
        // xde the kernel driver and the kernel bits needed to recognize it will
        // be packaged as part of our OS ramdisk, meaning it should not be
        // possible to get out of sync.
        Err(opte_ioctl::Error::IoctlFailed(_, ref message))
            if message.contains("unexpected errno: 48") =>
        {
            Err(Error::IncompatibleKernel)
        }
        // TODO-correctness: xde provides no way to get the current underlay
        // devices we're using, but we'd probably like the further check that
        // those are exactly what we're giving it now.
        Err(opte_ioctl::Error::CommandError(
            _,
            opte_ioctl::OpteError::System { errno: libc::EEXIST, .. },
        )) => Ok(()),
        Err(e) => Err(e.into()),
    }
}
