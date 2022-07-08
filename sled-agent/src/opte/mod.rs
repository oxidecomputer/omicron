// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interactions with the Oxide Packet Transformation Engine (OPTE)

use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use slog::Logger;
use std::net::IpAddr;
use std::net::Ipv6Addr;

mod port;
mod port_manager;

pub use port::Port;
pub use port_manager::PortManager;
pub use port_manager::PortTicket;

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        use crate::illumos::dladm;
        use crate::common::underlay;
        use opte_ioctl::OpteHdl;
        use std::fs;
        use std::path::Path;

        pub use opte::api::Vni;

        #[derive(thiserror::Error, Debug)]
        pub enum Error {
            #[error("Failure interacting with the OPTE ioctl(2) interface: {0}")]
            Opte(#[from] opte_ioctl::Error),

            #[error("Failed to wrap OPTE port in a VNIC: {0}")]
            CreateVnic(#[from] dladm::CreateVnicError),

            #[error("Failed to get VNICs for xde underlay devices: {0}")]
            GetVnic(#[from] underlay::Error),

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
            BadAddrObj(#[from] crate::illumos::addrobj::ParseError),

            #[error(transparent)]
            SetLinkpropError(#[from] crate::illumos::dladm::SetLinkpropError),

            #[error(transparent)]
            ResetLinkpropError(#[from] crate::illumos::dladm::ResetLinkpropError),
        }

        /// Delete all xde devices on the system.
        pub fn delete_all_xde_devices(log: &Logger) -> Result<(), Error> {
            let hdl = OpteHdl::open(OpteHdl::DLD_CTL)?;
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
        pub fn initialize_xde_driver(log: &Logger) -> Result<(), Error> {
            const XDE_CONF: &str = "/kernel/drv/xde.conf";
            let xde_conf = Path::new(XDE_CONF);
            if !xde_conf.exists() {
                return Err(Error::NoXdeConf);
            }

            // TODO-remove
            //
            // An additional part of the workaround to connect into instances. This is
            // required to tell OPTE to actually act as a 1-1 NAT when an instance is
            // provided with an external IP address, rather than do its normal job of
            // encapsulating the traffic onto the underlay (such as for delivery to
            // boundary services).
            use_external_ip_workaround(&log, &xde_conf);

            let underlay_nics = underlay::find_nics()?;
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
            match OpteHdl::open(OpteHdl::DLD_CTL)?.set_xde_underlay(
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
                    opte::api::OpteError::System { errno: libc::EEXIST, .. },
                )) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }

        fn use_external_ip_workaround(log: &Logger, xde_conf: &Path) {
            const NEEDLE: &str = "ext_ip_hack = 0;";
            const NEW_NEEDLE: &str = "ext_ip_hack = 1;";

            // NOTE: This only works in the real sled agent, which is run as root. The
            // file is not world-readable.
            let contents = fs::read_to_string(xde_conf)
                .expect("Failed to read xde configuration file");
            let new = contents.replace(NEEDLE, NEW_NEEDLE);
            if contents == new {
                info!(
                    log,
                    "xde driver configuration file appears to already use external IP workaround";
                    "conf_file" => ?xde_conf,
                );
            } else {
                info!(
                    log,
                    "updating xde driver configuration file for external IP workaround";
                    "conf_file" => ?xde_conf,
                );
                fs::write(xde_conf, &new)
                    .expect("Failed to modify xde configuration file");
            }

            // Ensure the driver picks up the updated configuration file, if it's been
            // loaded previously without the workaround.
            std::process::Command::new(crate::illumos::PFEXEC)
                .args(&["update_drv", "xde"])
                .output()
                .expect("Failed to reload xde driver configuration file");
        }

    } else {

        #[derive(Debug, Clone, Copy)]
        pub struct Vni(u32);

        impl Vni {
            pub fn new<N>(n: N) -> Result<Self, Error>
            where
                N: Into<u32>,
            {
                let x = n.into();
                if x <= 0x00_FF_FF_FF {
                    Ok(Self(x))
                } else {
                    Err(Error::InvalidArgument(format!("invalid VNI: {}", x)))
                }
            }
        }

        #[derive(thiserror::Error, Debug)]
        pub enum Error {
            #[error("Invalid argument: {0}")]
            InvalidArgument(String),
        }

        pub fn initialize_xde_driver(log: &Logger) -> Result<(), Error> {
            slog::warn!(log, "`xde` driver is a fiction on non-illumos systems");
            Ok(())
        }

        pub fn delete_all_xde_devices(log: &Logger) -> Result<(), Error> {
            slog::warn!(log, "`xde` driver is a fiction on non-illumos systems");
            Ok(())
        }
    }
}

/// Location information for reaching Boundary Services, for directing
/// inter-sled or off-rack traffic from guests.
#[derive(Debug, Clone, Copy)]
pub struct BoundaryServices {
    pub ip: Ipv6Addr,
    pub vni: Vni,
}

impl Default for BoundaryServices {
    fn default() -> Self {
        // TODO-completeness: Don't hardcode this.
        //
        // Boundary Services will be started on several Sidecars during rack
        // setup, and those addresses will need to be propagated here.
        // See https://github.com/oxidecomputer/omicron/issues/1382
        const BOUNDARY_SERVICES_ADDR: Ipv6Addr =
            Ipv6Addr::new(0xfd00, 0x99, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        let boundary_services_vni = Vni::new(99_u32).unwrap();

        Self { ip: BOUNDARY_SERVICES_ADDR, vni: boundary_services_vni }
    }
}

/// Information about the gateway for an OPTE port
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct Gateway {
    mac: MacAddr6,
    ip: IpAddr,
}

// The MAC address that OPTE exposes to guest NICs, i.e., the MAC of the virtual
// gateway OPTE operates as for each guest. See
// https://github.com/oxidecomputer/omicron/pull/955#discussion_r856432498 for
// more context on the genesis of this, but this is just a reserved address
// within the "system" portion of the virtual MAC address space.
// See https://github.com/oxidecomputer/omicron/issues/1381
const OPTE_VIRTUAL_GATEWAY_MAC: MacAddr6 =
    MacAddr6::new(0xa8, 0x40, 0x25, 0xff, 0x77, 0x77);

impl Gateway {
    pub fn from_subnet(subnet: &IpNetwork) -> Self {
        Self {
            mac: OPTE_VIRTUAL_GATEWAY_MAC,

            // See RFD 21, section 2.2 table 1
            ip: subnet
                .iter()
                .nth(1)
                .expect("IP subnet must have at least 1 address"),
        }
    }
}
